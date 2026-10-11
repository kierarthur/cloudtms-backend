/**
 * Small metadata wakes for the NEXT owner. Database jobs/commands are durable
 * authority; the queue is transport only. No Candidate/history rebuild, inline
 * financial drain, provider action, caller clock, or batch-wide money payload.
 * One wake claims fresh metadata and executes at most one bounded page. Every continuation is
 * awaited before acknowledgement; missed messages are rediscovered by bounded
 * server recovery pages. Neither an uncertain send nor a lost SQL reply is a
 * completed financial operation. Actual Worker wiring supplies fresh ownership.
 */
import { enrollBpayNextOne, readBpayNextJobWakePage, readBpayNextExpiryDispatchPage, cleanupBpayNextTerminalRows,
  readBpayNextWorkbenchWakePage,recoverBpayNextSnoozeDuePage,recoverBpayNextWeekRolloverPage,claimBpayNextWorkbenchTask,stepBpayNextWorkbenchTask } from './bpay-next-runtime-rpc.js';
import { receiveBpayNextPreparationExpiry } from './bpay-next-preparation-expiry-rpc.js';
import { handleBpayNextFinancialQueueMessage,bpayNextQueueIdentity } from './bpay-next-financial-queue.js';

const UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const ENROLL=Object.freeze({id:'1',kind:'ENROLL'});
export const BPAY_NEXT_BACKGROUND_LIMITS=Object.freeze({pageRows:100,retrySeconds:30,leaseSeconds:120,duePages:2});
const claim=item=>Object.freeze({id:item.job_id,kind:item.job_kind});
const expiry=item=>Object.freeze({id:item.run_id,kind:'EXPIRY'});
const workbenchClaim=item=>Object.freeze({id:item.task_id,kind:'WORKBENCH'});
function binding(deps) {
  if(!deps||typeof deps.runtimeRpc!=='function'||typeof deps.queue?.sendBatch!=='function') {
    throw new TypeError('BPAY_NEXT_BACKGROUND_DEPENDENCY_UNAVAILABLE');
  }
}
async function publish(deps,messages) {
  if(messages.length===0) return;
  if(messages.length>100||messages.some(body=>!bpayNextQueueIdentity(body))) throw new TypeError('BPAY_NEXT_BACKGROUND_PAGE_INVALID');
  // Cloudflare binding, not its control-plane API; closed small JSON envelopes.
  await deps.queue.sendBatch(messages.map(body=>({body,contentType:'json'})));
}
async function enroll(deps) {
  const outcome=await enrollBpayNextOne({runtimeRpc:deps.runtimeRpc});
  if(!outcome.ok) return outcome;
  if(outcome.result.blocked_command_id!==null) return {ok:false,error_code:'BPAY_NEXT_ENROLLMENT_BLOCKED'};
  const messages=[];
  if(outcome.result.job_status==='READY') messages.push(claim(outcome.result));
  if(outcome.result.progressed) messages.push(ENROLL);
  await publish(deps,messages);
  return {ok:true,published:messages.length,progressed:outcome.result.progressed};
}
function retry(message,code) {
  message.retry({delaySeconds:BPAY_NEXT_BACKGROUND_LIMITS.retrySeconds});
  return {disposition:'RETRY',error_code:code};
}

/** Explicit disposition for one platform Message; no successful implicit ACK. */
export async function handleBpayNextBackgroundMessage(message,deps) {
  if(!message||typeof message.ack!=='function'||typeof message.retry!=='function') {
    throw new TypeError('BPAY_NEXT_QUEUE_MESSAGE_INTERFACE_INVALID');
  }
  try {
    binding(deps);
    const body=message.body;
    const wake=bpayNextQueueIdentity(body);
    if(!wake) return retry(message,'BPAY_NEXT_BACKGROUND_ENVELOPE_INVALID');
    if(wake.kind==='WORKBENCH') {
      const claimed=await claimBpayNextWorkbenchTask({task_id:wake.id,lease_seconds:120},{runtimeRpc:deps.runtimeRpc});
      if(!claimed.ok) return retry(message,claimed.error_code);
      const fresh=claimed.result;
      if(fresh.status==='DONE') {message.ack();return {disposition:'ACK',reason:'WORKBENCH_TASK_FINISHED'};}
      if(fresh.status!=='LEASED'||fresh.stale) return retry(message,'BPAY_NEXT_BACKGROUND_WORKBENCH_PENDING');
      const outcome=await stepBpayNextWorkbenchTask({task_id:wake.id,lease_nonce:fresh.lease_nonce,
        owner_epoch:fresh.owner_epoch,generation:fresh.generation,step_no:fresh.step_no,
        phase:fresh.phase,cursor_key:fresh.cursor_key,limit:100},{runtimeRpc:deps.runtimeRpc});
      if(!outcome.ok) return retry(message,outcome.error_code);
      if(outcome.result.status==='DONE') {message.ack();return {disposition:'ACK',reason:'WORKBENCH_TASK_FINISHED'};}
      if(outcome.result.status!=='READY'||outcome.result.stale) return retry(message,'BPAY_NEXT_BACKGROUND_WORKBENCH_PENDING');
      await publish(deps,[wake]);
      message.ack();return {disposition:'ACK',reason:'WORKBENCH_CONTINUATION_PERSISTED'};
    }
    if(wake.kind==='ENROLL') {
      const outcome=await enroll(deps);
      if(!outcome.ok) return retry(message,outcome.error_code);
      message.ack();return {disposition:'ACK',reason:'ENROLLMENT_WAKE_PERSISTED'};
    }
    if(wake.kind==='EXPIRY') {
      if(typeof deps.preparationExpiryRpc!=='function') return retry(message,'BPAY_NEXT_BACKGROUND_DEPENDENCY_UNAVAILABLE');
      const outcome=await receiveBpayNextPreparationExpiry({run_id:wake.id},{preparationExpiryRpc:deps.preparationExpiryRpc});
      if(!outcome.ok) {
        if(outcome.error_code==='BPAY_NEXT_EXPIRY_NOT_ELIGIBLE'&&!outcome.outcome_unknown) {
          message.ack();return {disposition:'ACK',reason:'OFFICE_ALREADY_FINISHED'};
        }
        return retry(message,outcome.error_code);
      }
      await publish(deps,[ENROLL]);
      message.ack();return {disposition:'ACK',reason:'EXPIRY_RECEIVED_ENROLLMENT_WAKE_PERSISTED'};
    }
    return await handleBpayNextFinancialQueueMessage(message,{
      financialJobRpc:deps.financialJobRpc,
      onStepMeasurement:deps.onStepMeasurement,
      sendContinuation:envelope=>publish(deps,[envelope]),
      onFailure:deps.onFailure
    });
  } catch {
    // Token-free; never echo a provider, SQL, JWT or Message body to logs.
    return retry(message,'BPAY_NEXT_BACKGROUND_DEPENDENCY_OR_SEND_FAILED');
  }
}

/** One recovery tick: one enrollment, two <=100 job pages, one <=100 expiry
 * page and one <=100 terminal temporary-row cleanup page. Independent lanes
 * each get a turn despite another lane failing. All
 * sends settle; no recursive drain/large collection or unawaited money work.
 */
export async function runBpayNextRecoveryTick(deps) {
  binding(deps);
  const results=[];
  async function lane(name,action) {
    try {
      const outcome=await action();
      results.push(outcome.ok?{lane:name,ok:true,published:outcome.published}
        :{lane:name,ok:false,error_code:outcome.error_code});
    } catch {results.push({lane:name,ok:false,error_code:'BPAY_NEXT_BACKGROUND_DEPENDENCY_OR_SEND_FAILED'});}
  }
  await lane('ENROLL',()=>enroll(deps));
  for(const jobLane of ['READY','LEASE_EXPIRED']) {
    await lane(jobLane,async()=>{
      const outcome=await readBpayNextJobWakePage({lane:jobLane,limit:100},{runtimeRpc:deps.runtimeRpc});
      if(!outcome.ok) return outcome;
      await publish(deps,outcome.result.items.map(claim));
      return {ok:true,published:outcome.result.items.length};
    });
  }
  await lane('EXPIRY',async()=>{
    const outcome=await readBpayNextExpiryDispatchPage({limit:100},{runtimeRpc:deps.runtimeRpc});
    if(!outcome.ok) return outcome;
    await publish(deps,outcome.result.items.map(expiry));
    return {ok:true,published:outcome.result.items.length};
  });
  await lane('CLEANUP',async()=>{
    const outcome=await cleanupBpayNextTerminalRows({limit:100},{runtimeRpc:deps.runtimeRpc});
    return outcome.ok?{ok:true,published:0}:outcome;
  });
  // Each lane has its own retained SQL seek. A lost reply waits for native
  // recovery, and a busy/failing lane cannot suppress the other lane's turn.
  for(const [name,page] of [['SNOOZE_DUE',recoverBpayNextSnoozeDuePage],['WEEK_ROLLOVER',recoverBpayNextWeekRolloverPage]]) {
    await lane(name,async()=>{
      for(let n=0;n<BPAY_NEXT_BACKGROUND_LIMITS.duePages;n+=1) {
        const outcome=await page({limit:100},{runtimeRpc:deps.runtimeRpc});
        if(!outcome.ok) return outcome;
        if(outcome.result.complete) break;
      }
      return {ok:true,published:0};
    });
  }
  await lane('WORKBENCH',async()=>{
    const outcome=await readBpayNextWorkbenchWakePage({limit:100},{runtimeRpc:deps.runtimeRpc});
    if(!outcome.ok) return outcome;
    await publish(deps,outcome.result.items.map(workbenchClaim));
    return {ok:true,published:outcome.result.items.length};
  });
  return Object.freeze({ok:results.every(result=>result.ok),lanes:Object.freeze(results)});
}
