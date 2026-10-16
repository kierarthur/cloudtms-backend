const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const UTC=/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/;
const PHASES=new Set(['SOURCE_ACTIVATION','SOURCE_WORKER','SOURCE_MANAGER','LEGACY_OUTBOX']);
const NATIVE_DISPOSITIONS=new Set(['ORIGINAL_ATTEMPT_OWNER','CURRENT_CAPTURE_UNCHANGED','NO_CURRENT_ELIGIBLE_SUBSET',
 'ORIGINAL_SUBSET_REPLAY','EXACT_CURRENT_SUBSET_QUEUED','CURRENT','RECOMPOSED','CHECK_ORIGINAL_ATTEMPT','PAUSED','OBSOLETE',
 'FIRST_ACTIVATION_RECHECKED','CURRENT_SOURCE_NOT_INITIAL_CAMPAIGN']);
function fail(){throw new Error('CONTACT_RECHECK_REPLY_INVALID');}
function object(value){return value!==null&&typeof value==='object'&&!Array.isArray(value);}
function closed(value,keys){if(!object(value)||Object.keys(value).length!==keys.length||keys.some(key=>!Object.hasOwn(value,key)))fail();}
function uuid(value){if(typeof value!=='string'||!UUID.test(value))fail();}
export function decodeContactRecheckResult(value,limit){
 if(!Number.isInteger(limit)||limit<1||limit>100)fail();
 closed(value,['ok','version','processed','held','selection_complete','results']);
 if(value.ok!==true||value.version!=='CONTACT_RECHECK_BATCH_RESULT_V1'||value.selection_complete!==false
  ||!Number.isInteger(value.processed)||value.processed<0||!Number.isInteger(value.held)||value.held<0
  ||!Array.isArray(value.results)||value.results.length>limit||value.processed+value.held!==value.results.length)fail();
 const tasks=new Set();let held=0;
 for(const item of value.results){
  if(!object(item)||item.version!=='CONTACT_RECHECK_TASK_RESULT_V1')fail();uuid(item.task_id);
  if(tasks.has(item.task_id))fail();tasks.add(item.task_id);
  switch(item.disposition){
   case 'NOT_DUE':closed(item,['version','task_id','disposition']);break;
   case 'CONTINUE':
    closed(item,['version','task_id','disposition','phase','native_id','native_disposition']);uuid(item.native_id);
    if(!PHASES.has(item.phase)||!NATIVE_DISPOSITIONS.has(item.native_disposition))fail();break;
   case 'COMPLETE':case 'WAITING_ACTUAL_NATIVE_DUE':
    closed(item,['version','task_id','disposition','next_due_at_utc']);
    if(item.disposition==='COMPLETE'){if(item.next_due_at_utc!==null)fail();}
    else if(typeof item.next_due_at_utc!=='string'||!UTC.test(item.next_due_at_utc)||!Number.isFinite(Date.parse(item.next_due_at_utc))
     ||new Date(item.next_due_at_utc).toISOString().slice(0,23)!==item.next_due_at_utc.slice(0,23))fail();
    break;
   case 'HELD_DEPENDENCY':
    closed(item,['version','task_id','disposition','code']);if(item.code!=='CONTACT_RECHECK_ITEM_HELD')fail();held++;break;
   default:fail();
  }
 }
 if(held!==value.held)fail();return value;
}

// Host passes its actual existing deployment-context projection. No browser
// clock, scope list, ordinal, new queue or provider is consulted by this leaf.
export async function runWeeklySourceContactRecheck(dependencies,{deployment,workerId,limit}){
 if(!object(deployment)||Object.keys(deployment).length!==2||!Object.hasOwn(deployment,'agency_id')
  ||!Object.hasOwn(deployment,'environment')||typeof deployment.agency_id!=='string'||!UUID.test(deployment.agency_id)
  ||!['TEST','LIVE'].includes(deployment.environment)||typeof workerId!=='string'||workerId.trim()===''
  ||workerId.length>120||!Number.isInteger(limit)||limit<1||limit>100||typeof dependencies?.rpc!=='function'){
  throw new Error('CONTACT_RECHECK_REQUEST_INVALID');
 }
 const raw=await dependencies.rpc('weekly_source_contact_recheck_batch_v1',{p_request:{version:'CONTACT_RECHECK_BATCH_V1',
  ...deployment,worker_id:workerId,limit}},{timeoutMs:30_000});
 if(raw?.error)throw new Error('CONTACT_RECHECK_UNAVAILABLE');
 let result=object(raw)&&Object.hasOwn(raw,'data')?raw.data:raw;
 if(Array.isArray(result)&&result.length===1)[result]=result;
 return decodeContactRecheckResult(result,limit);
}
