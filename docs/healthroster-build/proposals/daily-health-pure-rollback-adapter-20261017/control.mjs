// Finite local root protocol. No SQL, filesystem, credentials or native calls.
export const BUDGET=Object.freeze({wholeMilliseconds:180000,workMilliseconds:120000,cleanupReserveMilliseconds:60000,smallFileBytes:262144,controlBytes:256});
const error=code=>Error(code);
export class FiniteControls {
 constructor(input,signal,processLike=process){
  this.input=input;this.processLike=processLike;this.controller=new AbortController();this.signal=this.controller.signal;this.buffer='';this.pending=null;
  this.data=chunk=>this.feed(chunk);this.eof=()=>this.abort('CONTROL_EOF_ABORT');
  this.external=()=>this.abort(signal?.reason?.message==='PURE_SEQUENCE_WORK_BUDGET_HELD'?'PURE_SEQUENCE_WORK_BUDGET_HELD':'CONTROL_OWNED_TURN_LOST');this.interrupt=()=>this.abort('CONTROL_ROOT_ABORT');
  this.externalSignal=signal;input.setEncoding?.('utf8');input.on('data',this.data);input.once('end',this.eof);
  processLike.once?.('SIGINT',this.interrupt);processLike.once?.('SIGTERM',this.interrupt);signal?.addEventListener('abort',this.external,{once:true});input.resume?.();
  if(signal?.aborted)this.external();
 }
 abort(code){if(!this.signal.aborted)this.controller.abort(error(code));if(this.pending){this.pending.reject(this.signal.reason);this.pending=null;}}
 feed(chunk){
  if(this.signal.aborted)return;this.buffer+=String(chunk);
  if(Buffer.byteLength(this.buffer)>BUDGET.controlBytes){this.abort('CONTROL_ENVELOPE_REFUSED');return;}
  let end;while((end=this.buffer.indexOf('\n'))>=0){const line=this.buffer.slice(0,end).replace(/\r$/,'');this.buffer=this.buffer.slice(end+1);
   if(line==='ABORT'){this.abort('CONTROL_ROOT_ABORT');return;}
   if(!['PREPARE','RELEASE'].includes(line)||!this.pending||this.pending.expected!==line){this.abort('CONTROL_PHASE_REFUSED');return;}
   const {resolve}=this.pending;this.pending=null;resolve(line);
  }
 }
 wait(expected){
  if(this.signal.aborted)return Promise.reject(this.signal.reason);
  if(this.pending||!['PREPARE','RELEASE'].includes(expected))return Promise.reject(error('CONTROL_PHASE_REFUSED'));
  return new Promise((resolve,reject)=>{this.pending={expected,resolve,reject};});
 }
 dispose(){
  this.input.removeListener('data',this.data);this.input.removeListener('end',this.eof);this.input.pause?.();
  this.processLike.removeListener?.('SIGINT',this.interrupt);this.processLike.removeListener?.('SIGTERM',this.interrupt);
  this.externalSignal?.removeEventListener('abort',this.external);if(this.pending)this.abort('CONTROL_CLOSED');
 }
}
export function assertElapsed(start,now=performance.now(),limit=BUDGET.wholeMilliseconds){
 if(!Number.isFinite(start)||!Number.isFinite(now)||now<start||now-start>limit)throw error('WHOLE_SEQUENCE_BUDGET_HELD');return now-start;
}
export function assertNoMaps(value,depth=0){
 if(depth>20)throw error('SMALL_PROOF_DEPTH_REFUSED');
 if(value&&typeof value==='object')for(const [key,item]of Object.entries(value)){
  if(['backgroundRows','backgroundSnapshotXip','fixtureObservations','observations','keyHash','rowHash','stableHash','nativeClaimProved','terminal_response_body','lease_owner','source_hmac'].includes(key))throw error('RAW_CANONICAL_MAP_PERSISTENCE_REFUSED');
  assertNoMaps(item,depth+1);
 }
 return value;
}
