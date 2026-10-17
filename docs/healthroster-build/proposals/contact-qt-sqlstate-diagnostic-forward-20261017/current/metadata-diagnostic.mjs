// Closed diagnostic metadata only. Never stores/emits raw stderr, SQL, maps or bodies.
// This brand is unrelated to (and cannot qualify) original restoration WeakMaps.
const CAP=16*1024,frames=new WeakMap();
const iso=value=>{try{return typeof value==='string'&&/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/.test(value)&&new Date(value).toISOString()===value;}catch{return false;}};
export class SqlstateStderrCapture{
 #bytes=0;#invalid=false;#mode='HEAD';#heads=['ERROR:  ','FATAL:  ','psql:<stdin>:'];#offset=0;#digits=0;#state='';#sealed=false;
 invalidate(){this.#invalid=true;this.#heads=[];this.#state='';this.#offset=0;this.#digits=0;}
 write(chunk){
  if(this.#sealed){this.invalidate();return;}
  const bytes=Buffer.isBuffer(chunk)?chunk:Buffer.from(chunk);this.#bytes=Math.min(CAP+1,this.#bytes+bytes.length);
  if(this.#bytes>CAP){this.invalidate();return;}
  for(const b of bytes){
   if(this.#invalid)break;
   if(this.#mode==='HEAD'){
    this.#heads=this.#heads.filter(s=>s.charCodeAt(this.#offset)===b);if(!this.#heads.length){this.invalidate();break;}
    this.#offset++;const complete=this.#heads.find(s=>s.length===this.#offset);
    if(complete){this.#mode=complete==='psql:<stdin>:'?'LINE':'STATE';this.#offset=0;this.#heads=[];}
   }else if(this.#mode==='LINE'){
    if(b>=48&&b<=57&&this.#digits<10&&(this.#digits>0||b!==48)){this.#digits++;}
    else if(b===58&&this.#digits>0)this.#mode='PREFIX_SPACE';else this.invalidate();
   }else if(this.#mode==='PREFIX_SPACE'){
    if(b!==32)this.invalidate();else{this.#mode='HEAD';this.#heads=['ERROR:  ','FATAL:  '];this.#offset=0;}
   }else if(this.#mode==='STATE'){
    if((b>=48&&b<=57)||(b>=65&&b<=90)){this.#state+=String.fromCharCode(b);if(this.#state.length===5)this.#mode='END';}else this.invalidate();
   }else if(this.#mode==='END'){
    if(b===10)this.#mode='DONE';else if(b===13)this.#mode='LF';else this.invalidate();
   }else if(this.#mode==='LF'){
    if(b===10)this.#mode='DONE';else this.invalidate();
   }else this.invalidate();
  }
 }
 finish({closed=false}={}){
  const state=closed&&!this.#sealed&&!this.#invalid&&['END','DONE'].includes(this.#mode)&&/^[A-Z0-9]{5}$/.test(this.#state)?this.#state:null;
  this.#sealed=true;this.invalidate();return state;
 }
}
export function diagnosticContext(phase){
 if(!['BEFORE','AFTER'].includes(phase))throw Error('LOCAL_METADATA_PHASE_REFUSED');
 return{phase,startedAt:new Date().toISOString(),startedMonotonic:performance.now(),spawnReturnedAt:null,firstStderrAt:null};
}
export function diagnosticError(code,context,sqlstate=null){
 const error=Error(/^LOCAL_METADATA_[A-Z_]+$/.test(code)?code:'LOCAL_METADATA_READ_REFUSED');
 const finishedAt=new Date().toISOString(),elapsedMilliseconds=Math.ceil(performance.now()-context.startedMonotonic);
 if(['BEFORE','AFTER'].includes(context.phase)&&iso(context.startedAt)&&iso(finishedAt)
  &&(context.spawnReturnedAt===null||iso(context.spawnReturnedAt))&&(context.firstStderrAt===null||iso(context.firstStderrAt))
  &&Number.isSafeInteger(elapsedMilliseconds)&&elapsedMilliseconds>=0&&(sqlstate===null||/^[A-Z0-9]{5}$/.test(sqlstate))){
  frames.set(error,Object.freeze({version:'CONTACT_QT_FIXED_METADATA_DIAGNOSTIC_V1',phase:context.phase,startedAt:context.startedAt,
   spawnReturnedAt:context.spawnReturnedAt,firstStderrAt:context.firstStderrAt,finishedAt,elapsedMilliseconds,sqlstate}));
 }
 return error;
}
export function preserveMetadataDiagnostic(from,to){if(frames.has(from))frames.set(to,frames.get(from));return to;}
export function metadataDiagnosticFor(error){const value=frames.get(error);return value?{...value}:null;}
