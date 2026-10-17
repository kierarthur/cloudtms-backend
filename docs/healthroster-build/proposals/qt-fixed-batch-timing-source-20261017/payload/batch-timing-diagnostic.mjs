// Fixed batch transport timings only. No database, file, provider or execution API.
import {createHash} from 'node:crypto';
const check=(v,c)=>{if(!v)throw Error(c);},sha=s=>createHash('sha256').update(s).digest('hex');
const labels=Object.freeze(['BEGIN_COMPLETE','GUARD_COMPLETE','PEER_COMPLETE','SEARCH_PATH_COMPLETE','AUDIT_COMPLETE','OTHER_NINE_COMPLETE','FINAL_COMPLETE','ROLLBACK_COMPLETE']);
const framesAt=[0,0,1,1,1,1,3,3],failures=new WeakMap();
const instant=t=>{try{return t&&Number.isFinite(t.mono)&&t.mono>=0&&typeof t.wall==='string'&&new Date(t.wall).toISOString()===t.wall;}catch{return false;}};
export const BATCH_TIMING_LABELS=labels;
export const batchTimingMarker=i=>{check(Number.isSafeInteger(i)&&i>=0&&i<labels.length,'LOCAL_METADATA_BATCH_TIMING_ORDINAL_REFUSED');return 'HR_CONTACT_BATCH_TIMING_V1_'+(i+1)+'_'+labels[i];};
export function compileBatchTimingInput(built,managed){
 const s=managed.scanManagedSql(built.originalInput);check(s.meta.length===0&&s.statements.length===6,'LOCAL_METADATA_BATCH_TIMING_PARENT_REFUSED');const pieces=s.statements.map(x=>built.originalInput.slice(x.start,x.end));
 check(pieces[0]==='begin isolation level repeatable read read only;'&&pieces[3]==='set local search_path = pg_catalog, public;'&&pieces[5]==='rollback;'&&s.statements.map(x=>x.tokens[0]).join('|')==='BEGIN|DO|SELECT|SET|WITH|ROLLBACK','LOCAL_METADATA_BATCH_TIMING_PARENT_REFUSED');
 check(built.bagQueries?.length===2&&built.descriptor?.version==='CONTACT_SAME_RR_NATIVE_ARRAY_CSE_V1'&&built.descriptor.bagQuerySha256.every((h,i)=>h===sha(built.bagQueries[i]))&&sha(built.input)===built.descriptor.executedBatchInputSha256&&sha(built.finalQuery)===built.descriptor.finalQuerySha256,'LOCAL_METADATA_BATCH_TIMING_SOURCE_REFUSED');
 const edits=[],prefix=pieces.slice(0,4).join('\n')+'\n';check(built.input.startsWith(prefix),'LOCAL_METADATA_BATCH_TIMING_PREFIX_REFUSED');
 let input=built.input;function edit(before,after){check(input.split(before).length===2,'LOCAL_METADATA_BATCH_TIMING_ANCHOR_REFUSED');input=input.replace(before,()=>after);edits.push({before,after});}
 edit(prefix,pieces.slice(0,4).map((p,i)=>p+'\n\\echo '+batchTimingMarker(i)+'\n').join(''));
 for(let i=0;i<2;i++){const q=built.bagQueries[i]+'\n\\gset\n';edit(q,q+'\\echo '+batchTimingMarker(4+i)+'\n');}
 edit(built.finalQuery+'\n'+pieces[5]+'\n',built.finalQuery+'\n\\echo '+batchTimingMarker(6)+'\n'+pieces[5]+'\n\\echo '+batchTimingMarker(7)+'\n');
 const stripped=input.split('\n').filter(x=>!/^\\echo HR_CONTACT_BATCH_TIMING_V1_/.test(x)).join('\n');check(stripped===built.input,'LOCAL_METADATA_BATCH_TIMING_SQL_CHANGED');
 let inverse=input;for(const e of [...edits].reverse()){check(inverse.split(e.after).length===2,'LOCAL_METADATA_BATCH_TIMING_INVERSE_REFUSED');inverse=inverse.replace(e.after,()=>e.before);}check(inverse===built.input,'LOCAL_METADATA_BATCH_TIMING_INVERSE_REFUSED');
 return{input,descriptor:{version:'CONTACT_FIXED_BATCH_TIMING_ONLY_V1',referenceInputSha256:sha(built.input),instrumentedInputSha256:sha(input),canonicalSqlSha256:built.descriptor.referenceCanonicalSqlSha256,markers:labels.length,existingQueries:3,sqlBytesUnchangedAfterRemovingEcho:true,noB:true,restorationQualified:false},edits};
}
// Markers are removed byte-for-byte; every original peer/base/map byte reaches
// the unchanged bounded reader. Only <=256-byte marker lines are buffered here.
export class BatchTimingCapture{
 #mode='START';#marker=[];#frames=0;#events=[];#received=0;#delegate;#now;#start;
 constructor(delegate,now=()=>({wall:new Date().toISOString(),mono:performance.now()})){check(delegate&&['stdout','stderr','finish','diagnosticSqlstate'].every(k=>typeof delegate[k]==='function'),'LOCAL_METADATA_BATCH_TIMING_DELEGATE_REFUSED');this.#delegate=delegate;this.#now=now;this.#start=now();check(instant(this.#start),'LOCAL_METADATA_BATCH_TIMING_CLOCK_REFUSED');this.bytes=0;this.stderrBytes=0;this.failed=null;}
 #fail(code){this.failed??=code;this.#marker=[];}
 stdout(chunk){if(this.failed)return;const b=Buffer.isBuffer(chunk)?chunk:Buffer.from(chunk);this.bytes+=b.length;if(this.bytes>64*1024*1024){this.#fail('LOCAL_METADATA_STDOUT_BOUND');return;}
  let at=0;while(at<b.length&&!this.failed){
   if(this.#mode==='START'){if(this.#events.length===labels.length){this.#fail('LOCAL_METADATA_BATCH_TIMING_EXTRA_OUTPUT');break;}if(b[at]===123){this.#mode='JSON';}else if(b[at]===72){this.#mode='MARKER';}else{this.#fail('LOCAL_METADATA_BATCH_TIMING_FRAME_REFUSED');break;}}
   const end=b.indexOf(10,at),last=end<0?b.length:end+1,part=b.subarray(at,last);at=last;
   if(this.#mode==='JSON'){this.#delegate.stdout(part);if(this.#delegate.failed){this.#fail(this.#delegate.failed);break;}if(end>=0){this.#frames++;this.#mode='START';}}
   else{if(this.#marker.length+part.length>256){this.#fail('LOCAL_METADATA_CONTROL_FRAME_BOUND');break;}this.#marker.push(...part);if(end>=0){let raw=Buffer.from(this.#marker);if(raw.at(-2)===13)raw=raw.subarray(0,-2);else raw=raw.subarray(0,-1);const n=this.#events.length;
     if(!raw.equals(Buffer.from(batchTimingMarker(n)))||this.#frames!==framesAt[n]){this.#fail('LOCAL_METADATA_BATCH_TIMING_ORDER_REFUSED');break;}
     const t=this.#now();if(!instant(t)||t.mono<this.#start.mono||t.mono<this.#received){this.#fail('LOCAL_METADATA_BATCH_TIMING_CLOCK_REFUSED');break;}
     this.#received=t.mono;this.#events.push({ordinal:n+1,completion:labels[n],receivedAt:t.wall,elapsedMilliseconds:t.mono-this.#start.mono});this.#marker=[];this.#mode='START';
    }}
  }
 }
 stderr(chunk){this.#delegate.stderr(chunk);this.stderrBytes=this.#delegate.stderrBytes;this.#fail(this.#delegate.failed??'LOCAL_METADATA_DIAGNOSTIC_REFUSED');}
 diagnosticSqlstate(closed){return this.#delegate.diagnosticSqlstate(closed);}
 progress(closed){return{version:'CONTACT_FIXED_BATCH_TIMING_ONLY_V1',childClosed:closed,complete:this.#events.map(x=>({...x})),nextUnconfirmedOrdinal:this.#events.length===labels.length?null:this.#events.length+1,statementFailureOrdinal:'UNKNOWN_COMPLETION_MARKERS_ARE_LOWER_BOUNDS',restorationQualified:false,completeMapsPersisted:false};}
 attachFailure(error,closed){failures.set(error,this.progress(closed));return error;}
 finish(){check(!this.failed,this.failed);check(this.#mode==='START'&&this.#marker.length===0&&this.#events.length===labels.length&&this.#frames===3,'LOCAL_METADATA_BATCH_TIMING_INCOMPLETE');const actual=this.#delegate.finish();return{...actual,timingProgress:this.progress(true),transport:{...actual.transport,instrumentedStdoutBytes:this.bytes,completionMarkers:labels.length}};}
}
export function batchTimingProgressFor(error){const p=failures.get(error);return p?structuredClone(p):null;}
