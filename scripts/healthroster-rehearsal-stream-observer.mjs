// Diagnostic-only observer. Partial records can never qualify SQL or restoration.
import {StringDecoder} from 'node:string_decoder';
import {closedRehearsalDiagnostic} from './healthroster-full-rehearsal-protocol.mjs';
const requireThat=(ok)=>{if(!ok)throw Error('REHEARSAL_STREAM_OBSERVER_INPUT_REFUSED');};
function lineSink(accept){
 const decoder=new StringDecoder('utf8');let pending='',discarding=false,overlong=0;
 return {feed(chunk){const text=decoder.write(chunk);let start=0;
  for(let end=text.indexOf('\n');end!==-1;end=text.indexOf('\n',start)){
   const part=text.slice(start,end);if(!discarding&&pending.length+part.length<=4096)accept((pending+part).replace(/\r$/,''));
   else if(!discarding)overlong++;pending='';discarding=false;start=end+1;
  }
  const rest=text.slice(start);if(!discarding){if(pending.length+rest.length>4096){pending='';discarding=true;overlong++;}else pending+=rest;}
 },stats(){return {incompleteLine:discarding||pending.length>0,overlongLines:overlong};}};
}
export function createRehearsalStreamObserver({units,lineMap,allowedCodes,onRecord}){
 requireThat(Array.isArray(units)&&units.length>0&&Array.isArray(lineMap)&&allowedCodes instanceof Set&&typeof onRecord==='function');
 const paths=new Map();for(const unit of units){requireThat(Number.isSafeInteger(unit.index)&&unit.index>=0&&!paths.has(unit.index)&&typeof unit.file==='string');paths.set(unit.index,unit.file);}
 let controls=0,diagnostics=0;
 const stdout=lineSink(line=>{
  const marker=line.match(/^HR_FILE_(BEGIN|END) (0|[1-9][0-9]*)$/);
  if(marker){const index=Number(marker[2]);if(paths.has(index)){controls++;onRecord({kind:'UNIT_PROGRESS',unit:index,file:paths.get(index),event:marker[1],unitCount:units.length});}return;}
  const prefix='HR_REHEARSAL_TRANSACTION_JSON ';if(!line.startsWith(prefix))return;
  let frame;try{frame=JSON.parse(line.slice(prefix.length));}catch{return;}
  if(!frame||typeof frame!=='object'||Array.isArray(frame)||Object.keys(frame).sort().join('|')!=='backendPid|database|epochZero|xid'
   ||frame.database!=='cloudtms_test_clone'||frame.epochZero!==true||!Number.isSafeInteger(frame.backendPid)||frame.backendPid<1
   ||typeof frame.xid!=='string'||!/^[1-9][0-9]{0,9}$/.test(frame.xid)||BigInt(frame.xid)>=4294967296n)return;
  controls++;onRecord({kind:'PARTIAL_TRANSACTION_IDENTITY',database:frame.database,backendPid:frame.backendPid,xid:frame.xid,epochZero:true,qualified:false});
 });
 const stderr=lineSink(line=>{
  const diagnostic=closedRehearsalDiagnostic(line,lineMap,allowedCodes);
  if(diagnostic){diagnostics++;onRecord({kind:'PARTIAL_SQL_DIAGNOSTIC',...diagnostic,errorCollectionComplete:false});return;}
  if(/^(?:psql:(?:[^\r\n]*?:)?\s+)?(?:ERROR|FATAL|error|fatal):/.test(line)){
   diagnostics++;onRecord({kind:'PARTIAL_SQL_DIAGNOSTIC',file:'HARNESS_CONTROL',line:null,statement:null,object:null,
    sqlstate:line.match(/(?:ERROR|FATAL):\s+([0-9A-Z]{5}):/)?.[1]??null,error:'DATABASE_DIAGNOSTIC_LOCATION_UNPROVED',errorCollectionComplete:false});
  }
 });
 return Object.freeze({onStdout:chunk=>stdout.feed(chunk),onStderr:chunk=>stderr.feed(chunk),summary:()=>({diagnosticOnly:true,controls,diagnostics,stdout:stdout.stats(),stderr:stderr.stats(),sqlQualified:false,restorationQualified:false})});
}
