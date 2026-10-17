// Bounded diagnostic projection only. Never qualifies canonical restoration.
import {SqlstateStderrCapture}from'./metadata-diagnostic.mjs';
import fs from'node:fs';import{createHash}from'node:crypto';
const scopeMapRaw=fs.readFileSync(new URL('./PLAN_SOURCE_SCOPES.json',import.meta.url)),scopeMap=JSON.parse(scopeMapRaw),scopeMapSha256=createHash('sha256').update(scopeMapRaw).digest('hex');
if(scopeMap.version!=='CONTACT_CANONICAL_PLAN_SOURCE_SCOPE_ORDINALS_V1'||scopeMap.relations.length!==37||scopeMap.ctes.length!==16)throw Error('LOCAL_METADATA_PLAN_SOURCE_MAP_REFUSED');
const scopeRelations=new Map(scopeMap.relations.map(r=>[r.name,r])),scopeCtes=new Map(scopeMap.ctes.map(r=>[r.name,r])),scopeSchemas=new Set(scopeMap.relations.flatMap(r=>r.schemas));
const failures=new WeakMap(),check=(v,c)=>{if(!v)throw Error(c);};
const marker=(ordinal,label)=>`HR_CONTACT_CBEFORE_DIAG_V1_${ordinal}_${label}`;
export const DIAGNOSTIC_MARKERS=Object.freeze({PREFIX:['BEGIN_COMPLETE','GUARD_COMPLETE','PEER_COMPLETE','SEARCH_PATH_COMPLETE','ROLLBACK_COMPLETE'],PLAN:['BEGIN_COMPLETE','GUARD_COMPLETE','PEER_COMPLETE','SEARCH_PATH_COMPLETE','EXPLAIN_COMPLETE','ROLLBACK_COMPLETE'],AUDIT:['BEGIN_COMPLETE','GUARD_COMPLETE','PEER_COMPLETE','SEARCH_PATH_COMPLETE','AUDIT_COMPLETE','ROLLBACK_COMPLETE'],CATALOGUE:['BEGIN_COMPLETE','GUARD_COMPLETE','PEER_COMPLETE','SEARCH_PATH_COMPLETE','CATALOGUE_COMPLETE','ROLLBACK_COMPLETE'],BACKGROUND:['BEGIN_COMPLETE','GUARD_COMPLETE','PEER_COMPLETE','SEARCH_PATH_COMPLETE','BACKGROUND_COMPLETE','ROLLBACK_COMPLETE']});
const componentCounts=Object.freeze({AUDIT:1,CATALOGUE:1,BACKGROUND:2});
function componentFrame(mode,p){check(p&&typeof p==='object'&&!Array.isArray(p)&&Object.keys(p).sort().join('|')==='component|counts|digest|version'&&p.version==='CONTACT_CANONICAL_COMPONENT_V1'&&p.component===mode&&Array.isArray(p.counts)&&p.counts.length===componentCounts[mode]&&p.counts.every(v=>typeof v==='string'&&/^(?:0|[1-9][0-9]{0,18})$/.test(v)&&BigInt(v)<=(mode==='BACKGROUND'?2147483647n:9223372036854775807n))&&typeof p.digest==='string'&&/^[0-9a-f]{64}$/.test(p.digest)&&(mode!=='CATALOGUE'||p.counts[0]==='1'),'LOCAL_METADATA_COMPONENT_FRAME_REFUSED');return{version:p.version,component:p.component,counts:[...p.counts],digest:p.digest};}
export const diagnosticMarker=(mode,index)=>marker(index+1,DIAGNOSTIC_MARKERS[mode][index]);
const peerKeys='database|databaseOid|holderPid|isolation|noRoleSwitch|readOnly|readerPid';
const nodeTypes=new Set('Aggregate|Append|Bitmap Heap Scan|Bitmap Index Scan|BitmapAnd|BitmapOr|CTE Scan|Custom Scan|Foreign Scan|Function Scan|Gather|Gather Merge|Group|Hash|Hash Join|Incremental Sort|Index Only Scan|Index Scan|Limit|LockRows|Materialize|Memoize|Merge Append|Merge Join|ModifyTable|Nested Loop|ProjectSet|Recursive Union|Result|Sample Scan|Seq Scan|SetOp|Sort|Subquery Scan|Table Function Scan|Tid Range Scan|Tid Scan|Unique|Values Scan|WindowAgg|WorkTable Scan'.split('|'));
export function projectEstimatedPlan(value){
 check(Array.isArray(value)&&value.length===1&&value[0]&&Object.keys(value[0]).join('|')==='Plan','LOCAL_METADATA_PLAN_ENVELOPE_REFUSED');
 const nodes=[];let ordinal=0;
 const visit=(node,parent=null)=>{check(node&&typeof node==='object'&&!Array.isArray(node)&&nodeTypes.has(node['Node Type']),'LOCAL_METADATA_PLAN_NODE_REFUSED');check(!Object.keys(node).some(k=>/^Actual |^Execution Time$|^Triggers$/.test(k)),'LOCAL_METADATA_ANALYZE_DATA_REFUSED');
  const id=++ordinal;check(id<=16384,'LOCAL_METADATA_PLAN_NODE_BOUND');const row={ordinal:id,parent,nodeType:node['Node Type']},relations=new Set(),ctes=new Set();
  for(const [key,out]of[['Startup Cost','startupCost'],['Total Cost','totalCost'],['Plan Rows','estimatedRows'],['Plan Width','estimatedWidth']]){const n=node[key];check(typeof n==='number'&&Number.isFinite(n)&&n>=0,'LOCAL_METADATA_PLAN_NUMERIC_REFUSED');row[out]=n;}
  if(node.Schema!==undefined)check(typeof node.Schema==='string'&&scopeSchemas.has(node.Schema),'LOCAL_METADATA_PLAN_SOURCE_SCHEMA_REFUSED');
  if(node['Relation Name']!==undefined){const name=node['Relation Name'],scope=scopeRelations.get(name);check(typeof name==='string'&&scope&&(!node.Schema||scope.schemas.includes(node.Schema)),'LOCAL_METADATA_PLAN_SOURCE_RELATION_REFUSED');row.relationOrdinal=scope.ordinal;relations.add(scope.ordinal);}
  const addCte=name=>{const scope=scopeCtes.get(name);check(typeof name==='string'&&scope,'LOCAL_METADATA_PLAN_SOURCE_CTE_REFUSED');ctes.add(scope.ordinal);return scope.ordinal;};
  if(node['CTE Name']!==undefined)row.cteOrdinal=addCte(node['CTE Name']);
  if(typeof node['Subplan Name']==='string'&&node['Subplan Name'].startsWith('CTE ')){const match=node['Subplan Name'].match(/^CTE ([a-z_][a-z0-9_]*)$/);check(match,'LOCAL_METADATA_PLAN_SOURCE_CTE_REFUSED');row.cteProducerOrdinal=addCte(match[1]);}
  nodes.push(row);check(node.Plans===undefined||Array.isArray(node.Plans),'LOCAL_METADATA_PLAN_CHILDREN_REFUSED');for(const child of node.Plans??[]){const scope=visit(child,id);for(const r of scope.relations)relations.add(r);for(const c of scope.ctes)ctes.add(c);}
  row.sourceRelationOrdinals=[...relations].sort((a,b)=>a-b);row.sourceCteOrdinals=[...ctes].sort((a,b)=>a-b);return{relations,ctes};};visit(value[0].Plan);
 const top=nodes.slice().sort((a,b)=>b.totalCost-a.totalCost||a.ordinal-b.ordinal).slice(0,32);
 return{version:'CONTACT_CBEFORE_NO_ANALYZE_ESTIMATED_PLAN_V2',sourceScopeMapSha256:scopeMapSha256,nodeCount:nodes.length,topEstimatedCostNodes:top,actualRowsOrTimings:false,rawPlanPersisted:false};
}

export class DiagnosticMemoryCapture{
 #stderr=new SqlstateStderrCapture();#line=[];#lineBytes=0;#plan=[];#peer=null;#events=[];#planSummary=null;#component=null;
 constructor(mode){check(Object.hasOwn(DIAGNOSTIC_MARKERS,mode),'LOCAL_METADATA_DIAGNOSTIC_MODE_REFUSED');this.mode=mode;this.bytes=0;this.stderrBytes=0;this.failed=null;}
 #discard(){this.#line=[];this.#lineBytes=0;this.#plan=[];this.#component=null;}
 #fail(code){this.failed??=code;this.#discard();}
 stdout(chunk){if(this.failed)return;const raw=Buffer.isBuffer(chunk)?chunk:Buffer.from(chunk);this.bytes+=raw.length;if(this.bytes>64*1024*1024){this.#fail('LOCAL_METADATA_STDOUT_BOUND');return;}
  for(const byte of raw){if(byte===10){let line;try{line=new TextDecoder('utf-8',{fatal:true}).decode(Buffer.from(this.#line));}catch{this.#fail('LOCAL_METADATA_UTF8_REFUSED');return;}this.#line=[];this.#lineBytes=0;if(line.endsWith('\r'))line=line.slice(0,-1);try{this.#accept(line);}catch(error){this.#fail(/^LOCAL_METADATA_[A-Z_]+$/.test(error?.message??'')?error.message:'LOCAL_METADATA_DIAGNOSTIC_FRAME_REFUSED');return;}}else{this.#line.push(byte);if(++this.#lineBytes>16384){this.#fail('LOCAL_METADATA_CONTROL_FRAME_BOUND');return;}}}}
 #accept(line){const index=this.#events.length,want=DIAGNOSTIC_MARKERS[this.mode];check(index<want.length,'LOCAL_METADATA_DIAGNOSTIC_EXTRA_FRAME');
  if(this.mode==='PLAN'&&index===4&&line!==diagnosticMarker(this.mode,index)){this.#plan.push(line);return;}
  if(index===2&&line.startsWith('{')&&!this.#peer){let p;try{p=JSON.parse(line);}catch{throw Error('LOCAL_METADATA_JSON_REFUSED');}check(p&&Object.keys(p).sort().join('|')===peerKeys&&p.database==='cloudtms_test_clone'&&Number.isSafeInteger(p.databaseOid)&&p.databaseOid>0&&Number.isSafeInteger(p.readerPid)&&p.readerPid>0&&Number.isSafeInteger(p.holderPid)&&p.holderPid>0&&p.readOnly===true&&p.isolation==='repeatable read'&&p.noRoleSwitch===true,'LOCAL_METADATA_NATIVE_PEER_REFUSED');this.#peer=p;return;}
  if(Object.hasOwn(componentCounts,this.mode)&&index===4&&line.startsWith('{')&&!this.#component){let p;try{p=JSON.parse(line);}catch{throw Error('LOCAL_METADATA_JSON_REFUSED');}this.#component=componentFrame(this.mode,p);return;}
  if(Object.hasOwn(componentCounts,this.mode)&&index===4)check(this.#component,'LOCAL_METADATA_COMPONENT_FRAME_REFUSED');
  check(line===diagnosticMarker(this.mode,index),'LOCAL_METADATA_DIAGNOSTIC_FRAME_REFUSED');if(index===2)check(this.#peer,'LOCAL_METADATA_NATIVE_PEER_REFUSED');if(this.mode==='PLAN'&&index===4){let p;try{p=JSON.parse(this.#plan.join('\n'));}catch{throw Error('LOCAL_METADATA_PLAN_JSON_REFUSED');}this.#planSummary=projectEstimatedPlan(p);this.#plan=[];}
  this.#events.push(Object.freeze({ordinal:index+1,originalOrdinal:this.mode==='PREFIX'&&index===4?6:index+1,completion:want[index],receivedAt:new Date().toISOString()}));}
 stderr(chunk){this.stderrBytes=Math.min(16385,this.stderrBytes+Buffer.byteLength(chunk));this.#stderr.write(chunk);this.#fail(this.stderrBytes>16384?'LOCAL_METADATA_STDERR_BOUND':'LOCAL_METADATA_DIAGNOSTIC_REFUSED');}
 diagnosticSqlstate(closed){return this.#stderr.finish({closed});}
 progress(closed){return{version:'CONTACT_CBEFORE_PREFIX_COMPLETION_V1',mode:this.mode,childClosed:closed,completed:this.#events.map(row=>({...row})),nextUnconfirmedOrdinal:this.#events.length===DIAGNOSTIC_MARKERS[this.mode].length?null:this.#events.length+1,failedStatementOrdinal:'UNKNOWN_COMPLETION_MARKERS_ARE_A_LOWER_BOUND',canonicalRowsRead:false,...Object.hasOwn(componentCounts,this.mode)?{componentOnly:true,fullCanonicalSnapshotRead:false}:{},restorationQualified:false};}
 attachFailure(error,closed){failures.set(error,this.progress(closed));return error;}
 finish(){check(!this.failed,this.failed);check(this.#lineBytes===0&&this.#events.length===DIAGNOSTIC_MARKERS[this.mode].length&&this.#peer,'LOCAL_METADATA_DIAGNOSTIC_INCOMPLETE');const result={peer:{...this.#peer},progress:this.progress(true),...this.#planSummary?{plan:this.#planSummary}:{},...this.#component?{component:this.#component}:{},transport:{stdoutBytes:this.bytes,frameCount:this.#events.length+1}};this.#discard();return result;}
}
export function metadataProgressFor(error){const row=failures.get(error);return row?structuredClone(row):null;}
