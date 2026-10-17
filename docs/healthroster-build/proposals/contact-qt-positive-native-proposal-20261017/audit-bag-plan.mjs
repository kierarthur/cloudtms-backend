// Fixed estimated-plan diagnostic only. No connection, arbitrary query or maps.
import {createHash} from 'node:crypto';
import {SqlstateStderrCapture} from './metadata-diagnostic.mjs';
import {diagnosticMarker} from './metadata-progress-diagnostic.mjs';
const check=(v,c)=>{if(!v)throw Error(c);},sha=s=>createHash('sha256').update(s).digest('hex');
const explain='EXPLAIN (ANALYZE FALSE, VERBOSE FALSE, COSTS TRUE, SETTINGS FALSE, SUMMARY FALSE, FORMAT JSON) ';
export function compileAuditBagPlan(batch,managed){
 check(batch&&batch.descriptor?.version==='CONTACT_SAME_RR_NATIVE_ARRAY_CSE_V1'&&typeof batch.input==='string'&&Array.isArray(batch.bagQueries)&&batch.bagQueries.length===2&&typeof batch.finalQuery==='string','LOCAL_METADATA_AUDIT_PLAN_BATCH_REQUIRED');
 check(sha(batch.input)===batch.descriptor.executedBatchInputSha256&&batch.bagQueries.every((q,i)=>sha(q)===batch.descriptor.bagQuerySha256[i])&&sha(batch.finalQuery)===batch.descriptor.finalQuerySha256,'LOCAL_METADATA_AUDIT_PLAN_SOURCE_REQUIRED');
 const tail=batch.bagQueries.map(q=>q+'\n\\gset\n').join('')+batch.finalQuery+'\nrollback;\n';
 check(batch.input.endsWith(tail)&&batch.input.split(tail).length===2,'LOCAL_METADATA_AUDIT_PLAN_EXACT_TAIL_REQUIRED');
 const prefix=batch.input.slice(0,-tail.length),parsed=managed.scanManagedSql(prefix),s=parsed.statements;
 check(parsed.meta.length===0&&s.length===4&&s.map(x=>x.tokens[0]).join('|')==='BEGIN|DO|SELECT|SET','LOCAL_METADATA_AUDIT_PLAN_PREFIX_REQUIRED');
 const pieces=s.map(x=>prefix.slice(x.start,x.end));check(pieces[0]==='begin isolation level repeatable read read only;'&&pieces[3]==='set local search_path = pg_catalog, public;'&&prefix===pieces.join('\n')+'\n','LOCAL_METADATA_AUDIT_PLAN_PREFIX_REQUIRED');
 check(batch.parts&&typeof batch.parts.audit==='string','LOCAL_METADATA_AUDIT_PLAN_AUDIT_SOURCE_REQUIRED');const audit=batch.bagQueries[0],auditParsed=managed.scanManagedSql(audit);
 check(auditParsed.meta.length===0&&auditParsed.statements.length===1&&auditParsed.statements[0].tokens[0]==='WITH'&&!audit.endsWith(';'),'LOCAL_METADATA_AUDIT_PLAN_QUERY_REQUIRED');
 check(audit===`with retained_business as (${batch.parts.audit}), fixed_bag as (select jsonb_agg(jsonb_build_object('relation',relation,'row_count',row_count,'row_fingerprint',row_fingerprint) order by relation)::text value from retained_business)\nselect value as hr_contact_audit_bag from fixed_bag where octet_length(value)<=16384`,'LOCAL_METADATA_AUDIT_PLAN_EXACT_BAG_REQUIRED');
 const statement=explain+audit+';',selected=[...pieces,statement,'rollback;'];
 const input=selected.map((q,i)=>q+'\n\\echo '+diagnosticMarker('PLAN',i)+'\n').join('');
 check(managed.scanManagedSql(statement).statements.length===1&&statement.slice(explain.length,-1)===audit,'LOCAL_METADATA_AUDIT_PLAN_WHOLE_QUERY_REQUIRED');
 return{input,auditQuery:audit,explainStatement:statement,prefix,descriptor:{version:'CONTACT_EXACT_EXECUTED_AUDIT_BAG_ESTIMATED_PLAN_V1',executedBatchInputSha256:sha(batch.input),executedAuditBagSha256:sha(audit),explainStatementSha256:sha(statement),diagnosticInputSha256:sha(input),canonicalReferenceExecuted:false,auditQueryExecuted:false,analyze:false,statements:6,fullCanonicalSnapshotRead:false,completeMapsPersisted:false,restorationQualified:false}};
}
const nodeTypes=new Set('Aggregate|Append|Bitmap Heap Scan|Bitmap Index Scan|BitmapAnd|BitmapOr|CTE Scan|Custom Scan|Foreign Scan|Function Scan|Gather|Gather Merge|Group|Hash|Hash Join|Incremental Sort|Index Only Scan|Index Scan|Limit|LockRows|Materialize|Memoize|Merge Append|Merge Join|ModifyTable|Nested Loop|ProjectSet|Recursive Union|Result|Sample Scan|Seq Scan|SetOp|Sort|Subquery Scan|Table Function Scan|Tid Range Scan|Tid Scan|Unique|Values Scan|WindowAgg|WorkTable Scan'.split('|'));
const ctes=Object.freeze(['retained_business','fixed_bag']);
const noActual=k=>/^(?:Actual |Execution Time$|Planning Time$|Triggers$|Workers$|Rows Removed |Sort Method$|Sort Space |Peak Memory Usage$|Disk Usage$|HashAgg Batches$|Hash Batches$|Hash Buckets$|Shared |Local |Temp |I\/O |WAL )/.test(k);
export function projectAuditBagPlan(value){
 check(Array.isArray(value)&&value.length===1&&value[0]&&['Plan','JIT|Plan'].includes(Object.keys(value[0]).sort().join('|')),'LOCAL_METADATA_AUDIT_PLAN_ENVELOPE_REQUIRED');
 let jit=null;if(Object.hasOwn(value[0],'JIT')){const j=value[0].JIT;check(j&&Object.keys(j).sort().join('|')==='Functions|Options'&&Number.isSafeInteger(j.Functions)&&j.Functions>0&&j.Options&&Object.keys(j.Options).sort().join('|')==='Deforming|Expressions|Inlining|Optimization'&&Object.values(j.Options).every(x=>typeof x==='boolean'),'LOCAL_METADATA_AUDIT_PLAN_JIT_REQUIRED');jit={functions:j.Functions,inlining:j.Options.Inlining,optimization:j.Options.Optimization,expressions:j.Options.Expressions,deforming:j.Options.Deforming};}
 const nodes=[];
 const visit=(n,parent=null,depth=0)=>{
  check(depth<=256,'LOCAL_METADATA_AUDIT_PLAN_DEPTH_BOUND');
  check(n&&typeof n==='object'&&!Array.isArray(n)&&nodeTypes.has(n['Node Type'])&&!Object.keys(n).some(noActual),'LOCAL_METADATA_AUDIT_PLAN_NODE_REQUIRED');
  const ordinal=nodes.length+1;check(ordinal<=16384,'LOCAL_METADATA_AUDIT_PLAN_NODE_BOUND');const row={ordinal,parent,nodeType:n['Node Type']};
  for(const[k,out]of[['Startup Cost','startupCost'],['Total Cost','totalCost'],['Plan Rows','estimatedRows'],['Plan Width','estimatedWidth']]){const v=n[k];check(typeof v==='number'&&Number.isFinite(v)&&v>=0,'LOCAL_METADATA_AUDIT_PLAN_ESTIMATE_REQUIRED');row[out]=v;}
  check(row.startupCost<=row.totalCost&&Number.isSafeInteger(row.estimatedRows)&&Number.isSafeInteger(row.estimatedWidth),'LOCAL_METADATA_AUDIT_PLAN_ESTIMATE_REQUIRED');
  for(const[k,out]of[['Parallel Aware','parallelAware'],['Async Capable','asyncCapable']])if(Object.hasOwn(n,k)){check(typeof n[k]==='boolean','LOCAL_METADATA_AUDIT_PLAN_BOOLEAN_REQUIRED');row[out]=n[k];}
  for(const[k,out]of[['Workers Planned','workersPlanned'],['Planned Partitions','plannedPartitions']])if(Object.hasOwn(n,k)){check(Number.isSafeInteger(n[k])&&n[k]>=0,'LOCAL_METADATA_AUDIT_PLAN_INTEGER_REQUIRED');row[out]=n[k];}
  for(const[k,out,allowed]of[['Strategy','strategy',['Plain','Sorted','Hashed','Mixed']],['Partial Mode','partialMode',['Simple','Partial','Finalize']],['Parent Relationship','parentRelationship',['Outer','Inner','Subquery','Member','InitPlan','SubPlan']]])if(Object.hasOwn(n,k)){check(allowed.includes(n[k]),'LOCAL_METADATA_AUDIT_PLAN_ENUM_REQUIRED');row[out]=n[k];}
  if(Object.hasOwn(n,'Schema'))check(n.Schema==='public','LOCAL_METADATA_AUDIT_PLAN_RELATION_REQUIRED');
  if(Object.hasOwn(n,'Relation Name')){check(n['Relation Name']==='audit_events','LOCAL_METADATA_AUDIT_PLAN_RELATION_REQUIRED');row.relationOrdinal=1;}
  if(Object.hasOwn(n,'CTE Name')){check(ctes.includes(n['CTE Name']),'LOCAL_METADATA_AUDIT_PLAN_CTE_REQUIRED');row.cteOrdinal=ctes.indexOf(n['CTE Name'])+1;}
  if(Object.hasOwn(n,'Subplan Name')){const m=/^CTE (retained_business|fixed_bag)$/.exec(n['Subplan Name']);check(m,'LOCAL_METADATA_AUDIT_PLAN_CTE_REQUIRED');row.cteProducerOrdinal=ctes.indexOf(m[1])+1;}
  for(const[k,out]of[['Sort Key','sortKeyCount'],['Presorted Key','presortedKeyCount'],['Group Key','groupKeyCount'],['Hash Key','hashKeyCount']])if(Object.hasOwn(n,k)){check(Array.isArray(n[k])&&n[k].length<=64&&n[k].every(v=>typeof v==='string'&&Buffer.byteLength(v)<=16384),'LOCAL_METADATA_AUDIT_PLAN_KEYS_REQUIRED');row[out]=n[k].length;}
  nodes.push(row);check(n.Plans===undefined||Array.isArray(n.Plans),'LOCAL_METADATA_AUDIT_PLAN_CHILDREN_REQUIRED');for(const child of n.Plans??[])visit(child,ordinal,depth+1);
 };
 visit(value[0].Plan);
 return{version:'CONTACT_EXACT_AUDIT_BAG_FULL_ESTIMATED_PLAN_V1',nodeCount:nodes.length,nodes,...jit?{jit}:{},allNodesRetained:true,rawExpressionsPersisted:false,rawPlanPersisted:false,actualRowsOrTimings:false,actualSpillOrPeakMemory:false,restorationQualified:false};
}
const failures=new WeakMap();
export class AuditBagPlanCapture{
 #stderr=new SqlstateStderrCapture();#line=[];#plan=[];#peer=null;#summary=null;#events=[];
 constructor(){this.bytes=0;this.stderrBytes=0;this.failed=null;}
 #fail(c){this.failed??=c;this.#line=[];this.#plan=[];this.#summary=null;}
 stdout(chunk){if(this.failed)return;const raw=Buffer.isBuffer(chunk)?chunk:Buffer.from(chunk);this.bytes+=raw.length;if(this.bytes>64*1024*1024){this.#fail('LOCAL_METADATA_STDOUT_BOUND');return;}
  for(const b of raw){if(b!==10){this.#line.push(b);if(this.#line.length>16384){this.#fail('LOCAL_METADATA_CONTROL_FRAME_BOUND');return;}continue;}let line;try{line=new TextDecoder('utf-8',{fatal:true}).decode(Buffer.from(this.#line));}catch{this.#fail('LOCAL_METADATA_UTF8_REFUSED');return;}this.#line=[];if(line.endsWith('\r'))line=line.slice(0,-1);try{this.#accept(line);}catch(e){this.#fail(/^LOCAL_METADATA_[A-Z_]+$/.test(e?.message??'')?e.message:'LOCAL_METADATA_AUDIT_PLAN_FRAME_REQUIRED');return;}}
 }
 #accept(line){const i=this.#events.length;check(i<6,'LOCAL_METADATA_AUDIT_PLAN_EXTRA_FRAME');
  if(i===2&&line.startsWith('{')&&!this.#peer){const p=JSON.parse(line);check(p&&Object.keys(p).sort().join('|')==='database|databaseOid|holderPid|isolation|noRoleSwitch|readOnly|readerPid'&&p.database==='cloudtms_test_clone'&&[p.databaseOid,p.holderPid,p.readerPid].every(v=>Number.isSafeInteger(v)&&v>0)&&p.holderPid!==p.readerPid&&p.readOnly===true&&p.isolation==='repeatable read'&&p.noRoleSwitch===true,'LOCAL_METADATA_NATIVE_PEER_REFUSED');this.#peer=p;return;}
  if(i===4&&line!==diagnosticMarker('PLAN',i)){this.#plan.push(line);return;}
  check(line===diagnosticMarker('PLAN',i),'LOCAL_METADATA_AUDIT_PLAN_FRAME_REQUIRED');if(i===2)check(this.#peer,'LOCAL_METADATA_NATIVE_PEER_REFUSED');if(i===4){this.#summary=projectAuditBagPlan(JSON.parse(this.#plan.join('\n')));this.#plan=[];}this.#events.push({ordinal:i+1,completion:['BEGIN_COMPLETE','GUARD_COMPLETE','PEER_COMPLETE','SEARCH_PATH_COMPLETE','EXPLAIN_COMPLETE','ROLLBACK_COMPLETE'][i],receivedAt:new Date().toISOString()});
 }
 stderr(chunk){this.stderrBytes=Math.min(16385,this.stderrBytes+Buffer.byteLength(chunk));this.#stderr.write(chunk);this.#fail(this.stderrBytes>16384?'LOCAL_METADATA_STDERR_BOUND':'LOCAL_METADATA_DIAGNOSTIC_REFUSED');}
 diagnosticSqlstate(closed){return this.#stderr.finish({closed});}
 progress(closed){return{version:'CONTACT_EXACT_AUDIT_BAG_PLAN_PROGRESS_V1',childClosed:closed,completed:this.#events.map(x=>({...x})),nextUnconfirmedOrdinal:this.#events.length===6?null:this.#events.length+1,actualRowsOrTimings:false,restorationQualified:false};}
 attachFailure(error,closed){failures.set(error,this.progress(closed));return error;}
 finish(){check(!this.failed,this.failed);check(!this.#line.length&&this.#events.length===6&&this.#peer&&this.#summary,'LOCAL_METADATA_AUDIT_PLAN_INCOMPLETE');return{peer:{...this.#peer},plan:this.#summary,progress:this.progress(true),transport:{stdoutBytes:this.bytes,frameCount:7}};}
}
export function auditBagPlanProgressFor(error){const v=failures.get(error);return v?structuredClone(v):null;}
