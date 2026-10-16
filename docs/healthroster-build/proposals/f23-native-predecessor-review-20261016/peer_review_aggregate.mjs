// Independent read-only review of Root's finite aggregate classifier; no SQL execution.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';

const dir=path.dirname(fileURLToPath(import.meta.url));
const root=path.resolve(dir,'../../../../../healthroster-install2-14102026');
const hash=x=>createHash('sha256').update(x).digest('hex');
const read=p=>fs.readFileSync(path.join(root,p));
const json=p=>JSON.parse(read(p));
const canon=x=>JSON.stringify(Object.fromEntries(Object.entries(x).sort(([a],[b])=>a<b?-1:a>b?1:0)));
const approvalPath='docs/healthroster-build/v6-owner-delivery-20261016/AGGREGATE_TYPE_CLASSIFICATION.json';
assert.equal(hash(read(approvalPath)),'26dc83bb7ad2ccf6881b8c75de806e24cae018f7e4086e827c81c7522b188127');
const approval=json(approvalPath),evidence=json(approval.sourceReview.path),contract=json(evidence.schema.path);
const sources=['scripts/healthroster-aggregate-type-review.mjs','tests/healthroster-aggregate-type-review.test.mjs','scripts/bpay-next-source-gates.mjs',approvalPath,approval.sourceReview.path,approval.routeReview.path,...approval.schemaInputs.map(x=>x.path)];
const pins=[...new Set(sources)].map(file=>({file,sha256:hash(read(file))}));
const importFile=p=>import(pathToFileURL(path.join(root,p)).href);
const {reviewedHealthAggregateHazards}=await importFile('scripts/healthroster-aggregate-type-review.mjs');
const {specialConstructViolations,executableParts}=await importFile('scripts/bpay-next-source-gates.mjs');
const {scanManagedSql}=await importFile('scripts/cloudtms-managed-release-sql.mjs');
const baselineKey='INDEPENDENT_EXISTING_BASELINE_SENTINEL',baseline=new Map([[baselineKey,3]]);
let consumed=0,otherKindsPreserved=0;
for(const file of approval.files){
 const source=read(file.path).toString().replaceAll('\r\n','\n');
 const actual=specialConstructViolations(source,file.path),parts=executableParts(source,file.path);
 const accepted=reviewedHealthAggregateHazards(source,file.path,baseline,actual,parts);
 assert.equal(accepted.get(baselineKey),3);assert.equal(baseline.size,1);
 assert.equal(accepted.size,file.points.length+1);consumed+=accepted.size-1;
 for(const key of accepted.keys())if(key!==baselineKey)assert.equal(JSON.parse(key)[1],'AGGREGATE_TYPE_UNCLASSIFIED');
 for(const row of actual)if(row.hazard!=='AGGREGATE_TYPE_UNCLASSIFIED'){assert(!accepted.has(row.key));otherKindsPreserved++;}
}
assert.equal(consumed,74);
let fullColumns=0,ddlColumns=0;
for(const [ref,row] of Object.entries(evidence.schema.columns)){
 const [schema,table,name]=ref.split('.');
 assert.equal(hash(canon(row.column)),row.columnObjectSha256);
 if(row.column.proof==='native migration DDL'){
  const ddl=evidence.schema.additiveNativeDdl[ref],source=read(ddl.path).toString().replaceAll('\r\n','\n');
  assert.equal(hash(source),ddl.sha256);
  const tableStatement=scanManagedSql(source).statements.map(x=>source.slice(x.start,x.end))
   .filter(x=>new RegExp(`^create\\s+table\\s+${schema}\\.${table}\\s*\\(`,'i').test(x));
  assert.equal(tableStatement.length,1);assert(tableStatement[0].includes(ddl.declaration));ddlColumns++;
 }else{
  const actual=contract.relations.find(x=>x.schema===schema&&x.name===table)?.columns.find(x=>x.name===name);
  assert.equal(canon(actual),canon(row.column));fullColumns++;
 }
}
assert.equal(fullColumns,77);assert.equal(ddlColumns,4);
const aggregates=evidence.rows.flatMap(x=>x.aggregates);
assert.equal(aggregates.length,356);assert.equal(new Set(aggregates.map(x=>x.expressionIndex)).size,146);
for(const x of aggregates){
 assert(!['uuid','jsonb','bytea','record'].includes(x.inputType));
 if(x.aggregate==='SUM')assert(['numeric','integer','bigint'].includes(x.inputType));
 if(x.aggregate==='SUM'&&x.inputType==='bigint')assert.equal(x.resultType,'numeric');
}
const route=aggregates.find(x=>x.expressionIndex===119);assert.equal(route.inputType,'text');
assert.equal(route.role,'ROUTE_IDENTITY_REPRESENTATIVE_REQUIRES_REVIEW');
assert.match(approval.routeReview.qualification,/not a manager recipient or complete policy-route seal/);
const gate=read('scripts/bpay-next-source-gates.mjs').toString();
assert.equal(gate.split('reviewedHealthAggregateHazards(').length-1,1);
assert(gate.includes('reviewedHealthAggregateHazards(source,file,reviewedHealthUuidHazards'));
const tests=spawnSync(process.execPath,['--test','tests/healthroster-aggregate-type-review.test.mjs'],{cwd:root,encoding:'utf8',windowsHide:true});
assert.equal(tests.status,0,tests.stdout+tests.stderr);assert.match(tests.stdout,/tests 9/);assert.match(tests.stdout,/pass 9/);assert.match(tests.stdout,/fail 0/);
const head=spawnSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8',windowsHide:true});assert.equal(head.status,0);
for(const pin of pins)assert.equal(hash(read(pin.file)),pin.sha256,'reviewed input moved during this peer check');
const result={version:'HEALTH_F23_AGGREGATE_CLASSIFIER_PEER_REVIEW_V1',status:'NO_BLOCKING_SOURCE_ISSUE_IN_EXACT_SCALAR_TYPE_CLASSIFIER_NATIVE_NOT_RUN',rootHead:head.stdout.trim(),pins,
 counts:{files:approval.files.length,exactPoints:consumed,aggregateOccurrences:aggregates.length,distinctInputs:new Set(aggregates.map(x=>x.expressionIndex)).size,fullNativeColumns:fullColumns,exactNativeDdlColumns:ddlColumns,otherHazardOccurrencesUnacceptedByAggregateOnly:otherKindsPreserved,focusedTestsPassed:9},
 findings:['The only new baseline entries have exact AGGREGATE_TYPE_UNCLASSIFIED keys. Whole file/body/signature/statement/key/count pins prevent alias/path/body/cast borrowing.',
 'Complete native column objects and four exact declarations inside their actual complete CREATE TABLE statements substantiate the fixed evidence. No generic alias or scalar-type inference was added to the gate.',
 'Explicit UUID-to-text aggregates are scalar TEXT evidence only; UUID/cardinality hazards remain independent. Native singleton/row ownership, economic authority and post-draft invariants are not waived.',
 'Expression119 remains a lossy route-key representative. Scalar TEXT classification is correct; no singleton recipient, policy-route seal or complete manager-routing approval follows.',
 'The existing module cache assumes one immutable source tree for a gate process. Run final gates against the exact committed/archive source; do not treat an already-running cached process as fresh evidence after editing input artifacts.'],
 limitations:['Peer review covers finite classifier boundaries and the pinned scalar schema/role restrictions, not final Part B and not an independent native/runtime qualification of all 74 producers.',
 'The source-review role labels/bindingProof text are hash-bound manual evidence, not a generic SQL type checker or runtime cardinality proof.',
 'Approval does not consume unrelated dynamic SQL, UUID cardinality, rowtype or other hazard kinds; full F23 and all native gates remain separate.'],native:'NOT_RUN',databaseCalls:0,rootEdits:0,deployments:0,secretsPrinted:false};
fs.writeFileSync(path.join(dir,'AGGREGATE_CLASSIFIER_PEER_REVIEW.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({status:result.status,counts:result.counts,rootHead:result.rootHead,native:'NOT_RUN'}));
