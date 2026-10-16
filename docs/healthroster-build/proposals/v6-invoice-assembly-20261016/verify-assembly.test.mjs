import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const root=process.env.HEALTHROSTER_READ_ONLY_ROOT??path.resolve(repo,'../healthroster-install2-14102026');
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const {routineDefinitions}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-final-definers.mjs')));
const sha=value=>createHash('sha256').update(value).digest('hex');
const m=JSON.parse(fs.readFileSync(path.join(here,'ASSEMBLY_MANIFEST.json')));
const selectionBytes=fs.readFileSync(path.join(repo,m.selection.path)),selection=JSON.parse(selectionBytes);
const source=fs.readFileSync(path.join(here,m.output.path),'utf8');
const statements=scanManagedSql(source).statements.map(row=>source.slice(row.start,row.end).trim());
const functionRows=statements.flatMap(sql=>routineDefinitions(sql,m.output.path).map(signature=>({signature,sha256:sha(sql)})));
const effects=statements.filter(sql=>!routineDefinitions(sql,m.output.path).length).map(sha);
const sorted=rows=>[...rows].sort();
test('exact immutable selection and complete 94-function census survive joining',()=>{
 assert.equal(sha(selectionBytes),'42fe73e938a141588fa60d629353dc2706069a7491cfb31570ea67626fbbc7a4');
 assert.equal(sha(fs.readFileSync(path.join(repo,m.selection.manifestPath))),'d0a6cf0fc76c9c96476c5364f07479b695389d4f71b514a27553e6c00c5df58c');
 assert.equal(functionRows.length,94);assert.equal(new Set(functionRows.map(row=>row.signature)).size,94);
 assert.deepEqual(sorted(functionRows.map(row=>`${row.signature}\t${row.sha256}`)),
  sorted(selection.selected.flatMap(file=>file.functions.map(row=>`${row.signature}\t${row.statementSha256}`))));
 assert.equal(sha(source),m.output.sha256);assert.equal(Buffer.byteLength(source),m.output.bytes);assert(!source.includes('\r'));
});
test('every selected DDL/ACL/effect is retained or exactly suppressed in Root01',()=>{
 const schema=selection.selected.find(row=>row.path.endsWith('/001_billing_schema.sqlinc'));
 const retained=selection.selected.filter(row=>row!==schema).flatMap(row=>row.nonFunctionEffects.map(effect=>effect.statementSha256));
 const native=selection.retainedNativeParentNonFunctionDependencies[0];
 assert.deepEqual(sorted(effects),sorted([...retained,...native.nonFunctionEffects.map(row=>row.statementSha256)]));
 assert.equal(effects.length,165);assert.equal(statements.length,259);
 const savedRoot=fs.readFileSync(path.join(root,m.schemaSuppression.existingRootPath),'utf8').replaceAll('\r\n','\n');
 assert.equal(sha(savedRoot),m.schemaSuppression.existingRootCanonicalLfSha256);
 const schemaSource=fs.readFileSync(path.join(repo,schema.path),'utf8').replaceAll('\r\n','\n');
 assert(savedRoot.includes(schemaSource.trim()));assert.equal(m.schemaSuppression.statements.length,16);
 assert.equal(statements.filter(sql=>/^create table private\.timesheet_contract_move_/i.test(sql)).length,0);
});
test('VAT retains native no-CASCADE DROP, unchanged complete CREATE, service-only ACL in order',()=>{
 const at=statements.findIndex(sql=>/^drop function if exists private\._invoice_generation_vat_policy_batch\(jsonb\);$/i.test(sql));
 assert(at>=0);assert(!/\bcascade\b/i.test(statements[at]));
 assert.equal(sha(statements[at+1]),m.vat.selectedCompleteCreateStatementSha256);
 assert.match(statements[at+2],/^revoke all on function private\._invoice_generation_vat_policy_batch\(jsonb\)\s+from public,anon,authenticated;$/i);
 assert.match(statements[at+3],/^grant execute on function private\._invoice_generation_vat_policy_batch\(jsonb\)\s+to service_role;$/i);
 assert.equal(functionRows.filter(row=>row.signature===m.vat.signature).length,1);
 assert.match(m.vat.dependencyEvidence.nativeDependencyDropProof,/NOT_RUN/);
});
test('final099 follows all selected functions and Physical workflow is not overwritten',()=>{
 const aclFile=selection.selected.find(row=>row.path.endsWith('/099_new_private_acl.sqlinc'));
 const firstAcl=statements.findIndex(sql=>sha(sql)===aclFile.nonFunctionEffects[0].statementSha256);
 assert(firstAcl>0);assert.equal(statements.slice(firstAcl).filter(sql=>routineDefinitions(sql,m.output.path).length).length,0);
 assert(!functionRows.some(row=>row.signature.startsWith('public.candidate_workflow_transition_atomic_v1(')));
 const kernel=selection.selected.find(row=>row.path.endsWith('/001_family_entry.sqlinc'));
 assert.equal(kernel.functions.length,2);
 for(const fn of kernel.functions)assert.equal(functionRows.filter(row=>row.signature===fn.signature).length,1);
 assert(!/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i.test(source));
});
test('CREATE order needs no new pending preload and preserves f566 finalCandidate ordering',()=>{
 const review=JSON.parse(fs.readFileSync(path.join(here,'CREATE_ORDER_REVIEW.json')));
 assert.equal(review.assemblySha256,sha(source));assert.equal(review.sqlDefinitionCount,10);
 assert.deepEqual(review.requiredCandidateFoundationPreload,[]);
 assert.equal(review.pendingRuntimeCallers.length,2);assert(review.pendingRuntimeCallers.every(row=>row.language==='PLPGSQL'));
 const calls=review.sqlDefinitions.flatMap(row=>row.candidateCalls);
 assert.deepEqual(calls.map(row=>row.signature),['private.timesheet_contract_move_evidence_source_v1(uuid)']);
 assert.equal(review.existingCreateTimeCandidateDependency.returns,'jsonb');
 assert(review.order[2].startsWith('Complete finalCandidate55'));
});
