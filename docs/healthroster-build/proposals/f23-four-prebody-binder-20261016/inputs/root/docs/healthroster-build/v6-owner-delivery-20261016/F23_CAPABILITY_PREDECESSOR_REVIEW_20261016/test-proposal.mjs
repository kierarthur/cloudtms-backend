// Data-free contract/refusal model only. Native SQL cases remain required.
import fs from'node:fs';import path from'node:path';import assert from'node:assert/strict';import{createHash}from'node:crypto';import{fileURLToPath,pathToFileURL}from'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(dir,'../../../..'),root=path.resolve(own,'../healthroster-install2-14102026'),hash=x=>createHash('sha256').update(x).digest('hex');
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const p=JSON.parse(fs.readFileSync(path.join(dir,'PROPOSAL.json'))),expected=p.exactExpectedSelectedChecks,tests=[];
const canonical=x=>{if(Array.isArray(x))return x.map(canonical);if(x&&typeof x==='object')return Object.fromEntries(Object.entries(x).sort(([a],[b])=>a.localeCompare(b)).map(([k,v])=>[k,canonical(v)]));return x;};
const exact=x=>JSON.stringify(canonical(x.slice().sort((a,b)=>a.constraint_name.localeCompare(b.constraint_name))));
function accepts(rows,total=10,structure=true){return structure&&total===10&&rows.length===2&&exact(rows)===exact(expected);}
function run(name,fn){fn();tests.push({name,status:'PASS_SOURCE_MODEL_ONLY'});}
function mutation(name,fn){run(name,()=>{const rows=structuredClone(expected);fn(rows);assert(!accepts(rows));});}
run('actual captured two-set passes exact source model',()=>assert(accepts(expected)));
run('row order only does not alter JSONB ordered-set contract',()=>assert(accepts(expected.toReversed())));
mutation('renamed original CHECK refused',x=>x[0].constraint_name+='_unreviewed');
mutation('same CHECK NOT VALID refused',x=>x[0].is_validated=false);
mutation('extra matching CHECK refused',x=>x.push({...x[0],constraint_name:'unreviewed_extra_matching'}));
mutation('missing original matching CHECK refused',x=>x.pop());
mutation('same expected name with changed expression refused',x=>x[0].evidence_expression_pretty_true+=' AND false');
mutation('default-false selector definition drift refused',x=>x[0].selector_definition_default_false+=' /* drift */');
mutation('pretty-true evidence definition drift refused',x=>x[0].evidence_definition_pretty_true+=' /* drift */');
mutation('native key order drift refused',x=>x.find(r=>r.selector_role==='VERSION').key_columns_in_native_order.reverse());
mutation('inherited CHECK refused',x=>{x[0].is_local=false;x[0].inherited_count=1;});
mutation('parent-bound CHECK refused',x=>x[0].has_no_parent_constraint=false);
mutation('deferrable CHECK metadata refused',x=>x[0].is_deferrable=true);
mutation('already extended expression refused',x=>x[0].selector_definition_default_false=x[0].selector_definition_default_false.replace('SIGNED_CHECK_ONLY','PROTECTED_SOURCE'));
run('extra unmatched CHECK count refused conservatively',()=>assert(!accepts(expected,11)));
run('nonordinary, foreign owner or inherited relation refused',()=>assert(!accepts(expected,10,false)));
run('complete proposed program has LOCK then one DO and no transaction/effect statements',()=>{
 const program=fs.readFileSync(path.join(dir,path.basename(p.program.proposedPath)),'utf8');assert.equal(hash(program),p.program.sha256);
 const s=scanManagedSql(program).statements;assert.equal(s.length,2);assert.equal(s[0].tokens[0],'LOCK');assert.equal(s[1].tokens[0],'DO');assert(s[0].tokens.includes('ACCESS')&&s[0].tokens.includes('EXCLUSIVE'));
 const body=program.slice(s[1].start,s[1].end);assert(!/\bexecute\s+format|\bexecute\s+'|\binsert\s+into|\bupdate\s+public\.|\bdelete\s+from|\balter\s+table|\bcreate\s+table/i.test(body));
 const embedded=JSON.parse(body.split('$expected0400$')[1]);assert.equal(exact(embedded),exact(expected));
 assert(body.includes("pg_catalog.pg_get_constraintdef(c.oid) definition"));assert(body.includes('pg_catalog.pg_get_constraintdef(c.oid,true)'));assert(body.includes('pg_catalog.pg_get_expr(c.conbin,c.conrelid,true)'));
});
run('immutable source and historical false approval flags preserved',()=>{
 for(const i of p.inputs)assert.equal(hash(fs.readFileSync(path.join(dir,i.preserved))),i.sha256);
 for(const i of p.prefixInputs)assert.equal(hash(fs.readFileSync(path.join(dir,i.preserved))),i.sourceSha256);
 const c=JSON.parse(fs.readFileSync(path.join(dir,'inputs/NATIVE_PREDECESSOR_CENSUS.json')));assert.equal(c.expression_approved,false);assert.equal(c.native_companion_approved,false);assert.equal(p.successor.nativeCompanionApproved,false);assert.equal(p.successor.nativeCompanion,'NOT_RUN');
});
const out={version:'HEALTHROSTER_CAPABILITY0400_SOURCE_PROPOSAL_TEST_RESULTS_V1',status:'PASS_SOURCE_MODEL_AND_GRAMMAR_ONLY',passed:tests.length,tests,proposalSha256:hash(fs.readFileSync(path.join(dir,'PROPOSAL.json'))),databaseCalls:0,nativeCompanion:'NOT_RUN',limits:'JS source-model refusals do not replace actual native companion/consumer/negative/two-session concurrency/restoration tests or integrated compiler/harness packet proof.'};
fs.writeFileSync(path.join(dir,'TEST_RESULTS.json'),JSON.stringify(out,null,2)+'\n');console.log(JSON.stringify({passed:tests.length,nativeCompanion:'NOT_RUN'}));
