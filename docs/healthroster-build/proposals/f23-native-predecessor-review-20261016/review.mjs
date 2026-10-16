// Data-free source review only. No database client, network, or executable SQL call.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { sqlDateKey } from '../../../../scripts/cloudtms-db-release-lib.mjs';
import { scanManagedSql } from '../../../../scripts/cloudtms-managed-release-sql.mjs';

const dir=path.dirname(fileURLToPath(import.meta.url)), root=path.resolve(dir,'../../../..');
const sourceCommit='3b2f6f385052b51df4004be6b68766ad62c6c17e';
const physicalReviewParent='0716b661530c6147e778231db8663a8abefb56f3';
const moneyRoot=path.resolve(root,'../healthroster-v2-money-build-13102026');
const observedRoot=path.resolve(root,'../healthroster-install2-14102026');
const sha=x=>createHash('sha256').update(x).digest('hex');
const lf=x=>x.toString().replaceAll('\r\n','\n');
function git(args,input){const r=spawnSync('git',args,{cwd:root,input,maxBuffer:120*1024*1024,windowsHide:true});assert.equal(r.status,0,r.stderr?.toString());return r.stdout;}
const blobs=new Map();
const read=file=>{if(!blobs.has(file))blobs.set(file,git(['show',`${sourceCommit}:${file}`]));return blobs.get(file);};
function cache(files){
 const output=git(['cat-file','--batch'],files.map(file=>`${sourceCommit}:${file}\n`).join(''));let at=0;
 for(const file of files){const end=output.indexOf(10,at),head=output.subarray(at,end).toString().split(' ');assert.equal(head[1],'blob');const length=Number(head[2]);at=end+1;blobs.set(file,output.subarray(at,at+length));at+=length+1;}
 assert.equal(at,output.length);
}
const pin=file=>({file,sha256:sha(read(file))});
const moneyInput='\.codex-tmp/f23-uuid-review/dynamic/';
const moneyJson=fs.readFileSync(path.join(moneyRoot,moneyInput,'REVIEW.json'));
const moneyMd=fs.readFileSync(path.join(moneyRoot,moneyInput,'REVIEW.md'));
assert.equal(sha(moneyJson),'ec6085b5746ef5542c38c1cb4624b04e86040ba66aef3a82e45f82b0d2de631a');
assert.equal(sha(moneyMd),'3cd43dfd2e3fd95d84c233f1849af3f23aedf51f329f6075a566408a4ace2822');
const money=JSON.parse(moneyJson);
assert.deepEqual([money.counts.diagnostics,money.counts.file_signature_groups,money.counts.enclosing_producers],[95,35,41]);
const origin='supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql';
const migration='supabase/migrations/13102026_1430_weekly_source_invoice_money_origins.sql';
const capabilityOrigin='supabase/migrations/13102026_1133_weekly_source_rule_sets_schema.sql';
const capabilityMigration='supabase/migrations/14102026_0400_healthroster_protected_source_capability.sql';
const contractFile='supabase/release/current-contract.json',contract=JSON.parse(read(contractFile));
const targetNames=['weekly_source_charge_checks','weekly_source_billing_movements','weekly_source_invoice_presentation_lines','weekly_source_group_rule_assignments'];
const allMigrations=git(['ls-tree','-r','--name-only',sourceCommit,'supabase/migrations']).toString().trim().split('\n').filter(x=>x.toLowerCase().endsWith('.sql')).sort((a,b)=>sqlDateKey(a).localeCompare(sqlDateKey(b))||a.localeCompare(b));
cache(allMigrations);
const lock=JSON.parse(read('supabase/release/migration-lock.json'));
assert.equal(lock.migrations.length,407);
assert.equal(allMigrations.length,407);
for(const row of lock.migrations){assert.equal(sha(read(row.path)),row.sha256);assert.equal(sha(Buffer.from(lf(fs.readFileSync(path.join(observedRoot,row.path))))),row.sha256);}
const position=x=>allMigrations.indexOf(x);
assert(position(origin)<position(migration));assert(position(capabilityOrigin)<position(capabilityMigration));
const sourcePins=[origin,migration,capabilityOrigin,capabilityMigration,contractFile,'supabase/release/export_contract.sql','supabase/release/migration-lock.json','scripts/cloudtms-db-release-lib.mjs','scripts/cloudtms-managed-release.mjs','scripts/cloudtms-managed-release-sql.mjs','scripts/healthroster-contact-claim-boundary.mjs','scripts/healthroster-install1-full-rehearsal.mjs'].map(pin);
for(const p of sourcePins)assert.equal(sha(Buffer.from(lf(fs.readFileSync(path.join(observedRoot,p.file))))),p.sha256,p.file);
const groups=[
 {relation:'weekly_source_charge_checks',expectedCount:11,needles:['comparison_profile_version','comparison_result','row_sign_kind'],statementSha256:'de65c105cb2c826faf50b02c53b2b5a4299219a8e6e605a7d4309bfe82d81d96'},
 {relation:'weekly_source_billing_movements',expectedCount:12,needles:['transition_id','nhsp_upload_row_id','expense_authority_generation_id','source_profile_kind','source_line_kind','movement_role','price_check_result'],statementSha256:'0a76cc05e5b41b81096a60989ee5b7769a971fe1db767acbcf0222a3c2ef9575'}
];
for(const group of groups){
 const relation=contract.relations.find(x=>x.schema==='public'&&x.name===group.relation);assert(relation);
 const matches=x=>x.type==='c'&&group.needles.some(y=>x.definition.includes(y));
 group.constraints=relation.constraints.filter(matches);assert.equal(group.constraints.length,group.expectedCount);
 assert.deepEqual(group.constraints.map(x=>({name:x.name,definition:x.definition})),money.native_predecessor_evidence.find(x=>x.relation===`public.${group.relation}`).checks);
 group.independentRetainedChecks=relation.constraints.filter(x=>x.type==='c'&&!matches(x));
 assert(group.constraints.every(x=>x.validated&&!x.deferrable&&!x.deferred));
}
const priceRelation=contract.relations.find(x=>x.schema==='public'&&x.name==='weekly_source_invoice_presentation_lines');
const price=priceRelation.constraints.find(x=>x.name==='weekly_source_invoice_presentation_lin_price_check_result_check');assert(price);
assert.equal(price.definition,"CHECK (price_check_result = ANY (ARRAY['NOT_APPLICABLE'::text, 'EXACT'::text, 'SOURCE_ROUNDING_EQUIVALENT'::text, 'ACCEPTED_DISPARITY'::text, 'ACCEPTED_ZERO'::text]))");
assert(!contract.relations.some(x=>x.name==='weekly_source_group_rule_assignments'));
const relevant=[];
for(const file of allMigrations){
 if(position(file)<position(origin)||position(file)>position(capabilityMigration))continue;
 const source=read(file).toString();if(!targetNames.some(x=>source.toLowerCase().includes(x)))continue;
 const statements=scanManagedSql(source).statements.map(x=>source.slice(x.start,x.end));
 const ddl=statements.filter(x=>/^(?:create|alter|drop)\s+(?:unique\s+)?(?:table|index)|^do\b/i.test(x)&&targetNames.some(y=>x.toLowerCase().includes(y)));
 relevant.push({file,sha256:sha(read(file)),order:position(file),ddl:ddl.map(x=>({statementSha256:sha(x),head:x.split('\n')[0].slice(0,180)}))});
}
assert.deepEqual(relevant.map(x=>x.file),[origin,'supabase/migrations/04102026_0470_banking_pay_next_source_current_selector.sql','supabase/migrations/04102026_0500_banking_pay_next_source_current_hooks.sql',capabilityOrigin,migration,'supabase/migrations/13102026_1680_weekly_source_invoice_window_receipt_schema.sql',capabilityMigration]);
const repeated=git(['grep','-l','-i','-E',targetNames.join('|'),sourceCommit,'--','supabase/repeatable']).toString().trim().split('\n').filter(Boolean).map(x=>x.slice(sourceCommit.length+1));
cache(repeated);
const repeatableDdl=[];
for(const file of repeated){
 const source=read(file).toString();
 const relevantStatements=scanManagedSql(source).statements.map(x=>source.slice(x.start,x.end)).filter(x=>/^(?:create|alter|drop)\s+table\b/i.test(x)&&targetNames.some(y=>x.toLowerCase().includes(y)));
 if(relevantStatements.length)repeatableDdl.push({file,sha256:sha(read(file)),statements:relevantStatements.map(sha)});
}
assert.deepEqual(repeatableDdl,[]);
const quoted=x=>`'${x.replaceAll("'","''")}'`;
function assertion(group){
 const vals=group.constraints.map(x=>`      (${quoted(x.name)}, ${quoted(x.definition)}, ${x.validated}, ${x.deferrable}, ${x.deferred})`).join(',\n');
 const selector=group.needles.map(x=>`pg_catalog.pg_get_constraintdef(c.oid) like ${quoted('%'+x+'%')}`).join('\n        or ');
 return `  if exists (\n    with expected(name, definition, is_validated, is_deferrable, is_deferred) as (values\n${vals}\n    ), actual as (\n      select c.conname::text name, pg_catalog.pg_get_constraintdef(c.oid,true) definition,\n        c.convalidated is_validated,c.condeferrable is_deferrable,c.condeferred is_deferred\n      from pg_catalog.pg_constraint c\n      where c.conrelid='public.${group.relation}'::pg_catalog.regclass and c.contype='c'\n        and (${selector})\n    )\n    select 1 from expected e full join actual a using (name)\n    where (e.definition,e.is_validated,e.is_deferrable,e.is_deferred)\n      is distinct from (a.definition,a.is_validated,a.is_deferrable,a.is_deferred)\n  ) then raise exception 'HEALTH_F23_${group.relation.toUpperCase()}_PREDECESSOR_CHANGED' using errcode='55000'; end if;`;
}
const prebody=`-- REVIEW PROPOSAL ONLY. Not installed, not executed, native qualification NOT_RUN.\n-- Attach only before the exact immutable 13102026_1430 body in one existing atomic unit.\n-- Keep the existing writer/identity admission, ledger and outer transaction.\nlock table public.weekly_source_charge_checks, public.weekly_source_billing_movements,\n  public.weekly_source_invoice_presentation_lines in access exclusive mode;\ndo $health_f23_money_predecessor$\ndeclare previous_path text;\nbegin\n  previous_path:=pg_catalog.current_setting('search_path');\n  perform pg_catalog.set_config('search_path','pg_catalog, public',true);\n${groups.map(assertion).join('\n')}\n  if (select count(*) from pg_catalog.pg_constraint c\n    where c.conrelid='public.weekly_source_invoice_presentation_lines'::pg_catalog.regclass\n      and c.contype='c'\n      and c.conkey=array[(select a.attnum from pg_catalog.pg_attribute a\n        where a.attrelid=c.conrelid and a.attname='price_check_result' and not a.attisdropped)]::smallint[]\n      and pg_catalog.pg_get_constraintdef(c.oid) like '%NOT_APPLICABLE%')<>1\n    or not exists (select 1 from pg_catalog.pg_constraint c\n      where c.conrelid='public.weekly_source_invoice_presentation_lines'::pg_catalog.regclass\n        and c.contype='c' and c.conname=${quoted(price.name)}\n        and c.conkey=array[(select a.attnum from pg_catalog.pg_attribute a\n          where a.attrelid=c.conrelid and a.attname='price_check_result' and not a.attisdropped)]::smallint[]\n        and pg_catalog.pg_get_constraintdef(c.oid,true)=${quoted(price.definition)}\n        and c.convalidated and not c.condeferrable and not c.condeferred) then\n    raise exception 'HEALTH_F23_PRICE_ENUM_PREDECESSOR_CHANGED' using errcode='55000';\n  end if;\n  perform pg_catalog.set_config('search_path',previous_path,true);\nend;\n$health_f23_money_predecessor$;\n`;
assert.deepEqual(scanManagedSql(prebody).statements.map(x=>x.tokens[0]),['LOCK','DO']);
fs.writeFileSync(path.join(dir,'16102026_0943_money_prebody_assertion_proposal.sqlinc'),prebody);
// Finite equality model: validates intended fail-closed set algebra, not SQL/native semantics.
function equalSet(expected,actual){return actual.length===expected.length&&expected.every(e=>actual.filter(a=>a.name===e.name&&a.definition===e.definition&&a.validated===e.validated&&a.deferrable===e.deferrable&&a.deferred===e.deferred).length===1);}
const model=[];
for(const g of groups){
 const expected=g.constraints;
 const cases=[['exact',structuredClone(expected),true],['missing',expected.slice(1),false],['extra',[...expected,{...expected[0],name:'unexpected_matching_check'}],false],['expression-change',expected.map((x,i)=>i?x:{...x,definition:x.definition+' OR true'}),false],['renamed',expected.map((x,i)=>i?x:{...x,name:'renamed_check'}),false],['not-validated',expected.map((x,i)=>i?x:{...x,validated:false}),false],['swapped-name-expressions',expected.map((x,i)=>i===0?{...x,definition:expected[1].definition}:i===1?{...x,definition:expected[0].definition}:x),false]];
 for(const [name,actual,want] of cases){assert.equal(equalSet(expected,actual),want);model.push({relation:g.relation,case:name,result:want?'ADMIT_EXACT_PREDECESSOR':'REFUSE_CHANGED_PREDECESSOR'});}
}
const output={version:'HEALTH_F23_FINITE_NATIVE_PREDECESSOR_REVIEW_V1',status:'SOURCE_DERIVED_PREDECESSOR_SET_CLOSED_IMPLEMENTATION_AND_NATIVE_NOT_RUN',sourceCommit,physicalReviewParent,observedRoot,
 moneyInput:{jsonSha256:sha(moneyJson),mdSha256:sha(moneyMd),counts:money.counts},sourcePins,lockedMigrationsVerified:407,
 sourceOrder:{migrationCount:allMigrations.length,orderedMigrationPathsSha256:sha(JSON.stringify(allMigrations)),originIndex:position(origin),moneyIndex:position(migration),capabilityOriginIndex:position(capabilityOrigin),capabilityIndex:position(capabilityMigration),relevantMigrations:relevant,repeatableFilesMentioningTargets:repeated.length,repeatableMentionSourcePins:repeated.map(pin),topLevelExplicitTargetTableDdlInRepeatables:repeatableDdl,migrationPhasePrecedesRepeatablePhase:true},
 groups,priceEnum:{expectedCount:1,constraint:price,definitionEvidence:'Pinned current-contract exporter uses pg_get_constraintdef(oid,true), unlike immutable selector default-false form. Compare true-to-true under pg_catalog, public; preserve original selector for candidate census.',statementSha256:'a5d195541446398e49ef0bf39c0c14fab07384bbb817f30fdd2e39e0b9a11462'},
 capability:{expectedMatchingCount:2,origin:capabilityOrigin,migration:capabilityMigration,statementSha256:'ab5bdf121ab1e57c4ae894025cd8e7b176fb5f43d1ed6b7c6110103b44e0331a',exactSourceExpressions:["feature_purpose in ('RULE_SET','SIGNED_CHECK_ONLY')","feature_purpose is null or ((rule_set_version_id is null)=(feature_purpose='SIGNED_CHECK_ONLY'))"],nativeExpressionAndNameCensus:'NOT_AVAILABLE_IN_BASELINE_CURRENT_CONTRACT; DO_NOT_GUESS_POSTGRES_DEPARSE_OR_AUTONAME',recommendation:'Before exact 14102026_0400 body, obtain a data-free native PG17 receipt after the full pending prefix; lock the exact assignment relation and compare the complete two matching CHECK names/expressions/metadata to that reviewed receipt. No inferred deparse/autoname, temporary-table constructor assumption, substring waiver or approval is supplied here.'},
 proposal:{file:'16102026_0943_money_prebody_assertion_proposal.sqlinc',sha256:sha(prebody),statementCount:2,placement:'BEGIN / writer assertion / LOCK+prebody assertion / immutable body / writer assertion / immutable migration ledger / COMMIT; rehearsal identical prebody before same body in rollback transaction',integration:'Root must add exact immutable file, closure, producer and statement bindings to compiler/harness; proposal is not a generic runtime exemption or new permanent migration.',native:'NOT_RUN',lockPurpose:'Closes check/drop race; same transaction retains ACCESS EXCLUSIVE until immutable body and ledger commit. No early commit, unrelated table lock, external call, or persisted helper.'},
 model:{status:'PASS_SOURCE_MODEL_ONLY',checks:model},nativeStatus:'NOT_RUN',databaseCalls:0,deployments:0,secretsPrinted:false,policyX:'No finance, payment, provider, settlement, banking or post-draft economic behavior changed. Constraint/predecessor review only.'};
const grammarPath=path.join(dir,'GRAMMAR.json');
if(fs.existsSync(grammarPath)){output.offlineGrammar=JSON.parse(fs.readFileSync(grammarPath,'utf8'));assert.equal(output.offlineGrammar.sourceSha256,sha(prebody));assert.equal(output.offlineGrammar.status,'PASS_OFFLINE_GRAMMAR_ONLY');}
fs.writeFileSync(path.join(dir,'REVIEW.json'),JSON.stringify(output,null,2)+'\n');
console.log(JSON.stringify({status:output.status,lockedMigrationsVerified:407,expectedCounts:groups.map(x=>x.expectedCount),priceExpectedCount:1,capabilityExpectedCount:2,sourceModelChecks:model.length,proposalSha256:sha(prebody),native:'NOT_RUN'}));
