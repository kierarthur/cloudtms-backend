// Read-only committed native evidence; writes a separate source proposal only.
import fs from'node:fs';import path from'node:path';import assert from'node:assert/strict';import{createHash}from'node:crypto';import{execFileSync}from'node:child_process';import{fileURLToPath,pathToFileURL}from'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(dir,'../../../..'),root=path.resolve(own,'../healthroster-install2-14102026'),hash=x=>createHash('sha256').update(x).digest('hex');
const commit=execFileSync('git',['rev-parse','32ee8711'],{cwd:root}).toString().trim(),get=file=>execFileSync('git',['show',`${commit}:${file}`],{cwd:root,maxBuffer:25e6});
const packagePath='docs/healthroster-build/v6-owner-delivery-20261016/native-capability-prefix-bcfd75a0',inputs=[];
fs.mkdirSync(path.join(dir,'inputs'),{recursive:true});
function preserve(file,name){const bytes=get(file);fs.writeFileSync(path.join(dir,'inputs',name),bytes);inputs.push({sourceCommit:commit,path:file,preserved:'inputs/'+name,sha256:hash(bytes)});return bytes;}
const captureBytes=preserve(packagePath+'/NATIVE_PREDECESSOR_CENSUS.json','NATIVE_PREDECESSOR_CENSUS.json');assert.equal(hash(captureBytes),'a482d9cb8fb9814ca2a33967bebc5e18138fea61d4c8611012c5a665d772bea6');
const capture=JSON.parse(captureBytes),summary=JSON.parse(preserve(packagePath+'/SUMMARY.json','SUMMARY.json'));
const native=JSON.parse(preserve(packagePath+'/NATIVE_R2.json','NATIVE_R2.json')),receipt=JSON.parse(preserve(packagePath+'/INPUT_RECEIPT_R1.json','INPUT_RECEIPT_R1.json'));
preserve(packagePath+'/PLAN.json','PLAN.json');preserve(packagePath+'/RESTORATION.json','RESTORATION.json');
for(const item of inputs.filter(x=>!x.path.endsWith('SUMMARY.json'))){const entry=summary.artifactHashes.find(a=>a.file===path.basename(item.path));assert(entry);assert.equal(item.sha256,entry.sha256);}
const constructor=preserve(capture.constructor_path,'CONSTRUCTOR_1133.sql'),consumer=preserve(capture.consumer_path,'IMMUTABLE_CONSUMER_0400.sql');
const prefixInputs=[];for(const [i,u]of receipt.sourcePlan.units.entries()){
 const source=get(u.file);assert.equal(hash(source),u.sourceSqlHash);const name='PREFIX_'+String(i+1).padStart(2,'0')+'_'+path.basename(u.file);fs.writeFileSync(path.join(dir,'inputs',name),source);
 prefixInputs.push({path:u.file,sourceSha256:hash(source),compiledSha256:u.sqlHash,preserved:'inputs/'+name});
}
assert.equal(hash(constructor),capture.constructor_sha256);assert.equal(hash(consumer),capture.consumer_sha256);
assert.equal(capture.expression_approved,false);assert.equal(capture.native_companion_approved,false);assert.equal(capture.all_check_count,10);assert.equal(capture.retained_check_count,8);assert.equal(capture.selected_checks.length,2);
assert.equal(summary.candidate,'bcfd75a051d5c88e524a5ddb552b269c71c407b1');assert.equal(summary.retry.accepted,true);assert.equal(summary.retry.statements,99);assert.equal(summary.restoration.pass,true);
assert.equal(native.accepted,true);assert.equal(native.database,'cloudtms_test_clone');assert.equal(native.rolledBack,true);assert.equal(native.rollbackVerification.connectedFresh,true);assert.equal(native.sqlSha256,summary.retry.sqlSha256);
assert.equal(summary.retry.canonicalPrefix.length,9);assert.equal(receipt.sourcePlan.pendingCount,9);assert.equal(receipt.sourcePlan.stopBefore,capture.consumer_path);
const expected=capture.selected_checks.slice().sort((a,b)=>a.constraint_name.localeCompare(b.constraint_name));
for(const c of expected){assert(c.is_local&&c.is_validated&&c.has_no_parent_constraint);assert.equal(c.inherited_count,0);assert(!c.is_deferred&&!c.is_deferrable&&!c.is_no_inherit);}
const program=`-- Exact0400 predecessor companion; no transaction wrapper or migration edit.
-- Capture32ee8711 / actual canonical pending prefixbcfd75a0 / PG17.11.
-- Same existing atomic transaction: LOCK + assertion BEFORE immutable0400 DO.
lock table public.weekly_source_group_rule_assignments in access exclusive mode;

do $health_capability_prebody$
declare v_actual jsonb; v_all_checks integer;
begin
  if not exists(select 1 from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where c.oid='public.weekly_source_group_rule_assignments'::regclass
      and n.nspname='public' and c.relname='weekly_source_group_rule_assignments'
      and c.relkind='r' and not c.relispartition
      and c.relowner=(select oid from pg_catalog.pg_roles where rolname=current_user)
      and not exists(select 1 from pg_catalog.pg_inherits i where i.inhrelid=c.oid or i.inhparent=c.oid)) then
    raise exception 'HEALTHROSTER_CAPABILITY_PREDECESSOR_RELATION_CHANGED' using errcode='55000';
  end if;
  select count(*)::integer into v_all_checks from pg_catalog.pg_constraint c
    where c.conrelid='public.weekly_source_group_rule_assignments'::regclass and c.contype='c';
  select coalesce(jsonb_agg(jsonb_build_object(
    'constraint_name',c.conname::text,
    'selector_definition_default_false',d.definition,
    'evidence_definition_pretty_true',pg_catalog.pg_get_constraintdef(c.oid,true),
    'evidence_expression_pretty_true',pg_catalog.pg_get_expr(c.conbin,c.conrelid,true),
    'selector_role',case when position('PROTECTED_SOURCE' in d.definition)>0 then 'ALREADY_EXTENDED'
      when position('rule_set_version_id' in d.definition)>0 then 'VERSION'
      when position('RULE_SET' in d.definition)>0 then 'PURPOSE' else 'UNEXPECTED' end,
    'key_columns_in_native_order',array(select a.attname::text
      from unnest(c.conkey) with ordinality k(attnum,ord)
      join pg_catalog.pg_attribute a on a.attrelid=c.conrelid and a.attnum=k.attnum
      order by k.ord),
    'key_columns_sorted',array(select a.attname::text
      from unnest(c.conkey) k(attnum)
      join pg_catalog.pg_attribute a on a.attrelid=c.conrelid and a.attnum=k.attnum
      order by a.attname),
    'is_local',c.conislocal,'is_validated',c.convalidated,
    'is_deferred',c.condeferred,'is_deferrable',c.condeferrable,
    'is_no_inherit',c.connoinherit,'inherited_count',c.coninhcount,
    'has_no_parent_constraint',c.conparentid=0
  ) order by c.conname),'[]'::jsonb) into v_actual
  from pg_catalog.pg_constraint c
  cross join lateral(select pg_catalog.pg_get_constraintdef(c.oid) definition)d
  where c.conrelid='public.weekly_source_group_rule_assignments'::regclass and c.contype='c'
    and position('feature_purpose' in d.definition)>0
    and position('SIGNED_CHECK_ONLY' in d.definition)>0;
  if v_all_checks<>10 or jsonb_array_length(v_actual)<>2
     or v_actual is distinct from $expected0400$${JSON.stringify(expected)}$expected0400$::jsonb then
    raise exception 'HEALTHROSTER_CAPABILITY_PREDECESSOR_CHECKS_CHANGED' using errcode='55000';
  end if;
end;
$health_capability_prebody$;
`;
const programName='16102026_1120_healthroster_capability_predecessor_assertion.sqlinc';fs.writeFileSync(path.join(dir,programName),program);
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const consumerSource=consumer.toString(),consumerBody=scanManagedSql(consumerSource).statements.filter(s=>s.tokens[0]==='DO').map(s=>consumerSource.slice(s.start,s.end));assert.equal(consumerBody.length,1);
const programStatementSha256=scanManagedSql(program).statements.map(s=>hash(program.slice(s.start,s.end)));
const dynamic=JSON.parse(fs.readFileSync(path.resolve(dir,'../f23-current-dynamic-c1ebb691-20261016/EXTRACTION.json'))).groups.find(g=>g.file===capture.consumer_path);assert.equal(dynamic.points.length,1);
const proof={version:'HEALTHROSTER_CAPABILITY0400_PREBODY_SOURCE_DESIGN_V1',sourceCommit:commit,sourceCandidate:summary.candidate,inputs,prefixInputs,program:{proposedPath:'supabase/release/'+programName,sha256:hash(program),placement:'BEFORE_IMMUTABLE_BODY_SAME_TRANSACTION',statements:2,statementSha256:programStatementSha256},
 consumer:{kind:'migration',path:capture.consumer_path,sourceSha256:capture.consumer_sha256,bodyStatementSha256:hash(consumerBody[0]),f23StatementSha256:dynamic.points[0].statementSha256,f23KeySha256:dynamic.points[0].keySha256,enclosingBodySha256:dynamic.points[0].bodySha256,closureFiles:[{file:capture.consumer_path,sha256:capture.consumer_sha256}],immutableBytesUnchanged:true},constructor:{path:capture.constructor_path,sha256:capture.constructor_sha256,rawSourceOnlyLogicalOwner:'postgres',managedOwnerMapping:'Only audited postgres -> CURRENT_USER; unchanged protected compiler gate'},
 exactExpectedSelectedChecks:expected,allCheckCount:10,retainedCheckCount:8,
 historicalCapture:{sha256:hash(captureBytes),expressionApproved:false,nativeCompanionApproved:false,preservedWithoutEdit:true},
 successor:{expressionSourceComparison:'PENDING_OFFLINE_AST_REVIEW',sourceDesignApproved:false,nativeCompanionApproved:false,nativeCompanion:'NOT_RUN'},
 requiredIntegration:['Exact kind/path/source body/closure pins; bind this program and all evidence hashes. No dynamic exemption yet.','Canonical actual managed prefix includes constructor1133 and unchanged0400 predecessor order; no fake/partial schema.','Within the existing BEGIN/writer/context/ledger envelope: LOCK/assertion, then unchanged0400 body, then existing writer guard/original migration ledger/COMMIT.','Same ordering in full rehearsal; exact compiled receipt, source binding and companion statement hashes must enter review/manifest admission.','Native positive exact predecessor+companion+unchanged0400, negative rename/NOTVALID/extra matching/expression/key/metadata/count drift and two-session DDL lock race, complete fresh restoration required.'],
 limits:['No SQL executed by this lane. Published native prefix capture is evidence for exact predecessor state only.','Eight retained CHECKs are not selected/dropped; their full native expressions were not captured. Count10 is conservative; do not infer approval of arbitrary retained expressions.','Ordinary table/actual current owner/no inheritance are additional conservative constructor-derived refusals, not claimed native capture fields.','Lock prevents concurrent DDL between assertion and consumer; existing protected writer/context/ledger gates remain mandatory.','No Bank/business rows/financial/provider/API/release/PartB approval.'],databaseCalls:0,native:'NOT_RUN'};
fs.writeFileSync(path.join(dir,'PROPOSAL.json'),JSON.stringify(proof,null,2)+'\n');console.log(JSON.stringify({sourceCommit:commit,programSha256:hash(program),constructor:hash(constructor),consumer:hash(consumer)}));
