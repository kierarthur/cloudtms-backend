import fs from'node:fs';import cp from'node:child_process';import path from'node:path';import assert from'node:assert/strict';import{createHash}from'node:crypto';import{scanManagedSql,readManagedClosure}from'../scripts/cloudtms-managed-release-sql.mjs';
import{functionEvents}from'../docs/healthroster-build/proposals/a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const sha=b=>createHash('sha256').update(b).digest('hex'),inputHead=cp.execFileSync('git',['rev-parse','HEAD'],{encoding:'utf8'}).trim();assert.equal(inputHead,'a7bb9f54214a0c2ee539f9efb2f7444084953799');
const parts=Object.fromEntries(new Intl.DateTimeFormat('en-GB',{timeZone:'Europe/London',day:'2-digit',month:'2-digit',year:'numeric',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(new Date()).filter(x=>x.type!=='literal').map(x=>[x.type,x.value]));
const stem=parts.day+parts.month+parts.year+'_'+parts.hour+parts.minute+'_healthroster_source_arbiter_kernel',migration='supabase/migrations/'+stem+'.sql',repeatable='supabase/repeatable/'+stem+'_v1.sql',verifier='supabase/verification/'+stem+'_verification.sql',leaf='supabase/repeatable/'+stem+'/',archive='docs/healthroster-build/install2-source-kernel-canonical-20261015';assert(stem.startsWith('15102026_'));assert(!fs.existsSync(archive));
const pin='d48f05205ae9ce066762be8a307d87b304ce71da',base='docs/healthroster-build/proposals/a0-source-policy-selector-successor-20261015/forward/',sources=[];
function input(commit,file){const b=cp.execFileSync('git',['cat-file','blob',commit+':'+file]);assert.deepEqual(b,fs.readFileSync(file));sources.push({commit,file,bytes:b.length,sha256:sha(b)});return b;}
const schema=input(pin,base+'01_complete_same_arbiter_schema.sqlinc'),grammar=input(inputHead,'docs/healthroster-build/proposals/a0-source-grammar-case-compilation-fix-20261015/forward/00_source_token_grammar.sqlinc'),scalar=input(pin,base+'02_complete_scalar_helpers.sqlinc').toString().replaceAll('\r\n','\n'),coverage=input(pin,base+'03_coverage_and_compatibility_acquire.sqlinc');
const scalarName='private.timesheet_correction_a0_fence_scalar_acquire_core_v1',selected=scanManagedSql(scalar).statements.map(x=>scalar.slice(x.start,x.end)).filter(s=>s.includes(scalarName+'(')&&/^(?:create or replace function|alter function|revoke all on function)\b/i.test(s.trim()));assert.equal(selected.length,3);const selectedBytes=Buffer.from(selected.join('\n')+'\n');
const definitions=functionEvents(grammar.toString().replaceAll('\r\n','\n')+'\n'+selectedBytes+'\n'+coverage.toString().replaceAll('\r\n','\n')).events.filter(e=>e.kind==='CREATE');assert.equal(definitions.length,5);assert.equal(new Set(definitions.map(x=>x.id)).size,5);
const expected=definitions.map(e=>({signature:e.id,body_sha256:sha(e.body),volatility:e.id.includes('source_token_assert')?'i':'v'}));
const rows=expected.map(e=>"('"+e.signature+"','"+e.body_sha256+"','"+e.volatility+"')").join(',\n   ');
const verifierSql=`-- Mandatory constructor-only Source kernel owner/body/ACL/schema proof.
-- No application fixture or financial/worker function is executed.
begin;
do $source_kernel_verify$
declare e record;p record;role_name text;n integer;
begin
 for e in select * from(values
   ${rows}
 ) expected(signature,body_sha256,volatility) loop
  select f.* into p from pg_catalog.pg_proc f where f.oid=to_regprocedure(e.signature);
  if not found or p.proowner<>(select oid from pg_catalog.pg_roles where rolname=current_user)
   or not p.prosecdef or p.provolatile::text<>e.volatility
   or p.proconfig is distinct from array['search_path=pg_catalog, pg_temp']::text[]
   or encode(pg_catalog.sha256(pg_catalog.convert_to(p.prosrc,'UTF8')),'hex')<>e.body_sha256 then
    raise exception 'HEALTHROSTER_SOURCE_KERNEL_OWNER_BODY_PROFILE_INVALID' using errcode='55000';end if;
  foreach role_name in array array['anon','authenticated','service_role'] loop
   if has_function_privilege(role_name,p.oid,'EXECUTE') then
    raise exception 'HEALTHROSTER_SOURCE_KERNEL_PRIVATE_EXECUTE_EXPOSED' using errcode='42501';end if;
  end loop;
 end loop;
 select c.* into p from pg_catalog.pg_class c where c.oid=to_regclass('private.timesheet_correction_a0_transaction_fences');
 if not found or p.relkind<>'r' or not p.relrowsecurity or p.relforcerowsecurity
  or p.relowner<>(select oid from pg_catalog.pg_roles where rolname=current_user) then
   raise exception 'HEALTHROSTER_SOURCE_KERNEL_TABLE_OWNER_RLS_INVALID' using errcode='55000';end if;
 foreach role_name in array array['anon','authenticated','service_role'] loop
  if has_table_privilege(role_name,p.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') then
   raise exception 'HEALTHROSTER_SOURCE_KERNEL_TABLE_PRIVILEGE_EXPOSED' using errcode='42501';end if;
 end loop;
 select count(*) into n from pg_catalog.pg_constraint c where c.conrelid=p.oid and c.contype='x'
  and c.conname in('timesheet_correction_a0_fences_uuid_excl','timesheet_correction_a0_fences_text_excl','timesheet_correction_a0_fences_coverage_excl');
 if n<>3 or not exists(select 1 from pg_catalog.pg_constraint c where c.conrelid=p.oid
  and c.conname='timesheet_correction_a0_fences_coverage_excl' and pg_get_constraintdef(c.oid) like '%retained WITH <>%')
  or to_regclass('private.timesheet_correction_a0_fences_coverage_retained_unique') is null then
   raise exception 'HEALTHROSTER_SOURCE_KERNEL_NATIVE_EXCLUSION_PROFILE_INVALID' using errcode='55000';end if;
 if not exists(select 1 from pg_catalog.pg_policy policy where policy.polrelid=p.oid
  and policy.polname='cloudtms_miget_service_owner_all' and policy.polcmd='*'
  and policy.polroles @> array[(select oid from pg_catalog.pg_roles where rolname=current_user),(select oid from pg_catalog.pg_roles where rolname='service_role')]
  and cardinality(policy.polroles)=2 and pg_get_expr(policy.polqual,policy.polrelid)='true'
  and pg_get_expr(policy.polwithcheck,policy.polrelid)='true') then
   raise exception 'HEALTHROSTER_SOURCE_KERNEL_PRIVATE_POLICY_INVALID' using errcode='55000';end if;
end $source_kernel_verify$;
rollback;
`;
const outputs=[[migration,Buffer.concat([Buffer.from('-- One-time exact Source/A0 arbiter schema; no application data.\nbegin;\n'),schema,Buffer.from('\ncommit;\n')])],[leaf+'00_source_token_grammar.sqlinc',grammar],[leaf+'01_complete_scalar_core.sqlinc',selectedBytes],[leaf+'02_complete_coverage_and_dispatch.sqlinc',coverage],[repeatable,Buffer.from('-- Source kernel foundation only; full caller/RAW/ACTIVE integration is separate.\nbegin;\n'+['00_source_token_grammar.sqlinc','01_complete_scalar_core.sqlinc','02_complete_coverage_and_dispatch.sqlinc'].map(f=>'\\ir '+stem+'/'+f).join('\n')+"\nnotify pgrst, 'reload schema';\ncommit;\n")],[verifier,Buffer.from(verifierSql)]];
for(const[file]of outputs)assert(!fs.existsSync(file),'NEW_CANONICAL_TARGET_EXISTS '+file);
const releaseFile='supabase/release/current-release.json',beforeRelease=fs.readFileSync(releaseFile),release=JSON.parse(beforeRelease);assert.equal(release.verificationFiles.length,173);assert.equal(release.newVerificationFiles.length,174);release.verificationFiles.push(verifier);release.newVerificationFiles.push(verifier);const afterRelease=Buffer.from(JSON.stringify(release,null,2)+'\n');
// Prepare every byte before writing any canonical target; preserve current release prefix exactly as data.
assert.deepEqual(fs.readFileSync(releaseFile),beforeRelease);for(const[file,b]of outputs){fs.mkdirSync(path.dirname(file),{recursive:true});fs.writeFileSync(file,b,{flag:'wx'});}fs.writeFileSync(releaseFile,afterRelease);fs.mkdirSync(archive,{recursive:true});fs.writeFileSync(archive+'/CURRENT_RELEASE_BEFORE.json',beforeRelease,{flag:'wx'});
const closure=readManagedClosure(repeatable,process.cwd());assert.equal(functionEvents(closure.expanded).events.filter(e=>e.kind==='CREATE').length,5);
const receipt={source_only_mount:true,deployable:false,input_head:inputHead,native_kernel_receipt:'docs/healthroster-build/root-source-arbiter-native-kernel-20261015/RETRY_01_NATIVE_RESULT.json',independent_case_fix_review:'PENDING',sources,outputs:outputs.map(([file,b])=>({file,bytes:b.length,sha256:sha(b)})),expected_definitions:expected,repeatable_closure_sha256:closure.closureHash,release:{file:releaseFile,before_sha256:sha(beforeRelease),after_sha256:sha(afterRelease),before_upgrade:173,after_upgrade:174,before_new:174,after_new:175,appended_verifier:verifier,existing_prefix_preserved:true},remaining:['full native owner union/callers/RAW/ACTIVE triggers/current roles and final engine contract','full exact-candidate safety/rehearsal and ordered protected Install2','all financial/security/document/API/Office/MyTMS/TestFlight/phone/both PartB gates']};
fs.writeFileSync(archive+'/COMPOSITION.json',JSON.stringify(receipt,null,2)+'\n',{flag:'wx'});console.log(JSON.stringify(receipt));
