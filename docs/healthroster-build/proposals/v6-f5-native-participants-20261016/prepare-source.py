"""Finite root-owner source proposal; never writes the current root checkout."""
from pathlib import Path
import sys,json,hashlib,subprocess,re
root=Path(sys.argv[1]).resolve();here=Path(__file__).resolve().parent
(here/'reference').mkdir(exist_ok=True)
changes=[]
base='supabase/repeatable/14102026_0430_healthroster_money_native_interfaces/'
def rewrite(path,name,edits,routine=None):
    raw=(root/path).read_bytes();old=raw.decode('utf-8').replace('\r\n','\n')
    if routine:
        m=re.search(r'(?im)^create or replace function '+re.escape(routine)+r'\(',old);assert m,routine
        end=old.index('$function$;',old.index('as $function$',m.start())+len('as $function$'))+len('$function$;')
        old=old[m.start():end]+'\n'
    new=old
    for before,after in edits:
        assert new.count(before)==1,(path,before,new.count(before))
        new=new.replace(before,after,1)
    (here/'reference'/name).write_text(old,encoding='utf-8',newline='\n')
    (here/'forward'/name).write_text(new,encoding='utf-8',newline='\n')
    changes.append({'target':path,'target_routine':routine,'reference':'reference/'+name,'forward':'forward/'+name,
       'input_raw_sha256':hashlib.sha256(raw).hexdigest(),'input_lf_sha256':hashlib.sha256(old.encode()).hexdigest(),
       'output_lf_sha256':hashlib.sha256(new.encode()).hexdigest(),'edits':[{'before':a,'after':b} for a,b in edits]})
rewrite(base+'01_OFF_RULE_AUTHORITY.sqlinc','010_off_rule_scope.sqlinc',[
 ("item->>'cli_ref' !~ '^CLI-913013[0-9]{2}$'","not private.weekly_source_test_fixture_reference_v1('CLIENT',(item->>'client_id')::uuid,item->>'cli_ref')"),
 ("item->>'tms_ref' !~ '^CCR-913013[0-9]{2}$'","not private.weekly_source_test_fixture_reference_v1('CANDIDATE',(item->>'candidate_id')::uuid,item->>'tms_ref')")],routine='private.weekly_source_rule_qualification_scope_assert_v1')
rewrite(base+'02_HISTORICAL_FIXTURE_ASSIGNMENT.sqlinc','020_historical_assignment.sqlinc',[
 ("coalesce(ref->>'cli_ref','') !~ '^CLI-913013[0-9]{2}$'","not private.weekly_source_test_fixture_reference_v1('CLIENT',(ref->>'client_id')::uuid,ref->>'cli_ref')"),
 ("coalesce(ref->>'tms_ref','') !~ '^CCR-913013[0-9]{2}$'","not private.weekly_source_test_fixture_reference_v1('CANDIDATE',(ref->>'candidate_id')::uuid,ref->>'tms_ref')")],routine='private.weekly_source_historical_fixture_assignment_scope_v1')
rewrite(base+'21_SETUP_STATIC_SHAPE_ASSERT.sqlinc','030_off_rule_shape.sqlinc',[
 ("coalesce(ref->>'cli_ref','') !~ '^CLI-913013[0-9]{2}$'","not private.weekly_source_test_fixture_reference_v1('CLIENT',(ref->>'client_id')::uuid,ref->>'cli_ref')"),
 ("coalesce(ref->>'tms_ref','') !~ '^CCR-913013[0-9]{2}$'","not private.weekly_source_test_fixture_reference_v1('CANDIDATE',(ref->>'candidate_id')::uuid,ref->>'tms_ref')")])
rewrite(base+'27_POST_VERIFIED_EVIDENCE_RECORD.sqlinc','040_post_verified_record.sqlinc',[
 ("        if artifact->>'schema_version'='WEEKLY_SOURCE_PROTECTED_TEST_SETUP_AUTHORIZATION_V1' then",
  "        if artifact->>'schema_version'='WEEKLY_SOURCE_NATIVE_FIXTURE_PARTICIPANTS_V1' then\n          perform private.weekly_source_native_fixture_participants_artifact_assert_v1(artifact,p_execution);\n        elsif artifact->>'schema_version'='WEEKLY_SOURCE_NATIVE_FIXTURE_SOURCE_METADATA_V1' then\n          perform private.weekly_source_native_fixture_source_artifact_assert_v1(artifact,p_execution);\n        elsif artifact->>'schema_version'='WEEKLY_SOURCE_PROTECTED_TEST_SETUP_AUTHORIZATION_V1' then")],routine='private.weekly_source_post_verified_artifact_assert_v1')
rewrite('scripts/weekly-source-post-verified-evidence-controller.mjs','weekly-source-post-verified-evidence-controller.mjs',[
 ("import {execFileSync} from 'node:child_process';","import {execFileSync} from 'node:child_process';\nimport {buildNativeFixtureParticipants,nativeParticipantSchema,buildNativeFixtureSourceMetadata,nativeSourceSchema} from './weekly-source-native-fixture-participants.mjs';"),
 ("  artifacts=rows.map(row=>{\n   if(Object.hasOwn(row,'purpose')){",
  "  artifacts=rows.map(row=>{\n   if(row.schema_version===nativeParticipantSchema)return buildNativeFixtureParticipants(context,row);\n   if(row.schema_version===nativeSourceSchema)return buildNativeFixtureSourceMetadata(context,row);\n   if(Object.hasOwn(row,'purpose')){")])
rewrite('scripts/weekly-source-off-rule-setup-evidence.mjs','weekly-source-off-rule-setup-evidence.mjs',[
 ("/^CLI-913013[0-9]{2}$/","/^CLI-[0-9]+$/"),("/^CCR-913013[0-9]{2}$/","/^CCR-[0-9]+$/")])
rewrite('scripts/healthroster-f5-native-setup.mjs','healthroster-f5-native-setup.mjs',[
 ("and cl.cli_ref ~ '^CLI-913013[0-9]{2}$'","and private.weekly_source_test_fixture_reference_v1('CLIENT',cl.id,cl.cli_ref)"),
 ("/^CCR-913013[0-9]{2}$/","/^CCR-[0-9]+$/"),
 ("::text from native n;`", "::text from native n where not exists(select 1 from contract_scope k\n where not private.weekly_source_test_fixture_reference_v1('CANDIDATE',k.candidate_id,k.tms_ref));`")])
# Refuse new post-VERIFIED plans at the actual pre-VERIFIED transport boundary.
# Do not weaken or skip the independently qualified original BULK positives.
rewrite('scripts/weekly-source-protected-test-setup-evidence.mjs','weekly-source-protected-test-setup-evidence.mjs',[
 (" const bundle=loadCommittedBundle(root,release,context,'weeklySourceProtectedTestSetup');if(bundle===null)return [];",
  " const bundle=loadCommittedBundle(root,release,context,'weeklySourceProtectedTestSetup');if(bundle===null)return [];\n if(bundle.authorizations?.some(row=>['WEEKLY_SOURCE_NATIVE_FIXTURE_PARTICIPANTS_V1','WEEKLY_SOURCE_NATIVE_FIXTURE_SOURCE_METADATA_V1'].includes(row?.schema_version)))\n  fail('NATIVE_FIXTURE_POST_VERIFIED_PHASE_REQUIRED');")])
dependency='scripts/weekly-source-protected-release-qualification.mjs'
(here/'reference/weekly-source-protected-release-qualification.mjs').write_text((root/dependency).read_text(encoding='utf-8'),encoding='utf-8',newline='\n')
out={'pure_test_dependency':dependency,'root_head':subprocess.check_output(['git','-c','maintenance.auto=false','-c','gc.auto=0','rev-parse','HEAD'],cwd=root,text=True).strip(),
 'new_private_sql':'forward/001_native_participants.sqlinc','new_script':'scripts/weekly-source-native-fixture-participants.mjs','changes':changes}
(here/'FINITE_REPLACEMENTS.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8',newline='\n')
print(json.dumps({'complete_replacements':len(changes),'root_head':out['root_head']}))
