import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),base=path.resolve(dir,'../rekey-source-alias-current-native-successor-20261014');
const sha=b=>createHash('sha256').update(b).digest('hex');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const records=[];
function compose(input,output,expected,hunks){
 const before=fs.readFileSync(path.join(base,'forward',input));assert.equal(sha(before),expected);
 const eol=before.toString().includes('\r\n')?'\r\n':'\n';
 hunks=hunks.map(h=>({...h,before:h.before.replaceAll('\n',eol),after:h.after.replaceAll('\n',eol)}));
 let after=before.toString();for(const h of hunks){assert.equal(after.split(h.before).length,2,h.name);after=after.replace(h.before,h.after);}
 let reverse=after;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2,h.name);reverse=reverse.replace(h.after,h.before);}
 assert.equal(reverse,before.toString());
 for(const side of ['before','forward'])fs.mkdirSync(path.join(dir,side),{recursive:true});
 fs.writeFileSync(path.join(dir,'before',output),before);fs.writeFileSync(path.join(dir,'forward',output),after);
 records.push({input:path.relative(path.resolve(dir,'../../../..'),path.join(base,'forward',input)).replaceAll('\\','/'),output,
  before_sha256:sha(before),after_sha256:sha(after),before_bytes:before.length,after_bytes:Buffer.byteLength(after),hunks,whole_reverse_verified:true});
}
compose('02_complete_upload_context.sqlinc','04_complete_upload_context.sqlinc','1a23b8da7645ed0ce0482ed1647165076c7b0d2747dba1dee3fe2c95d417e4df',[
 {name:'current choice and native overlap inputs',before:'  v_office_choice private.weekly_source_office_row_choices%rowtype;',after:'  v_office_choice private.weekly_source_office_row_choices%rowtype;\n  v_paid_choice jsonb;v_paid_inputs jsonb;'},
 {name:'reset raw paid inputs each source row',before:"    v_contracts:='[]'::jsonb;",after:"    v_paid_choice:=null;v_paid_inputs:=null;\n    v_contracts:='[]'::jsonb;"},
 {name:'genuine native paid overlap after identity and Contract discovery',before:"    v_rows:=v_rows||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(",after:`    if v_candidate_id is not null and v_row_client_id is not null and v_upload.purpose='ORDINARY'
      and v_profile.profile_code in ('HEALTHROSTER_WEEKLY_FROM_TO_ACTUAL_V1','HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1','ROSTER_WEEKLY_SUMMARY_ACTUAL_V1')
      and v_source_row.row_finalisation_state='SOURCE_WORKED' then
      v_paid_inputs:=private.weekly_source_paid_overlap_candidates_v1(v_source_row.id,v_actor,v_candidate_id,v_row_client_id);
      v_paid_choice:=private.weekly_source_paid_overlap_choice_current_v1(v_source_row.id,v_actor);
      if v_paid_choice is not null then
        if v_paid_choice->>'candidate_id' is distinct from v_candidate_id::text
          or v_paid_choice->>'client_id' is distinct from v_row_client_id::text then
          raise exception 'WEEKLY_SOURCE_PAID_OVERLAP_CHOICE_SCOPE_CHANGED' using errcode='40001';end if;
        v_prior_work_event_id:=case when v_paid_choice->>'pay_effect'='NONE'
          then (v_paid_choice->>'paid_overlap_target_work_event_id')::uuid else null end;
        v_prior_contract_id:=case when v_paid_choice->>'pay_effect'='NONE'
          then (v_paid_choice->>'contract_id')::uuid else null end;
      end if;
    end if;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object(`},
 {name:'server derived choice and facts in existing context row',before:"      'office_separate_shift',coalesce(v_office_choice.separate_shift,false),",after:"      'office_separate_shift',coalesce(v_office_choice.separate_shift,false),\n      'paid_overlap_inputs',v_paid_inputs,'paid_overlap_choice',v_paid_choice,"}
]);
compose('03_complete_projection_rows.sqlinc','05_complete_projection_rows.sqlinc','14f01e7ca69c1f1ada8720194d0aa7311e2b9bd408f6865e9e563b4505a1fe60',[
 {name:'owner derived choice state',before:'  v_row_json jsonb;',after:"  v_row_json jsonb;\n  v_paid_choices jsonb;v_paid_recheck jsonb;v_paid_choice jsonb;v_paid_inputs jsonb;v_pay_effect text;v_paid_choice_id bigint;"},
 {name:'complete upload native families before original scope gate',before:'  perform pg_catalog.pg_advisory_xact_lock(\n    pg_catalog.hashtextextended(\n      pg_catalog.encode(v_upload.declared_scope_fingerprint,\'hex\'),73241837\n    )\n  );',after:`  v_paid_choices:=private.weekly_source_paid_overlap_projection_lock_v1(v_upload.id,p_actor_user_id,p_publication_id);
  perform pg_catalog.pg_advisory_xact_lock(
    pg_catalog.hashtextextended(
      pg_catalog.encode(v_upload.declared_scope_fingerprint,'hex'),73241837
    )
  );
  if jsonb_array_length(v_paid_choices)>0 then
    perform 1 from public.weekly_source_groups where id=v_group.id for update;
  end if;`},
 {name:'whole choice census reproof after gate and cycle before any row insert',before:'  for v_row_json in select value from pg_catalog.jsonb_array_elements(p_rows)\n  loop',after:`  -- Re-read all current choices after the same scope/group/cycle order.
  -- No second I-1 acquisition is attempted while holding business locks.
  select coalesce(jsonb_agg(private.weekly_source_paid_overlap_choice_current_v1(r.id,p_actor_user_id) order by r.id),'[]'::jsonb)
    into v_paid_recheck from public.weekly_source_upload_rows r
    join lateral(select c.* from private.weekly_source_office_row_choices c
      where c.upload_row_id=r.id order by c.id desc limit 1) choice on true
    where r.upload_id=v_upload.id and choice.paid_overlap_target_work_event_id is not null;
  if v_paid_recheck is distinct from v_paid_choices then
    raise exception 'WEEKLY_SOURCE_PAID_OVERLAP_CHOICE_CHANGED' using errcode='40001';end if;
  for v_row_json in select value from pg_catalog.jsonb_array_elements(p_rows)
  loop`},
 {name:'no caller pay effect authority',before:"    v_mapping_state:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_row_json->>'mapping_state','')));",after:`    if v_row_json ?| array['pay_effect','paid_overlap_choice_id','paid_overlap_choice','paid_overlap_inputs'] then
      raise exception 'WEEKLY_SOURCE_CALLER_PAY_EFFECT_FORBIDDEN' using errcode='22023';end if;
    v_paid_choice:=null;v_paid_inputs:=null;v_pay_effect:='NORMAL';v_paid_choice_id:=null;
    v_mapping_state:=pg_catalog.upper(pg_catalog.btrim(coalesce(v_row_json->>'mapping_state','')));`},
 {name:'confirmed same and separate native constructor admission',before:'      if v_source_authority then\n        v_protected_matches:=private.weekly_source_protected_match_candidates_v1(v_source_row.id,v_candidate_id,v_client_id);',after:`      if v_source_authority then
        select item into v_paid_choice from jsonb_array_elements(v_paid_choices) item
          where item->>'upload_row_id'=v_source_row.id::text;
        if v_paid_choice is not null then
          if v_paid_choice->>'candidate_id' is distinct from v_candidate_id::text
            or v_paid_choice->>'client_id' is distinct from v_client_id::text
            or v_paid_choice->>'contract_id' is distinct from v_contract_id::text
            or v_prior_work_event_id is distinct from case when v_paid_choice->>'pay_effect'='NONE'
              then (v_paid_choice->>'paid_overlap_target_work_event_id')::uuid else null end then
            raise exception 'WEEKLY_SOURCE_PAID_OVERLAP_CHOICE_SCOPE_CHANGED' using errcode='40001';end if;
          v_pay_effect:=v_paid_choice->>'pay_effect';v_paid_choice_id:=(v_paid_choice->>'choice_id')::bigint;
        elsif v_identity_kind='PROFILE_EXTERNAL_KEY' and v_source_row.row_finalisation_state='SOURCE_WORKED'
          and not exists(select 1 from public.weekly_work_events key_event where key_event.first_source_group_id=v_group.id
            and key_event.source_format_profile_id=v_upload.source_format_profile_id and key_event.identity_kind='PROFILE_EXTERNAL_KEY'
            and key_event.profile_external_key=v_profile_external_key)
          and private.weekly_source_known_source_key_v1(v_source_row.id,v_candidate_id,v_client_id) is null
          and private.weekly_source_identical_rekey_prior_v1(v_source_row.id) is null then
          v_paid_inputs:=private.weekly_source_paid_overlap_candidates_v1(v_source_row.id,p_actor_user_id,v_candidate_id,v_client_id);
          if exists(select 1 from jsonb_array_elements(v_paid_inputs->'overlaps') item
            where item#>'{native_payment_facts,paid}' is distinct from 'false'::jsonb) then
            raise exception 'WEEKLY_SOURCE_PAID_SHIFT_MATCH_REQUIRED' using errcode='55000';end if;
        end if;
      end if;
      if v_source_authority and v_paid_choice is null then
        v_protected_matches:=private.weekly_source_protected_match_candidates_v1(v_source_row.id,v_candidate_id,v_client_id);`},
 {name:'paid choice is independently admitted native identity proof',before:"        and not (v_source_authority and v_protected_choice is not null\n          and v_protected_choice=v_prior_work_event_id) then",after:"        and v_paid_choice is null\n        and not (v_source_authority and v_protected_choice is not null\n          and v_protected_choice=v_prior_work_event_id) then"},
 {name:'immutable row resolution carries actual choice and separate pay effect',before:'      contract_and_rate_fingerprint,effective_policy_fingerprint\n    ) values (',after:'      contract_and_rate_fingerprint,effective_policy_fingerprint,pay_effect,paid_overlap_choice_id\n    ) values ('},
 {name:'preserve all native economic values',before:"      case when v_mapping_state='RESOLVED' then v_effective_policy_fingerprint end\n    ) returning id into v_resolution_id;",after:"      case when v_mapping_state='RESOLVED' then v_effective_policy_fingerprint end,\n      v_pay_effect,v_paid_choice_id\n    ) returning id into v_resolution_id;"}
]);
fs.writeFileSync(path.join(dir,'PROJECTION_REVERSES.json'),JSON.stringify({source_only:true,accepted_rekey_predecessor:'4c63b81b94406da7d750746520b2ae57b9b24a80',all_owner_alias_helper_required:'3efed0771093465c90faed992811b660b8a6f885bcbe7bfd17adfc36eb16d56d',records,native_calculator_unchanged:true,pipeline_complete:false,canonical_mounted:false,native_proof:false},null,2)+'\n');
console.log(JSON.stringify(records.map(r=>({output:r.output,hunks:r.hunks.length,sha256:r.after_sha256}))));
