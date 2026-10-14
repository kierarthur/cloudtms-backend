import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),sha=s=>createHash('sha256').update(s).digest('hex');assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const input=fs.readFileSync(path.join(dir,'before/actual_corrected_fin4.sqlinc'));assert.equal(sha(input),'762454575b7a6844dba087fb4a76e7c46a374a863e5eac77af13645fd4f5af62');
const old=input.toString('utf8'),nl=old.includes('\r\n')?'\r\n':'\n',norm=s=>s.replaceAll('\r\n','\n').replaceAll('\n',nl);let source=old;const hunks=[];
const replace=(name,before,after)=>{before=norm(before);after=norm(after);assert.equal(source.split(before).length,2,name+' before');source=source.replace(before,after);assert.equal(source.split(after).length,2,name+' after');hunks.push({name,before,after});};
replace('owned missing prewrite cache locals','  v_prior_snapshot public.weekly_source_final_snapshot_lines%rowtype;',
 `  v_prior_snapshot public.weekly_source_final_snapshot_lines%rowtype;
  v_missing_row record;
  v_missing_candidate jsonb;
  v_missing_cache jsonb:='{}'::jsonb;
  v_prior_missing_cancel_frame text:=coalesce(current_setting('cloudtms.weekly_source_missing_cancel_frame',true),'');
  v_cancel_basis jsonb;
  v_exclude_cancel boolean;`);
// Complete retained Legacy root union at discovery and BOTH native rechecks.
// Every occurrence is retained separately with a unique surrounding context.
const rootPattern=/  select pg_catalog\.array_agg\(distinct contract_week\.timesheet_id order by contract_week\.timesheet_id\)\r?\n  into (v_finalisation_root_ids|v_finalisation_root_recheck)[\s\S]*?    and contract_week\.timesheet_id is not null;/g;
const roots=[...source.matchAll(rootPattern)];assert.equal(roots.length,3);
for(let i=roots.length-1;i>=0;i--){const m=roots[i],before=m[0],after=norm(`  select pg_catalog.array_agg(inventory.root_timesheet_id order by inventory.root_timesheet_id)
  into ${m[1]}
  from private.weekly_source_missing_finalisation_roots_v1(
    v_upload.id,v_generation,case when p_prepare_only then v_correction.expected_current_final_revision_id else null end,
    v_group.id,v_client_id,v_coverage_start,v_coverage_end
  ) inventory;`);
 // The two identical original recheck statements need their actual lexical
 // source context to make each whole-file edit/reverse unique.
 let width=250,prefix,b,a;
 do{prefix=source.slice(Math.max(0,m.index-width),m.index);b=prefix+before;a=prefix+after;width*=2;}while((source.split(b).length!==2||source.includes(a))&&width<=source.length*2);
 assert.equal(source.split(b).length,2,'root context '+i);source=source.replace(b,a);assert.equal(source.split(a).length,2,'root after '+i);hunks.push({name:'complete Legacy root discovery/recheck '+i,before:b,after:a});
}
// The existing previous transition state is real absence when excluded. A
// subsequent missing file must carry its exact old immutable snapshot, not
// invent a current row or lose the event through a NULL new-snapshot join.
const priorBefore=`               prior_transition.new_snapshot_line_id`;
replace('previous retained state inventory',priorBefore,
 `               coalesce(prior_transition.new_snapshot_line_id,
                 case when prior_transition.exclude_cancel then prior_transition.previous_snapshot_line_id end)
                 as new_snapshot_line_id`);
// Discover/certify absence BEFORE inserting the new CURRENT/PREPARED revision
// or any movements. This avoids reading a partially built current ladder.
const priorStart=source.indexOf('      with prior_states as (');const priorEnd=source.indexOf('      ), current_lines as (',priorStart);assert.ok(priorStart>0&&priorEnd>priorStart);
const priorSql=source.slice(priorStart,priorEnd)+norm(`      )
      select prior_lines.id as previous_snapshot_line_id,prior_lines.work_event_id
      from prior_lines join public.weekly_work_events event on event.id=prior_lines.work_event_id
      where not exists(select 1 from public.weekly_source_upload_rows source_row
        join public.weekly_source_row_resolutions resolution on resolution.upload_row_id=source_row.id
          and resolution.generation=v_generation and resolution.mapping_state='RESOLVED'
        where source_row.upload_id=v_upload.id and resolution.work_event_id=prior_lines.work_event_id)
        and not (event.identity_kind='PROFILE_EXTERNAL_KEY' and exists(
          select 1 from public.weekly_source_upload_rows source_row
          where source_row.upload_id=v_upload.id and source_row.external_source_key=event.profile_external_key))
      order by prior_lines.work_event_id`);
const cache=norm(`  -- All genuinely missing dispositions share the retained native I-1/root
  -- scope and one pre-write current financial observation. No own-mutation CAS.
  if v_source_profile_kind in ('GENERIC_COMPLETE_SNAPSHOT','HEALTHROSTER_ACTUAL_ROWS') then
    for v_missing_row in
`)+priorSql+norm(`
    loop
      v_missing_candidate:=private.weekly_source_missing_cancel_basis_v1(
        v_publication.id,v_missing_row.previous_snapshot_line_id,v_actor);
      v_missing_cache:=jsonb_set(v_missing_cache,array[v_missing_row.work_event_id::text],v_missing_candidate,true);
    end loop;
    perform set_config('cloudtms.weekly_source_missing_cancel_frame',jsonb_build_object(
      'schema_version','WEEKLY_SOURCE_MISSING_CANCEL_OWNER_FRAME_V1',
      'owner_transaction_id',pg_current_xact_id()::text,'owner_backend_pid',pg_backend_pid(),
      'actor_user_id',v_actor,'projection_publication_id',v_publication.id,
      'upload_id',v_upload.id,'source_cycle_id',v_cycle.id,'source_group_id',v_group.id,
      'client_id',v_client_id,'census',v_missing_cache,
      'census_sha256',encode(private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_MISSING_CANCEL_CENSUS_V1',v_missing_cache),'hex'))::text,true);
  end if;

`);
replace('prewrite complete missing basis census','  select coalesce(pg_catalog.max(revision.revision_number),0)+1 into v_revision_number',
 cache+'  select coalesce(pg_catalog.max(revision.revision_number),0)+1 into v_revision_number');
replace('basis in revision manifest only when a real census exists',`    )||case when v_rule_version is null then '{}'::jsonb else jsonb_build_object(`,
 `    )||case when v_missing_cache='{}'::jsonb then '{}'::jsonb else jsonb_build_object(
      'missing_cancel_basis_census',v_missing_cache) end
    ||case when v_rule_version is null then '{}'::jsonb else jsonb_build_object(`);
replace('literal retained revision census column','finalised_at_utc,manifest_hash,policy_fingerprint,state,rule_set_version_id',
 'finalised_at_utc,manifest_hash,policy_fingerprint,state,rule_set_version_id,missing_cancel_basis_census');
replace('literal retained revision census value',`v_revision_manifest_hash,v_policy_fingerprint,
    case when p_prepare_only then 'PREPARED' else 'CURRENT' end,v_rule_version`,
 `v_revision_manifest_hash,v_policy_fingerprint,
    case when p_prepare_only then 'PREPARED' else 'CURRENT' end,v_rule_version,v_missing_cache`);
replace('real CANCEL exclusion from owned prewrite observation',`        else 'AMEND' end;
      v_transition_fingerprint:=private.weekly_source_sha256_jsonb_v1(`,
 `        else 'AMEND' end;
      v_exclude_cancel:=v_outcome='CANCEL' and coalesce((v_missing_cache#>>array[v_transition.work_event_id::text,'exclude_cancel'])::boolean,false);
      v_cancel_basis:=case when v_exclude_cancel then v_missing_cache#>array[v_transition.work_event_id::text,'basis'] else null end;
      if v_exclude_cancel and (v_cancel_basis->>'previous_snapshot_line_id' is distinct from v_transition.prior_snapshot_id::text
        or v_cancel_basis->>'work_event_id' is distinct from v_transition.work_event_id::text) then
        raise exception 'WEEKLY_SOURCE_MISSING_CANCEL_PREWRITE_CENSUS_CHANGED' using errcode='40001';
      end if;
      v_transition_fingerprint:=private.weekly_source_sha256_jsonb_v1(`);
replace('basis in immutable transition fingerprint only for excluded CANCEL',`          'new_state_fingerprint',case when v_current_state is null then null
            else pg_catalog.encode(v_current_state,'hex') end
        )`,
 `          'new_state_fingerprint',case when v_current_state is null then null
            else pg_catalog.encode(v_current_state,'hex') end
        )||case when v_exclude_cancel then jsonb_build_object(
          'exclude_cancel',true,'cancel_exclusion_basis',v_cancel_basis) else '{}'::jsonb end`);
replace('literal Source transition columns',`        new_state_fingerprint,transition_fingerprint,
        ordinary_source_entitlement_projection_state`,
 `        new_state_fingerprint,transition_fingerprint,exclude_cancel,cancel_exclusion_basis,
        ordinary_source_entitlement_projection_state`);
replace('literal owned Source transition values',`        v_prior_state,v_current_state,v_transition_fingerprint,
        case when v_outcome='NO_CHANGE' then 'NOT_APPLICABLE' else 'PENDING' end`,
 `        v_prior_state,v_current_state,v_transition_fingerprint,v_exclude_cancel,v_cancel_basis,
        case when v_outcome='NO_CHANGE' or v_exclude_cancel then 'NOT_APPLICABLE' else 'PENDING' end`);
replace('exclude only genuine absent CANCEL reversal, keep all AMEND/native branches',"        if v_outcome in ('AMEND','CANCEL') then",
 "        if v_outcome in ('AMEND','CANCEL') and not v_exclude_cancel then");
replace('retain real existing expense authority on excluded raw omission',`      loop
        v_expense_policy_id:=v_expense_observation.expense_policy_snapshot_id;`,
 `      loop
        -- A kept genuinely absent worked event keeps its existing expense
        -- authority too. No fabricated ROW_PRESENT or zero/recovery generation.
        if v_expense_observation.expense_policy_snapshot_id is null
          and coalesce((v_missing_cache#>>array[v_expense_observation.work_event_id::text,'exclude_cancel'])::boolean,false) then
          continue;
        end if;
        v_expense_policy_id:=v_expense_observation.expense_policy_snapshot_id;`);
// A private transaction-local frame has no authority without this actual PG
// owner stack. Restore it on every successful post-construction return and
// both native exception handlers; earlier replay never changed it.
const returnPattern=/  return pg_catalog\.jsonb_build_object\(\r?\n    'ok',true,'status',case when p_correction_session_id is null/g;
const returnMatches=[...source.matchAll(returnPattern)];assert.equal(returnMatches.length,1);
replace('restore missing frame before CURRENT return',returnMatches[0][0],
 `  perform set_config('cloudtms.weekly_source_missing_cancel_frame',v_prior_missing_cancel_frame,true);\n`+returnMatches[0][0]);
const preparedReturn=/    return pg_catalog\.jsonb_build_object\(\r?\n      'ok',true,'status','PREPARED'/g;
const preparedMatches=[...source.matchAll(preparedReturn)];assert.equal(preparedMatches.length,1);
replace('restore missing frame before PREPARED return',preparedMatches[0][0],
 `    perform set_config('cloudtms.weekly_source_missing_cancel_frame',v_prior_missing_cancel_frame,true);\n`+preparedMatches[0][0]);
replace('restore missing frame on value error',`  when invalid_text_representation or numeric_value_out_of_range or datetime_field_overflow then`,
 `  when invalid_text_representation or numeric_value_out_of_range or datetime_field_overflow then
    perform set_config('cloudtms.weekly_source_missing_cancel_frame',v_prior_missing_cancel_frame,true);`);
replace('restore missing frame on native error',`  when others then
    if v_finalisation_lock_context then`,
 `  when others then
    perform set_config('cloudtms.weekly_source_missing_cancel_frame',v_prior_missing_cancel_frame,true);
    if v_finalisation_lock_context then`);
// Exactly preserved corrected NO_CHANGE/1431, original financial writers,
// native Bank callbacks, lock contexts, source/assignment/audit and Report arm.
let reversed=source;for(const h of [...hunks].reverse()){assert.equal(reversed.split(h.after).length,2,h.name+' reverse');reversed=reversed.replace(h.after,h.before);}assert.equal(reversed,old);
fs.writeFileSync(path.join(dir,'forward/06_complete_fin4_default_draft.sqlinc'),source);
fs.writeFileSync(path.join(dir,'FIN4_DEFAULT_DRAFT_COMPLETE_REVERSE.json'),JSON.stringify({source_only:true,unfrozen_draft:true,input_current_native_commit:'33927fff2f7b90b90241d746d9e35aa21c57bd07',input_file:'supabase/repeatable/14102026_0821_healthroster_fin4_rebill_record_fields/01_complete_fin4.sqlinc',input_sha256:sha(input),input_bytes:input.length,output_file:'forward/06_complete_fin4_default_draft.sqlinc',output_sha256:sha(source),output_bytes:Buffer.byteLength(source),full_unique_reverse_verified:true,unique_hunks:hunks.length,hunks,shared_native_capture_composition_approved:false,canonical_changed:false,sql_compiled:false,native_proof:false,known_unfinished:['Native excluded CANCEL capture + explicit raw-export versus financial selection','Excluded source-fixed expense retention and correct-final baseline','Original immutable native insert/footprint assertions','Genuine accepted removal/full proposal/Gate5 and repeat omission after acceptance','Paid overlap choices/native empty-subject/ordinary-to-Source cash proof','Complete prepared/Office/mandatory verifier body-source coupling']},null,2)+'\n');console.log(JSON.stringify({source_only:true,draft:true,full_reverse:true,hunks:hunks.length,shared_approved:false,sql_compiled:false,native_proof:false}));
