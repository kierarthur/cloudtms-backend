import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),sha=b=>createHash('sha256').update(b).digest('hex');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const commit='fa66183f01913e86651e82816f5e88f97e07123c',currentPath='supabase/repeatable/14102026_0821_healthroster_fin4_rebill_record_fields/01_complete_fin4.sqlinc';
const current=execFileSync('git',['show',commit+':'+currentPath],{cwd:root,maxBuffer:2e6});
assert.equal(sha(current),'762454575b7a6844dba087fb4a76e7c46a374a863e5eac77af13645fd4f5af62');
assert.deepEqual(current,fs.readFileSync(path.join(dir,'../missing-default-current-native-proposal-20261014/before/actual_corrected_fin4.sqlinc')));
const input='../missing-default-current-native-proposal-20261014/forward/06_complete_fin4_default_draft.sqlinc',before=fs.readFileSync(path.join(dir,input));
assert.equal(sha(before),'96838b0e1b34d81e2649df2f84191cea3277d81e123170091fc9ec25dced4db2');
const eol=before.toString().includes('\r\n')?'\r\n':'\n';
let hunks=[
 {name:'same one-classifier choice census',before:'  v_snapshot_hash bytea;',after:"  v_snapshot_hash bytea;\n  v_paid_cache jsonb:='{}'::jsonb;v_paid_choice jsonb;v_paid_source_rows jsonb;"},
 {name:'under complete current native locks reprove all choice effects before any financial writes',before:'  -- The source Timesheet owner is the only component permitted to create or',after:`  -- Actual immutable confirmations, re-proved under the existing complete
  -- I-1/root/source/window locks. No caller pay flag or second classifier.
  for v_row in select source_row.id,resolution.id as resolution_id,resolution.work_event_id,
      resolution.pay_effect,resolution.paid_overlap_choice_id
    from public.weekly_source_upload_rows source_row join public.weekly_source_row_resolutions resolution
      on resolution.upload_row_id=source_row.id and resolution.generation=v_generation
    where source_row.upload_id=v_upload.id order by source_row.source_row_ordinal,source_row.id
  loop
    v_paid_choice:=private.weekly_source_paid_overlap_choice_current_v1(v_row.id,v_actor);
    if v_paid_choice is null then
      if v_row.pay_effect<>'NORMAL' or v_row.paid_overlap_choice_id is not null then
        raise exception 'WEEKLY_SOURCE_PAID_OVERLAP_FINAL_CHOICE_UNBOUND' using errcode='55000';end if;
    else
      if v_paid_choice->>'schema_version' is distinct from 'WEEKLY_SOURCE_PAID_OVERLAP_CURRENT_CHOICE_V1'
        or v_row.paid_overlap_choice_id is distinct from (v_paid_choice->>'choice_id')::bigint
        or v_row.pay_effect is distinct from v_paid_choice->>'pay_effect'
        or (v_row.pay_effect='NONE' and v_row.work_event_id is distinct from
          (v_paid_choice->>'paid_overlap_target_work_event_id')::uuid) then
        raise exception 'WEEKLY_SOURCE_PAID_OVERLAP_FINAL_CHOICE_UNBOUND' using errcode='55000';end if;
      -- This first controlled branch has no expense write authority. The
      -- separate real source-fixed-expense composition remains engineering,
      -- and is not represented as an invented omitted-zero expense.
      if v_row.pay_effect='NONE' and exists(select 1 from public.weekly_source_row_expense_policy_snapshots expense
        where expense.row_resolution_id=v_row.resolution_id) then
        raise exception 'WEEKLY_SOURCE_PAID_MATCH_EXPENSE_COMPOSITION_REQUIRED' using errcode='55000';end if;
      v_paid_cache:=v_paid_cache||jsonb_build_object(v_row.id::text,v_paid_choice);
    end if;
  end loop;

  -- The source Timesheet owner is the only component permitted to create or`},
 {name:'freeze real choices in the new revision manifest only when present',before:"      'missing_cancel_basis_census',v_missing_cache) end",after:"      'missing_cancel_basis_census',v_missing_cache) end\n    ||case when v_paid_cache='{}'::jsonb then '{}'::jsonb else jsonb_build_object('paid_overlap_choice_census',v_paid_cache) end"},
 {name:'immutable census is supplied at genuine revision INSERT',before:'    finalised_at_utc,manifest_hash,policy_fingerprint,state,rule_set_version_id,missing_cancel_basis_census\n  ) values (',after:'    finalised_at_utc,manifest_hash,policy_fingerprint,state,rule_set_version_id,missing_cancel_basis_census,paid_overlap_choice_census\n  ) values ('},
 {name:'no later census rewrite',before:"    case when p_prepare_only then 'PREPARED' else 'CURRENT' end,v_rule_version,v_missing_cache\n  ) returning id into v_revision_id;",after:"    case when p_prepare_only then 'PREPARED' else 'CURRENT' end,v_rule_version,v_missing_cache,v_paid_cache\n  ) returning id into v_revision_id;"},
 {name:'real snapshot companion carries effect and immutable choice id',before:'             resolution.work_event_match_fingerprint\n      from public.weekly_source_upload_rows source_row',after:'             resolution.work_event_match_fingerprint,resolution.pay_effect,resolution.paid_overlap_choice_id\n      from public.weekly_source_upload_rows source_row'},
 {name:'bind snapshot hash to real choice without changing its economic vectors',before:"          'policy_fingerprint',pg_catalog.encode(v_economic.effective_policy_fingerprint,'hex')\n        )\n      );",after:"          'policy_fingerprint',pg_catalog.encode(v_economic.effective_policy_fingerprint,'hex')\n        )||case when v_row.paid_overlap_choice_id is null then '{}'::jsonb else jsonb_build_object(\n          'pay_effect',v_row.pay_effect,'paid_overlap_choice_id',v_row.paid_overlap_choice_id,\n          'paid_overlap_choice_proof_sha256',v_paid_cache#>>array[v_row.id::text,'choice_proof_sha256']) end\n      );"},
 {name:'insert snapshot effect independent from original calculated raw values',before:'        policy_fingerprint,snapshot_line_hash,authority_subset_json,authority_subset_sha256,invoice_economics_sha256\n      ) values (',after:'        policy_fingerprint,snapshot_line_hash,authority_subset_json,authority_subset_sha256,invoice_economics_sha256,pay_effect,paid_overlap_choice_id\n      ) values ('},
 {name:'snapshot fields use server resolution only',before:"            case when v_economic.invoice_vat_chargeable then v_economic.invoice_vat_rate_pct else 0 end) else null end\n      ) returning id into v_snapshot_id;",after:"            case when v_economic.invoice_vat_chargeable then v_economic.invoice_vat_rate_pct else 0 end) else null end,\n        v_row.pay_effect,v_row.paid_overlap_choice_id\n      ) returning id into v_snapshot_id;"},
 {name:'payable current selection excludes invoice companions and retains genuine prior basis for SAME',before:`      ), current_lines as (
        select current_line.id,current_line.work_event_id
        from public.weekly_source_final_snapshot_lines current_line
        where current_line.final_revision_id=v_revision_id
      )`,after:`      ), current_normal_lines as (
        select current_line.id,current_line.work_event_id
        from public.weekly_source_final_snapshot_lines current_line
        where current_line.final_revision_id=v_revision_id and current_line.pay_effect='NORMAL'
      ), none_basis as (
        select distinct companion.work_event_id,(item->>'snapshot_line_id')::uuid as id
        from public.weekly_source_final_snapshot_lines companion
        cross join lateral jsonb_array_elements(v_paid_cache#>array[companion.upload_row_id::text,'inputs','overlaps']) item
        where companion.final_revision_id=v_revision_id and companion.pay_effect='NONE'
          and item->>'work_event_id'=companion.work_event_id::text
          and companion.paid_overlap_choice_id=(v_paid_cache#>>array[companion.upload_row_id::text,'choice_id'])::bigint
      ), current_lines as (
        select id,work_event_id from current_normal_lines
        union all
        select basis.id,basis.work_event_id from none_basis basis
          join prior_lines prior on prior.work_event_id=basis.work_event_id and prior.id=basis.id
          where not exists(select 1 from current_normal_lines normal where normal.work_event_id=basis.work_event_id)
      )`},
 {name:'derive real source presence separately from retained pay basis',before:'      select * into v_prior_snapshot\n      from public.weekly_source_final_snapshot_lines\n      where id=v_transition.prior_snapshot_id;',after:`      select coalesce(jsonb_agg(jsonb_build_object('snapshot_line_id',companion.id,'upload_row_id',companion.upload_row_id,
        'paid_overlap_choice_id',companion.paid_overlap_choice_id,'snapshot_line_hash',encode(companion.snapshot_line_hash,'hex'))
        order by companion.upload_row_id),'[]'::jsonb) into v_paid_source_rows
        from public.weekly_source_final_snapshot_lines companion
        where companion.final_revision_id=v_revision_id and companion.work_event_id=v_transition.work_event_id
          and companion.pay_effect='NONE';
      if jsonb_array_length(v_paid_source_rows)>0 and v_transition.current_snapshot_id is null then
        raise exception 'WEEKLY_SOURCE_PAID_MATCH_PRIOR_PAY_BASIS_CHANGED' using errcode='40001';end if;
      -- Retained basis is lawful only because these actual current raw rows
      -- explicitly confirm SAME. Absence/CANCEL never enters this branch.
      select * into v_prior_snapshot
      from public.weekly_source_final_snapshot_lines
      where id=v_transition.prior_snapshot_id;`},
 {name:'transition immutable hash carries raw companion proof',before:"        )||case when v_exclude_cancel then jsonb_build_object(\n          'exclude_cancel',true,'cancel_exclusion_basis',v_cancel_basis) else '{}'::jsonb end",after:"        )||case when v_exclude_cancel then jsonb_build_object(\n          'exclude_cancel',true,'cancel_exclusion_basis',v_cancel_basis) else '{}'::jsonb end\n        ||case when jsonb_array_length(v_paid_source_rows)>0 and v_transition.current_snapshot_id=v_transition.prior_snapshot_id\n          then jsonb_build_object('pay_basis_kind','RETAINED_PAID_MATCH','paid_match_source_rows',v_paid_source_rows) else '{}'::jsonb end"},
 {name:'persist immutable retained pay basis provenance',before:'        ordinary_source_entitlement_projection_state\n      ) values (',after:'        ordinary_source_entitlement_projection_state,pay_basis_kind,paid_match_source_rows\n      ) values ('},
 {name:'actual retained basis is NO_CHANGE; real current raw rows remain present',before:"        case when v_outcome='NO_CHANGE' or v_exclude_cancel then 'NOT_APPLICABLE' else 'PENDING' end\n      ) returning id into v_transition_id;",after:"        case when v_outcome='NO_CHANGE' or v_exclude_cancel then 'NOT_APPLICABLE' else 'PENDING' end,\n        case when jsonb_array_length(v_paid_source_rows)>0 and v_transition.current_snapshot_id=v_transition.prior_snapshot_id\n          then 'RETAINED_PAID_MATCH' else 'CURRENT_SOURCE_SNAPSHOT' end,\n        case when jsonb_array_length(v_paid_source_rows)>0 and v_transition.current_snapshot_id=v_transition.prior_snapshot_id\n          then v_paid_source_rows else null end\n      ) returning id into v_transition_id;"},
 {name:'retained same basis cannot manufacture old-source invoice correction',before:"      if v_outcome='NO_CHANGE' then\n        v_unchanged_count:=v_unchanged_count+1;",after:"      if v_outcome='NO_CHANGE' then\n        v_unchanged_count:=v_unchanged_count+1;\n        if jsonb_array_length(v_paid_source_rows)>0 and v_transition.current_snapshot_id=v_transition.prior_snapshot_id then\n          continue; -- New genuine invoice companions are materialised below.\n        end if;"},
 {name:'native invoice companion producer after all pay transitions',before:"    if v_profile.profile_code='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1' then\n      -- The profile can serve Clients with or without source-fixed expenses.",after:`    for v_row in select companion.id from public.weekly_source_final_snapshot_lines companion
      where companion.final_revision_id=v_revision_id and companion.pay_effect='NONE'
      order by companion.upload_row_id,companion.id
    loop
      v_movement_id:=private.weekly_source_paid_match_invoice_movement_insert_v1(v_row.id,v_revision_id);
      if v_movement_id is not null then v_movement_count:=v_movement_count+1;end if;
    end loop;
    if v_profile.profile_code='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1' then
      -- The profile can serve Clients with or without source-fixed expenses.`}
].map(h=>({...h,before:h.before.replaceAll('\n',eol),after:h.after.replaceAll('\n',eol)}));
let after=before.toString();for(const h of hunks){assert.equal(after.split(h.before).length,2,h.name);after=after.replace(h.before,h.after);}
let reverse=after;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2,h.name);reverse=reverse.replace(h.after,h.before);}assert.equal(reverse,before.toString());
fs.writeFileSync(path.join(dir,'before/06_complete_fin4.sqlinc'),before);fs.writeFileSync(path.join(dir,'forward/06_complete_fin4.sqlinc'),after);
fs.writeFileSync(path.join(dir,'FIN4_REVERSE.json'),JSON.stringify({source_only:true,commit,current_path:currentPath,current_sha256:sha(current),selected_missing_before:input,before_sha256:sha(before),after_sha256:sha(after),hunks,whole_reverse_verified:true,native_calc_unchanged:true,source_fixed_expense_composition_implemented:false,invoice_producer_required:true,canonical_mounted:false,native_proof:false,pipeline_complete:false},null,2)+'\n');
console.log(JSON.stringify({signature:'private.weekly_source_finalise_engine_v1(jsonb,uuid,boolean,text)',hunks:hunks.length,after_sha256:sha(after),pipeline_complete:false}));
