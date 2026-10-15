import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),commit='7351816ce8cfecd29fcf7dbdffbee60ca46c1791',native='fa66183f01913e86651e82816f5e88f97e07123c';
const sha=b=>createHash('sha256').update(b).digest('hex'),get=(c,f)=>execFileSync('git',['show',c+':'+f],{cwd:root,maxBuffer:6e6}),write=(f,b)=>fs.writeFileSync(path.join(dir,f),b);
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));for(const f of ['input','before','forward'])fs.mkdirSync(path.join(dir,f),{recursive:true});
const source='docs/healthroster-build/proposals/paid-shift-match-writer-none-construction-20261014/forward/05_complete_projection_rows.sqlinc',bytes=get(commit,source),before=bytes.toString();assert.deepEqual(bytes,fs.readFileSync(path.join(root,source)));write('before/01_complete_projection_rows.sqlinc',bytes);
const eol=before.includes('\r\n')?'\r\n':'\n',nl=s=>s.replaceAll('\n',eol);
const decl='  v_paid_choices jsonb;v_paid_recheck jsonb;v_paid_choice jsonb;v_paid_inputs jsonb;v_pay_effect text;v_paid_choice_id bigint;';
const reset="    v_paid_choice:=null;v_paid_inputs:=null;v_pay_effect:='NORMAL';v_paid_choice_id:=null;";
const choice="          v_pay_effect:=v_paid_choice->>'pay_effect';v_paid_choice_id:=(v_paid_choice->>'choice_id')::bigint;";
const hashTuple=nl(`              'start_at_local',v_source_row.start_at_local,'end_at_local',v_source_row.end_at_local
            )
          );
        end if;
        if v_identity_kind='SCHEDULE_TUPLE' and v_compatible_count=1 then`);
const hunks=[
 {name:'row-local finite NHSP separate intent',before:decl,after:decl+eol+'  v_nhsp_paid_separate boolean;'},
 {name:'every physical row resets separate intent',before:reset,after:reset+eol+'    v_nhsp_paid_separate:=false;'},
 {name:'derive separate only from existing native immutable choice after its full current proof',before:choice,after:choice+nl(`
          if v_identity_kind='SCHEDULE_TUPLE' and v_pay_effect='NORMAL' then
            if v_profile.profile_code<>'NHSP_FINAL_BACKING_V1'
              or v_publication.authority_scope_kind<>'NHSP_REPORT_SCOPE'
              or v_upload.purpose<>'ORDINARY' or v_group.source_family<>'NHSP'
              or v_source_row.row_finalisation_state<>'SOURCE_WORKED'
              or v_source_row.source_shift_charge_pence is null or v_source_row.source_shift_charge_pence<0
              or v_paid_choice_id is null or v_prior_work_event_id is not null then
              raise exception 'WEEKLY_SOURCE_PAID_OVERLAP_SEPARATE_SCOPE_INVALID' using errcode='55000';end if;
            -- The actual paid-choice owner already proved actor/Contract,
            -- paid target, no protection, full facts and I-1 membership. The
            -- next NHSP admission stage must also prove its real no-reversal
            -- chain. No caller boolean may create this distinct event.
            v_nhsp_paid_separate:=true;
          end if;`)},
 {name:'explicit proved Separate disambiguates the compatible set without choosing an old event',before:'          if v_compatible_count>1 then',after:'          if v_compatible_count>1 and not v_nhsp_paid_separate then'},
 {name:'choice-owned stable uniqueness supplements genuine native schedule only for Separate',before:hashTuple,after:nl(`              'start_at_local',v_source_row.start_at_local,'end_at_local',v_source_row.end_at_local
            )||case when v_nhsp_paid_separate then
              -- This is the persisted native Office choice identity. It
              -- creates a distinct event without rewriting any old key,
              -- treating Ref Num as identity, or creating an economic key.
              pg_catalog.jsonb_build_object('office_separate_choice_id',v_paid_choice_id,
                'office_separate_target_work_event_id',v_paid_choice->>'paid_overlap_target_work_event_id')
              else '{}'::jsonb end
          );
        end if;
        if v_identity_kind='SCHEDULE_TUPLE' and v_compatible_count=1 and not v_nhsp_paid_separate then`)}];
let after=before;for(const h of hunks){assert.equal(after.split(h.before).length,2,h.name);after=after.replace(h.before,()=>h.after);}
let reverse=after;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2,h.name);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,before);write('forward/01_complete_projection_rows.sqlinc',after);
const inputs=[];for(const [c,f,saved] of [
 [native,'supabase/repeatable/04102026_0475_banking_pay_next_source_current_selector_v1.sql','input/04102026_0475_current_selector.sql'],
 [native,'supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql','input/15092026_1534_source_schema.sql'],
 [native,'broker/src/weekly-source/upload-staging-adapter.mjs','input/current_upload_staging_adapter.mjs'],
 [commit,'docs/healthroster-build/proposals/paid-match-legacy-native-consumer-construction-20261014/forward/06_complete_choice_admit.sqlinc','input/current_choice_admit.sqlinc'],
 [commit,'docs/healthroster-build/proposals/paid-match-legacy-current-viewer-consumer-20261015/forward/01_complete_choice_current.sqlinc','input/current_choice_and_projection_lock.sqlinc']]){const b=get(c,f);write(saved,b);inputs.push({commit:c,file:f,saved,bytes:b.length,sha256:sha(b)});}
write('COMPLETE_REVERSE_AND_INPUT_PINS.json',JSON.stringify({source_only:true,independent_review:'PENDING',commit,controlling_native_commit:native,source,before_sha256:sha(bytes),after_sha256:sha(after),signature:'public.weekly_source_projection_rows_apply_atomic_v1(uuid,uuid,jsonb)',hunks,whole_reverse:true,inputs,
 intent_derived_only_from_actual_native_current_choice:true,existing_I1_scope_group_cycle_family_and_schedule_lock_order_unchanged:true,ordinary_Same_ROSTER_C1_calculations_unchanged:true,existing_event_keys_never_rewritten:true,stable_uniqueness_uses_actual_immutable_office_choice_not_Ref_Num:true,new_public_private_signatures:[],new_stores_columns:0,
 current_NHSP_paid_choice_admission_implemented:false,remaining_construction:['actual NHSP current MOVEMENT/magnitude paid basis','complete native no-reversal chain proof','NHSP raw B NONE invoice origin and capture exclusions','report-scope Office/current/prepared/confirmation constructors','real finalised history replay/readback','final actual Source38/Report/native whole-owner composition and ACL/verifier union'],native_calls:0,canonical_mounted:false,whole_primary_complete:false},null,2)+'\n');
console.log(JSON.stringify({source_only:true,before:sha(bytes),after:sha(after),whole_reverse:1,hunks:hunks.length,inputs:inputs.length}));
