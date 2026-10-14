import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const out=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(out,'../../../..'),current='5db7abee7925fffbeaf7c4e885cce809980fa079';
const sha=b=>createHash('sha256').update(b).digest('hex'),git=file=>execFileSync('git',['--no-replace-objects','show',current+':'+file],{cwd:root,maxBuffer:32*1024*1024});
const old=JSON.parse(fs.readFileSync(path.join(out,'../kept-pay-identical-rekey-source-reconciliation-20261014/SOURCE_INPUTS_AND_OWNER_PINS.json'),'utf8'));
fs.mkdirSync(path.join(out,'before'),{recursive:true});fs.mkdirSync(path.join(out,'forward'),{recursive:true});const changes=[];
function compose(name,leaf,sourceHunks){const pin=old.complete_existing_owners.find(x=>x.name===name);assert.ok(pin);const raw=git(pin.file),event=functionEvents(raw.toString('utf8')).events.find(x=>x.kind==='CREATE'&&x.id===pin.id);assert.ok(event);assert.equal(sha(event.definition),pin.definition_sha256);
 const before=event.definition,newline=before.includes('\r\n')?'\r\n':'\n';let after=before;const hunks=[];
 for(const [anchor,addition]of sourceHunks){const normalizedAnchor=anchor.replaceAll('\n',newline),normalizedAddition=addition.replaceAll('\n',newline);
  assert.equal(after.split(normalizedAnchor).length,2,'Exactly one current caller hunk '+name);after=after.replace(normalizedAnchor,normalizedAddition);hunks.push({before:normalizedAnchor,after:normalizedAddition});}
 let reverse=after;for(const h of hunks.toReversed()){assert.equal(reverse.split(h.after).length,2);reverse=reverse.replace(h.after,h.before);}assert.equal(reverse,before);
 const e=functionEvents(after).events;assert.equal(e.length,1);assert.equal(e[0].id,pin.id);
 fs.writeFileSync(path.join(out,'before',leaf),before+'\n');fs.writeFileSync(path.join(out,'forward',leaf),after+'\n');
 changes.push({signature:pin.id,input_commit:current,input_file:pin.file,input_file_sha256:sha(raw),input_definition_sha256:sha(before),after_definition_sha256:sha(after),before_file:'before/'+leaf,forward_file:'forward/'+leaf,hunks,whole_definition_reverse:true,existing_acl_signature_unchanged:true});
}
const contextAnchor=`        order by event.created_at_utc,event.id limit 1;`;
compose('public.weekly_source_upload_context_v1','02_complete_upload_context.sqlinc',[[contextAnchor,`        order by event.created_at_utc,event.id limit 1;
        -- Primary03 section6.1: unmatched finalised external keys use one
        -- native exact-tuple/absent-old-key proof, not schedule compatibility.
        if v_prior_work_event_id is null then
          begin
            v_prior_work_event_id:=private.weekly_source_identical_rekey_prior_v1(v_source_row.id);
          exception when sqlstate '55000' then
            if SQLERRM is distinct from 'WEEKLY_SOURCE_IDENTICAL_REKEY_AMBIGUOUS' then raise; end if;
            -- Keep discovery usable, but expose a real ambiguity. The native
            -- projection independently repeats the helper and refuses it.
            v_prior_candidate_count:=2; v_prior_work_event_id:=null;
          end;
          if v_prior_work_event_id is not null then
            select saved.contract_id into v_prior_contract_id
            from public.weekly_source_row_resolutions saved
            where saved.work_event_id=v_prior_work_event_id and saved.mapping_state='RESOLVED'
            order by saved.created_at_utc desc,saved.id desc limit 1;
          end if;
        end if;`]]);
const projectionAnchor=`      if v_prior_work_event_id is not null then`;
compose('public.weekly_source_projection_rows_apply_atomic_v1','03_complete_projection_rows.sqlinc',[
 ['  v_prior_work_event_id uuid;','  v_prior_work_event_id uuid;\n  v_identical_rekey_prior uuid;'],
 [projectionAnchor,`      -- A new external key may reuse an old event only through the native
      -- identical proof or the independently admitted explicit Office choice.
      -- The current protected choice above was re-proved, never caller flags.
      if v_source_authority and v_identity_kind='PROFILE_EXTERNAL_KEY'
        and not exists(select 1 from public.weekly_work_events key_event
          where key_event.first_source_group_id=v_group.id
            and key_event.source_format_profile_id=v_upload.source_format_profile_id
            and key_event.identity_kind='PROFILE_EXTERNAL_KEY'
            and key_event.profile_external_key=v_profile_external_key)
        and not (v_source_authority and v_protected_choice is not null
          and v_protected_choice=v_prior_work_event_id) then
        v_identical_rekey_prior:=private.weekly_source_identical_rekey_prior_v1(v_source_row.id);
        if v_identical_rekey_prior is distinct from v_prior_work_event_id
          or (v_identical_rekey_prior is not null and (select saved.contract_id from public.weekly_source_row_resolutions saved
            where saved.work_event_id=v_prior_work_event_id and saved.mapping_state='RESOLVED'
            order by saved.created_at_utc desc,saved.id desc limit 1) is distinct from v_contract_id) then
          raise exception 'WEEKLY_SOURCE_REKEY_MATCH_UNPROVEN' using errcode='40001';
        end if;
      end if;
      if v_prior_work_event_id is not null then`]
]);
const auxiliaryFiles=[
 'supabase/repeatable/15092026_1534_weekly_source_settings_expenses_rates_v1.sql',
 'broker/src/weekly-source/healthroster-normalise.js','broker/src/weekly-source/roster-summary-normalise.js',
 'broker/src/weekly-source/upload-staging-adapter.mjs','broker/src/weekly-source/upload-publication-owner.mjs',
 'broker/src/weekly-source/routes.js','broker/src/weekly-source/profiles.js',
 'supabase/repeatable/14102026_0821_healthroster_fin4_rebill_record_fields/01_complete_fin4.sqlinc'
];
const inputs=auxiliaryFiles.map(file=>{const bytes=git(file);return {commit:current,file,bytes:bytes.length,sha256:sha(bytes)};});
assert.equal(inputs.at(-1).sha256,'762454575b7a6844dba087fb4a76e7c46a374a863e5eac77af13645fd4f5af62');
const helper=fs.readFileSync(path.join(out,'forward','01_identical_rekey_prior.sqlinc'),'utf8'),events=functionEvents(helper).events;
assert.equal(events.length,1);assert.equal(events[0].id,'private.weekly_source_identical_rekey_prior_v1(uuid)');
assert.match(helper,/FINALISED_ROW_ONLY/);assert.match(helper,/COMPLETE_FILE_ROWS/);assert.match(helper,/v_row\.bounded_raw_columns_json->>'finalised_by' is distinct from v_row\.finalised_by/);
assert.match(helper,/if v_count>1 then/);assert.match(helper,/WEEKLY_SOURCE_IDENTICAL_REKEY_AMBIGUOUS/);
assert.match(helper,/if v_count=0 then return null/);assert.ok(!helper.includes('schedule_compatible'));
fs.writeFileSync(path.join(out,'REKEY_CURRENT_CALLERS_AND_INPUT_PINS.json'),JSON.stringify({source_only:true,unfinished_construction:true,current_sql_and_runtime_selected_commit:current,changes,auxiliary_inputs:inputs,planned_helper_signature:events[0].id,planned_helper_definition_sha256:sha(events[0].definition),source_definition_lexer_checks:true,current_signature_changes:0,full_unique_reversals:2,sql_compiled:false,native_proof:false,paid_overlap_default_credit_prepared_and_policy_completion:'REQUIRED_BEFORE_PACKET_FREEZE_OR_MOUNT'},null,2)+'\n');
console.log(JSON.stringify({source_only:true,unfinished:true,changed_current_callers:changes.length,new_planned_private_owners:1,full_reversals:2,selected_auxiliary_inputs:inputs.length,current}));
