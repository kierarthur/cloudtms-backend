import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),dir=path.join(here,'issue-support-construction');
assert.equal(fs.existsSync(path.join(dir,'REVIEW_MANIFEST.json')),false);
const input='docs/healthroster-build/proposals/agency-native-whole-week-validation-runtime-forward-20261014/construction/71_complete_current_mode_a_with_preset_refusal.sqlinc';
const before=fs.readFileSync(path.join(repo,input),'utf8'),eol=before.includes('\r\n')?'\r\n':'\n',native=s=>s.replace(/\r?\n/g,eol),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const oldManifest=JSON.parse(fs.readFileSync(path.join(repo,'docs/healthroster-build/proposals/agency-native-whole-week-validation-runtime-forward-20261014/REVIEW_MANIFEST.json')));assert.equal(sha(before),oldManifest.entries.find(x=>x.path==='construction/'+path.basename(input)).sha256);
const output='12_complete_native_mode_a_with_preset_reference_only.sqlinc',reverse='MODE_A_REFERENCE_FULL_REVERSE.json';
if(fs.existsSync(path.join(dir,output))){assert.ok(process.argv.includes('--refresh-unfrozen'));assert.equal(sha(fs.readFileSync(path.join(dir,output))),JSON.parse(fs.readFileSync(path.join(dir,reverse))).forward_sha256);}
let forward=before;const hunks=[];function replace(a,b){a=native(a);b=native(b);assert.equal(forward.split(a).length,2,a.slice(0,120));forward=forward.replace(a,()=>b);hunks.push({before:a,after:b});}
replace('  v_weekly_source_family record;','  v_weekly_source_family record;\n  v_agency_reference_targets uuid[];\n  v_agency_main_id uuid;\n  v_agency_publication_id uuid;');
replace('  -- Mode A/reference apply cannot feed a second validation writer for a preset.\n  perform private.weekly_source_agency_legacy_import_validation_admit_v1(p_import_id);',`  -- The same native import-review guard and reference engine remain below.
  -- A genuine Source bridge may carry preset reference-only work; all older
  -- imports retain their actual legacy preset refusal. No caller mode flag.
  v_agency_reference_targets:=private.weekly_source_agency_mode_a_reference_targets_v1(p_import_id,p_actor_user_id);
  if cardinality(v_agency_reference_targets)>0 then
    select (hi.parse_summary_json->>'weekly_source_projection_publication_id')::uuid into strict v_agency_publication_id
      from public.hr_imports hi where hi.id=p_import_id;
  end if;`);
replace("  where vm.mode = 'MODE_A'\n    and vr.timesheet_id is not null;\n\n  -- ✅ UPDATED: include pre_validated", "  where vm.mode = 'MODE_A'\n    and vr.timesheet_id is not null\n    and not(vr.timesheet_id=any(v_agency_reference_targets));\n\n  -- ✅ UPDATED: include pre_validated");
replace('  get diagnostics v_validations_upserted = row_count;',`  get diagnostics v_validations_upserted = row_count;

  -- Only native guard-selected units occur in tmp_val_rows. Preset rows never
  -- enter the LEGACY upsert or its named auto-authorise list. One actual main
  -- week is refreshed by the sole typed writer after genuine references.
  for v_agency_main_id in
    select distinct main_week.timesheet_id from tmp_val_rows vr
    join tmp_val_mode vm on vm.timesheet_id=vr.timesheet_id and vm.mode='MODE_A'
    join public.timesheets native_target on native_target.timesheet_id=vr.timesheet_id
    join public.contract_weeks main_week on main_week.contract_id=native_target.contract_id
      and main_week.week_ending_date=native_target.week_ending_date
      and coalesce(main_week.additional_seq,0)=0 and not main_week.is_adjustment
    where vr.timesheet_id=any(v_agency_reference_targets)
    order by main_week.timesheet_id
  loop
    perform public.weekly_source_agency_week_validation_refresh_v1(v_agency_main_id,v_agency_publication_id);
  end loop;`);
let restored=forward;for(const h of [...hunks].reverse()){assert.equal(restored.split(h.after).length,2);restored=restored.replace(h.after,()=>h.before);}assert.equal(restored,before);
fs.writeFileSync(path.join(dir,output),forward);fs.writeFileSync(path.join(dir,reverse),JSON.stringify({source_only:true,native:false,input,input_sha256:sha(before),output,forward_sha256:sha(forward),hunks,complete_reverse:true,shared_native_references_and_financial_hooks_retained:true,independent_review_pending:true,mount_ready:false},null,2)+'\n');
console.log(JSON.stringify({source_only:true,native:false,forward_sha256:sha(forward),hunks:hunks.length,complete_reverse:true}));
