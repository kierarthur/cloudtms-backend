import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),dir=path.join(here,'mode-a-policy-construction'),prior=path.join(here,'issue-support-construction');
assert.equal(fs.existsSync(path.join(dir,'REVIEW_MANIFEST.json')),false);fs.mkdirSync(dir,{recursive:true});
const input='before/05_public_weekly_source_mode_a_dispatch_atomic_v1.sql.txt',before=fs.readFileSync(path.join(prior,input),'utf8'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
assert.equal(sha(before),'5cd589e5b903ad0dd2b09ecad1be083bff49af090042574c7a83c4289791e4ce');
const a=`    and exists(
      select 1 from public.weekly_timesheet_authority_resolutions authority
      where authority.source_cycle_id=v_cycle.id
        and authority.contract_id=resolution.contract_id
        and authority.work_date=source_row.work_date
    );`.replace(/\r?\n/g,before.includes('\r\n')?'\r\n':'\n');
const b=`    and (exists(
      select 1 from public.weekly_timesheet_authority_resolutions authority
      where authority.source_cycle_id=v_cycle.id
        and authority.contract_id=resolution.contract_id
        and authority.work_date=source_row.work_date
    ) or private.weekly_source_configured_agency_invoice_v1(resolution.client_id,v_group.id,
      v_group.agency_id,v_group.environment,source_row.work_date));`.replace(/\r?\n/g,before.includes('\r\n')?'\r\n':'\n');
assert.equal(before.split(a).length,2);const forward=before.replace(a,()=>b);assert.equal(forward.split(b).length,2);assert.equal(forward.replace(b,()=>a),before);
const output='19_complete_native_mode_a_dispatch_with_preset_selection.sqlinc';assert.equal(fs.existsSync(path.join(dir,output)),false);fs.writeFileSync(path.join(dir,output),forward);
fs.writeFileSync(path.join(dir,'DISPATCH_SELECTION_FULL_REVERSE.json'),JSON.stringify({source_only:true,native:false,input,input_sha256:sha(before),output,forward_sha256:sha(forward),hunks:[{before:a,after:b}],complete_reverse:true,signature_changes:[],reference_apply_preset_refusal_must_remain:true,independent_review_pending:true,mount_ready:false},null,2)+'\n');
console.log(JSON.stringify({source_only:true,native:false,forward_sha256:sha(forward),hunks:1,complete_reverse:true}));
