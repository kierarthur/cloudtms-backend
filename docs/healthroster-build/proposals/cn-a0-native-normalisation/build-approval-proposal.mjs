import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const root=process.argv[2],here=path.dirname(fileURLToPath(import.meta.url));
const file='supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_approval.sqlinc';
const input=fs.readFileSync(path.join(root,file)),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
assert.equal(sha(input),'9d3129c7fdbb4ddf1b8ca5c2a195b3fcfa7ea661e4c4dc0a7f233eea77571fc1','controlling approval/sign-copy source changed');
let next=input.toString('utf8').replace(/\r\n/g,'\n');const hunks=[];
function change(a,b){assert.equal(next.split(a).length,2,a);hunks.push({anchor:a,replacement:b});next=next.replace(a,b);}
change('perform private.timesheet_correction_snapshot_identity_assert_v1(v_original,v_request.reversed_timesheet_id,',
 'perform private.timesheet_correction_canonical_input_assert_v1(v_original,v_request.reversed_timesheet_id,');
change(`    select to_jsonb(tf) into strict v_original from public.timesheets_financials tf
      where tf.timesheet_id=v_request.reversed_timesheet_id and tf.is_current;
  end if;`,
`    select to_jsonb(tf) into strict v_original from public.timesheets_financials tf
      where tf.timesheet_id=v_request.reversed_timesheet_id and tf.is_current;
    perform private.timesheet_correction_snapshot_identity_assert_v1(v_original,v_request.reversed_timesheet_id,
      v_request.reversed_version,v_request.candidate_id,v_request.client_id);
  end if;`);
change("perform private.timesheet_correction_snapshot_identity_assert_v1(p_prepared->'replacement_snapshot',v_p,v_version,",
 "perform private.timesheet_correction_canonical_input_assert_v1(p_prepared->'replacement_snapshot',v_p,v_version,");
change(`  v_leg:=private.timesheet_correction_leg_create_v1(v_request,'WHOLE_TIMESHEET_REVERSAL','[]'::jsonb,v_now);`,
`  select to_jsonb(tf) into strict v_after_fin from public.timesheets_financials tf where tf.timesheet_id=v_p and tf.is_current;
  perform private.timesheet_correction_snapshot_identity_assert_v1(v_after_fin,v_p,v_version,
    v_request.candidate_id,v_request.client_id);
  v_leg:=private.timesheet_correction_leg_create_v1(v_request,'WHOLE_TIMESHEET_REVERSAL','[]'::jsonb,v_now);`);
change(`  v_hash:=encode(extensions.digest(v_context::text,'sha256'),'hex');`,
`  if v_fin.id is null and v_original.sheet_scope::text='WEEKLY' then
    -- Exact server-owned scope participates in the native PREPARE/CAS hash.
    -- Missing A0 never silently changes Source/Rule/ordinary pricing origin.
    v_context:=v_context||jsonb_build_object('original_a0_clock_scope',
      private.timesheet_correction_a0_clock_scope_v1(v_request.id));
  end if;
  v_hash:=encode(extensions.digest(v_context::text,'sha256'),'hex');`);
const output=path.join(here,'13102026_1127_approval.complete-proposed.sqlinc');fs.writeFileSync(output,next);
const diff=spawnSync('git',['diff','--no-index','--',path.join(root,file),output],{encoding:'utf8',windowsHide:true,maxBuffer:4*1024*1024});assert.ok([0,1].includes(diff.status),diff.stderr);
const lines=diff.stdout.split('\n');fs.writeFileSync(path.join(here,'APPROVAL_ROOT.diff'),`--- a/${file}\n+++ b/${file}\n${lines.slice(lines.findIndex(l=>l.startsWith('@@'))).join('\n')}`);
const facts={status:'COMPLETE_NATIVE_APPROVAL_SUCCESSOR_NOT_INSTALLED',input:{file,sha256:sha(input)},output:{file:path.basename(output),sha256:sha(next)},hunks,
 newPrivateOwners:['private.timesheet_correction_canonical_input_assert_v1(jsonb,uuid,integer,uuid,uuid)',
  'private.timesheet_correction_a0_clock_scope_v1(uuid)'],
 integration:'Define both new private owners after existing strict assertion and before PREPARE/atomic owner, replace complete1127 approval include, union exact owner-only signatures in root registries; no public signature/route change.',
 retained:['same atomic unit','actual original stored reread/sign-copy','existing Original FIN hash fence','R0 targeted real queue/batch writer','postwrite reversal assertion','3/2 member native bulk authorisation','request/CAS/replay','P0 notification only','D1 existing round trip','SIGNED/AGENCY origin and real manager witnesses']};
fs.writeFileSync(path.join(here,'APPROVAL_SOURCE_GUARD.json'),JSON.stringify(facts,null,2)+'\n');
console.log(JSON.stringify({inputSha256:sha(input),outputSha256:sha(next),hunks:hunks.length}));
