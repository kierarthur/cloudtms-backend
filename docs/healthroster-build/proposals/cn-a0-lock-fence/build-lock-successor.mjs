import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),old=path.resolve(here,'../cn-a0-native-normalisation'),lane=path.resolve(here,'../../../..');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),inputs=[],hunks=[];
const oldManifest=fs.readFileSync(path.join(old,'A0_SOURCE_MANIFEST.json'));
assert.equal(sha(oldManifest),'f04bd3656846b5ba1e395095b64689fef839ada1e4dcc0102aa7f030cb16709e');
for(const f of JSON.parse(oldManifest).files)assert.equal(sha(fs.readFileSync(path.join(lane,f.path))),f.sha256,f.path);
function read(file,expected){const b=fs.readFileSync(path.join(old,file));assert.equal(sha(b),expected,file);inputs.push({path:file,sha256:sha(b)});return b.toString('utf8');}
function change(s,a,b){assert.equal(s.split(a).length,2,a);hunks.push({anchor:a,replacement:b});return s.replace(a,b);}
function output(file,body,baseFile){fs.writeFileSync(path.join(here,file),body);
 const p=spawnSync('git',['diff','--no-index','--',path.join(old,baseFile),path.join(here,file)],{encoding:'utf8',windowsHide:true,maxBuffer:6*1024*1024});assert.ok([0,1].includes(p.status));
 const lines=p.stdout.split('\n');fs.writeFileSync(path.join(here,file+'.diff'),lines.slice(lines.findIndex(x=>x.startsWith('@@'))).join('\n'));
}
let approval=read('13102026_1127_approval.complete-proposed.sqlinc','e38e0df8046c04919f074117faad17d8dcfc06fcbc268f5947809ed692449cf0');
approval=change(approval,`    v_context:=v_context||jsonb_build_object('original_a0_clock_scope',
      private.timesheet_correction_a0_clock_scope_v1(v_request.id));`,
`    v_context:=v_context||jsonb_build_object('original_a0_clock_scope',
      private.timesheet_correction_a0_clock_scope_v1(v_request.id),
      'original_a0_native_input_witness',private.timesheet_correction_a0_native_input_witness_v1(v_request.id));`);
approval=change(approval,'  v_claim jsonb; v_batch_result record; v_reversal_snapshot jsonb;',
 '  v_claim jsonb; v_batch_result record; v_reversal_snapshot jsonb; v_a0_lock_fence jsonb;');
approval=change(approval,`  perform private.timesheet_correction_actor_assert_v1(p_actor_user_id);
  select request.* into strict v_request from private.timesheet_correction_requests request where request.id=p_request_id;
  perform 1 from public.invoices`,
`  perform private.timesheet_correction_actor_assert_v1(p_actor_user_id);
  select request.* into strict v_request from private.timesheet_correction_requests request where request.id=p_request_id;
  if v_request.state='WAITING' and v_request.origin='SIGNED_TIMESHEET_CORRECTION'
    and v_request.reversed_financial_id is null
    and v_request.frozen_original_json#>>'{timesheet,sheet_scope}'='WEEKLY' then
    -- Immutable request branch, never inferred from a concurrent current FIN.
    -- Complete native gate/I-1 precedes every established business row lock.
    v_a0_lock_fence:=private.timesheet_correction_a0_native_lock_fence_v1(v_request.id,p_actor_user_id);
  end if;
  perform 1 from public.invoices`);
output('13102026_2034_approval.complete-proposed.sqlinc',approval,'13102026_1127_approval.complete-proposed.sqlinc');
let scope=read('13102026_2002_a0_clock_scope.sqlinc','638f2dd037694c4cedf5a8aba015ab943260d4c9d174c305a2901589980eb300');
scope=change(scope,`      where e.removed_at_utc is null and (e.owning_timesheet_id=v_ts.timesheet_id
        or (e.owning_timesheet_id=v_request.replacement_timesheet_id`,
`      where e.removed_at_utc is null and (e.owning_timesheet_id=v_ts.timesheet_id
        or (e.owning_timesheet_id is null and e.workflow_id in(
          select w.id from public.candidate_submission_workflows w
          where w.target_timesheet_id in(v_ts.timesheet_id,v_request.replacement_timesheet_id)
            or w.anchor_timesheet_id in(v_ts.timesheet_id,v_request.replacement_timesheet_id)))
        or (e.owning_timesheet_id=v_request.replacement_timesheet_id`);
output('13102026_2034_clock_scope.complete-proposed.sqlinc',scope,'13102026_2002_a0_clock_scope.sqlinc');
let producer=read('native-correction-snapshot-producers.mjs','b1b7fb3e31b4745efe559e1a65eee0c1a499a07c4ae2474acf2edc67536cb585');
producer=change(producer,`    const ctx=rows[0],ts=ctx?.out_timesheet;`,
`    const ctx=rows[0],ts=ctx?.out_timesheet;
    if(prepare.original_materialisation_required===true&&expectedTs.sheet_scope==='WEEKLY'){
      const w=a.original_a0_native_input_witness;
      required(object(w)&&Object.keys(w).length===7&&['contract','request_id','contexts','weekly_contexts','import_contexts','validations','sha256'].every(k=>own(w,k))&&
        w.contract==='TIMESHEET_CORRECTION_A0_NATIVE_INPUT_WITNESS_V1'&&same(w.request_id,a.request_id)&&
        typeof w.sha256==='string'&&/^[0-9a-f]{64}$/.test(w.sha256)&&Array.isArray(w.contexts)&&w.contexts.length===2&&
        new Set(w.contexts.map(c=>c?.effective_timesheet_id)).size===2,'CORRECTION_A0_NATIVE_INPUT_UNPROVED');
      const preparedContext=w.contexts.filter(c=>same(c?.effective_timesheet_id,id));
      required(preparedContext.length===1&&jsonEqual(ctx,preparedContext[0]),'CORRECTION_A0_NATIVE_INPUT_CHANGED');
    }`);
producer=change(producer,`      const cw=weekly[0].out_cw,contract=weekly[0].out_contract;`,
`      if(prepare.original_materialisation_required===true&&expectedTs.sheet_scope==='WEEKLY'){
        const saved=a.original_a0_native_input_witness.weekly_contexts;
        required(Array.isArray(saved)&&saved.length===2&&new Set(saved.map(c=>c?.timesheet_id)).size===2,
          'CORRECTION_A0_NATIVE_INPUT_UNPROVED');
        const row=saved.filter(c=>same(c?.timesheet_id,id));
        required(row.length===1&&jsonEqual(row[0],weekly[0]),'CORRECTION_A0_NATIVE_INPUT_CHANGED');
      }
      const cw=weekly[0].out_cw,contract=weekly[0].out_contract;`);
producer=change(producer,`      const validations=await deps.readWeeklyValidation(id);`,
`      if(prepare.original_materialisation_required===true&&expectedTs.sheet_scope==='WEEKLY'){
        const saved=a.original_a0_native_input_witness.import_contexts;
        required(Array.isArray(saved)&&saved.length===2&&new Set(saved.map(c=>c?.timesheet_id)).size===2,
          'CORRECTION_A0_NATIVE_INPUT_UNPROVED');
        const row=saved.filter(c=>same(c?.timesheet_id,id));
        required(row.length===1&&jsonEqual(row[0],{timesheet_id:id,shifts}),'CORRECTION_A0_NATIVE_INPUT_CHANGED');
      }
      const validations=await deps.readWeeklyValidation(id);`);
producer=change(producer,`      if(validations.length===2){`,
`      if(prepare.original_materialisation_required===true&&expectedTs.sheet_scope==='WEEKLY'){
        const saved=a.original_a0_native_input_witness.validations;
        required(Array.isArray(saved)&&saved.length===2&&new Set(saved.map(c=>c?.timesheet_id)).size===2,
          'CORRECTION_A0_NATIVE_INPUT_UNPROVED');
        const row=saved.filter(c=>same(c?.timesheet_id,id));
        required(row.length===1&&jsonEqual(row[0],{timesheet_id:id,rows:validations}),'CORRECTION_A0_NATIVE_INPUT_CHANGED');
      }
      if(validations.length===2){`);
output('native-correction-snapshot-producers.mjs',producer,'native-correction-snapshot-producers.mjs');
for(const file of ['native-daily-role-helpers.mjs','native-weekly-correction-dispatch.mjs']){
 const b=fs.readFileSync(path.join(old,file));inputs.push({path:file,sha256:sha(b)});fs.writeFileSync(path.join(here,file),b);
}
fs.writeFileSync(path.join(here,'SUCCESSOR_SOURCE_GUARD.json'),JSON.stringify({status:'SOURCE_REVIEW_PROPOSAL_NOT_NATIVE_ACCEPTED',
 frozenA0ManifestSha256:sha(oldManifest),inputs,hunks,
 newPrivateOwners:['private.timesheet_correction_a0_native_input_witness_v1(uuid)','private.timesheet_correction_a0_native_lock_fence_v1(uuid,uuid)'],
 retained:'Ordinary/existing-finance/Daily, complete stored sign-copy/3or2 member approval, exact replay/cleanup/notifications and old A0 clock calculator/source scope retained. New fence only immutable WAITING signed missing-weekly-finance request.',
 timeout:'Temporary1ms only around actual native Candidate/I-1 call; restored on success/error. Every later authority row acquisition NOWAIT. These proposed engineering bounds need root runtime/compatibility acceptance.',
 noNewLockKey:true,noSharedSourceEdits:true},null,2)+'\n');
console.log(JSON.stringify({generated:5,hunks:hunks.length,inputs:inputs.length}));
