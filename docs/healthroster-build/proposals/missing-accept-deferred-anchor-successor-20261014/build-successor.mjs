import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..'),old=path.join(path.dirname(dir),'missing-accept-head-continuity-construction-20261014'),sha=b=>createHash('sha256').update(b).digest('hex');assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));const manifest=fs.readFileSync(path.join(old,'FROZEN_ARTIFACT_MANIFEST.json'));assert.equal(sha(manifest),'2d33b9542057ed971dbe57a60fdec1c946905a1c273ffdf5ba387b7ee97b9d5a');for(const p of JSON.parse(manifest).artifacts){const b=fs.readFileSync(path.join(old,p.file));assert.equal(b.length,p.bytes);assert.equal(sha(b),p.sha256);}
const before=fs.readFileSync(path.join(old,'forward/01_complete_current_removal.sqlinc'));assert.equal(sha(before),'768d81a741c8237c2527b1c3cbe72749ad3e85c7e9d7b31e5aec9a4c8a02825e');const eol=before.includes(Buffer.from('\r\n'))?'\r\n':'\n';let s=before.toString();const hunks=[];function hunk(name,b,a){b=b.replaceAll('\n',eol);a=a.replaceAll('\n',eol);assert.equal(s.split(b).length,2,name);s=s.replace(b,a);hunks.push({name,before:b,after:a});}
hunk('actual pending and deferred canonical locals','  v_chain jsonb;v_current_receipt jsonb;v_current_components jsonb;',`  v_chain jsonb;v_current_receipt jsonb;v_current_components jsonb;
  v_pending public.weekly_source_pending_entitlement_bundles%rowtype;v_deferred_canonical jsonb;`);
hunk('select real anchor publication receipt by the head digest',`  select count(*) into v_count from private.weekly_source_entitlement_publication_receipts receipt
    where receipt.decision_bundle_id=v_head.decision_bundle_id and receipt.bundle_revision=v_head.bundle_revision
      and receipt.request_digest=v_action.accepted_canonical_sha256;
  if v_count<>1 then raise exception 'WEEKLY_SOURCE_MISSING_ACCEPT_CURRENT_RECEIPT_UNBOUND' using errcode='55000';end if;
  select * into strict v_receipt from private.weekly_source_entitlement_publication_receipts receipt
    where receipt.decision_bundle_id=v_head.decision_bundle_id and receipt.bundle_revision=v_head.bundle_revision
      and receipt.request_digest=v_action.accepted_canonical_sha256;`,`  select count(*) into v_count from private.weekly_source_entitlement_publication_receipts receipt
    where receipt.decision_bundle_id=v_head.decision_bundle_id and receipt.bundle_revision=v_head.bundle_revision
      and receipt.request_digest=v_head.publication_receipt_digest;
  if v_count<>1 then raise exception 'WEEKLY_SOURCE_MISSING_ACCEPT_CURRENT_RECEIPT_UNBOUND' using errcode='55000';end if;
  select * into strict v_receipt from private.weekly_source_entitlement_publication_receipts receipt
    where receipt.decision_bundle_id=v_head.decision_bundle_id and receipt.bundle_revision=v_head.bundle_revision
      and receipt.request_digest=v_head.publication_receipt_digest;`);
hunk('independent anchor mode pending and whole native dual-canonical proof',`  if v_bundle.state is distinct from 'COMMITTED' or v_bundle.request_digest is distinct from v_action.accepted_canonical_sha256`,`  if to_jsonb(v_receipt) is distinct from (v_chain->'native_head_chain'->(jsonb_array_length(v_chain->'native_head_chain')-1)->'native_receipt') then
    raise exception 'WEEKLY_SOURCE_MISSING_ACCEPT_CURRENT_RECEIPT_UNBOUND' using errcode='55000';end if;
  if v_receipt.publication_mode='IMMEDIATE' then
    if v_receipt.pending_bundle_id is not null or v_receipt.request_digest is distinct from v_action.accepted_canonical_sha256 then
      raise exception 'WEEKLY_SOURCE_MISSING_ACCEPT_CURRENT_RECEIPT_UNBOUND' using errcode='55000';end if;
  elsif v_receipt.publication_mode='DEFERRED' then
    select * into strict v_pending from public.weekly_source_pending_entitlement_bundles where id=v_receipt.pending_bundle_id;
    v_deferred_canonical:=private.weekly_source_publication_request_canonical_v1(v_pending.request_json,'DEFERRED',v_pending.id);
    if v_pending.state is distinct from 'RELEASED'
      or v_pending.released_receipt_id is distinct from v_receipt.id
      or v_pending.released_receipt_digest is distinct from v_receipt.request_digest
      or v_pending.request_digest is distinct from v_receipt.request_digest
      or (v_pending.decision_bundle_id,v_pending.bundle_revision,v_pending.candidate_id,v_pending.decision_id,v_pending.decided_by_user_id)
        is distinct from (v_receipt.decision_bundle_id,v_receipt.bundle_revision,v_receipt.candidate_id,v_receipt.decision_id,v_receipt.decided_by_user_id)
      or v_pending.member_root_ids is distinct from v_receipt.member_root_ids
      or v_pending.member_family_booking_ids is distinct from v_receipt.member_family_booking_ids
      or v_pending.member_root_versions is distinct from v_receipt.member_root_versions
      or v_pending.proposed_head_ids is distinct from v_receipt.head_ids
      or v_pending.source_revision_digest is distinct from v_bundle.source_revision_digest
      or v_pending.contract_choice_digest is distinct from v_bundle.contract_choice_digest
      or v_pending.released_by_worker_id is distinct from v_receipt.released_by_worker_id
      or v_pending.released_by_worker_run_id is distinct from v_receipt.released_by_worker_run_id
      or private.weekly_source_publication_request_digest_v1(v_deferred_canonical) is distinct from v_receipt.request_digest
      or private.weekly_source_publication_request_canonical_v1(v_pending.request_json,'IMMEDIATE',null::uuid) is distinct from v_canonical then
      raise exception 'WEEKLY_SOURCE_MISSING_ACCEPT_CURRENT_RECEIPT_UNBOUND' using errcode='55000';end if;
  else raise exception 'WEEKLY_SOURCE_MISSING_ACCEPT_CURRENT_RECEIPT_UNBOUND' using errcode='55000';end if;
  if v_bundle.state is distinct from 'COMMITTED' or v_bundle.request_digest is distinct from v_action.accepted_canonical_sha256`);
hunk('remove obsolete immediate-only clause while retaining all common member bindings',"    or v_receipt.publication_mode is distinct from 'IMMEDIATE' or v_receipt.pending_bundle_id is not null\n    or v_receipt.member_root_ids is distinct from array[v_head.root_timesheet_id]",'    or v_receipt.member_root_ids is distinct from array[v_head.root_timesheet_id]');
hunk('retain actual deferred publication proof in the removal CAS result',`  if v_current_head.id is distinct from v_head.id then`,`  if v_receipt.publication_mode='DEFERRED' then
    v_result:=v_result||jsonb_build_object('deferred_acceptance_publication',jsonb_build_object(
      'pending_bundle_id',v_pending.id,'released_receipt_id',v_pending.released_receipt_id,
      'immediate_canonical',v_canonical,'deferred_canonical',v_deferred_canonical,
      'receipt_sha256',encode(v_receipt.request_digest,'hex')));
  end if;
  if v_current_head.id is distinct from v_head.id then`);
const after=Buffer.from(s);for(const h of [...hunks].reverse()){assert.equal(s.split(h.after).length,2,h.name);s=s.replace(h.after,h.before);}assert.deepEqual(Buffer.from(s),before);for(const side of ['before','forward'])fs.mkdirSync(path.join(dir,side),{recursive:true});fs.writeFileSync(path.join(dir,'before/01_complete_current_removal.sqlinc'),before);fs.writeFileSync(path.join(dir,'forward/01_complete_current_removal.sqlinc'),after);
fs.writeFileSync(path.join(dir,'COMPLETE_CURRENT_REVERSE.json'),JSON.stringify({source_only:true,finding:'P1-CONT1',predecessor_folder:'missing-accept-head-continuity-construction-20261014',predecessor_manifest_sha256:sha(manifest),before_bytes:before.length,before_sha256:sha(before),after_bytes:after.length,after_sha256:sha(after),unique_hunks:hunks.length,hunks,whole_reverse_verified:true,accepted_immediate_action_and_bundle_hash_checks_unchanged:true,no_new_signatures:true,canonical_mounted:false,native_proof:false},null,2)+'\n');
const commit='33927fff2f7b90b90241d746d9e35aa21c57bd07',files=['supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql','supabase/repeatable/17092026_0300_weekly_source_entitlement_publication_v1.sql','supabase/repeatable/17092026_0700_weekly_source_pending_entitlement_release_v1.sql'],pins=files.map(file=>{const b=execFileSync('git',['show',commit+':'+file],{cwd:root,maxBuffer:4e6});return {commit,file,bytes:b.length,sha256:sha(b)};});fs.writeFileSync(path.join(dir,'CURRENT_NATIVE_INPUT_PINS.json'),JSON.stringify({source_only:true,current_input_commit:commit,inputs:pins,chain_helper_from_frozen_predecessor:{file:'forward/00_missing_accept_head_chain.sqlinc',sha256:sha(fs.readFileSync(path.join(old,'forward/00_missing_accept_head_chain.sqlinc')))},canonical_mounted:false,native_proof:false},null,2)+'\n');console.log(JSON.stringify({complete_reversals:1,unique_hunks:hunks.length,after_sha256:sha(after),native_input_pins:pins.length}));
