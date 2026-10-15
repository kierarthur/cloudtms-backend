import fs from'node:fs';import path from'node:path';import assert from'node:assert/strict';import{execFileSync}from'node:child_process';import{fileURLToPath}from'node:url';
import{readCurrentNativeIndex,sha}from'../checklist-native-invoice-20261014/read-current-native-index.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');const commit='696bf40c1d1a4617cb0ae5a9f32963c3d45509e3',file='docs/healthroster-build/proposals/checklist-native-own-calendar-20261015/forward/wp_u3_ordinary_next_hold.sqlinc';
const b=execFileSync('git',['cat-file','blob',commit+':'+file],{cwd:repo,maxBuffer:4e6});assert.deepEqual(b,fs.readFileSync(path.join(repo,file)));
const hunks=[{label:'genuine treatment authority locals only',before:'  native_daily boolean:=false;v_identity_candidate uuid;v_identity_client uuid;daily_date date;daily_evidence jsonb;v_native_period jsonb;',after:'  native_daily boolean:=false;v_identity_candidate uuid;v_identity_client uuid;daily_date date;daily_evidence jsonb;v_native_period jsonb;\n  approval_revision_id uuid;approval_revision_id_after uuid;approval_revision jsonb;native_publication jsonb;'},
 {label:'exact native original approval and sealed clone publication proof, before any money',before:'  v_native_period:=private.bpay_next_work_period_proof_v1(work.id,revision.id,native_today);',after:`  begin
    approval_revision_id:=private.bpay_next_treatment_approval_revision_v1(revision.id);
  exception when sqlstate '23514' then
    return jsonb_build_object('known',false,'reason','NATIVE_TREATMENT_APPROVAL_UNAVAILABLE',
      'native_facts',facts||jsonb_build_object('native_qualification_sqlstate',sqlstate));end;
  select to_jsonb(a) into approval_revision from private.bpay_next_work_revision a where a.id=approval_revision_id and a.work_id=work.id;
  select to_jsonb(p) into native_publication from private.bpay_next_publication p where p.work_id=work.id and p.revision_id=revision.id;
  facts:=facts||jsonb_build_object('native_approval_revision_id',approval_revision_id,'native_approval_revision',approval_revision,
    'native_treatment_publication',native_publication);
  v_native_period:=private.bpay_next_work_period_proof_v1(work.id,revision.id,native_today);`},
 {label:'native proof replaces blanket rejection of legitimate treatment clones only',before:'    or revision.payment_treatment_approval_revision_id is not null or revision.source_head_id is not null or revision.source_event_id is not null',after:'    or revision.source_head_id is not null or revision.source_event_id is not null'},
 {label:'same actual native treatment proof and original approval/publication share the final frame',before:'  if v_native_period is distinct from private.bpay_next_work_period_proof_v1(work.id,revision.id,native_today)',after:`  begin
    approval_revision_id_after:=private.bpay_next_treatment_approval_revision_v1(revision.id);
  exception when sqlstate '23514' then
    raise exception 'WEEKLY_SOURCE_CHECKLIST_ANCHORED_REFRESH_REQUIRED' using errcode='40001';end;
  if approval_revision_id is distinct from approval_revision_id_after
    or approval_revision is distinct from (select to_jsonb(a) from private.bpay_next_work_revision a where a.id=approval_revision_id and a.work_id=work.id)
    or native_publication is distinct from (select to_jsonb(p) from private.bpay_next_publication p where p.work_id=work.id and p.revision_id=revision.id)
    or v_native_period is distinct from private.bpay_next_work_period_proof_v1(work.id,revision.id,native_today)`}];
let s=b.toString();for(const h of hunks){assert.equal(s.split(h.before).length,2,h.label);s=s.replace(h.before,()=>h.after);}let reversed=s;for(const h of [...hunks].reverse()){assert.equal(reversed.split(h.after).length,2,h.label);reversed=reversed.replace(h.after,()=>h.before);}assert.equal(reversed,b.toString());
fs.mkdirSync(path.join(here,'input'),{recursive:true});fs.mkdirSync(path.join(here,'forward'),{recursive:true});fs.writeFileSync(path.join(here,'input/wp_u3_ordinary_next_hold.sqlinc'),b);fs.writeFileSync(path.join(here,'forward/wp_u3_ordinary_next_hold.sqlinc'),s);
const n=readCurrentNativeIndex(repo),pins=[];for(const id of ['private.bpay_next_treatment_approval_revision_v1(uuid)','private.bpay_next_stage_treatment_revision_v1(uuid,uuid)','private.bpay_next_treatment_payload_assert_v1(uuid)']){const r=n.index.live.get(id);assert(r,id);const saved='input/'+id.replaceAll(/[^a-z0-9_.-]/g,'_')+'.sqlinc';fs.writeFileSync(path.join(here,saved),r.definition);pins.push({source_commit:n.sourceCommit,source_file:r.file,source_file_sha256:r.source_file_sha256,id,definition_sha256:sha(r.definition),saved_file:saved});}
fs.writeFileSync(path.join(here,'COMPLETE_FORWARD_AND_REVERSE.json'),JSON.stringify({source_only:true,native:false,records:[{source_commit:commit,source_path:file,input:'input/wp_u3_ordinary_next_hold.sqlinc',output:'forward/wp_u3_ordinary_next_hold.sqlinc',before_sha256:sha(b),after_sha256:sha(s),whole_reverse_equal:true,hunks}],native_inputs:pins},null,2)+'\n');console.log(JSON.stringify({whole_owners:1,hunks:4,whole_native_treatment_inputs:3,SQL_executed:false}));
