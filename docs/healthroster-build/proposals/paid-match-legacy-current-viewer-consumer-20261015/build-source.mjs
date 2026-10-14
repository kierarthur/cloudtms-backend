import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..');
const commit='1a7ad79f9c4e2796fe067a05c253e39bd07482c7',native='fa66183f01913e86651e82816f5e88f97e07123c';
const sha=b=>createHash('sha256').update(b).digest('hex'),get=f=>execFileSync('git',['show',commit+':'+f],{cwd:root,maxBuffer:5e6});
const write=(f,b)=>fs.writeFileSync(path.join(dir,f),b);
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
for(const f of ['input','before','forward'])fs.mkdirSync(path.join(dir,f),{recursive:true});
const source='docs/healthroster-build/proposals/paid-match-legacy-native-consumer-construction-20261014/forward/07_complete_choice_current.sqlinc';
const bytes=get(source);assert.deepEqual(bytes,fs.readFileSync(path.join(root,source)));const before=bytes.toString();
write('before/01_complete_choice_current.sqlinc',bytes);
const eol=before.includes('\r\n')?'\r\n':'\n',nl=s=>s.replaceAll('\n',eol);
const old=nl(`  -- The only omitted fields identify the legitimate reader and the domain hash
  -- that includes that reader. Every economic/native identity remains exact.
  if (v_observed-'actor_user_id'-'inputs_sha256') is distinct from
       ((v_saved->'inputs')-'actor_user_id'-'inputs_sha256') then
    raise exception 'WEEKLY_SOURCE_PAID_OVERLAP_FACTS_CHANGED' using errcode='40001';end if;`);
const replacement=nl(`  -- Validate both complete raw reader proofs before comparing facts. The
  -- immutable confirmation keeps its original actor; current native facts are
  -- read under this viewer's independently proved authority, never that actor.
  -- Only the two literal actor-bound fields of the existing Legacy INPUT
  -- producer are omitted for equality. Its whole native financial/member
  -- payload, unknown fields and every NEXT page remain byte-for-byte JSONB
  -- facts. This is not a recursive actor/key scrub or a new paid classifier.
  for v_compare_side in 0..1 loop
    v_compare_inputs:=case when v_compare_side=0 then v_observed else v_saved->'inputs' end;
    v_compare_actor:=case when v_compare_side=0 then p_actor_user_id else v_choice.actor_user_id end;
    if jsonb_typeof(v_compare_inputs) is distinct from 'object'
      or v_compare_inputs->>'schema_version' is distinct from 'WEEKLY_SOURCE_PAID_OVERLAP_INPUTS_V1'
      or v_compare_inputs->>'actor_user_id' is distinct from v_compare_actor::text
      or jsonb_typeof(v_compare_inputs->'overlaps') is distinct from 'array'
      or v_compare_inputs->>'inputs_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_PAID_OVERLAP_INPUTS_V1',v_compare_inputs-'inputs_sha256'),'hex') then
      raise exception 'WEEKLY_SOURCE_PAID_OVERLAP_CHOICE_UNBOUND' using errcode='55000';end if;
    v_compare_overlaps:='[]'::jsonb;
    for v_compare_item in select value from jsonb_array_elements(v_compare_inputs->'overlaps') loop
      if jsonb_typeof(v_compare_item#>'{native_payment_facts,native_paid_basis}') is distinct from 'array' then
        raise exception 'WEEKLY_SOURCE_PAID_OVERLAP_CHOICE_UNBOUND' using errcode='55000';end if;
      v_compare_bases:='[]'::jsonb;
      for v_compare_basis in select value from jsonb_array_elements(v_compare_item#>'{native_payment_facts,native_paid_basis}') loop
        if v_compare_basis->>'schema_version'='WEEKLY_SOURCE_LEGACY_SIGNED_SOURCE_MEMBERSHIP_INPUT_V1' then
          if v_compare_basis->>'actor_user_id' is distinct from v_compare_actor::text
            or v_compare_basis->>'inputs_sha256' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
              'WEEKLY_SOURCE_LEGACY_SIGNED_SOURCE_MEMBERSHIP_INPUT_V1',v_compare_basis-'inputs_sha256'),'hex') then
            raise exception 'WEEKLY_SOURCE_PAID_OVERLAP_CHOICE_UNBOUND' using errcode='55000';end if;
          v_compare_basis:=v_compare_basis-'actor_user_id'-'inputs_sha256';
        end if;
        v_compare_bases:=v_compare_bases||jsonb_build_array(v_compare_basis);
      end loop;
      v_compare_item:=jsonb_set(v_compare_item,'{native_payment_facts,native_paid_basis}',v_compare_bases,false);
      v_compare_overlaps:=v_compare_overlaps||jsonb_build_array(v_compare_item);
    end loop;
    v_compare_inputs:=jsonb_set(v_compare_inputs,'{overlaps}',v_compare_overlaps,false)-'actor_user_id'-'inputs_sha256';
    if v_compare_side=0 then v_compare_observed:=v_compare_inputs;else v_compare_saved:=v_compare_inputs;end if;
  end loop;
  if v_compare_observed is distinct from v_compare_saved then
    raise exception 'WEEKLY_SOURCE_PAID_OVERLAP_FACTS_CHANGED' using errcode='40001';end if;`);
const decl='  v_revision public.weekly_source_final_revisions%rowtype;v_cycle public.weekly_source_cycles%rowtype;';
const hunks=[{name:'finite local comparison scratch only',before:decl,after:decl+nl(`
  v_compare_side integer;v_compare_actor uuid;
  v_compare_inputs jsonb;v_compare_item jsonb;v_compare_basis jsonb;v_compare_bases jsonb;
  v_compare_overlaps jsonb;v_compare_observed jsonb;v_compare_saved jsonb;`)},
 {name:'both real actor-bound native inputs verified before finite viewer-independent equality',before:old,after:replacement}];
let after=before;for(const h of hunks){assert.equal(after.split(h.before).length,2);after=after.replace(h.before,()=>h.after);}
let reverse=after;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,before);
write('forward/01_complete_choice_current.sqlinc',after);
const dependencies=[];
for(const file of [
 'docs/healthroster-build/proposals/paid-match-legacy-native-consumer-construction-20261014/forward/02_legacy_signed_source_membership.sqlinc',
 'docs/healthroster-build/proposals/source-next-paid-purpose-current-consumer-20261015/forward/04_complete_payment_facts.sqlinc',
 'docs/healthroster-build/proposals/paid-match-correct-final-choice-construction-20261014/forward/01_complete_overlap_inputs.sqlinc',
 'docs/healthroster-build/proposals/source-next-paid-purpose-current-consumer-20261015/input/private.weekly_source_office_authority_v1_uuid_text_uuid_uuid_date_.sqlinc',
 'docs/healthroster-build/proposals/source-next-paid-purpose-current-consumer-20261015/input/private_weekly_source_pay_actor_allowed_v1_uuid_.sqlinc']){
 const b=get(file);assert.deepEqual(b,fs.readFileSync(path.join(root,file)));const saved='input/'+path.posix.basename(file);write(saved,b);
 dependencies.push({commit,file,saved,bytes:b.length,sha256:sha(b)});
}
write('COMPLETE_REVERSE_AND_INPUT_PINS.json',JSON.stringify({source_only:true,independent_review:'PENDING',commit,native_source_commit:native,source,before_sha256:sha(bytes),after_sha256:sha(after),hunks,whole_reverse:true,
 existing_signatures:['private.weekly_source_paid_overlap_choice_current_v1(uuid,uuid)','private.weekly_source_paid_overlap_projection_lock_v1(uuid,uuid,uuid)'],changed_signature:'private.weekly_source_paid_overlap_choice_current_v1(uuid,uuid)',unchanged_projection_lock:true,
 dependencies,omitted_for_facts_comparison_only:[{path:'actor_user_id',existing_outer_rule:true},{path:'inputs_sha256',existing_outer_rule:true},{path:'overlaps[*].native_payment_facts.native_paid_basis[*].actor_user_id',only_schema:'WEEKLY_SOURCE_LEGACY_SIGNED_SOURCE_MEMBERSHIP_INPUT_V1',verified_against_actual_reader:true},{path:'overlaps[*].native_payment_facts.native_paid_basis[*].inputs_sha256',only_schema:'WEEKLY_SOURCE_LEGACY_SIGNED_SOURCE_MEMBERSHIP_INPUT_V1',whole_domain_rederived_before_omission:true}],
 immutable_saved_and_current_proofs_not_modified:true,current_real_viewer_authority_preserved:true,no_actor_substitution:true,no_new_classifier:true,new_private_signatures:[],native_calls:0,canonical_mounted:false},null,2)+'\n');
console.log(JSON.stringify({source_only:true,before:sha(bytes),after:sha(after),whole_reverse:1,hunks:hunks.length,dependencies:dependencies.length}));
