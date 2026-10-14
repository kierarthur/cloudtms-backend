// Full adjacent forwards and byte reversals against exact current native inputs.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
import {functionEvents} from '../cn-a0-isolation-source-followup/sql-function-source-index.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(process.argv[2]),stamp='14102026_1216',sha=s=>crypto.createHash('sha256').update(s).digest('hex'),coverage=JSON.parse(fs.readFileSync(path.join(here,'CURRENT_NATIVE_OWNER_COVERAGE.json'))),prior=JSON.parse(fs.readFileSync(path.join(here,'../a0-hidden-rr-source-implementation-20261014/CURRENT_SOURCE_REVERSE_AND_CENSUS.json'))),hunks=[],outputs=[],changes=[];
function change(file,text,before,after,label){const matches=[before,before.replaceAll('\n','\r\n')].filter((x,i,a)=>a.indexOf(x)===i).filter(x=>text.includes(x));assert.equal(matches.length,1,label);before=matches[0];assert.equal(text.split(before).length,2,label);if(before.includes('\r\n'))after=after.replaceAll('\n','\r\n');hunks.push({file,label,before,after});return text.replace(before,after);}
for(const p of prior.reversals){const source=fs.readFileSync(path.join(root,p.input),'utf8');assert.equal(sha(source),p.beforeSha256);let text=source;for(const h of p.hunks)text=change(p.input,text,h.before,h.after,'Retained predecessor '+h.label);
  if(p.input.endsWith('/04_complete_native_lock_fence.sqlinc')){
    const late=`  -- The exact owned frame/native CAS above supplies trusted intent. No new
  -- token can authorize or replace that admission. This precedes materialisation.
  perform private.timesheet_correction_a0_family_tokens_retain_v1(r.root_timesheet_id,v_members);
  perform private.timesheet_correction_a0_request_tokens_retain_v1(r);
`;
    text=change(p.input,text,late+"  return jsonb_build_object('contract','TIMESHEET_CORRECTION_A0_NATIVE_LOCK_FENCE_V1',", "  return jsonb_build_object('contract','TIMESHEET_CORRECTION_A0_NATIVE_LOCK_FENCE_V1',",'Move final CAS after all exact token phases');
    text=change(p.input,text,'  select array_agg(distinct gc.source_group_id order by gc.source_group_id) into v_groups_after',`  -- Native Candidate / complete I1 / existing row frame already obtained.
  -- PHYSICAL -> DEPENDENCIES -> genuine ALLOCATED, then final current CAS.
  perform private.timesheet_correction_a0_family_tokens_retain_v1(r.root_timesheet_id,v_members);
  perform private.timesheet_correction_a0_request_tokens_retain_v1(r);
  select array_agg(distinct gc.source_group_id order by gc.source_group_id) into v_groups_after`,'Atomic post-phase final native CAS');
  }else if(p.input.endsWith('/06_send_family_fence.sqlinc')){
    text=change(p.input,text,'  select count(*),jsonb_agg(to_jsonb(ctx))->0 into v_context_count,v_context',`  -- Bound acquisition can resolve an already-running ordinary probe. Recheck
  -- actual family/signature/chain after PHYSICAL and DEPENDENCY retention.
  select jsonb_agg(to_jsonb(s) order by s.requested_timesheet_id,s.family_version,s.family_timesheet_id)
    into v_after from public._pay_timesheet_rotation_scope(array[t.timesheet_id,(v_chain->>'root_timesheet_id')::uuid]) s;
  if v_after is distinct from v_before
    or public.timesheet_correction_chain_scope_v1(t.timesheet_id,false,32,100) is distinct from v_chain
    or private.timesheet_correction_signed_witness_v1(t.timesheet_id,false) is distinct from v_signed
    or exists(select 1 from public.timesheets_financials f where f.timesheet_id=t.timesheet_id) then
    raise exception 'CORRECTION_A0_TOKEN_DEPENDENCY_CHANGED' using errcode='40001';end if;
  select count(*),jsonb_agg(to_jsonb(ctx))->0 into v_context_count,v_context`,'SEND full native post-token CAS');
  }else if(p.input.endsWith('/08_complete_current_request.sqlinc')){
    text=change(p.input,text,'  -- The exact request exists before native overlap/derived triggers see P0.',`  -- P0 week is now genuinely allocated by the unchanged native owner. Retain
  -- its actual UUID before WAITING/P0 publication; no fabricated early UUID.
  if v_a0_transaction_fence then
    perform private.timesheet_correction_a0_request_tokens_retain_v1(v_req);
    if private.timesheet_correction_signed_witness_v1(v_id,false) is distinct from v_witness
      or public.timesheet_correction_chain_scope_v1(v_id,false,32,100) is distinct from v_chain
      or exists(select 1 from public.timesheets_financials f where f.timesheet_id=v_id)
      or exists(select 1 from public.timesheets t where t.timesheet_id=v_id
        and private.timesheet_correction_timesheet_authority_hash_v1(to_jsonb(t)) is distinct from
          private.timesheet_correction_timesheet_authority_hash_v1(v_req.frozen_original_json->'timesheet')) then
      raise exception 'CORRECTION_A0_TOKEN_DEPENDENCY_CHANGED' using errcode='40001';end if;
  end if;
  -- The exact request exists before native overlap/derived triggers see P0.`,'Post actual allocated P0-week retention and signed native CAS');
  }
  let reverse=text;for(const h of hunks.filter(h=>h.file===p.input).reverse()){assert.equal(reverse.split(h.after).length,2,'Unique reverse '+h.label);reverse=reverse.replace(h.after,h.before);}assert.equal(reverse,source,'Whole current file bytes');
  const file='sql/'+stamp+'_'+path.basename(p.input).replace(/\.sqlinc$/,'.complete-proposed.sqlinc');fs.writeFileSync(path.join(here,file),text);outputs.push({input:p.input,output:file,beforeBytes:Buffer.byteLength(source),beforeSha256:sha(source),afterBytes:Buffer.byteLength(text),afterSha256:sha(text),byteReverseEqual:true,hunks:hunks.filter(h=>h.file===p.input)});
  const before=new Map(functionEvents(source).events.filter(e=>e.kind==='CREATE').map(e=>[e.id,e]));for(const e of functionEvents(text).events.filter(e=>e.kind==='CREATE')){const b=before.get(e.id);assert.ok(b);if(sha(b.definition)!==sha(e.definition))changes.push({id:e.id,input:p.input,beforeSha256:sha(b.definition),afterSha256:sha(e.definition)});}
}
assert.equal(changes.length,6);assert.ok(changes.every(f=>!f.id.includes('bpay')));
const helpers=[stamp+'_a0_transaction_fence_helpers.sqlinc',stamp+'_a0_native_child_raw_intents.sqlinc'],newFunctions=helpers.flatMap(f=>functionEvents(fs.readFileSync(path.join(here,'sql',f),'utf8')).events.filter(e=>e.kind==='CREATE').map(e=>e.id));assert.equal(newFunctions.length,6);
const stage=fs.readFileSync(path.join(root,'supabase/repeatable/14102026_0519_healthroster_a0_current_native_composition_v1.sql'),'utf8'),acl=stage.slice(stage.indexOf('alter function private.timesheet_correction_canonical_input_assert_v1')).replace(/commit;\s*$/,'');
fs.writeFileSync(path.join(here,'sql',stamp+'_a0_owner_coverage_successor_v1.sql'),'-- UNMOUNTED SOURCE PROPOSAL. Native/canonical/ordinary/phased/Agency acceptance held.\nbegin;\n'+helpers.slice(0,1).map(f=>'\\ir '+f).join('\n')+'\n'+outputs.map(f=>'\\ir '+path.basename(f.output)).join('\n')+'\n\\ir '+helpers[1]+'\n'+acl+'commit;\n');
fs.writeFileSync(path.join(here,'COMPLETE_CURRENT_FORWARD_AND_REVERSE.json'),JSON.stringify({sourceOnly:true,currentRootCommit:coverage.currentRootCommit,nativeRun:false,reversals:outputs,changedCurrentOwners:changes,newPrivateFunctions:newFunctions,newPublicOrServiceSignatures:[],newPrivateConcurrencyTables:['private.timesheet_correction_a0_transaction_fences'],existingPublicAndPrivateACLReassertionPreserved:true,nativeFinancialAuthorityChanged:false,ordinaryAUTHUNABodiesChanged:false,bankBodiesChanged:false},null,2)+'\n');
console.log(JSON.stringify({completeCurrentFileReversals:outputs.length,changedCurrentOwners:changes.length,newPrivateFunctions:newFunctions.length,nativeRun:false}));
