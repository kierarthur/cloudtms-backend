import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),sha=x=>createHash('sha256').update(x).digest('hex'),proofs=[];
function emit(file,before,changes){let value=before;for(const h of changes){assert.equal(value.split(h.before).length-1,1,h.before);value=value.replace(h.before,()=>h.after);}
 let reverse=value;for(const h of [...changes].reverse()){assert.equal(reverse.split(h.after).length-1,1);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,before);
 fs.writeFileSync(path.join(out,file),value.trimEnd()+'\n');proofs.push({file,input_sha256:sha(before),output_sha256:sha(value),reverse_hunk_matches_entire_input_definition:true,
  changes:changes.map(h=>({before_sha256:sha(h.before),after_sha256:sha(h.after)}))});}
const sql=fs.readFileSync(path.join(out,'SAME_INSTALLED_OPERATIONAL_CERTIFICATE.sqlinc'),'utf8');
emit('FINAL_OPERATIONAL_CERTIFICATE.sqlinc',sql,[
 {before:"'native_execution_qualification','remote_gate_receipts','test_setup_closure','source_equivalence','qualification_sha256']",
  after:"'native_execution_qualification','remote_gate_receipts','test_setup_closure','source_equivalence','activation_scope','receipt_input_commit','qualification_sha256']"},
 {before:"native->>'effective_from_week_ending' is distinct from p_from::text",after:"native->>'effective_from_week_ending' is distinct from q#>>'{activation_scope,native_effective_from_week_ending}'"},
 {before:"native->>'effective_to_week_ending' is distinct from p_to::text",after:"native->>'effective_to_week_ending' is distinct from q#>>'{activation_scope,native_effective_to_week_ending}'"},
 {before:"(closure->>'week_ending')::date<p_from or (p_to is not null and (closure->>'week_ending')::date>p_to)",
  after:"(closure->>'week_ending')::date<(native->>'effective_from_week_ending')::date\n    or (native->>'effective_to_week_ending' is not null and (closure->>'week_ending')::date>(native->>'effective_to_week_ending')::date)"},
 {before:"and a.object_type='weekly_source_protected_test_journey' and a.object_id_text=closure->>'scope_id'\n      and a.action='WEEKLY_SOURCE_TEST_JOURNEY_CLOSED' and a.event_sequence_is_authoritative",
  after:"and a.object_id_text=closure->>'scope_id' and a.event_sequence_is_authoritative\n      and ((a.object_type='weekly_source_protected_test_journey' and a.action='WEEKLY_SOURCE_TEST_JOURNEY_CLOSED')\n        or (a.object_type='weekly_source_off_rule_test_journey' and a.action='WEEKLY_SOURCE_OFF_RULE_TEST_CLOSED'))"},
 {before:"  perform private.weekly_source_protected_execution_tree_assert_v2(q->'source_equivalence',executed.git_commit,r.git_commit);",
  after:`  if coalesce(q->>'receipt_input_commit','') !~ '^[0-9a-f]{40}$' then
    raise exception 'WEEKLY_SOURCE_PROTECTED_OPERATIONAL_GATES_REQUIRED' using errcode='55000'; end if;
  perform private.weekly_source_activation_window_binding_assert_v1(q->'activation_scope',native,closure,g.id,p_from,p_to);
  -- Installed execution is immutable. Only receipt-input Git data differs.
  perform private.weekly_source_protected_execution_tree_assert_v2(q->'source_equivalence',executed.git_commit,q->>'receipt_input_commit');`}
]);
const js=fs.readFileSync(path.join(out,'same-installed-operational-evidence.mjs'),'utf8');
emit('final-operational-evidence.mjs',js,[
 {before:"const fail=code=>{throw Error(code);}",after:"import {assertActivationWindowBinding} from './activation-window-binding.mjs';\nconst fail=code=>{throw Error(code);}"},
 {before:'setupClosure,sourceProof){',after:'setupClosure,sourceProof,activationScope){'},
 {before:"['agency_id','environment','database','source_group_id','effective_from_week_ending','effective_to_week_ending'].some(k=>nativeContext[k]!==context[k])",
  after:"['agency_id','environment','database','source_group_id'].some(k=>nativeContext[k]!==context[k])"},
 {before:'assertExecutionTreeProof(sourceProof,nativeContext.git_commit,context.git_commit);',
  after:'assertExecutionTreeProof(sourceProof,nativeContext.git_commit,sourceProof.installation_source_commit);'},
 {before:'sourceProof.installation_source_commit!==context.git_commit',after:"!/^[0-9a-f]{40}$/.test(sourceProof.installation_source_commit)"},
 {before:'setupClosure.week_ending<context.effective_from_week_ending',after:'setupClosure.week_ending<nativeContext.effective_from_week_ending'},
 {before:'(context.effective_to_week_ending!==null&&setupClosure.week_ending>context.effective_to_week_ending)',
  after:'(nativeContext.effective_to_week_ending!==null&&setupClosure.week_ending>nativeContext.effective_to_week_ending)'},
 {before:" const body={schema_version:operationalSchema,phase:",after:" assertActivationWindowBinding(activationScope,context,nativeContext,setupClosure);\n const body={activation_scope:activationScope,receipt_input_commit:sourceProof.installation_source_commit,schema_version:operationalSchema,phase:"},
 {before:" const bundle=loadCommittedBundle(root,release,context,'weeklySourceProtectedOperational');if(bundle===null)return [];",
  after:" if(!/^[0-9a-f]{40}$/.test(context.receiptInputCommit))fail('OPERATIONAL_RECEIPT_INPUT_COMMIT_INVALID');\n const bundle=loadCommittedBundle(root,release,{...context,gitCommit:context.receiptInputCommit},'weeklySourceProtectedOperational');if(bundle===null)return [];"},
 {before:"exact(row,['native_context','native_gate_inputs','remote_gate_inputs','setup_closure'],'OPERATIONAL_BUNDLE_SCOPE_INVALID');",
  after:"exact(row,['native_context','native_gate_inputs','remote_gate_inputs','setup_closure','activation_scope'],'OPERATIONAL_BUNDLE_SCOPE_INVALID');"},
 {before:'const source=proveCommittedSources(root,row.native_context.git_commit,context.gitCommit);',
  after:'const source=proveCommittedSources(root,row.native_context.git_commit,context.receiptInputCommit);'},
 {before:'unpack(row.remote_gate_inputs),row.setup_closure,source);',after:'unpack(row.remote_gate_inputs),row.setup_closure,source,row.activation_scope);'},
 {before:"'test_setup_closure','source_equivalence','qualification_sha256'],'OPERATIONAL_MANAGED_CERTIFICATE_INVALID');",
  after:"'test_setup_closure','source_equivalence','activation_scope','receipt_input_commit','qualification_sha256'],'OPERATIONAL_MANAGED_CERTIFICATE_INVALID');"},
 {before:"['agency_id','environment','source_group_id','effective_from_week_ending','effective_to_week_ending'].some(k=>native[k]!==q[k])",
  after:"['agency_id','environment','source_group_id'].some(k=>native[k]!==q[k])"},
 {before:'closure.week_ending<q.effective_from_week_ending',after:'closure.week_ending<native.effective_from_week_ending'},
 {before:'(q.effective_to_week_ending!==null&&closure.week_ending>q.effective_to_week_ending)',after:'(native.effective_to_week_ending!==null&&closure.week_ending>native.effective_to_week_ending)'},
 {before:'  const proof=proveCommittedSources(root,native.git_commit,context.gitCommit);',
  after:"  assertActivationWindowBinding(q.activation_scope,q,native,closure);\n  if(!/^[0-9a-f]{40}$/.test(q.receipt_input_commit))fail('OPERATIONAL_RECEIPT_INPUT_COMMIT_INVALID');\n  const proof=proveCommittedSources(root,native.git_commit,q.receipt_input_commit);"}
]);
fs.writeFileSync(path.join(out,'FINAL_CERTIFICATE_REVERSE.json'),JSON.stringify({source_only:true,no_raw_native_dates_relabelled:true,
 actual_installed_execution_preserved:true,no_third_install:true,no_native_remote_acceptance:true,proofs},null,2)+'\n');
