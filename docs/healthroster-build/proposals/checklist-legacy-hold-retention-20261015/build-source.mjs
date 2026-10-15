// Adjacent current-recipe source successor. No shared file or SQL execution.
import fs from'node:fs';import path from'node:path';import assert from'node:assert/strict';import{execFileSync}from'node:child_process';import{fileURLToPath}from'node:url';import{sha}from'../checklist-current-native-module-recipe-20261015/read-root-source-index.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),base='../checklist-current-native-module-recipe-20261015/',commit='0d002966b022734f7b86c9a6f98d7a2332080721',records=[];
for(const dir of ['input','forward'])fs.mkdirSync(path.join(here,dir),{recursive:true});
const json=f=>JSON.parse(fs.readFileSync(path.join(here,base+f),'utf8')),m=json('MODULE_RECIPE.json'),originalPlan=json('MOUNT_PLAN.json');
function edit(file,output,hunks){const p='docs/healthroster-build/proposals/checklist-current-native-module-recipe-20261015/'+file,b=execFileSync('git',['cat-file','blob',commit+':'+p],{cwd:repo,maxBuffer:24e6});assert.deepEqual(b,fs.readFileSync(path.join(here,base+file)));const before=b.toString();let after=before;for(const h of hunks){assert.equal(after.split(h.before).length,2,h.label);after=after.replace(h.before,()=>h.after);}let reverse=after;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,before);const input='input/'+path.basename(output);fs.writeFileSync(path.join(here,input),b);fs.writeFileSync(path.join(here,output),after);records.push({commit,file:p,input,output,before_sha256:sha(b),after_sha256:sha(after),hunks,whole_reverse_equal:true});return after;}
const owner=name=>m.owners.find(o=>o.name===name).output;
const legacyRead=`
        v_legacy:=private.weekly_source_checklist_legacy_hold_review_v1((v_money_row->>'timesheet_id')::uuid,v_money_scope,v_actor);
        v_scope_money_facts:=v_scope_money_facts||jsonb_build_array(v_legacy->'native_facts');
        if v_legacy->'review_required'='true'::jsonb then
          v_scope_money:=v_scope_money||jsonb_build_array((v_legacy-'native_facts')||jsonb_build_object('native_financial_kind','LEGACY_REVIEW'));
          v_scope_open:=v_scope_open+1;
        end if;`;
edit(owner('private.weekly_source_healthroster_checklist_leaf_v1'),'forward/wp_u3_client_week_context.sqlinc',[
 {label:'private Legacy review frame variable only',before:"  v_money jsonb;v_money_row jsonb;",after:"  v_money jsonb;v_legacy jsonb;v_money_row jsonb;"},
 {label:'genuine Legacy hold review participates before older signed-week admission',before:"          v_scope_open:=v_scope_open+1;\n        end if;\n      end loop;",after:"          v_scope_open:=v_scope_open+1;\n        end if;"+legacyRead+"\n      end loop;"},
 {label:'same ordinary Legacy review for actual Agency submitted Timesheets',before:"      if v_money->'known'='true'::jsonb then v_financial_items:=v_financial_items||jsonb_build_array(v_money-'native_facts'); end if;\n    end loop;",after:"      if v_money->'known'='true'::jsonb then v_financial_items:=v_financial_items||jsonb_build_array(v_money-'native_facts'); end if;\n      v_legacy:=private.weekly_source_checklist_legacy_hold_review_v1((v_money_row->>'timesheet_id')::uuid,v_money_scope,v_actor);\n      v_financial_facts:=v_financial_facts||jsonb_build_array(v_legacy->'native_facts');\n      if v_legacy->'review_required'='true'::jsonb then\n        v_financial_items:=v_financial_items||jsonb_build_array((v_legacy-'native_facts')||jsonb_build_object('native_financial_kind','LEGACY_REVIEW'));end if;\n    end loop;"}
]);
const legacyItem=`        if raw->>'native_financial_kind'='LEGACY_REVIEW' then
          if raw->'review_required' is distinct from 'true'::jsonb
            or raw#>'{native_legacy_review_witness,scope}' is distinct from row_scope then
            raise exception 'WEEKLY_SOURCE_CHECKLIST_NATIVE_FINANCIAL_FRAME_UNAVAILABLE' using errcode='55000';end if;
          row_items:=row_items||jsonb_build_array(jsonb_build_object(
            'item_id','native-legacy-review:'||(raw#>>'{native_legacy_review_witness,timesheet_id}'),
            'item_kind','NATIVE_ORDINARY_FINANCIAL_REVIEW','scope',row_scope,'route',route,'worker',raw->'worker',
            'work_date',raw->'work_date','sheet_scope',raw->'sheet_scope','reason_code','NATIVE_LEGACY_PAY_HOLD_REVIEW',
            'status',raw->'status','hold',raw->'hold','subject','PAY','permitted_pay_subject',true,
            'action',case when view='ADMIN' then jsonb_build_object('kind','TIMESHEET','label','Review native pay hold in Timesheet ›',
              'native_allowed',true,'target',jsonb_build_object('timesheet_id',raw#>>'{native_legacy_review_witness,timesheet_id}')) end,
            'counted_timesheet_id',raw#>>'{native_legacy_review_witness,timesheet_id}','correction_binding',null,'submission_read',null,
            'source_information_witness',null,'native_source_witness',null,'native_invoice_witness',null,'native_invoice_decision_witness',null,
            'queries_parent_witness',null,'native_ordinary_money_witness',null,'native_legacy_review_witness',raw->'native_legacy_review_witness','_rank',35));
          open_count:=open_count+1;pay_count:=pay_count+1;completed:=false;pay_complete:=false;complete_text:=open_count::text||' items';
          continue;
        end if;
`;
edit(owner('public.weekly_source_checklist_v1'),'forward/wp_u3_native_checklist.sqlinc',[
 {label:'do not discard a leaf-retained older signed week before native financial merge',before:"      if own_week<>latest_week and completed then continue; end if;\n      checked_text:=case when checked then 'Checked' else (actual_count-checked_count)::text||' Timesheets not yet checked' end;",after:"      checked_text:=case when checked then 'Checked' else (actual_count-checked_count)::text||' Timesheets not yet checked' end;"},
 {label:'separate unknown-quantity Legacy review consumes genuine native witness before positive NEXT branch',before:"      for raw in select value from jsonb_array_elements(leaf->'native_financial_items') loop\n",after:"      for raw in select value from jsonb_array_elements(leaf->'native_financial_items') loop\n"+legacyItem},
 {label:'older signed-week closure now follows both actual native financial arms',before:"    fingerprints:=fingerprints||jsonb_build_array(jsonb_build_object('row_id',row_id,'native_financial_facts',financial_facts));",after:"    if route='SIGNED_TIMESHEETS' and own_week<>latest_week and completed then continue; end if;\n    fingerprints:=fingerprints||jsonb_build_array(jsonb_build_object('row_id',row_id,'native_financial_facts',financial_facts));"}
]);
const reviewDecoder=`  } else if (item.item_kind === 'NATIVE_ORDINARY_FINANCIAL_REVIEW') {
    const w=item.native_legacy_review_witness;
    closed(w,['owner','actor_user_id','scope','timesheet_id','financial_id','native_basis','amount_basis','counts_for_row_admission','read_proof_hash']);
    uuid(w.actor_user_id);bindScope(w.scope);uuid(w.timesheet_id);uuid(w.financial_id);uuid(item.counted_timesheet_id);
    if(w.owner!=='private.weekly_source_checklist_legacy_read_frame_v1'||!same(w.actor_user_id,actor.actor_user_id)||
      !sameScope(w.scope,item.scope)||!same(w.timesheet_id,item.counted_timesheet_id)||
      w.native_basis!=='NATIVE_CURRENT_TSFIN_PAY_ON_HOLD'||w.amount_basis!=='UNAVAILABLE'||w.counts_for_row_admission!==false||
      typeof w.read_proof_hash!=='string'||!HASH.test(w.read_proof_hash)||item.item_id!=='native-legacy-review:'+w.timesheet_id||
      !['SIGNED_TIMESHEETS','AGENCY_INVOICE'].includes(item.route)||item.reason_code!=='NATIVE_LEGACY_PAY_HOLD_REVIEW'||
      item.subject!=='PAY'||item.permitted_pay_subject!==true||item.status?.text!=='Decision needed'||item.status?.tone!=='warning'||
      item.hold?.authority?.owner!=='ORDINARY'||item.hold.authority.hash!==w.read_proof_hash||item.hold.authority.withholding_known!==false||
      item.hold.holds_pay!==null||item.hold.holds_pay_scope!==null||item.hold.withheld_amount_pence!==null||
      item.hold.holds_invoice!==null||item.hold.pending_decision!==null||item.correction_binding!==null||item.submission_read!==null||
      item.source_information_witness!==null||item.native_source_witness!=null||item.native_invoice_witness!=null||item.native_invoice_decision_witness!=null||
      item.native_ordinary_money_witness!=null||item.queries_parent_witness!=null||item.action===undefined||
      (item.action!==null&&(item.action.kind!=='TIMESHEET'||!same(item.action.target?.timesheet_id,w.timesheet_id)))) {
      fail('CHECKLIST_NATIVE_LEGACY_REVIEW_INVALID');
    }
`;
edit('forward/runtime/healthroster-checklist-source.mjs','forward/healthroster-checklist-source.mjs',[
 {label:'closed native Legacy witness field',before:"'native_invoice_decision_witness', 'native_ordinary_money_witness']);",after:"'native_invoice_decision_witness', 'native_ordinary_money_witness', 'native_legacy_review_witness']);"},
 {label:'separate native Legacy review item discriminant',before:"'NATIVE_INVOICE', 'NATIVE_ORDINARY_FINANCIAL'].includes(item.item_kind)",after:"'NATIVE_INVOICE', 'NATIVE_ORDINARY_FINANCIAL', 'NATIVE_ORDINARY_FINANCIAL_REVIEW'].includes(item.item_kind)"},
 {label:'strict actor/scope/identity/unknown-amount review witness decoder',before:"  } else if (item.item_kind === 'NATIVE_ORDINARY_FINANCIAL') {",after:reviewDecoder+"  } else if (item.item_kind === 'NATIVE_ORDINARY_FINANCIAL') {"},
 {label:'review witness cannot be smuggled into another item kind',before:"  if (item.item_kind !== 'NATIVE_ORDINARY_FINANCIAL' && item.native_ordinary_money_witness != null) fail('CHECKLIST_SOURCE_CONTRACT_INVALID');",after:"  if (item.item_kind !== 'NATIVE_ORDINARY_FINANCIAL_REVIEW' && item.native_legacy_review_witness != null) fail('CHECKLIST_SOURCE_CONTRACT_INVALID');\n  if (item.item_kind !== 'NATIVE_ORDINARY_FINANCIAL' && item.native_ordinary_money_witness != null) fail('CHECKLIST_SOURCE_CONTRACT_INVALID');"}
]);
// The adjacent complete recipe successor replaces only the named current
// owners and adds one private helper. No stale whole dependency is copied.
const plan=structuredClone(originalPlan),replacement=new Map([
 [owner('private.weekly_source_healthroster_checklist_leaf_v1'),'forward/wp_u3_client_week_context.sqlinc'],
 [owner('public.weekly_source_checklist_v1'),'forward/wp_u3_native_checklist.sqlinc'],
 ['forward/runtime/healthroster-checklist-source.mjs','forward/healthroster-checklist-source.mjs']
]);
for(const p of plan.targets){p.output=replacement.get(p.output)??base+p.output;}
const helperTarget='supabase/repeatable/healthroster_checklist_current/39_private_weekly_source_checklist_legacy_hold_review_v1.sqlinc';plan.targets.push({output:'forward/wp_u3_legacy_hold_review.sqlinc',target:helperTarget,kind:'NEW_COMPLETE_FUNCTION_OWNER'});
edit('forward/sql/15102026_0600_healthroster_checklist_current_v1.sql','forward/15102026_0600_healthroster_checklist_current_v1.sql',[
 {label:'new private review helper is declared before leaf and public Checklist',before:'\\ir healthroster_checklist_current/'+path.basename(owner('private.weekly_source_healthroster_checklist_leaf_v1')),after:'\\ir healthroster_checklist_current/39_private_weekly_source_checklist_legacy_hold_review_v1.sqlinc\n\\ir healthroster_checklist_current/'+path.basename(owner('private.weekly_source_healthroster_checklist_leaf_v1'))}
]);
plan.targets.find(p=>p.kind==='NEW_REPEATABLE_ROOT'&&p.target.includes('0600')).output='forward/15102026_0600_healthroster_checklist_current_v1.sql';
const newId='private.weekly_source_checklist_legacy_hold_review_v1(uuid,jsonb,uuid)';
for(const [file,output]of [['forward/sql/healthroster_checklist_current/central_acl_inventory.sqlinc','forward/central_acl_inventory.sqlinc'],['forward/sql/healthroster_checklist_current/final_acl_union.sqlinc','forward/final_acl_union.sqlinc'],['forward/verification/15092026_1534_weekly_source_acl_contract_v1.sql','forward/15092026_1534_weekly_source_acl_contract_v1.sql']]){
 const before=file.includes('verification')?"    'private.weekly_source_checklist_legacy_read_frame_v1(uuid,jsonb,uuid)',":"    ('private.weekly_source_checklist_legacy_read_frame_v1(uuid,jsonb,uuid)'),";
 const after=file.includes('verification')?"    '"+newId+"',\n"+before:"    ('"+newId+"'),\n"+before;
 edit(file,output,[{label:'add exactly the genuine private Legacy review helper; all existing private/service/table contracts retained',before,after}]);
 plan.targets.find(p=>p.output===base+file).output=output;
}
for(const p of plan.targets){const b=fs.readFileSync(path.join(here,p.output));p.sha256=sha(b);p.bytes=b.length;}
plan.predecessor_recipe_commit=commit;plan.current_root_commit=originalPlan.current_root_commit;plan.function_owners=50;plan.new_module_owners=39;plan.source_only=true;plan.canonical_mounted=false;plan.private_registry_signatures=575;plan.service_registry_signatures=147;plan.qualification.Legacy_positive_withholding=false;plan.finite_runtime_function_amendments=originalPlan.finite_runtime_function_amendments.map(r=>({...r,input:base+r.input,output:base+r.output}));plan.invoice_admission=base+originalPlan.invoice_admission;plan.protected_current_Source_A0=originalPlan.protected_current_Source_A0;
fs.writeFileSync(path.join(here,'CURRENT_RECIPE_SUCCESSOR.json'),JSON.stringify(plan,null,2)+'\n');fs.writeFileSync(path.join(here,'COMPLETE_OWNER_REVERSALS.json'),JSON.stringify({source_only:true,predecessor:commit,SQL_executed:false,records},null,2)+'\n');console.log(JSON.stringify({source_only:true,whole_owner_reversals:records.length,unique_hunks:records.reduce((n,r)=>n+r.hunks.length,0),mount_targets:plan.targets.length,function_owners:plan.function_owners,private_registry:575,service_registry:147}));
