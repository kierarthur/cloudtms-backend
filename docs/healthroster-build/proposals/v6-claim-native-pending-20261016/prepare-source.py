"""Complete finite Invoice owner successors. No root/Physical/DB writes."""
from pathlib import Path
import json,hashlib
here=Path(__file__).resolve().parent;proposals=here.parent
(here/'reference').mkdir(exist_ok=True)
sources={'025_claim_placement.sqlinc':'v6-claim-native-rotation-20261016',
 '026_claim_current.sqlinc':'v6-invoice-native-20261016','027_claim_owned.sqlinc':'v6-invoice-native-20261016',
 '028_claim_lock.sqlinc':'v6-claim-native-rotation-20261016','030_claim_sync.sqlinc':'v6-invoice-native-20261016'}
changes=[]
for name,predecessor in sources.items():
 original=(proposals/predecessor/'forward'/name).read_text(encoding='utf-8');s=original;edits=[]
 def replace(a,b):
  global s
  assert s.count(a)==1,(name,a,s.count(a));s=s.replace(a,b,1);edits.append({'before':a,'after':b})
 if name.startswith('025'):
  replace('begin\n select a.family_root_id',"begin\n if private.timesheet_contract_claim_pending_v1(p_workflow_id,p_stored_timesheet_id,p_expense_component_id) is not null then return null;end if;\n select a.family_root_id")
  replace('and private.timesheet_contract_native_rotation_binding_v1(to_jsonb(p),false)))',
   'and (private.timesheet_contract_native_rotation_binding_v1(to_jsonb(p),false)\n        or private.timesheet_contract_native_allocation_binding_v1(to_jsonb(p),false)\n        or private.timesheet_contract_native_allocation_binding_v1(to_jsonb(p),true))))')
 elif name.startswith(('026','027')):
  replace('begin\n  v_result:=',"begin\n  if private.timesheet_contract_claim_pending_v1(p_workflow_id,p_stored_timesheet_id,null) is not null then return null;end if;\n  v_result:=")
 elif name.startswith('028'):
  replace("  if exists(select 1 from private.timesheet_contract_family_aliases a where a.timesheet_id=v_component.owning_timesheet_id) then\n    perform private.timesheet_contract_move_family_lock_v1(v_component.owning_timesheet_id);\n  end if;",
   "  perform private.timesheet_contract_expense_family_enter_v1(w.environment,w.id,null,null)\n    from public.candidate_submission_workflows w where w.id=v_component.workflow_id;")
  replace('  v_timesheet_id:=coalesce(',"  if private.timesheet_contract_claim_pending_v1(v_component.workflow_id,v_component.owning_timesheet_id,p_expense_component_id) is not null then\n    return null; -- Genuine unallocated owner; retain the immutable component owner/history.\n  end if;\n  v_timesheet_id:=coalesce(")
 else:
  replace('  v_before jsonb;','  v_before jsonb;\n  v_pending jsonb;\n  v_workflow_owner uuid;')
  replace("  v_document_generation:=case when v_workflow.state='FINALISED'\n    then greatest(v_workflow.generation-1,1) else v_workflow.generation end;\n",'')
  replace("  if exists(select 1 from private.timesheet_contract_family_aliases a where a.timesheet_id=v_workflow.target_timesheet_id) then\n    perform private.timesheet_contract_move_family_lock_v1(v_workflow.target_timesheet_id);\n  end if;\n  v_current_owner:=coalesce(private._candidate_expense_current_timesheet_id_v1(v_workflow.id,v_workflow.target_timesheet_id),v_workflow.target_timesheet_id);",
   "  perform private.timesheet_contract_expense_family_enter_v1(v_workflow.environment,v_workflow.id,null,null);\n  select * into strict v_workflow from public.candidate_submission_workflows where id=p_workflow_id;\n  v_document_generation:=case when v_workflow.state='FINALISED' then greatest(v_workflow.generation-1,1) else v_workflow.generation end;\n  v_pending:=private.timesheet_contract_claim_pending_v1(v_workflow.id,v_workflow.target_timesheet_id,null);\n  v_current_owner:=case when v_pending is not null then null else\n    coalesce(private._candidate_expense_current_timesheet_id_v1(v_workflow.id,v_workflow.target_timesheet_id),v_workflow.target_timesheet_id) end;")
  replace('  v_base_agency:=v_agency_state;\n  v_current_owner:=coalesce(private._candidate_expense_current_timesheet_id_v1(v_workflow.id,v_workflow.target_timesheet_id),v_workflow.target_timesheet_id);\n  for v_value in select * from private._candidate_expense_component_values_v1(v_workflow.id) loop',
   '  v_base_agency:=v_agency_state;\n  v_workflow_owner:=v_current_owner;\n  for v_value in select * from private._candidate_expense_component_values_v1(v_workflow.id) loop\n    v_current_owner:=v_workflow_owner;\n    v_agency_state:=v_base_agency;')
  replace('    if found then\n      v_current_owner:=coalesce(private.timesheet_contract_claim_placement_v1',
   '    if found then\n      v_pending:=private.timesheet_contract_claim_pending_v1(v_workflow.id,v_component.owning_timesheet_id,v_component.expense_component_id);\n      if v_pending is not null then\n        v_current_owner:=v_component.owning_timesheet_id;\n        v_fin:=null;v_timesheet:=null;\n        v_agency_state:=v_component.agency_authorisation_state;\n      else\n      v_current_owner:=coalesce(private.timesheet_contract_claim_placement_v1')
  replace("       else 'NOT_AUTHORISED' end;\n    end if;", "       else 'NOT_AUTHORISED' end;\n      end if;\n    end if;")
 (here/'reference'/name).write_text(original,encoding='utf-8',newline='\n');(here/'forward'/name).write_text(s,encoding='utf-8',newline='\n')
 changes.append({'predecessor_path':str((proposals/predecessor/'forward'/name).relative_to(proposals.parent.parent.parent)).replace('\\','/'),
  'reference':'reference/'+name,'forward':'forward/'+name,'before_sha256_lf':hashlib.sha256(original.encode()).hexdigest(),
  'after_sha256_lf':hashlib.sha256(s.encode()).hexdigest(),'edits':edits})
(here/'FINITE_REPLACEMENTS.json').write_text(json.dumps(changes,indent=2)+'\n',encoding='utf-8',newline='\n')
print(json.dumps({'complete_replacements':len(changes)}))
