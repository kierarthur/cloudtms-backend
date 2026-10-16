"""Finite native-owner prefix extraction. Writes this proposal only; no DB."""
from pathlib import Path
import sys,re,json,hashlib,subprocess
here=Path(__file__).resolve().parent
source=Path(sys.argv[1]).resolve()
reference=here/'reference';reference.mkdir(exist_ok=True)
base='supabase/repeatable/'
main=base+'06092026_1636_candidate_advanced_expense_component_policy_v1.sql'
completion=base+'06092026_2355_candidate_advanced_expense_completion_v1.sql'
targets=[
 ('010_remove',main,'private._candidate_expense_financial_remove_v1','component'),
 ('020_rebind',main,'public.candidate_expense_update_rebind_atomic_v1','service'),
 ('030_office_reject',main,'public.candidate_office_expense_category_reject_atomic_v1','office'),
 ('040_component',base+'24092026_2247_candidate_provisional_expense_carrier_lifecycle.sql','public.candidate_expense_component_action_atomic_v1','candidate'),
 ('050_whole',main,'public.candidate_whole_claim_action_atomic_v1','whole'),
 ('060_begin',main,'public.candidate_expense_update_begin_atomic_v1','branches'),
 ('070_submit',base+'08092026_0918_candidate_expense_update_root_snapshot_validation_v1.sql','public.candidate_expense_update_submit_atomic_v1','branches'),
 ('080_abort',main,'public.candidate_expense_update_abort_atomic_v1','abort'),
 ('090_manager_hold',main,'public.candidate_expense_update_manager_hold_v1','manager'),
 ('100_paper_begin',completion,'public.candidate_expense_paper_update_begin_atomic_v1','candidate'),
 ('110_category_resubmit',completion,'public.candidate_expense_category_resubmit_atomic_v1','resubmit'),
 ('120_recover',main,'public.candidate_expense_update_recover_expired_v1','recover')]
sha=lambda v:hashlib.sha256(v.encode()).hexdigest()
manifest=[]
for filename,input_path,name,kind in targets:
 text=(source/input_path).read_text(encoding='utf-8')
 start=re.search(r'(?im)^create or replace function '+re.escape(name)+r'\s*\(',text)
 assert start,name
 body_start=text.index('as $function$',start.start())+len('as $function$')
 end=text.index('$function$;',body_start)+len('$function$;')
 original=text[start.start():end]+'\n'
 acl='\n'.join(m.group(0) for m in re.finditer(r'(?im)^(?:alter function|revoke all on function|grant execute on function) '+re.escape(name)+r'\([\s\S]*?;',text))+'\n'
 assert 'owner to postgres' in acl and 'revoke all' in acl,name
 original+=acl
 (reference/(filename+'.sqlinc')).write_text(original,encoding='utf-8',newline='\n')
 edits=[]
 def insert(at,content):edits.append({'offset':at,'text':content})
 def enter(indent,workflow='p_workflow_id',candidate="(v_context->>'selected_candidate_id')::uuid",account="(v_context->>'account_id')::uuid",env='v_environment'):
  return indent+'-- HEALTH_EXPENSE_FAMILY_ENTRY: locks only, before native mutable row locks.\n'+indent+'perform private.timesheet_contract_expense_family_enter_v1(\n'+indent+'  '+env+','+workflow+','+candidate+','+account+');\n'
 matches=list(re.finditer(r'(?m)^( +)select[^\n]* into v_workflow(?:\n| )',original))
 if kind=='component':
  anchor=original.index('  select * into v_component')
  insert(anchor,"  -- HEALTH_EXPENSE_FAMILY_ENTRY: actual component discovery, no authority inferred.\n  perform private.timesheet_contract_expense_family_enter_v1(\n    w.environment,w.id,null,null) from public.candidate_expense_components e\n    join public.candidate_submission_workflows w on w.id=e.workflow_id\n    where e.expense_component_id=p_expense_component_id;\n")
 elif kind in('service','office','candidate','abort'):
  assert matches,name
  if kind in('service','office'):candidate=account='null'
  elif kind=='abort':
   candidate="case when p_service_recovery then null else (v_context->>'selected_candidate_id')::uuid end"
   account="case when p_service_recovery then null else (v_context->>'account_id')::uuid end"
  else:candidate="(v_context->>'selected_candidate_id')::uuid";account="(v_context->>'account_id')::uuid"
  m=matches[0]
  if kind in('office','candidate'):
   m=re.search(r'(?m)^( +)perform (?:pg_catalog\.)?pg_advisory_xact_lock\(',original);assert m,name
  insert(m.start(),enter(m.group(1),candidate=candidate,account=account))
 elif kind=='branches':
  assert len(matches)>=2,name
  for n,m in enumerate(matches[:2]):insert(m.start(),enter(m.group(1),candidate='null' if n==0 else "(v_context->>'selected_candidate_id')::uuid",account='null' if n==0 else "(v_context->>'account_id')::uuid"))
 elif kind=='whole':
  anchor=original.index('  perform pg_advisory_xact_lock(')
  insert(anchor,enter('  '))
 elif kind=='resubmit':
  m=re.search(r'(?m)^( +)perform pg_catalog\.pg_advisory_xact_lock\(',original);assert m
  insert(m.start(),enter(m.group(1),workflow='p_source_workflow_id'))
 elif kind=='manager':
  assert matches
  # Match the actual pending update/token before a locking-only read-ahead.
  prefix="""  -- HEALTH_EXPENSE_FAMILY_ENTRY: genuine existing manager token ownership.
  if not exists(select 1 from public.candidate_pending_expense_updates u
    join public.candidate_approval_requests a on a.id=u.approval_request_id and a.workflow_id=u.workflow_id
    join public.candidate_submission_workflows w on w.id=u.workflow_id
    where w.id=p_workflow_id and w.environment=private._candidate_assert_environment(p_environment)
      and u.state in('EDITING','RENDERING') and a.state='PENDING'
      and a.token_hash=decode(lower(p_approval_token_hash_hex),'hex')) then return null;end if;
"""
  insert(matches[0].start(),prefix+enter('  ',candidate='null',account='null',env='p_environment'))
 elif kind=='recover':
  # Preserve the original bounded expiry/ID selection and loop order. Lock all
  # selected Candidates before any per-workflow family or business row locks.
  declaration='  v_terminal_state text;'
  assert original.count(declaration)==1
  insert(original.index(declaration)+len(declaration),'\n  v_family_targets jsonb;')
  start_loop=original.index('  for v_target in\n')
  end_select=original.index('\n  loop',start_loop)
  selection=original[start_loop+len('  for v_target in\n'):end_select]
  selection=selection.replace('select update_row.update_id,update_row.workflow_id',
   'select update_row.update_id,update_row.workflow_id,\n      row_number() over(order by update_row.expires_at_utc,update_row.update_id) ordinal',1)
  freeze="""  -- HEALTH_EXPENSE_FAMILY_ENTRY: same original bounded selection, immutable local list.
  select coalesce(jsonb_agg(to_jsonb(selected) order by selected.ordinal),'[]'::jsonb) into v_family_targets
  from (
"""+selection+"""
  ) selected;
  perform private.timesheet_contract_expense_families_enter_v1(v_environment,
    array(select (item->>'workflow_id')::uuid from jsonb_array_elements(v_family_targets) item),null,null);
"""
  # Replacement is recorded as exact old/new span, unlike pure inserted hooks.
  edits.append({'offset':start_loop,'remove':original[start_loop:end_select],
    'text':freeze+"  for v_target in\n    select (item->>'update_id')::uuid update_id,(item->>'workflow_id')::uuid workflow_id\n    from jsonb_array_elements(v_family_targets) with ordinality rows(item,ordinal) order by ordinal"})
 else:raise AssertionError(kind)
 result=original
 for edit in sorted(edits,key=lambda e:e['offset'],reverse=True):
  at=edit['offset'];remove=edit.get('remove','');assert result[at:at+len(remove)]==remove
  result=result[:at]+edit['text']+result[at+len(remove):]
 (here/'forward'/(filename+'.sqlinc')).write_text(result,encoding='utf-8',newline='\n')
 manifest.append({'name':name,'kind':kind,'input':input_path,'input_file_sha256_lf':sha(text),'reference':'reference/'+filename+'.sqlinc',
  'original_complete_sha256_lf':sha(original),'forward':'forward/'+filename+'.sqlinc','forward_sha256_lf':sha(result),'edits':edits})
record={'root_readonly_path':str(source),'root_observed_head':subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD'],text=True).strip(),
 'qualification':'SOURCE_ONLY_NATIVE_NOT_RUN','targets':manifest}
(here/'NATIVE_OWNER_PREFIXES.json').write_text(json.dumps(record,indent=2)+'\n',encoding='utf-8',newline='\n')
print('Prepared '+str(len(manifest))+' exact native owners; no source root write or DB call')
