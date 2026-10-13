-- WB24 own-row complete split counts; existing choice helper is the sole membership owner.
-- No money calculation, new command, public routine or business mutation.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_selection_memory_row_v1(p_kind text,p_row jsonb,p_scope jsonb,p_baseline uuid)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare v_scope jsonb:=p_scope;v_work uuid;v_case uuid;v_component uuid;v_total bigint;v_selected bigint;v_changed bigint;v_work_count bigint;v_case_count bigint;v_basis jsonb;
begin
 v_scope:=jsonb_set(v_scope,'{candidate_id}',p_row->'candidate_id');
 if p_kind='CANDIDATE_GROUPS' then
  if p_row->>'presentation_kind'='WORK' or p_row->>'group_kind'='WORK' then
   v_work:=coalesce(p_row->>'presentation_id',p_row->>'work_id',split_part(p_row->>'group_key',':',2))::uuid;
  elsif p_row->>'presentation_kind'='ROW' or p_row->>'group_kind'='ROW' then
   v_component:=coalesce(p_row->>'case_component_id',p_row->>'presentation_id',split_part(p_row->>'group_key',':',2))::uuid;
  else v_case:=coalesce(p_row->>'case_id',p_row->>'presentation_id',split_part(p_row->>'group_key',':',2))::uuid;end if;
 elsif p_kind='GROUP_LINES' then
  v_work:=(p_row->>'work_id')::uuid;v_component:=(p_row->>'case_component_id')::uuid;
  if v_work is null and v_component is null then return p_row||jsonb_build_object('default_selected',false,'selection_eligible',false,
   'new_changed',false,'choice_member_count','0','choice_selected_count','0','selection_basis',null);end if;
 end if;
 select count(*),count(*) filter(where l.selected),count(*) filter(where l.new_changed),
  count(*) filter(where l.work_id is not null),count(*) filter(where l.case_component_id is not null)
 into v_total,v_selected,v_changed,v_work_count,v_case_count
 from private.bpay_next_selection_memory_choices_v1(v_scope,p_baseline,null) l
 where (v_work is null or l.work_id=v_work) and (v_component is null or l.case_component_id=v_component)
  and (v_case is null or exists(select 1 from private.bpay_next_case_component cc where cc.id=l.case_component_id and cc.case_id=v_case))
  and (p_kind<>'GROUP_LINES' or l.work_id is null or l.approved_line_id=(p_row->>'approved_line_id')::uuid);
 -- Fixed native version/position/control operands for local Keep. Detailed
 -- per-segment equality remains in the private baseline and set comparison.
 if p_kind='CANDIDATES' then
  select jsonb_build_object('kind','PARENT','financial_view_revision',c.financial_view_revision::text) into v_basis
   from private.bpay_next_worker_control c where c.candidate_id=(p_row->>'candidate_id')::uuid;
 elsif p_kind='CANDIDATE_GROUPS' and v_work is not null then
  select jsonb_build_object('kind','WORK','work_id',w.id,'revision_id',w.current_revision_id,'applied_revision_id',w.applied_revision_id,
   'position_count',(select count(*)::text from private.bpay_next_position pos where pos.work_id=w.id),
   'position_updated_at_utc',(select max(pos.updated_at_utc) from private.bpay_next_position pos where pos.work_id=w.id),
   'approved_source_ex_vat',(select sum(pos.approved_source_ex_vat)::text from private.bpay_next_position pos where pos.work_id=w.id),
   'realised_source_ex_vat',(select sum(pos.realised_source_ex_vat)::text from private.bpay_next_position pos where pos.work_id=w.id),
   'held_source_ex_vat',(select sum(pos.held_source_ex_vat)::text from private.bpay_next_position pos where pos.work_id=w.id),
   'source_book_revision',(select coalesce(sum(pos.source_book_revision),0)::text from private.bpay_next_position pos where pos.work_id=w.id),
   'target_pay_channel',candidate.pay_method,'target_umbrella_id',candidate.umbrella_id,
   'target_umbrella_vat_chargeable',umbrella.vat_chargeable,'min_take_home_wtd',candidate.min_take_home_wtd::text,
   'control_sequence',coalesce((select max(sn.control_sequence) from private.bpay_next_snooze sn where sn.work_id=w.id),0)::text) into v_basis
  from private.bpay_next_work w join public.candidates candidate on candidate.id=w.candidate_id
  left join public.umbrellas umbrella on umbrella.id=candidate.umbrella_id where w.id=v_work;
 elsif v_component is not null then
  select jsonb_build_object('kind','CASE','case_component_id',cc.id,'component_revision',cc.component_revision::text,
   'rule_id',cc.rule_id,'approved_source_ex_vat',cc.approved_source_ex_vat::text,
   'funded_source_ex_vat',cc.funded_source_ex_vat::text,'recovered_source_ex_vat',cc.recovered_source_ex_vat::text,
   'paid_credit_source_ex_vat',cc.paid_credit_source_ex_vat::text,'written_off_source_ex_vat',cc.written_off_source_ex_vat::text,
   'active_payout_source_ex_vat',cc.active_payout_source_ex_vat::text,'active_recovery_source_ex_vat',cc.active_recovery_source_ex_vat::text,
   'resolution_state',cc.resolution_state,'resolved_channel',cc.target_pay_channel,
   'target_ex_vat',cc.target_ex_vat::text,'target_vat',cc.target_vat::text,'valuation_policy_id',cc.valuation_policy_id,
   'valuation_window_id',cc.valuation_window_id,'target_pay_channel',candidate.pay_method,'target_umbrella_id',candidate.umbrella_id,
   'target_umbrella_vat_chargeable',umbrella.vat_chargeable,'min_take_home_wtd',candidate.min_take_home_wtd::text,
   'control_sequence',coalesce((select max(sn.control_sequence) from private.bpay_next_snooze sn where sn.case_component_id=cc.id),0)::text) into v_basis
  from private.bpay_next_case_component cc join public.candidates candidate on candidate.id=cc.candidate_id
  left join public.umbrellas umbrella on umbrella.id=candidate.umbrella_id where cc.id=v_component;
 elsif v_work is not null then
  select jsonb_build_object('kind','COMPONENT','work_id',w.id,'revision_id',w.current_revision_id,
   'approved_line_id',l.id,'component_key',l.component_key,'applied_revision_id',position.applied_revision_id,
   'approved_source_ex_vat',position.approved_source_ex_vat::text,'realised_source_ex_vat',position.realised_source_ex_vat::text,
   'held_source_ex_vat',position.held_source_ex_vat::text,'realised_target_ex_vat',position.realised_target_ex_vat::text,
   'realised_target_vat',position.realised_target_vat::text,'held_target_ex_vat',position.held_target_ex_vat::text,
   'held_target_vat',position.held_target_vat::text,'target_pay_channel',candidate.pay_method,
   'target_umbrella_id',candidate.umbrella_id,'target_umbrella_vat_chargeable',umbrella.vat_chargeable,
   'min_take_home_wtd',candidate.min_take_home_wtd::text,
   'control_sequence',coalesce((select max(sn.control_sequence) from private.bpay_next_snooze sn
     where sn.work_id=w.id and (sn.component_key is null or sn.component_key=l.component_key)),0)::text) into v_basis
  from private.bpay_next_work w join private.bpay_next_approved_line l on l.revision_id=w.current_revision_id and l.id=(p_row->>'approved_line_id')::uuid
  join public.candidates candidate on candidate.id=w.candidate_id left join public.umbrellas umbrella on umbrella.id=candidate.umbrella_id
  left join private.bpay_next_position position on position.work_id=w.id and position.component_key=l.component_key where w.id=v_work;
 end if;
 if octet_length(coalesce(v_basis,'null')::text)>32768 then
  raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
 return p_row||jsonb_build_object('default_selected',v_total>0 and v_selected=v_total,
  'selection_eligible',v_total>0 and (p_kind<>'CANDIDATES' or (p_row->>'payable_count')::bigint>0),
  'new_changed',v_changed>0,'choice_member_count',v_total::text,'choice_selected_count',v_selected::text,
  'selection_basis',v_basis)||case when p_kind in ('CANDIDATES','CANDIDATE_GROUPS') then
  jsonb_build_object('selectable_item_count',v_work_count::text,'selectable_case_component_count',v_case_count::text) else '{}'::jsonb end;
end
$function$;
alter function private.bpay_next_selection_memory_row_v1(text,jsonb,jsonb,uuid) owner to postgres;
revoke all on function private.bpay_next_selection_memory_row_v1(text,jsonb,jsonb,uuid) from public,anon,authenticated,service_role;
commit;
