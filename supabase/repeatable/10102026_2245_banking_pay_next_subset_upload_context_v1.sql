-- WB23 SQ01 original subset upload context; installed2232 unchanged.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_choice_inputs_page_v1(p_run_id uuid,p_after bigint,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_run private.bpay_next_pay_run%rowtype;v_rows jsonb;v_n integer;
begin
 select src.* into strict v_run from private.bpay_next_pay_run src where id=p_run_id for share;
 if p_limit not between 1 and 100 or p_after<0 or v_run.accepted_choice_descriptor is null then
  raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
 select coalesce(jsonb_agg(jsonb_build_object('member_no',member_no::text,'candidate_id',candidate_id,
  'work_id',work_id,'expected_revision_id',expected_revision_id,'selection_mode',selection_mode,'skip_code',skip_code,
  'choice_state',choice_state,'component_page_count',component_page_count::text,'component_count',component_count::text)
  order by member_no),'[]'),count(*) into v_rows,v_n from (
   select member.member_no,member.candidate_id,member.work_id,member.expected_revision_id,member.selection_mode,member.skip_code,
    choice.selection_state as choice_state,choice.page_count as component_page_count,choice.component_count
   from private.bpay_next_run_choice_member member left join private.bpay_next_work_choice choice
    on choice.run_id=member.run_id and choice.work_id=member.work_id
   where member.run_id=p_run_id and member.member_kind='WORK'
    and member.member_no>coalesce(p_after,0) order by member.member_no limit p_limit) page;
 return jsonb_build_object('header',jsonb_build_object('run_id',p_run_id,
  'uploaded_choice_work_count',v_run.uploaded_choice_work_count::text,'selection_page_count',v_run.selection_page_count::text,
  'selection_count',v_run.selection_count::text,'exclusion_page_count',v_run.exclusion_page_count::text),
  'rows',v_rows,'complete',not exists(select 1 from private.bpay_next_run_choice_member
    where run_id=p_run_id and member_kind='WORK' and member_no>coalesce(p_after,0)+v_n));
end
$function$;
alter function private.bpay_next_choice_inputs_page_v1(uuid,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_choice_inputs_page_v1(uuid,bigint,integer) from public,anon,authenticated,service_role;
notify pgrst,'reload schema';
commit;
