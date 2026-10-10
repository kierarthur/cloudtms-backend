-- F-25 canonical immutable comparison INSERT staging.
-- Actual inserting owners hold/create the incident before this callback.
-- Plain parent read; all staging and its actual FK work occur before clock.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_query_comparison_row_stage_v1()
returns trigger language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare owned private.bpay_next_query_producer%rowtype;parent public.weekly_discrepancy_incidents%rowtype;gate jsonb;
begin
 if tg_op<>'INSERT' then raise exception using errcode='55000',message='BPAY_NEXT_QUERY_COMPARISON_INSERT_ONLY';end if;
 select p.* into owned from private.bpay_next_query_producer p where p.preparing_transaction_id=pg_current_xact_id();
 if not found then
  if exists(select 1 from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT') then
   raise exception using errcode='55000',message='BPAY_NEXT_QUERY_MATERIAL_WRITER_UNOWNED';end if;
  return null;
 end if;
 if owned.phase<>'BUSINESS_OPEN' or owned.owner_stack[cardinality(owned.owner_stack)] not in
    ('QUERY_SYNC','CANDIDATE_APP_SUBMIT','CANDIDATE_MATERIALISE','CANDIDATE_SELF_SUBMIT')
   or not exists(select 1 from private.bpay_next_input_transaction t where t.preparing_transaction_id=owned.preparing_transaction_id
      and t.module_epoch=owned.module_epoch and t.phase='OPEN') then
  raise exception using errcode='55000',message='BPAY_NEXT_QUERY_COMPARISON_WRITER_NOT_OPEN';end if;
 select i.* into strict parent from public.weekly_discrepancy_incidents i where i.id=new.incident_id;
 gate:=jsonb_build_object('incident_id',new.incident_id,'revision_number',new.revision_number,'contract_id',new.contract_id,
  'issue_family',new.issue_family,'material_comparison_fingerprint',encode(new.material_comparison_fingerprint,'hex'));
 -- Signed candidate_timesheet_id is evidence, not canonical financial root.
 perform private.bpay_next_query_version_stage_v1('public.weekly_issue_comparison_revisions',new.id,parent.candidate_id,null,
   false,true,'{}'::jsonb,gate);
 return null;
end
$function$;
alter function private.bpay_next_query_comparison_row_stage_v1() owner to postgres;
revoke all on function private.bpay_next_query_comparison_row_stage_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_query_comparison_row_stage_v1 on public.weekly_issue_comparison_revisions;
create trigger bpay_next_query_comparison_row_stage_v1 after insert on public.weekly_issue_comparison_revisions
 for each row execute function private.bpay_next_query_comparison_row_stage_v1();
notify pgrst,'reload schema';
commit;
