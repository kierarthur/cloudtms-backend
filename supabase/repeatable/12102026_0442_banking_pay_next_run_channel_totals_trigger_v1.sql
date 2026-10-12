-- DR-16 native OLD/NEW contributions; no allocation or no-Case owner edit.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_run_channel_contribution_v1(p_worker private.bpay_next_run_worker)
returns table(candidate_count bigint,ready_count bigint,gross_ex_vat numeric,gross_vat numeric,
 gross_inc_vat numeric,gross_stage_deductions numeric,net_recovery_reserved numeric,cash_known numeric,
 net_missing_count bigint,applicable_net_missing_count bigint,net_replacement_pending_count bigint,net_refused_code text,frozen_offer_complete boolean)
language sql stable security invoker set search_path=pg_catalog,private as $function$
 with points as (
  select (p_worker).*,d.completed_at_utc draft_completed_at,d.allocated_gross_deductions,d.recovered_target_total,
   p.id projection_id,p.accepted_recoveries,p.cash_amount,req.command_id request_id
  from (select 1) one
  left join private.bpay_next_case_allocation_state d on d.run_worker_id=p_worker.id
   and d.preparation_revision=p_worker.captured_preparation_revision and d.selection_revision=p_worker.case_selection_revision
   and d.pass_kind='DRAFT' and d.projection_no=0
  -- Retirement removes executable eligibility, never immutable accepted money.
  left join private.bpay_next_net_projection p on p.run_worker_id=p_worker.id and p.projection_no=p_worker.net_projection_revision
  left join private.bpay_next_paye_net_request req on req.run_worker_id=p_worker.id and req.request_no=p_worker.net_request_revision
 ), qualified as (
  select x.*,x.status<>'CANCELLED' participating,x.status in ('READY','DRAFT','ISSUED','COMPLETE') ready,
   (x.status in ('READY','DRAFT','ISSUED','COMPLETE','CANCELLING')
    or (x.status='REVIEW' and (x.projection_id is not null or x.draft_completed_at is not null))) monetary,
   x.target_pay_channel='PAYE' and x.status='READY' and x.request_id is not null and x.net_request_revision>x.net_projection_revision pending,
   case when x.target_pay_channel='PAYE' and x.status='REVIEW' and x.request_id is not null
    and x.net_request_revision>x.net_projection_revision
    and x.review_issue_code in ('CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND') then x.review_issue_code end refusal
  from points x
 ) select participating::integer::bigint,ready::integer::bigint,
  case when monetary then q.gross_ex_vat else 0 end,case when monetary then q.gross_vat else 0 end,
  case when monetary then q.gross_inc_vat else 0 end,
  case when monetary and draft_completed_at is not null then coalesce(allocated_gross_deductions,0) else 0 end,
  case when monetary then case when projection_id is not null then accepted_recoveries
   when draft_completed_at is not null then greatest(0,coalesce(recovered_target_total,0)-coalesce(allocated_gross_deductions,0)) else 0 end else 0 end,
  case when monetary then case when projection_id is not null then cash_amount
   when target_pay_channel='UMBRELLA' and case_instruction_count=0 then q.gross_inc_vat else 0 end else 0 end,
  (monetary and status<>'CANCELLING' and target_pay_channel='PAYE' and projection_id is null and (captured_line_count>0 or q.gross_inc_vat>0))::integer::bigint,
  (monetary and status<>'CANCELLING' and target_pay_channel='PAYE' and projection_id is null and (captured_line_count>0 or q.gross_inc_vat>0))::integer::bigint,
  (pending and net_projection_revision>0)::integer::bigint,refusal,
  coalesce(monetary and status<>'CANCELLING',false) from qualified q
$function$;
alter function private.bpay_next_run_channel_contribution_v1(private.bpay_next_run_worker) owner to postgres;
revoke all on function private.bpay_next_run_channel_contribution_v1(private.bpay_next_run_worker) from public,anon,authenticated,service_role;

create or replace function private.bpay_next_run_channel_totals_trigger_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private as $function$
declare v_old record;v_new record;v_channel text;v_code text;
begin
 if tg_table_name='bpay_next_pay_run' then
  insert into private.bpay_next_run_channel_totals(run_id,pay_channel) values(new.id,'PAYE'),(new.id,'UMBRELLA');return new;
 end if;
 if tg_op='DELETE' then raise exception using errcode='23514',message='BPAY_NEXT_REVIEW_TOTALS_WORKER_DELETE_FORBIDDEN';end if;
 select * into strict v_new from private.bpay_next_run_channel_contribution_v1(new);
 select * into strict v_old from private.bpay_next_run_channel_contribution_v1(null::private.bpay_next_run_worker);
 if tg_op='UPDATE' then
  if (new.id,new.run_id,new.candidate_id) is distinct from (old.id,old.run_id,old.candidate_id) then
   raise exception using errcode='23514',message='BPAY_NEXT_REVIEW_TOTALS_WORKER_IDENTITY_CHANGED';end if;
  select * into strict v_old from private.bpay_next_run_channel_contribution_v1(old);
  if new.target_pay_channel=old.target_pay_channel and v_new is not distinct from v_old then return new;end if;
 end if;
 -- Original parents already own Run -> Candidate -> job -> worker. Only the
 -- summary is locked here, fixed channel order; no reverse business locks.
 for v_channel in select c from (values('PAYE'::text),('UMBRELLA'::text)) channels(c) order by c collate "C" loop
  if v_channel<>new.target_pay_channel and (tg_op<>'UPDATE' or v_channel<>old.target_pay_channel) then continue;end if;
  update private.bpay_next_run_channel_totals t set
      candidate_count=t.candidate_count+case when v_channel=new.target_pay_channel then v_new.candidate_count else 0 end-case when tg_op='UPDATE' and v_channel=old.target_pay_channel then v_old.candidate_count else 0 end,
      ready_count=t.ready_count+case when v_channel=new.target_pay_channel then v_new.ready_count else 0 end-case when tg_op='UPDATE' and v_channel=old.target_pay_channel then v_old.ready_count else 0 end,
      gross_ex_vat=t.gross_ex_vat+case when v_channel=new.target_pay_channel then v_new.gross_ex_vat else 0 end-case when tg_op='UPDATE' and v_channel=old.target_pay_channel then v_old.gross_ex_vat else 0 end,
      gross_vat=t.gross_vat+case when v_channel=new.target_pay_channel then v_new.gross_vat else 0 end-case when tg_op='UPDATE' and v_channel=old.target_pay_channel then v_old.gross_vat else 0 end,
      gross_inc_vat=t.gross_inc_vat+case when v_channel=new.target_pay_channel then v_new.gross_inc_vat else 0 end-case when tg_op='UPDATE' and v_channel=old.target_pay_channel then v_old.gross_inc_vat else 0 end,
      gross_stage_deductions=t.gross_stage_deductions+case when v_channel=new.target_pay_channel then v_new.gross_stage_deductions else 0 end-case when tg_op='UPDATE' and v_channel=old.target_pay_channel then v_old.gross_stage_deductions else 0 end,
      net_recovery_reserved=t.net_recovery_reserved+case when v_channel=new.target_pay_channel then v_new.net_recovery_reserved else 0 end-case when tg_op='UPDATE' and v_channel=old.target_pay_channel then v_old.net_recovery_reserved else 0 end,
      cash_known=t.cash_known+case when v_channel=new.target_pay_channel then v_new.cash_known else 0 end-case when tg_op='UPDATE' and v_channel=old.target_pay_channel then v_old.cash_known else 0 end,
      net_missing_count=t.net_missing_count+case when v_channel=new.target_pay_channel then v_new.net_missing_count else 0 end-case when tg_op='UPDATE' and v_channel=old.target_pay_channel then v_old.net_missing_count else 0 end,
      applicable_net_missing_count=t.applicable_net_missing_count+case when v_channel=new.target_pay_channel then v_new.applicable_net_missing_count else 0 end-case when tg_op='UPDATE' and v_channel=old.target_pay_channel then v_old.applicable_net_missing_count else 0 end,
      net_replacement_pending_count=t.net_replacement_pending_count+case when v_channel=new.target_pay_channel then v_new.net_replacement_pending_count else 0 end-case when tg_op='UPDATE' and v_channel=old.target_pay_channel then v_old.net_replacement_pending_count else 0 end,
      totals_revision=t.totals_revision+1,updated_at_utc=pg_catalog.transaction_timestamp()
   where t.run_id=new.run_id and t.pay_channel=v_channel;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_REVIEW_TOTALS_NOT_INITIALISED';end if;
  -- First current native closed NET-refusal entry, indexed, never a Run recount.
  select w.review_issue_code into v_code from private.bpay_next_run_worker w
   where w.run_id=new.run_id and w.target_pay_channel=v_channel and w.status='REVIEW'
    and w.net_request_revision>w.net_projection_revision and w.review_issue_code is not null
    and w.review_issue_code in ('CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND')
   order by w.review_issue_code collate "C",w.id limit 1;
  update private.bpay_next_run_channel_totals set net_refused_code=v_code where run_id=new.run_id and pay_channel=v_channel;
 end loop;return new;
end
$function$;
alter function private.bpay_next_run_channel_totals_trigger_v1() owner to postgres;
revoke all on function private.bpay_next_run_channel_totals_trigger_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_review_totals_run_insert_v1 on private.bpay_next_pay_run;
create trigger bpay_next_review_totals_run_insert_v1 after insert on private.bpay_next_pay_run for each row
 execute function private.bpay_next_run_channel_totals_trigger_v1();
drop trigger if exists bpay_next_run_channel_totals_v1 on private.bpay_next_run_worker;
create trigger bpay_next_run_channel_totals_v1 after insert or update of status,target_pay_channel,
 gross_ex_vat,gross_vat,gross_inc_vat,entered_paye_net,net_request_revision,net_projection_revision,
 review_issue_code,captured_line_count,case_instruction_count,case_selection_revision,captured_preparation_revision
 on private.bpay_next_run_worker for each row execute function private.bpay_next_run_channel_totals_trigger_v1();
-- Observational supplement only. Existing preparation already owns exact
-- Case/component/period locks and copies their revisions before this INSERT.
-- No new lock, balance transition, rule choice or allocation chain edit.
create or replace function private.bpay_next_case_review_outstanding_capture_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_component numeric;v_case numeric;
begin
 select case when new.instruction_kind='RECOVERY' then
    case when c.case_kind in ('LOAN','ADVANCE') then c.funded_source_ex_vat else c.approved_source_ex_vat end
     -c.recovered_source_ex_vat-c.written_off_source_ex_vat
   else c.approved_source_ex_vat-c.funded_source_ex_vat-c.paid_credit_source_ex_vat-c.written_off_source_ex_vat end,
  case when new.instruction_kind='RECOVERY' then
    case when f.case_kind in ('LOAN','ADVANCE') then f.principal_funded else f.principal_approved end
     -f.principal_recovered-f.principal_written_off-f.principal_paid_credit
   else f.principal_approved-f.principal_funded-f.principal_paid_credit-f.principal_written_off end
  into v_component,v_case
  from private.bpay_next_case_component c join private.bpay_next_finance_case f
   on f.id=c.case_id and f.candidate_id=c.candidate_id
  where c.id=new.case_component_id and c.case_id=new.case_id and c.candidate_id=new.candidate_id
   and c.component_revision=new.captured_component_revision and f.case_revision=new.captured_case_revision
   and c.instruction_kind=new.instruction_kind and c.source_pay_channel=new.source_pay_channel;
 if not found or v_component<0 or v_case<0 then
  raise exception using errcode='23514',message='BPAY_NEXT_CASE_REVIEW_CAPTURE_BASIS_CHANGED';end if;
 new.captured_component_outstanding_source_ex_vat:=v_component;
 new.captured_case_outstanding_source_ex_vat:=v_case;
 return new;
end
$function$;
alter function private.bpay_next_case_review_outstanding_capture_v1() owner to postgres;
revoke all on function private.bpay_next_case_review_outstanding_capture_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_case_review_outstanding_capture_v1 on private.bpay_next_run_case_instruction;
create trigger bpay_next_case_review_outstanding_capture_v1 before insert on private.bpay_next_run_case_instruction
 for each row execute function private.bpay_next_case_review_outstanding_capture_v1();
create or replace function private.bpay_next_case_review_excluded_capture_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v record;v_outstanding numeric;v_due numeric;
begin
 -- Selection replay returns before INSERT. Observe once; never acquire a new
 -- business lock or alter financial authority for a component that is unticked.
 new.review_observed_outstanding_source_ex_vat:=null;new.review_observed_due_source_ex_vat:=null;new.review_observed_at_utc:=null;
 if new.is_selected then return new;end if;
 select c.instruction_kind,c.case_kind,c.source_pay_channel,r.weekly_due_source_ex_vat,r.next_due_monday,
  r.id rule_id,m.basis,pay_run.pay_date,
  case when c.instruction_kind='RECOVERY' then
   least(case when c.case_kind in ('LOAN','ADVANCE') then c.funded_source_ex_vat else c.approved_source_ex_vat end-c.recovered_source_ex_vat-c.written_off_source_ex_vat,
    case when f.case_kind in ('LOAN','ADVANCE') then f.principal_funded else f.principal_approved end-f.principal_recovered-f.principal_written_off-f.principal_paid_credit)
   else least(c.approved_source_ex_vat-c.funded_source_ex_vat-c.paid_credit_source_ex_vat-c.written_off_source_ex_vat,
    f.principal_approved-f.principal_funded-f.principal_paid_credit-f.principal_written_off) end outstanding,
  exists(select 1 from private.bpay_next_work_collection collection where collection.id=c.id and collection.id=f.id
   and collection.case_id=f.id and collection.case_component_id=c.id and collection.candidate_id=c.candidate_id
   and collection.id=r.id and collection.original_source_pay_channel='PAYE' and collection.original_tax_treatment='TAXABLE') automatic
 into v from private.bpay_next_case_component c join private.bpay_next_finance_case f on f.id=c.case_id and f.candidate_id=c.candidate_id
 join private.bpay_next_pay_run pay_run on pay_run.id=new.run_id
 left join private.bpay_next_selection_exclusion_memory e on e.run_id=new.run_id and e.intent_kind='cases' and e.native_id=c.id and not e.selected
 join private.bpay_next_selection_mode_memory m on m.run_id=coalesce(e.basis_ref_run_id,new.run_id) and m.row_kind='LEAF'
  and m.leaf_key='CASE:'||c.id::text and m.candidate_id=new.candidate_id
 join private.bpay_next_case_rule r on r.id=(m.basis->>'rule_id')::uuid and r.case_id=f.id and r.candidate_id=c.candidate_id
  and r.rule_revision=(m.basis->>'case_rule_revision')::bigint
 where c.id=new.case_component_id and c.case_id=new.case_id and c.candidate_id=new.candidate_id
  and (m.basis->>'case_component_id',m.basis->>'case_id',m.basis->>'case_instruction_kind')=(c.id::text,f.id::text,c.instruction_kind);
 if not found or v.outstanding<0 then return new;end if;
 v_outstanding:=v.outstanding;
 v_due:=case when v.basis->>'case_state'<>'OPEN' or (v.basis->>'case_due_date')::date>v.pay_date then 0
  when v.instruction_kind<>'RECOVERY' then v_outstanding
  when v.weekly_due_source_ex_vat is not null then case when (v.pay_date-extract(isodow from v.pay_date)::integer+1)>=v.next_due_monday
    then least(v.weekly_due_source_ex_vat,v_outstanding) else 0 end
  when v.case_kind='OVERPAYMENT' and v.automatic then v_outstanding else null end;
 new.review_observed_outstanding_source_ex_vat:=v_outstanding;new.review_observed_due_source_ex_vat:=v_due;
 new.review_observed_at_utc:=pg_catalog.transaction_timestamp();return new;
end
$function$;
alter function private.bpay_next_case_review_excluded_capture_v1() owner to postgres;
revoke all on function private.bpay_next_case_review_excluded_capture_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_case_review_excluded_capture_v1 on private.bpay_next_case_selection_item;
create trigger bpay_next_case_review_excluded_capture_v1 before insert on private.bpay_next_case_selection_item
 for each row execute function private.bpay_next_case_review_excluded_capture_v1();
commit;
