-- DR13 original observation extensions only; existing private triggers retained.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_case_review_outstanding_capture_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_component numeric;v_case numeric;v_component_reserved numeric;v_case_reserved numeric;
begin
 select case when new.instruction_kind='RECOVERY' then
    case when c.case_kind in ('LOAN','ADVANCE') then c.funded_source_ex_vat else c.approved_source_ex_vat end
     -c.recovered_source_ex_vat-c.written_off_source_ex_vat
   else c.approved_source_ex_vat-c.funded_source_ex_vat-c.paid_credit_source_ex_vat-c.written_off_source_ex_vat end,
  case when new.instruction_kind='RECOVERY' then
    case when f.case_kind in ('LOAN','ADVANCE') then f.principal_funded else f.principal_approved end
     -f.principal_recovered-f.principal_written_off-f.principal_paid_credit
   else f.principal_approved-f.principal_funded-f.principal_paid_credit-f.principal_written_off end,
  case when new.instruction_kind='RECOVERY' then c.active_recovery_source_ex_vat else c.active_payout_source_ex_vat end,
  case when new.instruction_kind='RECOVERY' then f.active_recovery_hold_amount else f.active_payout_hold_amount end
  into v_component,v_case,v_component_reserved,v_case_reserved
  from private.bpay_next_case_component c join private.bpay_next_finance_case f
   on f.id=c.case_id and f.candidate_id=c.candidate_id
  where c.id=new.case_component_id and c.case_id=new.case_id and c.candidate_id=new.candidate_id
   and c.component_revision=new.captured_component_revision and f.case_revision=new.captured_case_revision
   and c.instruction_kind=new.instruction_kind and c.source_pay_channel=new.source_pay_channel;
 if not found or v_component<0 or v_case<0 then
  raise exception using errcode='23514',message='BPAY_NEXT_CASE_REVIEW_CAPTURE_BASIS_CHANGED';end if;
 new.captured_component_outstanding_source_ex_vat:=v_component;
 new.captured_case_outstanding_source_ex_vat:=v_case;
 new.captured_component_reserved_source_ex_vat:=v_component_reserved;
 new.captured_case_reserved_source_ex_vat:=v_case_reserved;
 return new;
end
$function$;
alter function private.bpay_next_case_review_outstanding_capture_v1() owner to postgres;
revoke all on function private.bpay_next_case_review_outstanding_capture_v1() from public,anon,authenticated,service_role;
create or replace function private.bpay_next_case_review_excluded_capture_v1()
returns trigger language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v record;v_outstanding numeric;v_due numeric;
begin
 -- Selection replay returns before INSERT. Observe once; never acquire a new
 -- business lock or alter financial authority for a component that is unticked.
 new.review_observed_outstanding_source_ex_vat:=null;new.review_observed_due_source_ex_vat:=null;new.review_observed_at_utc:=null;
 new.review_observed_component_principal_source_ex_vat:=null;new.review_observed_component_reserved_source_ex_vat:=null;new.review_observed_unreserved_source_ex_vat:=null;
 if new.is_selected then return new;end if;
 select c.instruction_kind,c.case_kind,c.source_pay_channel,r.weekly_due_source_ex_vat,r.next_due_monday,
  r.id rule_id,m.basis,pay_run.pay_date,
  case when c.instruction_kind='RECOVERY' then case when c.case_kind in ('LOAN','ADVANCE') then c.funded_source_ex_vat else c.approved_source_ex_vat end-c.recovered_source_ex_vat-c.written_off_source_ex_vat else c.approved_source_ex_vat-c.funded_source_ex_vat-c.paid_credit_source_ex_vat-c.written_off_source_ex_vat end component_principal,
  case when c.instruction_kind='RECOVERY' then case when f.case_kind in ('LOAN','ADVANCE') then f.principal_funded else f.principal_approved end-f.principal_recovered-f.principal_written_off-f.principal_paid_credit else f.principal_approved-f.principal_funded-f.principal_paid_credit-f.principal_written_off end case_principal,
  case when c.instruction_kind='RECOVERY' then c.active_recovery_source_ex_vat else c.active_payout_source_ex_vat end component_reserved,
  case when c.instruction_kind='RECOVERY' then f.active_recovery_hold_amount else f.active_payout_hold_amount end case_reserved,
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
 new.review_observed_component_principal_source_ex_vat:=v.component_principal;
 new.review_observed_component_reserved_source_ex_vat:=v.component_reserved;
 new.review_observed_unreserved_source_ex_vat:=least(greatest(v.component_principal-v.component_reserved,0),greatest(v.case_principal-v.case_reserved,0));
 new.review_observed_at_utc:=pg_catalog.transaction_timestamp();return new;
end
$function$;
alter function private.bpay_next_case_review_excluded_capture_v1() owner to postgres;
revoke all on function private.bpay_next_case_review_excluded_capture_v1() from public,anon,authenticated,service_role;
commit;
