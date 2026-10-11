-- FC14 / BD121: exact native audit and maintained Candidate report.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_record_case_hold_transition_v1(p_job_id uuid,p_hold_id uuid,p_transition text)
returns void language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare
 v_hold private.bpay_next_case_hold%rowtype;v_job private.bpay_next_job%rowtype;
 v_command uuid;v_kind text;v_actor uuid;v_actor_kind text:='UNKNOWN';v_reason text;v_time timestamptz;
 v_ordinal smallint;v_release record;v_prior private.bpay_next_case_hold_transition%rowtype;
begin
 select h.* into strict v_hold from private.bpay_next_case_hold h where h.id=p_hold_id;
 select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id;
 if v_job.candidate_id is distinct from v_hold.candidate_id or v_hold.status is distinct from p_transition
   or v_hold.instruction_id is null or v_hold.allocation_result_id is null
   or p_transition not in ('ACTIVE','RELEASED','REALISED') then
  raise exception using errcode='23514',message='BPAY_NEXT_CASE_HOLD_AUDIT_SCOPE_INVALID';end if;
 v_command:=v_job.command_id;v_kind:=v_job.job_kind;
 -- A system RELEASE can reuse the original PREPARE job. Its exact running
 -- action must precede PREPARE attribution; an allocation state's old job is not a cause.
 select r.command_id,r.action,r.initiator_kind,r.actor_user_id,r.original_issue_code into v_release
  from private.bpay_next_worker_reprepare_request r
  join private.bpay_next_run_worker w on w.id=r.run_worker_id and w.preparation_revision=r.target_preparation_revision
  where r.job_id=p_job_id and r.run_worker_id=v_hold.run_worker_id and r.candidate_id=v_hold.candidate_id
   and r.status='RUNNING' and ((p_transition='RELEASED' and r.action='RELEASE') or(p_transition='ACTIVE' and r.action='REPREPARE'));
 if found then
  v_command:=v_release.command_id;v_kind:='WORKER_'||v_release.action;v_actor:=v_release.actor_user_id;
  v_actor_kind:=case v_release.initiator_kind when 'SYSTEM' then 'SYSTEM' when 'OFFICE' then 'HUMAN' else 'UNKNOWN' end;
  v_reason:=v_release.original_issue_code;
 elsif v_job.job_kind='PREPARATION_EXPIRY' then
  select r.actor_user_id,case r.initiator_kind when 'SYSTEM' then 'SYSTEM' else 'HUMAN' end into v_actor,v_actor_kind
   from private.bpay_next_preparation_expiry_worker w join private.bpay_next_preparation_expiry_request r on r.command_id=w.command_id
   where w.job_id=p_job_id and w.run_worker_id=v_hold.run_worker_id and w.candidate_id=v_hold.candidate_id;
 elsif v_job.job_kind='PREPARATION_DISCARD' then
  select r.actor_user_id,'HUMAN',r.reason into v_actor,v_actor_kind,v_reason
   from private.bpay_next_preparation_discard_worker w join private.bpay_next_preparation_discard_request r on r.command_id=w.command_id
   where w.job_id=p_job_id and w.run_worker_id=v_hold.run_worker_id and w.candidate_id=v_hold.candidate_id;
 elsif v_job.job_kind='PAYE_NET_ENTRY' then
  select r.actor_user_id,'HUMAN' into v_actor,v_actor_kind from private.bpay_next_paye_net_request r
   where r.command_id=v_job.command_id and r.run_worker_id=v_hold.run_worker_id and r.candidate_id=v_hold.candidate_id;
 elsif v_job.job_kind='SIMPLE_CANCEL' then
  select p.actor_user_id,'HUMAN' into v_actor,v_actor_kind from private.bpay_next_reauth_proof p
   where p.consumed_command_id=v_job.command_id and p.action_kind='CANCEL_REQUEST';
 elsif v_job.job_kind in ('CSV_SETTLEMENT','INTERNAL_SETTLEMENT') then
  select coalesce(i.actor_user_id,r.actor_user_id),'HUMAN' into v_actor,v_actor_kind
   from private.bpay_next_outcome_request r left join private.bpay_next_internal_receipt i
    on i.id=r.internal_receipt_id and i.command_id=r.command_id and i.transfer_id=r.transfer_id
     and i.run_worker_id=r.run_worker_id and i.candidate_id=r.candidate_id
   where r.command_id=v_job.command_id and r.run_worker_id=v_hold.run_worker_id and r.candidate_id=v_hold.candidate_id;
 elsif v_job.job_kind='PREPARE' and p_transition='ACTIVE' then
  select r.actor_user_id,'HUMAN' into v_actor,v_actor_kind from private.bpay_next_run_worker w
   join private.bpay_next_run_command c on c.run_id=w.run_id and c.command_id=v_job.command_id
   join private.bpay_next_pay_run r on r.id=c.run_id
   where w.id=v_hold.run_worker_id and w.candidate_id=v_hold.candidate_id;
 end if;
 if v_actor_kind is null or(v_actor_kind='HUMAN' and v_actor is null) then v_actor_kind:='UNKNOWN';v_actor:=null;end if;
 v_time:=case when p_transition='ACTIVE' then v_hold.created_at_utc else v_hold.finished_at_utc end;
 v_ordinal:=case when p_transition='ACTIVE' then 1 else 2 end;
 if v_time is null or not isfinite(v_time) then raise exception using errcode='23514',message='BPAY_NEXT_CASE_HOLD_AUDIT_TIME_INVALID';end if;
 select t.* into v_prior from private.bpay_next_case_hold_transition t where t.hold_id=p_hold_id and t.event_ordinal=v_ordinal;
 if found then
  if (v_prior.transition,v_prior.cause_job_id,v_prior.command_id,v_prior.command_kind,v_prior.actor_kind,v_prior.actor_user_id,v_prior.reason,v_prior.occurred_at_utc)
   is distinct from(p_transition,p_job_id,v_command,v_kind,v_actor_kind,v_actor,v_reason,v_time) then
   raise exception using errcode='23514',message='BPAY_NEXT_CASE_HOLD_AUDIT_REPLAY_MISMATCH';end if;
  return;
 end if;
 insert into private.bpay_next_case_hold_transition(hold_id,event_ordinal,transition,case_id,candidate_id,run_worker_id,
  case_component_id,instruction_id,allocation_result_id,cause_job_id,command_id,command_kind,actor_kind,actor_user_id,reason,
  source_amount_ex_vat,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,occurred_at_utc)
 values(v_hold.id,v_ordinal,p_transition,v_hold.case_id,v_hold.candidate_id,v_hold.run_worker_id,v_hold.case_component_id,
  v_hold.instruction_id,v_hold.allocation_result_id,p_job_id,v_command,v_kind,v_actor_kind,v_actor,v_reason,
  v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat,v_time);
end
$function$;
-- FC14 native immutable source union. Every row is one actual receipt; no
-- current hold/status/date expands into invented events. Full-scope server
-- ordering is applied to finite scalar rows before bounded JSON packing.
create or replace function private.bpay_next_case_audit_sources_v1(p_case_id uuid,p_candidate_id uuid)
returns table(source text,source_id uuid,event_ordinal smallint,occurred_at_utc timestamptz,
 event_label text,actor_user_id uuid,actor_kind text,source_label text,
 source_amount_ex_vat numeric,target_amount_ex_vat numeric,target_amount_vat numeric,target_amount_inc_vat numeric,
 reason text,details text)
language sql stable security invoker set search_path=pg_catalog,private,public
as $function$
 select 'CREATE_REQUEST',r.command_id,1::smallint,r.accepted_at_utc,'Creation requested',r.actor_user_id,'HUMAN','Office',
  r.principal_source_ex_vat::numeric,null::numeric,null::numeric,null::numeric,r.approval_reason,
  concat_ws(' · ','Creation submitted',case when r.weekly_due_source_ex_vat is not null then 'Weekly £'||r.weekly_due_source_ex_vat::text end,
   case when r.schedule_week_count is not null then r.schedule_week_count::text||' weeks' end,
   case when r.input_start_monday is not null then 'From '||to_char(r.input_start_monday,'DD/MM/YYYY') end,
   case when r.due_date is not null then 'Due '||to_char(r.due_date,'DD/MM/YYYY') end,
   case when r.minimum_earnings_threshold is not null then 'Earnings threshold £'||r.minimum_earnings_threshold::text end,
   case when r.take_home_floor_override is not null then 'Take-home floor £'||r.take_home_floor_override::text end,
   nullif(r.note,''))
 from private.bpay_next_case_create_request r where r.case_id=p_case_id and r.candidate_id=p_candidate_id
 union all
 select 'EDIT_REQUEST',r.command_id,1::smallint,r.accepted_at_utc,'Change requested',r.actor_user_id,'HUMAN','Office',
  (r.principal-r.previous_principal)::numeric,null::numeric,null::numeric,null::numeric,r.input->>'reason',
  concat_ws(' · ','Principal £'||r.previous_principal::text||' → £'||r.principal::text,
   case when r.terms->>'weekly_due' is not null then 'Weekly £'||(r.terms->>'weekly_due') end,
   case when r.terms->>'week_count' is not null then (r.terms->>'week_count')||' weeks' end,
   case when r.input->>'start_monday' is not null then 'From '||to_char((r.input->>'start_monday')::date,'DD/MM/YYYY') end,
   case when r.input->>'due_date' is not null then 'Due '||to_char((r.input->>'due_date')::date,'DD/MM/YYYY') end,
   case when r.input->>'minimum_earnings_threshold' is not null then 'Earnings threshold £'||(r.input->>'minimum_earnings_threshold') end,
   case when r.input->>'take_home_floor_override' is not null then 'Take-home floor £'||(r.input->>'take_home_floor_override') end,
   case when r.input->>'note' is null then 'No note recorded' else r.input->>'note' end)
 from private.bpay_next_case_edit_request r where r.case_id=p_case_id and r.candidate_id=p_candidate_id
 union all
 select 'PAUSE_REQUEST',r.command_id,1::smallint,r.accepted_at_utc,
  case r.action when 'CASE_PAUSE' then 'Pause requested' else 'Resume requested' end,r.actor_user_id,'HUMAN','Office',
  null::numeric,null::numeric,null::numeric,null::numeric,r.reason,'Status change submitted'
 from private.bpay_next_case_pause_request r where r.case_id=p_case_id and r.candidate_id=p_candidate_id
 union all
 select 'PAUSE_OUTCOME',o.command_id,1::smallint,o.completed_at_utc,
  case when o.outcome='BLOCKED' then 'Change not applied' when o.action='CASE_PAUSE' then 'Paused' else 'Resumed' end,
  r.actor_user_id,'HUMAN','Office',null::numeric,null::numeric,null::numeric,null::numeric,r.reason,
  case when o.outcome='BLOCKED' then 'The Case could not be changed' else 'Recovery status changed' end
 from private.bpay_next_case_pause_outcome o join private.bpay_next_case_pause_request r
  on r.command_id=o.command_id and r.case_id=o.case_id and r.candidate_id=o.candidate_id and r.action=o.action
 where o.case_id=p_case_id and o.candidate_id=p_candidate_id
 union all
 select 'WRITE_OFF_REQUEST',r.command_id,1::smallint,r.accepted_at_utc,'Write-off requested',r.actor_user_id,'HUMAN','Office',
  r.requested_amount::numeric,null::numeric,null::numeric,null::numeric,r.reason,'Write-off requested'
 from private.bpay_next_write_off_request r where r.case_id=p_case_id and r.candidate_id=p_candidate_id
 union all
 select 'CASE_EVENT',e.id,1::smallint,e.occurred_at_utc,
  case e.event_kind when 'APPROVED' then case when er.command_id is not null then 'Changed' else 'Created' end
   when 'FUNDED' then 'Advance paid' when 'RECOVERED' then 'Recovered' when 'WRITTEN_OFF' then 'Written off'
   when 'PAID' then 'Credit paid' when 'CORRECTED' then 'Corrected' when 'SHORTFALL' then 'Repayment not collected in full' else 'Case balance changed' end,
  coalesce(er.actor_user_id,cr.actor_user_id,wr.actor_user_id,orr.actor_user_id),
  case when coalesce(er.actor_user_id,cr.actor_user_id,wr.actor_user_id,orr.actor_user_id) is null then 'UNKNOWN' else 'HUMAN' end,
  case when e.original_transfer_id is not null then 'Payment' when e.run_worker_id is not null then 'Draft' else 'Office' end,
  coalesce(e.source_amount_ex_vat,nullif(e.approved_delta,0),nullif(e.funded_delta,0),nullif(e.recovered_delta,0),nullif(e.written_off_delta,0),
   case when e.event_kind='APPROVED' then e.approved_delta else null end)::numeric,
  e.target_amount_ex_vat::numeric,e.target_amount_vat::numeric,e.target_amount_inc_vat::numeric,
  coalesce(er.input->>'reason',cr.approval_reason,wr.reason),
  case when er.command_id is not null then 'Principal or schedule changed'
   when e.event_kind='WRITTEN_OFF' then 'Outstanding amount written off'
   when e.event_kind='RECOVERED' then 'Repayment recorded' when e.event_kind='FUNDED' then 'Funding recorded'
   when e.event_kind='APPROVED' then 'Case created' when e.event_kind='SHORTFALL' then 'Uncollected amount remains outstanding' else 'Money recorded' end
 from private.bpay_next_case_event e
 left join private.bpay_next_case_edit_request er on er.command_id=e.operation_id and er.case_id=e.case_id and er.candidate_id=p_candidate_id
 left join private.bpay_next_case_create_request cr on cr.command_id=e.operation_id and cr.case_id=e.case_id and cr.candidate_id=p_candidate_id
 left join private.bpay_next_write_off_request wr on wr.command_id=e.operation_id and wr.case_id=e.case_id and wr.event_id=e.id and wr.candidate_id=p_candidate_id
 left join private.bpay_next_outcome_request orr on orr.command_id=e.operation_id and orr.candidate_id=p_candidate_id
  and orr.transfer_id=e.original_transfer_id and orr.run_worker_id=e.run_worker_id
 where e.case_id=p_case_id and exists(select 1 from private.bpay_next_finance_case c where c.id=e.case_id and c.candidate_id=p_candidate_id)
 union all
 select 'NET_REQUEST',r.command_id,1::smallint,r.accepted_at_utc,case when r.input_kind='CASE_PAYOUT' then 'Case payment requested' else 'PAYE net recorded' end,r.actor_user_id,
  case when r.actor_user_id is null then 'UNKNOWN' else 'HUMAN' end,'Draft',
  null::numeric,r.entered_paye_net::numeric,0::numeric,r.entered_paye_net::numeric,null::text,
  case when r.input_kind='CASE_PAYOUT' then 'Case payment requested' else 'Candidate PAYE net' end
 from private.bpay_next_paye_net_request r join private.bpay_next_case_allocation_state s
  on s.id=r.case_draft_state_id and s.run_worker_id=r.run_worker_id and s.candidate_id=r.candidate_id and s.pass_kind='DRAFT'
  and s.preparation_revision=r.preparation_revision and s.selection_revision=r.selection_revision
 where r.candidate_id=p_candidate_id and exists(select 1 from private.bpay_next_run_case_instruction i
  where i.case_id=p_case_id and i.candidate_id=p_candidate_id and i.run_worker_id=s.run_worker_id
   and i.preparation_revision=s.preparation_revision and i.selection_revision=s.selection_revision and i.is_selected)
 union all
 select 'HOLD_TRANSITION',h.hold_id,h.event_ordinal,h.occurred_at_utc,
  case h.transition when 'ACTIVE' then 'Reserved' when 'RELEASED' then 'Reservation released' else 'Reservation used' end,
  h.actor_user_id,h.actor_kind,'Draft',h.source_amount_ex_vat::numeric,h.target_amount_ex_vat::numeric,
  h.target_amount_vat::numeric,h.target_amount_inc_vat::numeric,
  case when h.command_kind in ('WORKER_RELEASE','PREPARATION_EXPIRY') and h.actor_kind='SYSTEM' then null else h.reason end,
  case h.transition when 'ACTIVE' then 'Amount reserved for this Draft' when 'RELEASED' then 'Reservation returned to Banking Pay'
   else 'Reserved amount recorded as paid or recovered' end
 from private.bpay_next_case_hold_transition h where h.case_id=p_case_id and h.candidate_id=p_candidate_id
 union all
 select 'FINANCIAL_EFFECT',f.id,1::smallint,f.occurred_at_utc,'Money recorded',orr.actor_user_id,
  case when orr.actor_user_id is null then 'UNKNOWN' else 'HUMAN' end,
  case when f.original_timesheet_id is not null then 'Timesheet'||coalesce(' '||t.reference_number,'') else 'Payment' end,
  f.source_disposition_ex_vat::numeric,f.target_amount_ex_vat::numeric,f.target_amount_vat::numeric,f.target_amount_inc_vat::numeric,
  null::text,case when f.effect_kind='WORK_COLLECTION_RECOVERED' then 'Timesheet recovery recorded' else 'Financial effect recorded' end
 from private.bpay_next_financial_effect f left join public.timesheets t on t.timesheet_id=f.original_timesheet_id
 left join private.bpay_next_outcome_request orr on orr.command_id=f.operation_id and orr.candidate_id=f.candidate_id and orr.transfer_id=f.original_transfer_id
 where f.case_id=p_case_id and f.candidate_id=p_candidate_id
 union all
 select 'SNOOZE_EVENT',e.id,1::smallint,e.occurred_at_utc,
  case e.event_kind when 'SET' then 'Snoozed' when 'AMENDED' then 'Snooze changed' when 'WOKEN' then 'Snooze ended'
   when 'CANCELLED' then 'Snooze cancelled' else 'Snooze cleared' end,e.actor_id,
  case when e.actor_id is not null then 'HUMAN' when e.event_kind in ('WOKEN','CANCELLED') then 'SYSTEM' else 'UNKNOWN' end,
  'Office',null::numeric,null::numeric,null::numeric,null::numeric,e.reason,
  case when e.after_state->>'until_date' is null then 'No end date recorded' else 'Until '||to_char((e.after_state->>'until_date')::date,'DD/MM/YYYY') end
 from private.bpay_next_snooze_event e where e.candidate_id=p_candidate_id and e.after_state->>'scope_kind'='CASE'
  and e.after_state->>'case_id'=p_case_id::text
$function$;

create or replace function private.bpay_next_case_audit_row_v1(p_source text,p_source_id uuid,p_event_ordinal smallint,p_case_id uuid,p_candidate_id uuid)
returns jsonb language sql stable security invoker set search_path=pg_catalog,private,public
as $function$
 select jsonb_build_object('source',s.source,'source_id',s.source_id,'event_ordinal',s.event_ordinal::text,
 'case_id',p_case_id,'candidate_id',p_candidate_id,'occurred_at_utc',s.occurred_at_utc,'event_label',s.event_label,
 'actor_user_id',s.actor_user_id,'actor_kind',s.actor_kind,
 'actor_name',case s.actor_kind when 'SYSTEM' then 'Banking Pay' when 'HUMAN' then coalesce(nullif(btrim(u.display_name),''),'Unknown') else 'Unknown' end,
 'source_label',s.source_label,'display_amount',(case when s.event_label='Recovered' then -coalesce(s.target_amount_inc_vat,s.source_amount_ex_vat) else coalesce(s.target_amount_inc_vat,s.source_amount_ex_vat) end)::text,'source_amount_ex_vat',s.source_amount_ex_vat::text,'target_amount_ex_vat',s.target_amount_ex_vat::text,
 'target_amount_vat',s.target_amount_vat::text,'target_amount_inc_vat',s.target_amount_inc_vat::text,
 'reason',s.reason,'details',s.details)
 from private.bpay_next_case_audit_sources_v1(p_case_id,p_candidate_id) s left join public.tms_users u on u.id=s.actor_user_id
 where s.source=p_source and s.source_id=p_source_id and s.event_ordinal=p_event_ordinal
$function$;

create or replace function private.bpay_next_candidate_case_report_sources_v1(p_candidate_id uuid,p_context jsonb,p_active_only boolean)
returns table(row_id uuid,source text,section integer,item text,created_at_utc timestamptz,client text,
 original numeric,funded numeric,recovered numeric,written_off numeric,reserved numeric,outstanding numeric,
 weekly_amount numeric,next_date date,snoozed_until date,has_active_snooze boolean,indefinite_snooze boolean,
 status text,notes text,work_id uuid,physical_timesheet_id uuid)
language sql stable security invoker set search_path=pg_catalog,private,public
as $function$
 select c.id,'CASE',case k.case_subtype when 'LOAN' then 1 when 'PAYMENT_ADVANCE' then 1 when 'OVERPAYMENT' then 2
  when 'UNDERPAYMENT' then 3 when 'MANUAL_DEBT' then 4 when 'MANUAL_CREDIT' then 5 else 6 end,
 k.case_type_label,k.created_at_utc,
 case k.client_association when 'UNASSOCIATED' then 'General / no specific Client' when 'FROZEN' then k.client_display_name else null end,
 c.principal_approved::numeric,c.principal_funded::numeric,c.principal_recovered::numeric,c.principal_written_off::numeric,
 c.active_hold_amount::numeric,
 (case when c.case_kind in ('LOAN','ADVANCE') then c.principal_funded else c.principal_approved end
  -c.principal_recovered-c.principal_written_off-c.principal_paid_credit)::numeric,
 r.weekly_due_source_ex_vat::numeric,coalesce(r.next_due_monday,c.due_date),sn.until_date,case when membership.n between 1 and 2 then sn.n>0 else null end,case when membership.n between 1 and 2 then sn.indefinite>0 else null end,
 case when c.status='PAUSED' then 'Paused' when c.status='WRITTEN_OFF' then 'Written off' when c.status='CLOSED' then 'Closed'
  when sn.n>0 then 'Snoozed' when k.case_subtype in ('LOAN','PAYMENT_ADVANCE') and c.principal_funded=0 then 'Not paid out yet' else 'Active' end,
 case when er.command_id is not null then er.input->>'note' when r.original_funding_event_id is not null and approved.id is null then null else cr.note end,null::uuid,null::uuid
 from private.bpay_next_finance_case c join private.bpay_next_case_list_key k on k.case_id=c.id and k.candidate_id=c.candidate_id
 left join private.bpay_next_case_rule r on r.id=c.current_rule_id and r.case_id=c.id and r.candidate_id=c.candidate_id
 left join private.bpay_next_case_create_request cr on cr.case_id=c.id and cr.candidate_id=c.candidate_id
 left join private.bpay_next_case_event funding on funding.id=r.original_funding_event_id and funding.case_id=c.id
  and funding.candidate_id=c.candidate_id and funding.event_kind='FUNDED' and funding.occurred_at_utc=r.original_funded_at_utc
 left join private.bpay_next_run_case_instruction funded_input on funded_input.id=funding.instruction_id and funded_input.case_id=c.id
  and funded_input.candidate_id=c.candidate_id and funded_input.instruction_kind='PAYOUT' and funded_input.case_component_id=cr.primary_component_id
 left join private.bpay_next_case_rule approved on approved.id=case when r.original_funding_event_id is null then r.id else funded_input.rule_id end
  and approved.case_id=c.id and approved.candidate_id=c.candidate_id and approved.original_funding_event_id is null
  and(approved.case_kind,approved.case_subtype,approved.tax_treatment,approved.case_created_at_utc,approved.minimum_earnings_threshold,
   approved.take_home_floor_override,approved.weekly_due_source_ex_vat,approved.schedule_start_monday,approved.next_due_monday,approved.schedule_week_count)
   is not distinct from(r.case_kind,r.case_subtype,r.tax_treatment,r.case_created_at_utc,r.minimum_earnings_threshold,
   r.take_home_floor_override,r.weekly_due_source_ex_vat,r.schedule_start_monday,r.next_due_monday,r.schedule_week_count)
 left join private.bpay_next_case_edit_request er on er.new_rule_id=approved.id and er.case_id=c.id and er.candidate_id=c.candidate_id
 cross join lateral(select count(*) n from(select cc.id from private.bpay_next_case_component cc where cc.case_id=c.id and cc.candidate_id=c.candidate_id order by cc.component_ordinal,cc.id limit 3) bounded) membership
 cross join lateral(
  select count(*) n,count(*) filter(where s.until_date is null) indefinite,min(s.until_date::date) until_date
  from private.bpay_next_case_component cc join private.bpay_next_case_rule rr
   on rr.id=cc.rule_id and rr.case_id=cc.case_id and rr.candidate_id=cc.candidate_id
  join private.bpay_next_snooze s on s.case_component_id=cc.id and s.case_id=cc.case_id and s.candidate_id=cc.candidate_id
   and s.scope_kind='CASE' and s.scope_identity=cc.id::text and s.active
   and(s.until_date is null or s.until_date>=(p_context->>'local_date')::date)
   and s.worker_pay_basis=sha256(convert_to(jsonb_build_array(cc.candidate_id,cc.case_id,cc.id,
    cc.case_kind,cc.case_subtype,cc.tax_treatment,cc.instruction_kind,cc.direction,cc.payroll_stage,cc.source_pay_channel,
    cc.approved_source_ex_vat,cc.funded_source_ex_vat,rr.minimum_earnings_threshold,rr.take_home_floor_override,
    rr.weekly_due_source_ex_vat,rr.schedule_start_monday,rr.next_due_monday,rr.schedule_week_count)::text,'UTF8'))
  where cc.case_id=c.id and cc.candidate_id=c.candidate_id and membership.n between 1 and 2
 ) sn
 where c.candidate_id=p_candidate_id and(not p_active_only or c.status in ('OPEN','PAUSED'))
 union all
 select s.id,'TIMESHEET_SNOOZE',7,'Timesheet'||coalesce(' '||t.reference_number,''),s.created_at_utc,
 origin.value->>'client_display_name',
 case s.scope_kind when 'SEGMENT' then a.approved_source_ex_vat::numeric when 'EXPENSE_LINE' then l.source_pay_ex_vat::numeric
  when 'EXPENSES_ALL' then(select sum(x.source_pay_ex_vat)::numeric from private.bpay_next_approved_line x
   where x.revision_id=r.id and x.component_kind in ('EXPENSE','MILEAGE'))
  else(select sum(x.source_pay_ex_vat)::numeric from private.bpay_next_approved_line x where x.revision_id=r.id) end,
 null::numeric,null::numeric,null::numeric,null::numeric,null::numeric,null::numeric,null::date,
 s.until_date,s.active and(s.until_date is null or s.until_date>=(p_context->>'local_date')::date),s.active and s.until_date is null,
 case when s.active and(s.until_date is null or s.until_date>=(p_context->>'local_date')::date) then 'Snoozed' else 'Snooze ended' end,
 s.note,w.id,r.physical_timesheet_id
 from private.bpay_next_snooze s join private.bpay_next_work w on w.id=s.work_id and w.candidate_id=s.candidate_id
 join private.bpay_next_work_revision r on r.id=w.current_revision_id and r.work_id=w.id and r.sealed_at_utc is not null
 left join private.bpay_next_revision_worker_basis b on b.revision_id=r.id
 left join private.bpay_next_approved_segment a on a.revision_id=r.id and a.segment_key=s.segment_key and s.scope_kind='SEGMENT'
 left join private.bpay_next_approved_line l on l.revision_id=r.id and l.component_key=s.component_key and s.scope_kind='EXPENSE_LINE'
 left join public.timesheets t on t.timesheet_id=r.physical_timesheet_id
 cross join lateral(select private.bpay_next_payment_client_origin_v1(r.id) value) origin
 where s.candidate_id=p_candidate_id and s.scope_kind<>'CASE'
  and(not p_active_only or(s.active and(s.until_date is null or s.until_date>=(p_context->>'local_date')::date)))
  and s.worker_pay_basis=case s.scope_kind when 'SEGMENT' then private.bpay_next_segment_pay_basis_v1(a.id)
   when 'EXPENSE_LINE' then private.bpay_next_line_pay_basis_v1(l.id) when 'EXPENSES_ALL' then b.expense_basis else b.whole_basis end
$function$;

create or replace function private.bpay_next_candidate_case_report_row_v1(p_candidate_id uuid,p_context jsonb,p_active_only boolean,p_source text,p_row_id uuid)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private,public
as $function$
declare v_source record;v_case jsonb;v_result jsonb;
begin
 select s.* into strict v_source from private.bpay_next_candidate_case_report_sources_v1(p_candidate_id,p_context,p_active_only) s
  where s.source=p_source and s.row_id=p_row_id;
 if p_source='CASE' then v_case:=private.bpay_next_case_list_row_v1(p_row_id,p_context);end if;
 v_result:=jsonb_build_object('row_id',v_source.row_id,'source',v_source.source,'case_id',case when p_source='CASE' then p_row_id else null end,
  'candidate_id',p_candidate_id,'item',v_source.item,'created_at_utc',v_source.created_at_utc,'client',v_source.client,
  'original',v_source.original::text,'funded',v_source.funded::text,'recovered',v_source.recovered::text,'written_off',v_source.written_off::text,
  'reserved',v_source.reserved::text,'outstanding',v_source.outstanding::text,'pending_payout',v_case->'pending_payout',
  'weekly_amount',v_source.weekly_amount::text,'next_date',v_source.next_date,'snoozed_until',v_source.snoozed_until,
  'has_active_snooze',v_source.has_active_snooze,'indefinite_snooze',v_source.indefinite_snooze,'status',v_source.status,'notes',v_source.notes,
  'gap_code',case when p_source='CASE' then v_case->'gap_code' when v_source.client is null then to_jsonb('CASE_CLIENT_UNAVAILABLE'::text) else null end,
  'work_id',v_source.work_id,'physical_timesheet_id',v_source.physical_timesheet_id);
 return v_result;
end
$function$;

-- Keys are typed JSON scalars (money numeric, dates ISO, text lower-case) with
-- a null discriminator; no lexical money ordering and no loaded-row sorting.
create or replace function private.bpay_next_case_finance_sort_key_v1(p_kind text,p_sort text,p_row jsonb)
returns jsonb language sql stable security invoker set search_path=pg_catalog
as $function$
 with key as(select case
  when p_kind='CASE_AUDIT' then case p_sort
   when 'WHEN' then to_jsonb(to_char((p_row->>'occurred_at_utc')::timestamptz at time zone 'UTC','YYYY-MM-DD HH24:MI:SS.US')) when 'EVENT' then to_jsonb(lower(p_row->>'event_label'))
   when 'WHO' then to_jsonb(lower(p_row->>'actor_name')) when 'SOURCE' then to_jsonb(lower(p_row->>'source_label'))
   when 'AMOUNT' then to_jsonb((p_row->>'display_amount')::numeric)
   when 'REASON' then to_jsonb(lower(p_row->>'reason')) when 'DETAILS' then to_jsonb(lower(p_row->>'details')) end
  else case p_sort
   when 'ITEM' then to_jsonb(lower(p_row->>'item')) when 'CLIENT' then to_jsonb(lower(p_row->>'client'))
   when 'ORIGINAL' then to_jsonb((p_row->>'original')::numeric) when 'OUTSTANDING' then to_jsonb((p_row->>'outstanding')::numeric)
   when 'WEEKLY_AMOUNT' then to_jsonb((p_row->>'weekly_amount')::numeric) when 'NEXT_DATE' then p_row->'next_date'
   when 'SNOOZED_UNTIL' then p_row->'snoozed_until' when 'STATUS' then to_jsonb(lower(p_row->>'status'))
   when 'NOTES' then to_jsonb(lower(p_row->>'notes')) end end value)
 select jsonb_build_array(value is null or value='null'::jsonb,case when jsonb_typeof(value)='number' then
  to_jsonb(case when(value::text)::numeric<0 then '0:'||lpad(trunc(999999999999999999-abs((value::text)::numeric)*100)::text,18,'0')
   else '1:'||lpad(trunc((value::text)::numeric*100)::text,18,'0') end) when value is not null and value<>'null'::jsonb and p_sort not in ('WHEN','NEXT_DATE','SNOOZED_UNTIL') then to_jsonb(encode(convert_to(value#>>'{}','UTF8'),'hex')) else coalesce(value,'null'::jsonb) end) from key
$function$;

create or replace function private.bpay_next_case_finance_page_v1(p_actor_user_id uuid,p_kind text,p_args jsonb)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private,public
as $function$
declare
 v_candidate uuid;v_case uuid;v_context jsonb;v_identity jsonb;v_after jsonb;v_scope jsonb;
 v_sort text;v_direction text;v_limit integer;v_active boolean;v_include_count boolean;
 v_row record;v_value jsonb;v_rows jsonb:='[]'::jsonb;v_bytes integer:=16384;v_count integer:=0;
 v_next jsonb;v_more boolean:=false;v_total text;v_summary jsonb;v_header jsonb;v_result jsonb;
 v_prior_section integer;v_prior_key jsonb;v_prior_source text;v_prior_id uuid;v_prior_ordinal smallint;
begin
 if p_kind not in ('CASE_AUDIT','CANDIDATE_CASE_REPORT') or p_args is null or jsonb_typeof(p_args) is distinct from 'object'
  or octet_length(p_args::text)>32768 or p_args-array['candidate_id','case_id','sort','direction','limit','after','active_only','include_count']<>'{}'::jsonb
  or not(p_args ?& array['candidate_id','sort','direction','limit','after','include_count'])
  or jsonb_typeof(p_args->'candidate_id') is distinct from 'string'
  or p_args->>'candidate_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
  or jsonb_typeof(p_args->'sort') is distinct from 'string' or jsonb_typeof(p_args->'direction') is distinct from 'string'
  or jsonb_typeof(p_args->'limit') is distinct from 'number' or p_args->>'limit'!~'^[1-9][0-9]{0,2}$'
  or jsonb_typeof(p_args->'include_count') is distinct from 'boolean'
  or jsonb_typeof(p_args->'after') not in ('object','null') then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_FINANCE_REQUEST_INVALID';end if;
 v_candidate:=(p_args->>'candidate_id')::uuid;v_limit:=(p_args->>'limit')::integer;
 v_sort:=p_args->>'sort';v_direction:=p_args->>'direction';v_include_count:=(p_args->>'include_count')::boolean;
 if v_limit>100 or v_direction not in ('ASC','DESC') then raise exception using errcode='22023',message='BPAY_NEXT_CASE_FINANCE_REQUEST_INVALID';end if;
 if p_kind='CASE_AUDIT' then
  if not(p_args ? 'case_id') or p_args ? 'active_only' or jsonb_typeof(p_args->'case_id') is distinct from 'string'
   or p_args->>'case_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
   or v_sort not in ('WHEN','EVENT','WHO','SOURCE','AMOUNT','REASON','DETAILS') then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_FINANCE_REQUEST_INVALID';end if;
  v_case:=(p_args->>'case_id')::uuid;
  if not exists(select 1 from private.bpay_next_finance_case c where c.id=v_case and c.candidate_id=v_candidate) then
   raise exception using errcode='P0002',message='BPAY_NEXT_CASE_FINANCE_NOT_FOUND';end if;
 else
  if p_args ? 'case_id' or not(p_args ? 'active_only') or jsonb_typeof(p_args->'active_only') is distinct from 'boolean'
   or v_sort not in ('ITEM','CLIENT','ORIGINAL','OUTSTANDING','WEEKLY_AMOUNT','NEXT_DATE','SNOOZED_UNTIL','STATUS','NOTES') then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_FINANCE_REQUEST_INVALID';end if;
  v_active:=(p_args->>'active_only')::boolean;
 end if;
 if not exists(select 1 from public.candidates where id=v_candidate) then raise exception using errcode='P0002',message='BPAY_NEXT_CASE_FINANCE_NOT_FOUND';end if;
 select jsonb_build_object('agency_id',d.installation_id,'actor_user_id',p_actor_user_id,'owner_epoch',m.owner_epoch::text)
  into strict v_identity from private.bpay_next_module_control m cross join private.cloudtms_database_identity d
  where m.id=1 and m.active_owner='NEXT' and d.singleton;
 v_context:=case when p_kind='CANDIDATE_CASE_REPORT' then private.bpay_next_case_list_context_v1(p_actor_user_id) else v_identity end;
 v_scope:=jsonb_build_object('kind',p_kind,'candidate_id',v_candidate,'case_id',v_case,'active_only',v_active,'sort',v_sort,'direction',v_direction);
 v_after:=nullif(p_args->'after','null'::jsonb);
 if v_after is not null then
  if (v_after-array['context','scope','section','key','source','source_id','event_ordinal'])<>'{}'::jsonb
   or not(v_after ?& array['context','scope','section','key','source','source_id','event_ordinal']) or octet_length(v_after::text)>16384
   or v_after->'scope' is distinct from v_scope then raise exception using errcode='22023',message='BPAY_NEXT_CASE_FINANCE_REQUEST_INVALID';end if;
  if v_after->'context' is distinct from v_context then raise exception using errcode='55000',message='BPAY_NEXT_CASE_FINANCE_CONTEXT_CHANGED';end if;
  if jsonb_typeof(v_after->'section') is distinct from 'number' or v_after->>'section'!~'^[0-7]$'
   or jsonb_typeof(v_after->'key') is distinct from 'array' or jsonb_array_length(v_after->'key')<>2
   or jsonb_typeof(v_after->'key'->0) is distinct from 'boolean'
   or jsonb_typeof(v_after->'source') is distinct from 'string' or length(v_after->>'source')>40
   or jsonb_typeof(v_after->'source_id') is distinct from 'string' or v_after->>'source_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
   or v_after->>'event_ordinal' not in ('1','2') then raise exception using errcode='22023',message='BPAY_NEXT_CASE_FINANCE_REQUEST_INVALID';end if;
  if jsonb_typeof(v_after->'event_ordinal') is distinct from 'string'
   or ((v_after->'key'->>0)::boolean and v_after->'key'->1 is distinct from 'null'::jsonb)
   or (not(v_after->'key'->>0)::boolean and(jsonb_typeof(v_after->'key'->1) is distinct from 'string'
    or octet_length(v_after->'key'->>1)>16384))
   or(p_kind='CASE_AUDIT' and ((v_after->>'section')::integer<>0 or v_after->>'source' not in
    ('CREATE_REQUEST','EDIT_REQUEST','PAUSE_REQUEST','PAUSE_OUTCOME','WRITE_OFF_REQUEST','CASE_EVENT','NET_REQUEST','HOLD_TRANSITION','FINANCIAL_EFFECT','SNOOZE_EVENT')))
   or(p_kind='CANDIDATE_CASE_REPORT' and ((v_after->>'section')::integer not between 1 and 7 or v_after->>'source' not in ('CASE','TIMESHEET_SNOOZE'))) then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_FINANCE_REQUEST_INVALID';end if;
  if not(v_after->'key'->>0)::boolean then
   if (v_sort in ('AMOUNT','ORIGINAL','OUTSTANDING','WEEKLY_AMOUNT') and v_after->'key'->>1!~'^[01]:[0-9]{18}$')
    or(v_sort='WHEN' and v_after->'key'->>1!~'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}$')
    or(v_sort in ('NEXT_DATE','SNOOZED_UNTIL') and v_after->'key'->>1!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
    or(v_sort not in ('AMOUNT','ORIGINAL','OUTSTANDING','WEEKLY_AMOUNT','WHEN','NEXT_DATE','SNOOZED_UNTIL')
      and v_after->'key'->>1!~'^([0-9a-f]{2})*$') then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_FINANCE_REQUEST_INVALID';end if;
  end if;
  v_prior_section:=(v_after->>'section')::integer;v_prior_key:=v_after->'key';v_prior_source:=v_after->>'source';
  v_prior_id:=(v_after->>'source_id')::uuid;v_prior_ordinal:=(v_after->>'event_ordinal')::smallint;
 end if;
 select jsonb_build_object('candidate_name',coalesce(nullif(btrim(display_name),''),nullif(btrim(concat_ws(' ',first_name,last_name)),'')),
  'candidate_reference',tms_ref,'case_label',(select k.case_type_label from private.bpay_next_case_list_key k where k.case_id=v_case and k.candidate_id=v_candidate))
 into v_header from public.candidates where id=v_candidate;
 if v_include_count then
  if p_kind='CASE_AUDIT' then select count(*)::text into v_total from private.bpay_next_case_audit_sources_v1(v_case,v_candidate);
  else
   select count(*)::text,jsonb_build_object('payment_advances_outstanding',coalesce(sum(s.outstanding) filter(where s.section=1),0)::text,
    'overpayments_outstanding',coalesce(sum(s.outstanding) filter(where s.section=2),0)::text,
    'snoozed_items',case when count(*) filter(where s.has_active_snooze is null)>0 then null else count(*) filter(where s.has_active_snooze)::text end)
   into v_total,v_summary from private.bpay_next_candidate_case_report_sources_v1(v_candidate,v_context,v_active) s;
  end if;
 end if;
 -- Scalar sources are sorted over the whole admitted scope on the server.
 -- No JSON aggregation over history and no user-supplied SQL identifiers.
 for v_row in
  with source_rows as (
   select a.source,a.source_id,a.event_ordinal,0 section,
    jsonb_build_object('occurred_at_utc',a.occurred_at_utc,'event_label',a.event_label,
     'actor_name',case a.actor_kind when 'SYSTEM' then 'Banking Pay' when 'HUMAN' then coalesce(nullif(btrim(u.display_name),''),'Unknown') else 'Unknown' end,
     'source_label',a.source_label,'display_amount',(case when a.event_label='Recovered' then -coalesce(a.target_amount_inc_vat,a.source_amount_ex_vat) else coalesce(a.target_amount_inc_vat,a.source_amount_ex_vat) end)::text,'target_amount_inc_vat',a.target_amount_inc_vat::text,
     'source_amount_ex_vat',a.source_amount_ex_vat::text,'reason',a.reason,'details',a.details) value
   from private.bpay_next_case_audit_sources_v1(v_case,v_candidate) a left join public.tms_users u on u.id=a.actor_user_id
    where p_kind='CASE_AUDIT'
   union all
   select s.source,s.row_id,1::smallint,s.section,
    jsonb_build_object('item',s.item,'client',s.client,'original',s.original::text,'outstanding',s.outstanding::text,
     'weekly_amount',s.weekly_amount::text,'next_date',s.next_date,'snoozed_until',s.snoozed_until,'status',s.status,'notes',s.notes)
    from private.bpay_next_candidate_case_report_sources_v1(v_candidate,v_context,v_active) s where p_kind='CANDIDATE_CASE_REPORT'
  ), keyed as(
   select s.*,private.bpay_next_case_finance_sort_key_v1(p_kind,v_sort,s.value) key from source_rows s
  )
  select k.* from keyed k where v_after is null or k.section>v_prior_section or(k.section=v_prior_section and
   ((v_direction='ASC' and k.key>v_prior_key) or(v_direction='DESC' and k.key<v_prior_key)
    or(k.key=v_prior_key and(k.source,k.source_id,k.event_ordinal)>(v_prior_source,v_prior_id,v_prior_ordinal))))
  order by k.section,case when v_direction='ASC' then k.key end asc,case when v_direction='DESC' then k.key end desc,
   k.source,k.source_id,k.event_ordinal limit v_limit+1
 loop
  if v_count=v_limit then v_more:=true;exit;end if;
  if octet_length(v_row.key->>1)>16384 then raise exception using errcode='54000',message='BPAY_NEXT_CASE_FINANCE_ROW_TOO_LARGE';end if;
  v_value:=case when p_kind='CASE_AUDIT' then private.bpay_next_case_audit_row_v1(v_row.source,v_row.source_id,v_row.event_ordinal,v_case,v_candidate) else private.bpay_next_candidate_case_report_row_v1(v_candidate,v_context,v_active,v_row.source,v_row.source_id) end
   ||jsonb_build_object('section',v_row.section,'sort_key',v_row.key);
  if v_bytes+octet_length(v_value::text)+1>120000 then
   if v_count=0 then raise exception using errcode='54000',message='BPAY_NEXT_CASE_FINANCE_ROW_TOO_LARGE';end if;
   v_more:=true;exit;
  end if;
  v_rows:=v_rows||jsonb_build_array(v_value);v_bytes:=v_bytes+octet_length(v_value::text)+1;v_count:=v_count+1;
  v_next:=jsonb_build_object('context',v_context,'scope',v_scope,'section',v_row.section,'key',v_row.key,
   'source',v_row.source,'source_id',v_row.source_id,'event_ordinal',v_row.event_ordinal::text);
 end loop;
 if v_more and octet_length(v_next::text)>16384 then raise exception using errcode='54000',message='BPAY_NEXT_CASE_FINANCE_ROW_TOO_LARGE';end if;
 if p_kind='CANDIDATE_CASE_REPORT' and private.bpay_next_case_list_context_v1(p_actor_user_id) is distinct from v_context then
  raise exception using errcode='55000',message='BPAY_NEXT_CASE_FINANCE_CONTEXT_CHANGED';end if;
 v_result:=jsonb_build_object('kind',p_kind,'context',v_context,'scope',v_scope,'header',v_header,'rows',v_rows,
  'complete',not v_more,'next_cursor',case when v_more then v_next else null end,'total_count',v_total,'summary',v_summary);
 if octet_length(v_result::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_CASE_FINANCE_ROW_TOO_LARGE';end if;
 return v_result;
end
$function$;

create or replace function private.bpay_next_prepare_case_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,
  p_expected_cursor bigint default null,p_limit integer default 100
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_plan jsonb;v_plan_period jsonb;v_plan_row jsonb;v_plan_instructions jsonb;
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_saved_cursor bigint;
  v_run private.bpay_next_pay_run%rowtype;
  v_control private.bpay_next_worker_control%rowtype;
  v_job private.bpay_next_job%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;
  v_selection private.bpay_next_case_selection%rowtype;
  v_state private.bpay_next_case_allocation_state%rowtype;
  v_channel private.bpay_next_case_allocation_channel%rowtype;
  v_week private.bpay_next_worker_week%rowtype;
  v_case private.bpay_next_finance_case%rowtype;
  v_component private.bpay_next_case_component%rowtype;
  v_rule private.bpay_next_case_rule%rowtype;
  v_period_rule private.bpay_next_case_rule%rowtype;
  v_origin private.bpay_next_case_create_request%rowtype;
  v_collection private.bpay_next_work_collection%rowtype;
  v_policy_id uuid;v_case_cut jsonb;v_memory_cut boolean;v_case_due date;v_case_status text;
  v_automatic boolean;v_week_not_affected boolean;
  v_period private.bpay_next_case_period%rowtype;
  v_item record;v_prior record;v_value record;v_row record;
  v_instruction private.bpay_next_run_case_instruction%rowtype;
  v_floor numeric;v_outstanding numeric;v_case_outstanding numeric;
  v_nominal numeric;v_opening_due numeric;v_capacity numeric;v_taken numeric;v_e numeric;v_h numeric;
  v_next_e numeric;v_next_h numeric;
  v_gross numeric;v_week_start date;v_age timestamptz;v_age_key bigint;
  v_purpose text;v_explanation text;v_issue text;v_seen bigint:=0;
  v_result_id uuid;v_hold_id uuid;v_instruction_id uuid;v_has_more boolean;v_terminal boolean:=false;
  v_cap_reason text;v_affordability_reason text;v_capacity_reason text;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1
     or p_expected_cursor<0 or p_limit is null or p_limit not between 1 and 100 then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_PREPARE_INPUT_INVALID';
  end if;
  select private.bpay_next_prepare_job_run_v1(j.id),j.candidate_id into strict v_run_id,v_candidate
    from private.bpay_next_job j
    where j.id=p_job_id and j.job_kind='PREPARE';
  select m.owner_epoch into v_epoch from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share nowait;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  -- Same lock order as the real lane; root UPDATE fences confirmation/cancel.
  select r.* into strict v_run from private.bpay_next_pay_run r where r.id=v_run_id for update nowait;
  select c.* into strict v_control from private.bpay_next_worker_control c where c.candidate_id=v_candidate for update nowait;
  select j.* into strict v_job from private.bpay_next_job j where j.id=p_job_id for update nowait;
  select w.* into strict v_worker from private.bpay_next_run_worker w
    where w.run_id=v_run_id and w.candidate_id=v_candidate for update nowait;
  select s.* into v_state from private.bpay_next_case_allocation_state s where s.run_worker_id=v_worker.id and s.preparation_revision=v_worker.captured_preparation_revision
    and s.run_worker_id=v_worker.id and s.pass_kind='DRAFT' and s.projection_no=0 for update nowait;
  if v_job.module_epoch<>v_epoch then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_PREPARE_OWNER_STALE';
  end if;
  if v_job.status='DONE' and v_job.phase='READY' and v_state.status='COMPLETE'
     and v_worker.status in ('READY','DRAFT','ISSUED','COMPLETE','CANCELLING','CANCELLED') then
    return pg_catalog.jsonb_build_object('done',true,'phase','READY','cursor',null,'rows_visited','0',
      'replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status);
  end if;
  if v_job.status='DONE' and v_job.phase='DONE' and v_worker.status='REVIEW'
     and v_worker.review_issue_code in ('CASE_SELECTION_REQUIRED','CASE_TARGET_UNSUPPORTED','CASE_INPUT_UNBOUND',
       'CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND','CASE_NO_PAYABLE_AMOUNT') then
    return pg_catalog.jsonb_build_object('done',true,'phase','REVIEW','cursor',null,'rows_visited','0',
      'replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status,'issue_code',v_worker.review_issue_code);
  end if;
  if not private.bpay_next_prepare_job_active_v1(p_job_id) then
    raise exception using errcode='55000',message='BPAY_NEXT_WORKER_ACTION_REVISION_STALE';end if;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'SEALED' or v_job.status<>'LEASED'
     or v_job.phase<>'FINANCE_ALLOCATE' or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() or v_control.active_owner_epoch<>p_owner_epoch
     or v_worker.status<>'PREPARING' or v_worker.review_issue_code is not null
     or v_worker.financial_resolution_count<>0 then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_PREPARE_STATE_INVALID';
  end if;
  if v_worker.captured_position_count<>v_worker.captured_line_count+v_worker.captured_managed_position_count
     or v_worker.expected_managed_position_count<>v_worker.captured_managed_position_count
     or exists(select 1 from private.bpay_next_run_position where run_worker_id=v_worker.id
       and work_collection_id is not null and not managed_capture_completed) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_LINE_COUNT_MISMATCH';
  end if;
  if v_job.cursor_key is not null and v_job.cursor_key !~ '^[0-9]{1,19}$' then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_CURSOR_CORRUPT';
  end if;
  v_saved_cursor:=coalesce(v_job.cursor_key::bigint,0);
  if coalesce(p_expected_cursor,0)<v_saved_cursor then
    -- Lost STEP reply: read back the persisted checkpoint without a second
    -- capture, hold, cursor spend or financial-view bump. Ahead is not replay.
    return pg_catalog.jsonb_build_object('done',false,'phase','FINANCE_ALLOCATE','cursor',v_job.cursor_key,
      'rows_visited','0','replay',true,'run_worker_id',v_worker.id,'worker_status',v_worker.status);
  end if;
  if coalesce(p_expected_cursor,0)>v_saved_cursor then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_PREPARE_CURSOR_STALE';
  end if;
  perform private.bpay_next_assert_candidate_financial_fence_v1(v_candidate,p_job_id,p_lease_nonce,p_owner_epoch);
  -- Only a positively proved no-case context may retain the old owner path.
  if v_state.id is null and not exists(select 1 from private.bpay_next_case_selection s where s.run_id=v_run_id and s.candidate_id=v_candidate)
     and (not exists(select 1 from private.bpay_next_finance_case c where c.candidate_id=v_candidate)
       or (v_run.accepted_choice_descriptor is not null
         and exists(select 1 from private.bpay_next_selection_candidate selected where selected.run_id=v_run_id and selected.candidate_id=v_candidate)
         and not exists(select 1 from private.bpay_next_run_choice_member member where member.run_id=v_run_id
           and member.member_kind='CASE_COMPONENT' and member.candidate_id=v_candidate))) then
    return private.bpay_next_complete_no_case_worker_v1(p_job_id,p_lease_nonce,p_owner_epoch);
  end if;

  <<prepare_page>>
  begin
    -- A resumed weekly/capture/allocation page uses its exact frozen selection
    -- revision. Refuse Source origins before any stage writes, including when
    -- the collection's current qualification reader would return no row.
    if v_state.id is not null and exists(
      select 1 from private.bpay_next_case_selection_item si
      join private.bpay_next_work_collection c on c.case_id=si.case_id
        and c.case_component_id=si.case_component_id and c.candidate_id=si.candidate_id
      join private.bpay_next_work w on w.id=c.work_id and w.candidate_id=si.candidate_id
      where si.run_id=v_run_id and si.candidate_id=v_candidate
        and si.selection_revision=v_state.selection_revision and si.is_selected
        and w.work_kind='SOURCE') then
      raise exception using errcode='55000',message='BPAY_NEXT_COMPARISON_NOT_INSTALLED';
    end if;
    if v_state.id is null then
      select s.* into v_selection from private.bpay_next_case_selection s
        where s.run_id=v_run_id and s.candidate_id=v_candidate order by s.selection_revision desc limit 1;
      if v_selection.run_id is null or v_selection.status<>'SEALED' then
        v_issue:='CASE_SELECTION_REQUIRED';exit prepare_page;
      end if;
      -- Comparison is owned by the later CM chain. Existing immutable Source
      -- origin identity is enough to refuse it; no absent pack column/type or
      -- fabricated financial certificate is consulted or created here.
      if exists(select 1 from private.bpay_next_case_selection_item si
        join private.bpay_next_work_collection c on c.case_id=si.case_id
          and c.case_component_id=si.case_component_id and c.candidate_id=si.candidate_id
        join private.bpay_next_work w on w.id=c.work_id and w.candidate_id=si.candidate_id
        where si.run_id=v_run_id and si.candidate_id=v_candidate
          and si.selection_revision=v_selection.selection_revision and si.is_selected
          and w.work_kind='SOURCE') then
        raise exception using errcode='55000',message='BPAY_NEXT_COMPARISON_NOT_INSTALLED';
      end if;
      if v_worker.target_pay_channel<>'PAYE' or v_worker.gross_vat<>0
         or v_worker.gross_ex_vat<>v_worker.gross_inc_vat then
        v_issue:='CASE_TARGET_UNSUPPORTED';exit prepare_page;
      end if;
      select selected.min_take_home_wtd into strict v_floor from private.bpay_next_selection_candidate selected
        where selected.run_id=v_run_id and selected.candidate_id=v_candidate;
      if v_floor is null or v_floor::text in ('NaN','Infinity','-Infinity') or v_floor<0
         or v_floor<>pg_catalog.trunc(v_floor,2) then
        v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
      end if;
      v_week_start:=v_run.pay_date-(extract(isodow from v_run.pay_date)::integer-1);
      -- OVERPAYMENT gross recovery does not consume the protected-debt H/floor
      -- operand (0290). Prove the exact nonempty SEALED selected set, not merely
      -- that no protected row happened to be found. Missing identities/counts
      -- remain week-dependent; instruction capture and allocation re-prove it.
      select count(*)=v_selection.selected_count and count(*)>0
        and coalesce(bool_and((fc.case_kind,cc.case_kind,cc.instruction_kind,cc.payroll_stage)
          is not distinct from ('OVERPAYMENT','OVERPAYMENT','RECOVERY','GROSS_DEDUCT')),false)
        into v_week_not_affected
        from private.bpay_next_case_selection_item si
        left join private.bpay_next_case_component cc
          on cc.id=si.case_component_id and cc.case_id=si.case_id and cc.candidate_id=si.candidate_id
        left join private.bpay_next_finance_case fc on fc.id=si.case_id and fc.candidate_id=si.candidate_id
        where si.run_id=v_selection.run_id and si.candidate_id=v_selection.candidate_id
          and si.selection_revision=v_selection.selection_revision and si.is_selected;
      -- Exact indexed metadata absence is required BEFORE establishing a new
      -- zero week. Missing prior NEW facts are unresolved, not zero/history SUM.
      if not v_week_not_affected and exists(select 1 from private.bpay_next_run_worker w
        join private.bpay_next_pay_run r on r.id=w.run_id
        where w.candidate_id=v_candidate and w.status in ('READY','DRAFT','ISSUED','COMPLETE')
          and r.pay_date>=v_week_start and r.pay_date<v_week_start+7
          and (r.pay_date,r.created_at_utc,w.id)<(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and not exists(select 1 from private.bpay_next_worker_week_contribution x where x.original_run_worker_id=w.id)) then
        v_issue:='CASE_WEEK_BASIS_UNBOUND';exit prepare_page;
      end if;
      insert into private.bpay_next_worker_week(candidate_id,pay_week_start,period_revision,
        default_floor,default_floor_revision,resolved_arranged_take_home)
        values(v_candidate,v_week_start,1,v_floor,1,0) on conflict(candidate_id,pay_week_start) do nothing;
      select w.* into strict v_week from private.bpay_next_worker_week w
        where w.candidate_id=v_candidate and w.pay_week_start=v_week_start for update;
      if not v_week_not_affected and v_week.unresolved_contribution_count<>0 then v_issue:='CASE_WEEK_BASIS_UNBOUND';exit prepare_page;end if;
      if v_week.default_floor<>v_floor then
        update private.bpay_next_worker_week set default_floor=v_floor,default_floor_revision=default_floor_revision+1,
          period_revision=period_revision+1 where candidate_id=v_candidate and pay_week_start=v_week_start returning * into v_week;
      end if;
      update private.bpay_next_run_worker set case_selection_revision=v_selection.selection_revision where id=v_worker.id;
      v_worker.case_selection_revision:=v_selection.selection_revision;
      insert into private.bpay_next_case_allocation_state
        (run_worker_id,run_id,candidate_id,selection_revision,preparation_revision,pass_kind,projection_no,
         command_id,job_id,module_epoch,owner_epoch,financial_view_revision,status,weekly_binding_state,
         expected_instruction_count,initial_worker_take_home,remaining_worker_take_home,prepare_stage,
         captured_default_floor,captured_default_floor_revision,pay_week_start,captured_week_revision,captured_work_gross)
        values(v_worker.id,v_run_id,v_candidate,v_selection.selection_revision,v_worker.captured_preparation_revision,'DRAFT',0,
         v_job.command_id,p_job_id,v_epoch,p_owner_epoch,v_control.financial_view_revision,'BUILDING',
         case when v_week_not_affected then 'NOT_AFFECTED' else 'UNBOUND' end,
         v_selection.selected_count,v_worker.gross_ex_vat,v_worker.gross_ex_vat,
         case when v_week_not_affected then 'INSTRUCTION_CAPTURE' else 'WEEK_CAPTURE' end,
         v_floor,v_week.default_floor_revision,v_week_start,v_week.period_revision,v_worker.gross_ex_vat)
        returning * into v_state;
      insert into private.bpay_next_case_allocation_channel(state_id,allocation_channel,initial_headroom,remaining_headroom)
        values(v_state.id,'PAYE',v_worker.gross_ex_vat,v_worker.gross_ex_vat);
    else
      if v_state.status<>'BUILDING' or v_state.preparation_revision<>v_worker.captured_preparation_revision
         or v_state.selection_revision<>v_worker.case_selection_revision or v_state.prepare_stage='NOT_CASE_PREPARE' then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_BINDING_MISMATCH';
      end if;
      -- Lease takeover does not reset the captured economic cursors/headroom.
      if v_state.owner_epoch<>p_owner_epoch then
        update private.bpay_next_case_allocation_state set owner_epoch=p_owner_epoch where id=v_state.id;
        v_state.owner_epoch:=p_owner_epoch;
      end if;
    end if;

    if v_state.prepare_stage='WEEK_CAPTURE' then
      for v_prior in select x.* from private.bpay_next_worker_week_contribution x
        where x.candidate_id=v_candidate and x.pay_week_start=v_state.pay_week_start
          and (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
            <(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and (v_state.prior_worker_cursor is null or
            (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
              >(v_state.prior_pay_date_cursor,v_state.prior_created_at_cursor,v_state.prior_worker_cursor))
        order by x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id limit p_limit
      loop
        v_seen:=v_seen+1;
        if v_prior.eligibility_state='UNBOUND' then
          v_issue:=case when v_prior.payment_state in ('RETURNED_OWED','REISSUED_PAID')
            then 'CASE_RETURN_FLOOR_UNBOUND' else 'CASE_WEEK_BASIS_UNBOUND' end;
          exit prepare_page;
        end if;
        v_state.captured_prior_take_home:=v_state.captured_prior_take_home+v_prior.eligible_arranged_amount;
        v_state.initial_worker_take_home:=v_state.initial_worker_take_home+v_prior.eligible_arranged_amount;
        v_state.remaining_worker_take_home:=v_state.remaining_worker_take_home+v_prior.eligible_arranged_amount;
        v_state.prior_pay_date_cursor:=v_prior.original_pay_date;
        v_state.prior_created_at_cursor:=v_prior.original_created_at_utc;
        v_state.prior_worker_cursor:=v_prior.original_run_worker_id;
      end loop;
      select exists(select 1 from private.bpay_next_worker_week_contribution x
        where x.candidate_id=v_candidate and x.pay_week_start=v_state.pay_week_start
          and (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)<(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and (v_state.prior_worker_cursor is null or (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
            >(v_state.prior_pay_date_cursor,v_state.prior_created_at_cursor,v_state.prior_worker_cursor))) into v_has_more;
      update private.bpay_next_case_allocation_state set captured_prior_take_home=v_state.captured_prior_take_home,
        initial_worker_take_home=v_state.initial_worker_take_home,remaining_worker_take_home=v_state.remaining_worker_take_home,
        prior_pay_date_cursor=v_state.prior_pay_date_cursor,prior_created_at_cursor=v_state.prior_created_at_cursor,
        prior_worker_cursor=v_state.prior_worker_cursor,weekly_binding_state=case when v_has_more then 'UNBOUND' else 'BOUND' end,
        prepare_stage=case when v_has_more then 'WEEK_CAPTURE' else 'INSTRUCTION_CAPTURE' end where id=v_state.id;
      exit prepare_page;
    end if;

    if v_state.prepare_stage='INSTRUCTION_CAPTURE' then
      for v_item in select i.* from private.bpay_next_case_selection_item i
        where i.run_id=v_run_id and i.candidate_id=v_candidate and i.selection_revision=v_state.selection_revision
          and i.is_selected and i.selection_no>v_state.instruction_capture_cursor order by i.selection_no limit p_limit
      loop
        v_seen:=v_seen+1;
        select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_item.case_id and c.candidate_id=v_candidate for update;
        select c.* into strict v_component from private.bpay_next_case_component c
          where c.id=v_item.case_component_id and c.case_id=v_case.id and c.candidate_id=v_candidate for update;
        -- Original Case/component locks precede the original period lock.
        perform 1 from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_state.pay_week_start for update;
        begin
          v_plan:=private.bpay_next_case_allocation_plan_v1(jsonb_build_object('mode','DERIVE',
            'candidate_id',v_candidate,'case_id',v_case.id,'case_component_id',v_component.id,
            'pay_date',v_run.pay_date,'pay_week_start',v_state.pay_week_start,
            'target_pay_channel',v_worker.target_pay_channel,'captured_default_floor',v_state.captured_default_floor,
            'captured_default_floor_revision',v_state.captured_default_floor_revision,
            'weekly_binding_state',v_state.weekly_binding_state,'accepted_run_id',v_run_id));
        exception when check_violation then
          if sqlerrm in ('BPAY_NEXT_CASE_INPUT_UNBOUND','BPAY_NEXT_CASE_TARGET_UNSUPPORTED','BPAY_NEXT_CASE_WEEK_BASIS_UNBOUND') then
            v_issue:=substring(sqlerrm from 11);exit prepare_page;
          else raise;end if;
        end;
        v_plan_period:=v_plan->'period';
        if v_plan->>'period_operation'='INSERT' then
          insert into private.bpay_next_case_period(case_component_id,case_id,candidate_id,pay_week_start,rule_id,
            period_revision,opening_outstanding_source_ex_vat,opening_due_source_ex_vat,work_collection_id)
            values(v_component.id,v_case.id,v_candidate,v_state.pay_week_start,(v_plan_period->>'rule_id')::uuid,
              (v_plan_period->>'period_revision')::bigint,(v_plan_period->>'opening_outstanding_source_ex_vat')::numeric,
              (v_plan_period->>'opening_due_source_ex_vat')::numeric,(v_plan_period->>'work_collection_id')::uuid)
            on conflict(case_component_id,pay_week_start) do nothing;
        end if;
        select p.* into strict v_period from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_state.pay_week_start for update;
        if v_plan->>'period_operation'='CASE_EDIT_OPENING' then
          perform private.bpay_next_apply_case_period_amendment_v1(
            (v_plan->'period_amendment'->>'edit_command_id')::uuid,v_plan->'period_amendment'->'before',v_plan_period);
          select amended_period.* into strict v_period from private.bpay_next_case_period amended_period
            where case_component_id=v_component.id and pay_week_start=v_state.pay_week_start;
        end if;
        if v_plan->>'period_operation'='FUNDING_ZERO' then
          update private.bpay_next_case_period set rule_id=(v_plan_period->>'rule_id')::uuid,
            opening_outstanding_source_ex_vat=(v_plan_period->>'opening_outstanding_source_ex_vat')::numeric,
            opening_due_source_ex_vat=(v_plan_period->>'opening_due_source_ex_vat')::numeric,period_revision=period_revision+1
            where case_component_id=v_component.id and pay_week_start=v_state.pay_week_start
              and period_revision=(v_plan_period->>'period_revision')::bigint-1
              and (opening_outstanding_source_ex_vat,opening_due_source_ex_vat,realised_recovery_source_ex_vat,
                active_unrealised_recovery_source_ex_vat,active_payout_source_ex_vat)
                is not distinct from (0::numeric,0::numeric,0::numeric,0::numeric,0::numeric) returning * into v_period;
          if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_INPUT_UNBOUND';end if;
        end if;
        if to_jsonb(v_period) is distinct from v_plan_period then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_PLAN_PERIOD_CHANGED';end if;
        update private.bpay_next_case_component set resolution_state='RESOLVED',target_pay_channel='PAYE',
          target_ex_vat=(v_plan->'resolution'->>'target_ex_vat')::numeric,target_vat=(v_plan->'resolution'->>'target_vat')::numeric,
          target_inc_vat=(v_plan->'resolution'->>'target_inc_vat')::numeric,
          valuation_policy_id=(v_plan->'resolution'->>'valuation_policy_id')::uuid,
          valuation_window_id=(v_plan->'resolution'->>'valuation_window_id')::uuid,
          resolution_ref=v_plan->'resolution'->>'resolution_ref',component_revision=component_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id returning * into v_component;
        v_plan:=jsonb_set(v_plan,'{instruction}',(v_plan->'instruction')||jsonb_build_object(
          'run_worker_id',v_worker.id,'run_id',v_run_id,'candidate_id',v_candidate,
          'selection_revision',v_state.selection_revision,'preparation_revision',v_worker.captured_preparation_revision,
          'captured_case_revision',v_case.case_revision,'captured_component_revision',v_component.component_revision,
          'captured_period_revision',v_period.period_revision));
        insert into private.bpay_next_run_case_instruction
          (run_worker_id,run_id,candidate_id,selection_revision,preparation_revision,case_component_id,case_id,
           captured_case_revision,captured_component_revision,captured_period_revision,rule_id,rule_revision,
           case_kind,case_subtype,tax_treatment,instruction_kind,direction,payroll_stage,hold_purpose,
           age_key,debt_age_at_utc,component_ordinal,pay_week_start,source_pay_channel,target_pay_channel,allocation_channel,currency,
           nominal_source_ex_vat,nominal_target_ex_vat,nominal_target_vat,nominal_target_inc_vat,
           capacity_source_ex_vat,capacity_target_ex_vat,capacity_target_vat,capacity_target_inc_vat,
           minimum_earnings_threshold,take_home_floor_override,captured_default_floor,captured_default_floor_revision,
           resolved_take_home_floor,valuation_policy_id,valuation_window_id,resolution_ref,explanation,work_collection_id)
          select planned.run_worker_id,planned.run_id,planned.candidate_id,planned.selection_revision,planned.preparation_revision,planned.case_component_id,planned.case_id,planned.captured_case_revision,planned.captured_component_revision,planned.captured_period_revision,planned.rule_id,planned.rule_revision,planned.case_kind,planned.case_subtype,planned.tax_treatment,planned.instruction_kind,planned.direction,planned.payroll_stage,planned.hold_purpose,planned.age_key,planned.debt_age_at_utc,planned.component_ordinal,planned.pay_week_start,planned.source_pay_channel,planned.target_pay_channel,planned.allocation_channel,planned.currency,planned.nominal_source_ex_vat,planned.nominal_target_ex_vat,planned.nominal_target_vat,planned.nominal_target_inc_vat,planned.capacity_source_ex_vat,planned.capacity_target_ex_vat,planned.capacity_target_vat,planned.capacity_target_inc_vat,planned.minimum_earnings_threshold,planned.take_home_floor_override,planned.captured_default_floor,planned.captured_default_floor_revision,planned.resolved_take_home_floor,planned.valuation_policy_id,planned.valuation_window_id,planned.resolution_ref,planned.explanation,planned.work_collection_id from jsonb_populate_record(null::private.bpay_next_run_case_instruction,v_plan->'instruction') planned
          returning id into v_instruction_id;
        perform private.bpay_next_capture_instruction_destination_v1(v_instruction_id);
        v_state.instruction_capture_cursor:=v_item.selection_no;
        v_state.captured_instruction_count:=v_state.captured_instruction_count+1;
        if v_component.payroll_stage='NET_ADD' then v_state.payout_instruction_count:=v_state.payout_instruction_count+1;end if;
        update private.bpay_next_run_worker set case_instruction_count=case_instruction_count+1 where id=v_worker.id;
      end loop;
      select exists(select 1 from private.bpay_next_case_selection_item i where i.run_id=v_run_id
        and i.candidate_id=v_candidate and i.selection_revision=v_state.selection_revision and i.is_selected
        and i.selection_no>v_state.instruction_capture_cursor) into v_has_more;
      if not v_has_more and v_state.captured_instruction_count<>v_state.expected_instruction_count then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_CAPTURE_COUNT_MISMATCH';
      end if;
      update private.bpay_next_case_allocation_state set instruction_capture_cursor=v_state.instruction_capture_cursor,
        captured_instruction_count=v_state.captured_instruction_count,payout_instruction_count=v_state.payout_instruction_count,
        prepare_stage=case when v_has_more then 'INSTRUCTION_CAPTURE' else 'ALLOCATE' end where id=v_state.id;
      exit prepare_page;
    end if;

    if v_state.weekly_binding_state='NOT_AFFECTED' then
      -- Only frozen instructions are authoritative from this point. This
      -- scalar metadata proof is scoped to this exact run worker/revisions;
      -- it never reconstructs money or reads prior payment history.
      select count(*)=v_state.expected_instruction_count and count(*)>0
        and coalesce(bool_and((i.case_kind,i.instruction_kind,i.payroll_stage,i.hold_purpose)
          is not distinct from ('OVERPAYMENT','RECOVERY','GROSS_DEDUCT','GROSS_RECOVERY')
          and si.case_id is not null),false)
        into v_week_not_affected
        from private.bpay_next_run_case_instruction i
        left join private.bpay_next_case_selection_item si
          on si.run_id=i.run_id and si.candidate_id=i.candidate_id and si.selection_revision=i.selection_revision
            and si.case_id=i.case_id and si.case_component_id=i.case_component_id and si.is_selected
        where i.run_worker_id=v_worker.id and i.run_id=v_run_id and i.candidate_id=v_candidate
          and i.preparation_revision=v_worker.captured_preparation_revision and i.selection_revision=v_state.selection_revision;
      if not v_week_not_affected
        or v_state.captured_instruction_count<>v_state.expected_instruction_count
        or v_worker.case_instruction_count<>v_state.expected_instruction_count
        or not exists(select 1 from private.bpay_next_case_selection cs where cs.run_id=v_run_id
          and cs.candidate_id=v_candidate and cs.selection_revision=v_state.selection_revision
          and cs.status='SEALED' and cs.selected_count=v_state.expected_instruction_count) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_ALLOCATION_NOT_BOUND';
      end if;
    end if;
    if v_state.prepare_stage<>'ALLOCATE' or v_state.weekly_binding_state not in ('BOUND','NOT_AFFECTED') then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_ALLOCATION_NOT_BOUND';
    end if;
    select c.* into strict v_channel from private.bpay_next_case_allocation_channel c
      where c.state_id=v_state.id and c.allocation_channel='PAYE' for update;
    v_e:=v_channel.remaining_headroom;v_h:=v_state.remaining_worker_take_home;
    select coalesce(jsonb_agg(page.value),'[]'::jsonb) into v_plan_instructions from (
      select to_jsonb(i) as value from private.bpay_next_run_case_instruction i
      where i.run_worker_id=v_worker.id and i.preparation_revision=v_worker.captured_preparation_revision
        and i.selection_revision=v_state.selection_revision
        and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
          >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))
      order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id limit p_limit
    ) page;
    v_plan:=private.bpay_next_case_allocation_plan_v1(jsonb_build_object('mode','ALLOCATE',
      'instructions',v_plan_instructions,'channel_headroom',v_e,'worker_take_home',v_h));
    for v_plan_row in select value from jsonb_array_elements(v_plan->'rows') loop
      v_instruction:=jsonb_populate_record(null::private.bpay_next_run_case_instruction,v_plan_row->'instruction');
      v_seen:=v_seen+1;
      v_taken:=(v_plan_row->>'taken_amount')::numeric;v_cap_reason:=v_plan_row->>'cap_reason';
      v_affordability_reason:=v_plan_row->>'affordability_reason';v_capacity_reason:=v_plan_row->>'capacity_reason';
      v_next_e:=(v_plan_row->>'channel_headroom_after')::numeric;v_next_h:=(v_plan_row->>'worker_take_home_after')::numeric;
      insert into private.bpay_next_case_allocation_result
        (state_id,instruction_id,run_worker_id,candidate_id,preparation_revision,selection_revision,result_no,
         pass_kind,allocation_channel,hold_purpose,nominal_target_ex_vat,usable_capacity_target_ex_vat,
         allocated_source_ex_vat,allocated_target_ex_vat,allocated_target_vat,allocated_target_inc_vat,shortfall_target_ex_vat,
         cap_reason,affordability_reason,capacity_reason,channel_headroom_before,channel_headroom_after,worker_take_home_before,worker_take_home_after)
        values(v_state.id,v_instruction.id,v_worker.id,v_candidate,v_worker.captured_preparation_revision,v_state.selection_revision,
         v_state.processed_instruction_count+1,'DRAFT','PAYE',v_instruction.hold_purpose,v_instruction.nominal_target_ex_vat,
         v_instruction.capacity_target_ex_vat,v_taken,v_taken,0,v_taken,v_instruction.nominal_target_ex_vat-v_taken,
         v_cap_reason,v_affordability_reason,v_capacity_reason,v_e,v_next_e,v_h,v_next_h)
        returning id into v_result_id;
      if v_taken>0 then
        select c.* into strict v_case from private.bpay_next_finance_case c where c.id=v_instruction.case_id for update;
        select c.* into strict v_component from private.bpay_next_case_component c where c.id=v_instruction.case_component_id for update;
        select p.* into strict v_period from private.bpay_next_case_period p
          where p.case_component_id=v_component.id and p.pay_week_start=v_instruction.pay_week_start for update;
        insert into private.bpay_next_case_hold
          (run_worker_id,candidate_id,case_id,amount,status,instruction_id,allocation_result_id,allocation_pass_kind,
           case_component_id,purpose,pay_week_start,source_reserved_ex_vat,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat)
          values(v_worker.id,v_candidate,v_case.id,v_taken,'ACTIVE',v_instruction.id,v_result_id,'DRAFT',
           v_component.id,v_instruction.hold_purpose,v_instruction.pay_week_start,v_taken,v_taken,0,v_taken) returning id into v_hold_id;
    perform private.bpay_next_record_case_hold_transition_v1(p_job_id,v_hold_id,'ACTIVE');
        insert into private.bpay_next_case_capacity_use(case_hold_id,case_component_id,case_id,candidate_id,
          pay_week_start,purpose,status,source_amount_ex_vat)
          values(v_hold_id,v_component.id,v_case.id,v_candidate,v_instruction.pay_week_start,v_instruction.hold_purpose,'ACTIVE',v_taken);
        if v_instruction.hold_purpose='PAYOUT' then
          update private.bpay_next_case_component set active_payout_source_ex_vat=active_payout_source_ex_vat+v_taken,
            component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id;
          update private.bpay_next_finance_case set active_payout_hold_amount=active_payout_hold_amount+v_taken,
            active_hold_amount=active_hold_amount+v_taken,case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_case.id;
          update private.bpay_next_case_period set active_payout_source_ex_vat=active_payout_source_ex_vat+v_taken,
            period_revision=period_revision+1 where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start;
        else
          update private.bpay_next_case_component set active_recovery_source_ex_vat=active_recovery_source_ex_vat+v_taken,
            component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id;
          update private.bpay_next_finance_case set active_recovery_hold_amount=active_recovery_hold_amount+v_taken,
            active_hold_amount=active_hold_amount+v_taken,case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_case.id;
          update private.bpay_next_case_period set active_unrealised_recovery_source_ex_vat=active_unrealised_recovery_source_ex_vat+v_taken,
            period_revision=period_revision+1 where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start;
          v_state.recovered_source_total:=v_state.recovered_source_total+v_taken;
          v_state.recovered_target_total:=v_state.recovered_target_total+v_taken;
        end if;
        update private.bpay_next_run_worker set active_case_hold_count=active_case_hold_count+1 where id=v_worker.id;
      end if;
      if v_instruction.payroll_stage='GROSS_ADD' then v_state.allocated_gross_additions:=v_state.allocated_gross_additions+v_taken;
      elsif v_instruction.payroll_stage='GROSS_DEDUCT' then v_state.allocated_gross_deductions:=v_state.allocated_gross_deductions+v_taken;
      elsif v_instruction.payroll_stage='NET_ADD' then v_state.allocated_net_additions:=v_state.allocated_net_additions+v_taken;end if;
      v_e:=v_next_e;v_h:=v_next_h;
      v_state.processed_instruction_count:=v_state.processed_instruction_count+1;
      v_state.cursor_age_key:=v_instruction.age_key;v_state.cursor_case_id:=v_instruction.case_id;
      v_state.cursor_component_ordinal:=v_instruction.component_ordinal;v_state.cursor_case_component_id:=v_instruction.case_component_id;
      update private.bpay_next_run_worker set case_allocated_count=case_allocated_count+1 where id=v_worker.id;
    end loop;
    update private.bpay_next_case_allocation_channel set remaining_headroom=v_e where state_id=v_state.id and allocation_channel='PAYE';
    update private.bpay_next_case_allocation_state set processed_instruction_count=v_state.processed_instruction_count,
      remaining_worker_take_home=v_h,recovered_source_total=v_state.recovered_source_total,recovered_target_total=v_state.recovered_target_total,
      cursor_age_key=v_state.cursor_age_key,cursor_case_id=v_state.cursor_case_id,cursor_component_ordinal=v_state.cursor_component_ordinal,
      cursor_case_component_id=v_state.cursor_case_component_id,allocated_gross_additions=v_state.allocated_gross_additions,
      allocated_gross_deductions=v_state.allocated_gross_deductions,allocated_net_additions=v_state.allocated_net_additions where id=v_state.id;
    select exists(select 1 from private.bpay_next_run_case_instruction i where i.run_worker_id=v_worker.id
      and i.preparation_revision=v_worker.captured_preparation_revision and i.selection_revision=v_state.selection_revision
      and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
        >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))) into v_has_more;
    if not v_has_more then
      if v_state.processed_instruction_count<>v_state.expected_instruction_count then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_PREPARE_ALLOCATION_COUNT_MISMATCH';
      end if;
      v_gross:=v_state.captured_work_gross+v_state.allocated_gross_additions-v_state.allocated_gross_deductions;
      if v_gross<0 or v_gross>=10000000000000000 or v_state.allocated_net_additions>=10000000000000000 then
        v_issue:='CASE_INPUT_UNBOUND';exit prepare_page;
      end if;
      -- Preserve certified-zero WORK/Source rows; case-only NET additions are
      -- real payable cash, not a made-up positive payroll gross or net.
      if v_worker.captured_line_count=0 and v_gross=0 and v_state.allocated_net_additions=0 then
        v_issue:='CASE_NO_PAYABLE_AMOUNT';exit prepare_page;
      end if;
      if v_worker.captured_line_count=0 and v_gross=0 and v_state.payout_instruction_count>0 then
        insert into private.bpay_next_case_payout_basis(run_worker_id,candidate_id,preparation_revision,currency,
          beneficiary_kind,beneficiary_id,expected_instruction_count,captured_instruction_count,source_total_ex_vat,
          target_total_ex_vat,target_total_vat,target_total_inc_vat,status)
          values(v_worker.id,v_candidate,v_worker.captured_preparation_revision,'GBP','CANDIDATE',v_candidate,
            v_state.payout_instruction_count,v_state.payout_instruction_count,v_state.allocated_net_additions,
            v_state.allocated_net_additions,0,v_state.allocated_net_additions,'SEALED');
      end if;
      select w.* into strict v_week from private.bpay_next_worker_week w
        where w.candidate_id=v_candidate and w.pay_week_start=v_state.pay_week_start for update;
      insert into private.bpay_next_worker_week_contribution(candidate_id,pay_week_start,original_run_worker_id,
        original_pay_date,original_created_at_utc,contribution_revision,basis_kind,original_gross_amount,
        payment_state,eligibility_state,eligible_arranged_amount,primary_binding_revision)
        values(v_candidate,v_state.pay_week_start,v_worker.id,v_run.pay_date,v_run.created_at_utc,1,'GROSS_FALLBACK',
          v_gross,'ARRANGED',case when v_gross>0 then 'ELIGIBLE' else 'EXCLUDED' end,v_gross,1);
      update private.bpay_next_worker_week set resolved_arranged_take_home=resolved_arranged_take_home+v_gross,
        period_revision=period_revision+1 where candidate_id=v_candidate and pay_week_start=v_state.pay_week_start;
      update private.bpay_next_case_allocation_state set status='COMPLETE',prepare_stage='COMPLETE',
        completed_at_utc=pg_catalog.transaction_timestamp() where id=v_state.id;
      update private.bpay_next_run_worker set gross_ex_vat=v_gross,gross_vat=0,gross_inc_vat=v_gross,
        status='READY',case_pending_binding_count=0 where id=v_worker.id;

      v_terminal:=true;
    end if;
  end prepare_page;

  if v_issue is not null then
    -- Candidate-wide issue: WORK FK remains NULL. Earlier frozen WORK and any
    -- already-created ACTIVE reservations remain exact and non-confirmable.
    update private.bpay_next_run_worker set status='REVIEW',review_issue_code=v_issue,review_issue_work_id=null,
      financial_resolution_count=financial_resolution_count+1,case_pending_binding_count=
        case when v_issue in ('CASE_SELECTION_REQUIRED','CASE_INPUT_UNBOUND','CASE_WEEK_BASIS_UNBOUND','CASE_RETURN_FLOOR_UNBOUND') then 1 else 0 end
      where id=v_worker.id;
    if v_state.id is not null then
      update private.bpay_next_case_allocation_state set retained_prepare_stage=v_state.prepare_stage,retained_weekly_binding_state=v_state.weekly_binding_state,
        prepare_stage='REVIEW',weekly_binding_state='UNBOUND',
        instruction_capture_cursor=v_state.instruction_capture_cursor,captured_instruction_count=v_state.captured_instruction_count,
        payout_instruction_count=v_state.payout_instruction_count where id=v_state.id;
    end if;

    v_terminal:=true;
  elsif not v_terminal then
    update private.bpay_next_job set cursor_key=(v_saved_cursor+1)::text where id=p_job_id;
  end if;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  if v_terminal then
    return private.bpay_next_begin_prepare_projection_v1(p_job_id,p_lease_nonce,p_owner_epoch,v_seen::integer);
  end if;
  return pg_catalog.jsonb_build_object('done',v_terminal,'phase',case when v_issue is not null then 'REVIEW'
    when v_terminal then 'READY' else 'FINANCE_ALLOCATE' end,'cursor',case when v_terminal then null else (v_saved_cursor+1)::text end,
    'rows_visited',v_seen::text,'replay',false,'run_worker_id',v_worker.id,
    'worker_status',case when v_issue is not null then 'REVIEW' when v_terminal then 'READY' else 'PREPARING' end)
    ||case when v_issue is not null then pg_catalog.jsonb_build_object('issue_code',v_issue) else '{}'::jsonb end;
end
$function$;

create or replace function private.bpay_next_apply_case_paye_net_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_expected_cursor bigint default null,
  p_limit integer default 100
) returns jsonb
language plpgsql security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_control_epoch bigint;v_checkpoint bigint;
  v_run private.bpay_next_pay_run%rowtype;v_worker private.bpay_next_run_worker%rowtype;
  v_job private.bpay_next_job%rowtype;v_request private.bpay_next_paye_net_request%rowtype;
  v_draft private.bpay_next_case_allocation_state%rowtype;v_state private.bpay_next_case_allocation_state%rowtype;
  v_week private.bpay_next_worker_week%rowtype;v_prior private.bpay_next_worker_week_contribution%rowtype;
  v_contribution private.bpay_next_worker_week_contribution%rowtype;
  v_instruction private.bpay_next_run_case_instruction%rowtype;v_draft_result private.bpay_next_case_allocation_result%rowtype;
  v_own private.bpay_next_case_hold%rowtype;v_case private.bpay_next_finance_case%rowtype;
  v_component private.bpay_next_case_component%rowtype;v_period private.bpay_next_case_period%rowtype;
  v_channel private.bpay_next_case_allocation_channel%rowtype;
  v_seen integer:=0;v_more boolean;v_issue text;v_terminal boolean:=false;v_requires_week boolean;
  v_e numeric;v_h numeric;v_own_amount numeric;v_capacity numeric;v_taken numeric;v_cash numeric;v_own_cash numeric;
  v_outstanding numeric;v_case_outstanding numeric;v_row record;v_result_id uuid;v_hold_id uuid;v_projection_id uuid;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1
     or (p_expected_cursor is not null and p_expected_cursor<1) or p_limit is null or p_limit not between 1 and 100 then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_NET_PAGE_INPUT_INVALID';
  end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select w.run_id,w.candidate_id into strict v_run_id,v_candidate from private.bpay_next_job j
    join private.bpay_next_paye_net_request r on r.command_id=j.command_id
    join private.bpay_next_run_worker w on w.id=r.run_worker_id and w.candidate_id=j.candidate_id
    where j.id=p_job_id and r.case_draft_state_id is not null;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;
  select active_owner_epoch into strict v_control_epoch from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  select * into strict v_request from private.bpay_next_paye_net_request where command_id=v_job.command_id;
  select * into strict v_worker from private.bpay_next_run_worker where id=v_request.run_worker_id for update;
  select * into strict v_draft from private.bpay_next_case_allocation_state where id=v_request.case_draft_state_id;
  select * into v_state from private.bpay_next_case_allocation_state where job_id=p_job_id and pass_kind='NET' for update;
  if v_job.job_kind<>'PAYE_NET_ENTRY' or v_job.module_epoch<>v_epoch or v_job.candidate_id<>v_candidate
     or v_request.candidate_id<>v_candidate or v_draft.run_worker_id<>v_worker.id or v_draft.pass_kind<>'DRAFT'
     or v_draft.preparation_revision<>v_request.preparation_revision or v_draft.selection_revision<>v_request.selection_revision then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_JOB_SCOPE_INVALID';
  end if;
  -- An immutable completed receipt survives later lane owners and net saves.
  if v_job.status='DONE' and v_state.id is not null and v_state.net_stage in ('COMPLETE','REVIEW') then
    return pg_catalog.jsonb_build_object('phase',case when v_state.net_stage='COMPLETE' then 'PROJECTED' else 'REVIEW' end,
      'cursor',null,'rows_visited','0','replay',true,'projection_id',v_state.projection_id,
      'run_worker_id',v_worker.id,'issue_code',v_state.net_issue_code);
  end if;
  if v_job.status<>'LEASED' or v_job.phase<>'FINANCE_ALLOCATE' or v_job.lease_nonce<>p_lease_nonce
     or v_job.owner_epoch<>p_owner_epoch or v_control_epoch<>p_owner_epoch
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() or v_run.status<>'DRAFT'
     or v_run.confirmed_at_utc is null or v_worker.status<>'READY'
     or v_worker.captured_preparation_revision<>v_request.preparation_revision or v_worker.case_selection_revision<>v_request.selection_revision
     or v_worker.net_request_revision<>v_request.request_no or v_worker.net_projection_revision<>v_request.expected_projection_revision
     or v_worker.target_pay_channel<>'PAYE' or v_worker.gross_vat<>0 or v_worker.gross_inc_vat<>v_request.frozen_gross_inc_vat
     or v_worker.realised_effect_count<>0 or v_draft.status<>'COMPLETE' or v_draft.prepare_stage<>'COMPLETE'
     or exists(select 1 from private.bpay_next_cancel_request r join private.bpay_next_command c on c.id=r.command_id
       where r.run_worker_id=v_worker.id and (r.status='CANCELLING'
         or (r.status='REQUESTED' and c.agency_sequence<v_job.command_sequence)))
     or exists(select 1 from private.bpay_next_transfer t where t.run_worker_id=v_worker.id) then
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_NET_OWNER_NOT_ELIGIBLE';
  end if;
  v_checkpoint:=v_job.cursor_key::bigint;
  if p_expected_cursor is distinct from v_checkpoint then
    if coalesce(p_expected_cursor,0)<coalesce(v_checkpoint,0) then
      return pg_catalog.jsonb_build_object('phase','FINANCE_ALLOCATE','cursor',v_job.cursor_key,'rows_visited','0',
        'replay',true,'projection_id',null,'run_worker_id',v_worker.id,'issue_code',null);
    end if;
    raise exception using errcode='55000',message='BPAY_NEXT_CASE_NET_CURSOR_STALE';
  end if;

  <<net_page>>
  begin
    if v_state.id is null then
      select * into strict v_week from private.bpay_next_worker_week
        where candidate_id=v_candidate and pay_week_start=v_draft.pay_week_start for update;
      select exists(select 1 from private.bpay_next_run_case_instruction i
        join private.bpay_next_case_allocation_result a on a.instruction_id=i.id and a.state_id=v_draft.id
        where i.run_worker_id=v_worker.id and i.preparation_revision=v_request.preparation_revision
          and i.selection_revision=v_request.selection_revision and i.payroll_stage='NET_DEDUCT'
          and i.case_kind in ('LOAN','ADVANCE','MANUAL_DEBT') and a.allocated_source_ex_vat>0) into v_requires_week;
      insert into private.bpay_next_case_allocation_state(run_worker_id,run_id,candidate_id,selection_revision,preparation_revision,
        pass_kind,projection_no,command_id,job_id,module_epoch,owner_epoch,financial_view_revision,status,weekly_binding_state,
        expected_instruction_count,initial_worker_take_home,remaining_worker_take_home,pay_week_start,captured_week_revision,
        captured_work_gross,net_stage,net_requires_week_binding)
        values(v_worker.id,v_run_id,v_candidate,v_request.selection_revision,v_request.preparation_revision,'NET',v_request.request_no,
          v_job.command_id,v_job.id,v_epoch,p_owner_epoch,
          (select financial_view_revision from private.bpay_next_worker_control where candidate_id=v_candidate),'BUILDING',
          case when v_requires_week then 'UNBOUND' else 'NOT_AFFECTED' end,v_draft.expected_instruction_count,
          coalesce(v_request.entered_paye_net,0),coalesce(v_request.entered_paye_net,0),v_draft.pay_week_start,v_week.period_revision,
          v_draft.captured_work_gross,case when v_requires_week then 'WEEK_CAPTURE' else 'ALLOCATE' end,v_requires_week)
        returning * into v_state;
      insert into private.bpay_next_case_allocation_channel(state_id,allocation_channel,initial_headroom,remaining_headroom)
        values(v_state.id,'PAYE',coalesce(v_request.entered_paye_net,0),coalesce(v_request.entered_paye_net,0));
      -- Only arrangements BEFORE this run affect its floor. A later return
      -- cannot invalidate an earlier run merely through a whole-week counter.
      -- Exact prior UNBOUND rows are handled by the bounded week pages below.
      if v_requires_week and exists(
        select 1 from private.bpay_next_run_worker w join private.bpay_next_pay_run r on r.id=w.run_id
        where w.candidate_id=v_candidate and w.status in ('READY','DRAFT','ISSUED','COMPLETE')
          and r.pay_date>=v_draft.pay_week_start and r.pay_date<v_draft.pay_week_start+7
          and (r.pay_date,r.created_at_utc,w.id)<(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and not exists(select 1 from private.bpay_next_worker_week_contribution x where x.original_run_worker_id=w.id)) then
        v_issue:='CASE_WEEK_BASIS_UNBOUND';exit net_page;
      end if;
    elsif v_state.status<>'BUILDING' or v_state.net_stage='NOT_CASE_NET' or v_state.projection_no<>v_request.request_no then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_STATE_INVALID';
    end if;
    if v_state.owner_epoch<>p_owner_epoch then
      update private.bpay_next_case_allocation_state set owner_epoch=p_owner_epoch where id=v_state.id;
    end if;
    select * into strict v_week from private.bpay_next_worker_week
      where candidate_id=v_candidate and pay_week_start=v_state.pay_week_start for update;
    if v_state.net_requires_week_binding and v_week.period_revision<>v_state.captured_week_revision then
      v_issue:='CASE_WEEK_BASIS_UNBOUND';exit net_page;
    end if;
    if v_state.net_stage='WEEK_CAPTURE' then
      for v_prior in select x.* from private.bpay_next_worker_week_contribution x
        where x.candidate_id=v_candidate and x.pay_week_start=v_state.pay_week_start
          and (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)<(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and (v_state.prior_worker_cursor is null or (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
            >(v_state.prior_pay_date_cursor,v_state.prior_created_at_cursor,v_state.prior_worker_cursor))
        order by x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id limit p_limit
      loop
        v_seen:=v_seen+1;
        if v_prior.eligibility_state='UNBOUND' then
          v_issue:=case when v_prior.payment_state in ('RETURNED_OWED','REISSUED_PAID') then 'CASE_RETURN_FLOOR_UNBOUND'
            else 'CASE_WEEK_BASIS_UNBOUND' end;exit net_page;
        end if;
        v_state.captured_prior_take_home:=v_state.captured_prior_take_home+v_prior.eligible_arranged_amount;
        v_state.initial_worker_take_home:=v_state.initial_worker_take_home+v_prior.eligible_arranged_amount;
        v_state.remaining_worker_take_home:=v_state.remaining_worker_take_home+v_prior.eligible_arranged_amount;
        v_state.prior_pay_date_cursor:=v_prior.original_pay_date;v_state.prior_created_at_cursor:=v_prior.original_created_at_utc;
        v_state.prior_worker_cursor:=v_prior.original_run_worker_id;
      end loop;
      select exists(select 1 from private.bpay_next_worker_week_contribution x
        where x.candidate_id=v_candidate and x.pay_week_start=v_state.pay_week_start
          and (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)<(v_run.pay_date,v_run.created_at_utc,v_worker.id)
          and (v_state.prior_worker_cursor is null or (x.original_pay_date,x.original_created_at_utc,x.original_run_worker_id)
            >(v_state.prior_pay_date_cursor,v_state.prior_created_at_cursor,v_state.prior_worker_cursor))) into v_more;
      update private.bpay_next_case_allocation_state set captured_prior_take_home=v_state.captured_prior_take_home,
        initial_worker_take_home=v_state.initial_worker_take_home,remaining_worker_take_home=v_state.remaining_worker_take_home,
        prior_pay_date_cursor=v_state.prior_pay_date_cursor,prior_created_at_cursor=v_state.prior_created_at_cursor,
        prior_worker_cursor=v_state.prior_worker_cursor,weekly_binding_state=case when v_more then 'UNBOUND' else 'BOUND' end,
        net_stage=case when v_more then 'WEEK_CAPTURE' else 'ALLOCATE' end where id=v_state.id;
      -- Week capture and allocation never share a >limit step.
      exit net_page;
    end if;
    if v_state.net_stage<>'ALLOCATE' or v_state.weekly_binding_state='UNBOUND' then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_ALLOCATION_UNBOUND';
    end if;
    select * into strict v_channel from private.bpay_next_case_allocation_channel
      where state_id=v_state.id and allocation_channel='PAYE' for update;
    v_e:=v_channel.remaining_headroom;v_h:=v_state.remaining_worker_take_home;
    for v_instruction in select i.* from private.bpay_next_run_case_instruction i
      where i.run_worker_id=v_worker.id and i.preparation_revision=v_request.preparation_revision
        and i.selection_revision=v_request.selection_revision
        and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
          >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))
      order by i.age_key,i.case_id,i.component_ordinal,i.case_component_id limit p_limit
    loop
      v_seen:=v_seen+1;
      select * into strict v_draft_result from private.bpay_next_case_allocation_result
        where state_id=v_draft.id and instruction_id=v_instruction.id;
      if v_instruction.source_pay_channel<>'PAYE' or v_instruction.target_pay_channel<>'PAYE'
         or v_instruction.allocation_channel<>'PAYE' or v_instruction.currency<>'GBP'
         or v_instruction.nominal_target_vat<>0 or v_draft_result.allocated_target_vat<>0
         or v_draft_result.allocated_source_ex_vat<>v_draft_result.allocated_target_ex_vat then
        v_issue:='CASE_INPUT_UNBOUND';exit net_page;
      end if;
      select * into v_own from private.bpay_next_case_hold where instruction_id=v_instruction.id and status='ACTIVE' for update;
      v_own_amount:=coalesce(v_own.source_reserved_ex_vat,0);
      if v_own.id is not null and (v_own.run_worker_id<>v_worker.id or v_own.purpose<>v_instruction.hold_purpose
          or v_own.source_reserved_ex_vat<>v_own.target_amount_ex_vat
          or v_own.target_amount_vat<>0 or not exists(select 1 from private.bpay_next_case_capacity_use u
            where u.case_hold_id=v_own.id and u.status='ACTIVE' and u.source_amount_ex_vat=v_own_amount)) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_OWN_HOLD_INVALID';
      end if;
      if v_instruction.payroll_stage<>'NET_DEDUCT' then
        -- Gross deductions/additions and payout additions remain the exact
        -- Draft result/hold. They neither enter the kernel nor seed E/H.
        if v_own_amount<>v_draft_result.allocated_source_ex_vat
           or (v_own_amount>0 and (v_own.allocation_result_id<>v_draft_result.id or v_own.allocation_pass_kind<>'DRAFT')) then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_FROZEN_GROSS_OR_ADDITION_CHANGED';
        end if;
        if v_instruction.payroll_stage='GROSS_ADD' then
          v_state.allocated_gross_additions:=v_state.allocated_gross_additions+v_draft_result.allocated_target_ex_vat;
        elsif v_instruction.payroll_stage='GROSS_DEDUCT' then
          v_state.allocated_gross_deductions:=v_state.allocated_gross_deductions+v_draft_result.allocated_target_ex_vat;
        elsif v_instruction.payroll_stage='NET_ADD' then
          v_state.allocated_net_additions:=v_state.allocated_net_additions+v_draft_result.allocated_target_inc_vat;
          perform private.bpay_next_accumulate_net_destination_v1(v_state.id,v_instruction.id,v_draft_result.id);
        else raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_STAGE_INVALID';end if;
      else
        select * into strict v_case from private.bpay_next_finance_case where id=v_instruction.case_id for update;
        select * into strict v_component from private.bpay_next_case_component where id=v_instruction.case_component_id for update;
        select * into strict v_period from private.bpay_next_case_period
          where case_component_id=v_instruction.case_component_id and pay_week_start=v_instruction.pay_week_start for update;
        if v_own_amount>v_component.active_recovery_source_ex_vat
           or v_own_amount>v_case.active_recovery_hold_amount
           or v_own_amount>v_period.active_unrealised_recovery_source_ex_vat
           or (v_own.id is not null and not (
             (v_request.expected_projection_revision=0 and v_own.allocation_pass_kind='DRAFT'
               and v_own.allocation_result_id=v_draft_result.id)
             or (v_request.expected_projection_revision>0 and v_own.allocation_pass_kind='NET' and exists(
               select 1 from private.bpay_next_case_allocation_result a
                 join private.bpay_next_case_allocation_state s on s.id=a.state_id
               where a.id=v_own.allocation_result_id and s.run_worker_id=v_worker.id
                 and s.pass_kind='NET' and s.projection_no=v_request.expected_projection_revision
                 and s.status='COMPLETE' and s.net_stage='COMPLETE' and s.projection_id is not null)))) then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_SURVIVING_CAPACITY_INVALID';
        end if;
        -- Current disjoint CAPACITY only, never a current rule, valuation,
        -- schedule, nominal, tax, channel or changed-case fallback.
        v_outstanding:=case when v_instruction.case_kind in ('LOAN','ADVANCE') then v_component.funded_source_ex_vat
          else v_component.approved_source_ex_vat end-v_component.recovered_source_ex_vat-v_component.written_off_source_ex_vat;
        v_case_outstanding:=case when v_instruction.case_kind in ('LOAN','ADVANCE') then v_case.principal_funded
          else v_case.principal_approved end-v_case.principal_recovered-v_case.principal_written_off;
        v_capacity:=least(v_draft_result.allocated_source_ex_vat,
          greatest(v_period.opening_due_source_ex_vat-v_period.realised_recovery_source_ex_vat
            -v_period.active_unrealised_recovery_source_ex_vat+v_own_amount,0),
          greatest(v_outstanding-v_component.active_recovery_source_ex_vat+v_own_amount,0),
          greatest(v_case_outstanding-v_case.active_recovery_hold_amount+v_own_amount,0));
        if v_own_amount>v_draft_result.allocated_source_ex_vat then
          raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_DRAFT_CAPACITY_EXCEEDED';
        end if;
        select x.* into strict v_row from private.bpay_next_case_allocation_row_v1(v_instruction.case_kind,
          v_instruction.nominal_target_ex_vat,v_capacity,v_e,v_h,v_instruction.minimum_earnings_threshold,
          v_instruction.take_home_floor_override,v_instruction.captured_default_floor) x;
        v_taken:=v_row.taken_amount;
        -- 0310 requires release/new exact rows, not an amount rewrite. Old
        -- reservation and all three active counters move in this same page.
        if v_own.id is not null then
          update private.bpay_next_case_hold set status='RELEASED',finished_at_utc=pg_catalog.transaction_timestamp() where id=v_own.id;
    perform private.bpay_next_record_case_hold_transition_v1(p_job_id,v_own.id,'RELEASED');
          update private.bpay_next_case_capacity_use set status='RELEASED' where case_hold_id=v_own.id;
        end if;
        insert into private.bpay_next_case_allocation_result(state_id,instruction_id,run_worker_id,candidate_id,
          preparation_revision,selection_revision,result_no,pass_kind,allocation_channel,hold_purpose,nominal_target_ex_vat,
          usable_capacity_target_ex_vat,allocated_source_ex_vat,allocated_target_ex_vat,allocated_target_vat,allocated_target_inc_vat,
          shortfall_target_ex_vat,cap_reason,affordability_reason,capacity_reason,channel_headroom_before,channel_headroom_after,
          worker_take_home_before,worker_take_home_after)
          values(v_state.id,v_instruction.id,v_worker.id,v_candidate,v_request.preparation_revision,v_request.selection_revision,
            v_state.processed_instruction_count+1,'NET','PAYE','NET_RECOVERY_CAPACITY',v_instruction.nominal_target_ex_vat,
            v_capacity,v_taken,v_taken,0,v_taken,v_instruction.nominal_target_ex_vat-v_taken,v_row.cap_reason,
            v_row.affordability_reason,v_row.capacity_reason,v_e,v_row.next_channel_headroom,v_h,v_row.next_worker_take_home)
          returning id into v_result_id;
        if v_taken>0 then
          insert into private.bpay_next_case_hold(run_worker_id,candidate_id,case_id,amount,status,instruction_id,
            allocation_result_id,allocation_pass_kind,case_component_id,purpose,pay_week_start,source_reserved_ex_vat,
            target_amount_ex_vat,target_amount_vat,target_amount_inc_vat)
            values(v_worker.id,v_candidate,v_instruction.case_id,v_taken,'ACTIVE',v_instruction.id,v_result_id,'NET',
              v_instruction.case_component_id,'NET_RECOVERY_CAPACITY',v_instruction.pay_week_start,v_taken,v_taken,0,v_taken)
            returning id into v_hold_id;
    perform private.bpay_next_record_case_hold_transition_v1(p_job_id,v_hold_id,'ACTIVE');
          insert into private.bpay_next_case_capacity_use(case_hold_id,case_component_id,case_id,candidate_id,pay_week_start,
            purpose,status,source_amount_ex_vat) values(v_hold_id,v_instruction.case_component_id,v_instruction.case_id,v_candidate,
              v_instruction.pay_week_start,'NET_RECOVERY_CAPACITY','ACTIVE',v_taken);
        end if;
        update private.bpay_next_case_component set active_recovery_source_ex_vat=active_recovery_source_ex_vat-v_own_amount+v_taken,
          component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id;
        update private.bpay_next_finance_case set active_recovery_hold_amount=active_recovery_hold_amount-v_own_amount+v_taken,
          active_hold_amount=active_hold_amount-v_own_amount+v_taken,case_revision=case_revision+1,
          updated_at_utc=pg_catalog.transaction_timestamp() where id=v_case.id;
        update private.bpay_next_case_period set active_unrealised_recovery_source_ex_vat=active_unrealised_recovery_source_ex_vat-v_own_amount+v_taken,
          period_revision=period_revision+1 where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start;
        update private.bpay_next_run_worker set active_case_hold_count=active_case_hold_count
          -case when v_own.id is null then 0 else 1 end+case when v_taken>0 then 1 else 0 end where id=v_worker.id;
        v_state.recovered_source_total:=v_state.recovered_source_total+v_taken;
        v_state.recovered_target_total:=v_state.recovered_target_total+v_taken;
        v_e:=v_row.next_channel_headroom;v_h:=v_row.next_worker_take_home;
      end if;
      v_state.processed_instruction_count:=v_state.processed_instruction_count+1;
      v_state.cursor_age_key:=v_instruction.age_key;v_state.cursor_case_id:=v_instruction.case_id;
      v_state.cursor_component_ordinal:=v_instruction.component_ordinal;v_state.cursor_case_component_id:=v_instruction.case_component_id;
    end loop;
    update private.bpay_next_case_allocation_channel set remaining_headroom=v_e where state_id=v_state.id and allocation_channel='PAYE';
    update private.bpay_next_case_allocation_state set processed_instruction_count=v_state.processed_instruction_count,
      remaining_worker_take_home=v_h,recovered_source_total=v_state.recovered_source_total,recovered_target_total=v_state.recovered_target_total,
      cursor_age_key=v_state.cursor_age_key,cursor_case_id=v_state.cursor_case_id,cursor_component_ordinal=v_state.cursor_component_ordinal,
      cursor_case_component_id=v_state.cursor_case_component_id,allocated_gross_additions=v_state.allocated_gross_additions,
      allocated_gross_deductions=v_state.allocated_gross_deductions,allocated_net_additions=v_state.allocated_net_additions where id=v_state.id;
    select exists(select 1 from private.bpay_next_run_case_instruction i where i.run_worker_id=v_worker.id
      and i.preparation_revision=v_request.preparation_revision and i.selection_revision=v_request.selection_revision
      and (v_state.cursor_case_component_id is null or (i.age_key,i.case_id,i.component_ordinal,i.case_component_id)
        >(v_state.cursor_age_key,v_state.cursor_case_id,v_state.cursor_component_ordinal,v_state.cursor_case_component_id))) into v_more;
    if not v_more then
      if v_state.processed_instruction_count<>v_state.expected_instruction_count
         or (v_state.allocated_gross_additions,v_state.allocated_gross_deductions,v_state.allocated_net_additions)
           is distinct from (v_draft.allocated_gross_additions,v_draft.allocated_gross_deductions,v_draft.allocated_net_additions) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_FROZEN_TOTALS_MISMATCH';
      end if;
      v_cash:=coalesce(v_request.entered_paye_net,0)+v_state.allocated_net_additions-v_state.recovered_target_total;
      if v_cash<0 or v_cash>=10000000000000000 then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_NET_CASH_RANGE_INVALID';
      end if;
      select * into strict v_contribution from private.bpay_next_worker_week_contribution where original_run_worker_id=v_worker.id for update;
      if v_contribution.eligibility_state='UNBOUND' or v_contribution.payment_state<>'ARRANGED'
         or v_contribution.original_gross_amount<>v_request.frozen_gross_inc_vat then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_OWN_WEEK_BASIS_INVALID';
      end if;
      insert into private.bpay_next_net_projection(run_worker_id,request_command_id,projection_no,input_kind,
        gross_ex_vat,gross_vat,gross_inc_vat,entered_paye_net,accepted_recoveries,cash_amount,
        accepted_net_additions,accepted_gross_additions,accepted_gross_deductions)
        values(v_worker.id,v_job.command_id,v_request.request_no,v_request.input_kind,v_worker.gross_ex_vat,0,
          v_worker.gross_inc_vat,v_request.entered_paye_net,v_state.recovered_target_total,v_cash,
          v_state.allocated_net_additions,v_state.allocated_gross_additions,v_state.allocated_gross_deductions)
        returning id into v_projection_id;
      v_own_cash:=private.bpay_next_seal_net_destination_v1(v_state.id,v_projection_id);
      if v_request.expected_projection_revision>0 then
        update private.bpay_next_net_projection set retired_at_utc=pg_catalog.transaction_timestamp()
          where run_worker_id=v_worker.id and projection_no=v_request.expected_projection_revision and retired_at_utc is null;
        if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_NET_PRIOR_SWITCH_INVALID';end if;
      end if;
      update private.bpay_next_worker_week_contribution set
        basis_kind=case when v_request.input_kind='CASE_PAYOUT' then 'GROSS_FALLBACK' else 'PAYROLL_NET' end,
        original_payroll_net=v_request.entered_paye_net,accepted_projection_id=v_projection_id,accepted_bank_cash=v_cash,
        eligibility_state=case when v_request.input_kind='CASE_PAYOUT' then 'EXCLUDED' else 'ELIGIBLE' end,
        eligible_arranged_amount=case when v_request.input_kind='CASE_PAYOUT' then 0 else v_own_cash end,
        contribution_revision=contribution_revision+1,primary_binding_revision=primary_binding_revision+1
        where id=v_contribution.id;
      update private.bpay_next_worker_week set resolved_arranged_take_home=resolved_arranged_take_home
        -v_contribution.eligible_arranged_amount+case when v_request.input_kind='CASE_PAYOUT' then 0 else v_own_cash end,
        period_revision=period_revision+1 where candidate_id=v_candidate and pay_week_start=v_state.pay_week_start;
      if v_request.input_kind='CASE_PAYOUT' then
        update private.bpay_next_case_payout_basis set projection_id=v_projection_id
          where run_worker_id=v_worker.id and preparation_revision=v_request.preparation_revision and projection_id is null;
        if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_PAYOUT_SWITCH_INVALID';end if;
      end if;
      update private.bpay_next_case_allocation_state set status='COMPLETE',net_stage='COMPLETE',projection_id=v_projection_id,
        completed_at_utc=pg_catalog.transaction_timestamp() where id=v_state.id;
      update private.bpay_next_run_worker set entered_paye_net=v_request.entered_paye_net,
        net_projection_revision=v_request.request_no where id=v_worker.id;
      update private.bpay_next_job set status='DONE',phase='PROJECTED',cursor_key=null,lease_nonce=null,lease_until_utc=null where id=p_job_id;
      update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
      v_terminal:=true;
    end if;
  end net_page;
  if v_issue is not null then
    update private.bpay_next_case_allocation_state set net_stage='REVIEW',net_issue_code=v_issue,weekly_binding_state='UNBOUND',
      status='OUTDATED' where id=v_state.id;
    update private.bpay_next_run_worker set status='REVIEW',review_issue_code=v_issue,review_issue_work_id=null,
      case_pending_binding_count=1,financial_resolution_count=financial_resolution_count+1 where id=v_worker.id;
    update private.bpay_next_job set status='DONE',phase='DONE',cursor_key=null,lease_nonce=null,lease_until_utc=null where id=p_job_id;
    update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
    v_terminal:=true;
  elsif not v_terminal then
    update private.bpay_next_job set cursor_key=(coalesce(v_checkpoint,0)+1)::text where id=p_job_id;
  end if;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,
    updated_at_utc=pg_catalog.transaction_timestamp() where candidate_id=v_candidate;
  select * into strict v_job from private.bpay_next_job where id=p_job_id;
  return pg_catalog.jsonb_build_object('phase',case when v_issue is not null then 'REVIEW'
      when v_terminal then 'PROJECTED' else 'FINANCE_ALLOCATE' end,'cursor',v_job.cursor_key,
    'rows_visited',v_seen::text,'replay',false,'projection_id',v_projection_id,'run_worker_id',v_worker.id,'issue_code',v_issue);
end
$function$;

create or replace function private.bpay_next_cancel_case_worker_page_v1(
  p_job_id uuid,p_lease_nonce uuid,p_owner_epoch bigint,p_expected_cursor bigint,p_limit integer)
returns jsonb language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
  v_epoch bigint;v_run_id uuid;v_candidate uuid;v_checkpoint bigint;v_seen integer:=0;v_holds integer:=0;v_block text;v_more boolean;
  v_run private.bpay_next_pay_run%rowtype;v_control private.bpay_next_worker_control%rowtype;v_job private.bpay_next_job%rowtype;
  v_request private.bpay_next_cancel_request%rowtype;v_worker private.bpay_next_run_worker%rowtype;v_binding private.bpay_next_case_cancel_binding%rowtype;
  v_projection private.bpay_next_net_projection%rowtype;v_net private.bpay_next_case_allocation_state%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;v_line private.bpay_next_run_line%rowtype;v_work_hold private.bpay_next_hold%rowtype;
  v_hold private.bpay_next_case_hold%rowtype;v_use private.bpay_next_case_capacity_use%rowtype;
  v_instruction private.bpay_next_run_case_instruction%rowtype;v_result private.bpay_next_case_allocation_result%rowtype;
  v_case private.bpay_next_finance_case%rowtype;v_component private.bpay_next_case_component%rowtype;
  v_period private.bpay_next_case_period%rowtype;v_amount numeric;v_now timestamptz:=pg_catalog.transaction_timestamp();
  v_group private.bpay_next_destination_group%rowtype;v_group_page jsonb;v_group_started boolean:=false;
begin
  if p_job_id is null or p_lease_nonce is null or p_owner_epoch is null or p_owner_epoch<1 or p_limit is null or p_limit not between 1 and 100
     or (p_expected_cursor is not null and p_expected_cursor<1) then raise exception using errcode='22023',message='BPAY_NEXT_CASE_CANCEL_PAGE_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select w.run_id,j.candidate_id into strict v_run_id,v_candidate from private.bpay_next_job j
    join private.bpay_next_cancel_request r on r.command_id=j.command_id join private.bpay_next_run_worker w on w.id=r.run_worker_id
    where j.id=p_job_id and j.candidate_id=w.candidate_id;
  select * into strict v_run from private.bpay_next_pay_run where id=v_run_id for update;
  select * into strict v_control from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  select * into strict v_job from private.bpay_next_job where id=p_job_id for update;
  select * into strict v_request from private.bpay_next_cancel_request where command_id=v_job.command_id for update;
  select * into strict v_worker from private.bpay_next_run_worker where id=v_request.run_worker_id for update;
  select * into strict v_binding from private.bpay_next_case_cancel_binding where command_id=v_job.command_id for update;
  if v_job.job_kind<>'SIMPLE_CANCEL' or v_job.module_epoch<>v_epoch or v_job.candidate_id<>v_binding.candidate_id
     or v_request.run_worker_id<>v_binding.run_worker_id or v_worker.candidate_id<>v_binding.candidate_id
     or v_worker.captured_preparation_revision<>v_binding.preparation_revision or v_worker.case_selection_revision<>v_binding.selection_revision
     or v_request.expected_line_count<>v_binding.expected_work_count or v_request.released_line_count<>v_binding.released_work_count
     or v_job.applied_line_count<>v_binding.released_work_count
     or coalesce(v_request.cursor_line_no,0)<>v_binding.checkpoint or v_job.cursor_key is distinct from nullif(v_binding.checkpoint,0)::text
     or not exists(select 1 from private.bpay_next_command where id=v_job.command_id and command_kind='SIMPLE_CANCEL'
       and agency_sequence=v_job.command_sequence and module_epoch=v_epoch and expected_member_count=1 and enrolled_member_count=1) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_SCOPE_INVALID';end if;
  if v_job.status='DONE' and v_request.status in ('CANCELLED','BLOCKED') and v_binding.status=v_request.status then
    return pg_catalog.jsonb_build_object('phase',v_request.status,'run_worker_id',v_worker.id,'cursor',v_job.cursor_key,
      'released_line_count',v_request.released_line_count,'blocked_code',v_request.blocked_code,'rows_visited',0,'holds_released',0,'replay',true);
  end if;
  if v_job.status<>'LEASED' or v_job.phase not in ('NEW','RELEASE') or v_job.lease_nonce<>p_lease_nonce or v_job.owner_epoch<>p_owner_epoch
     or v_control.active_owner_epoch<>p_owner_epoch or v_job.lease_until_utc<=pg_catalog.clock_timestamp()
     or exists(select 1 from private.bpay_next_job where candidate_id=v_candidate and command_sequence<v_job.command_sequence and status<>'DONE') then
    raise exception using errcode='55000',message='BPAY_NEXT_CANCEL_PAGE_LEASE_INVALID';end if;
  if coalesce(p_expected_cursor,0)<>v_binding.checkpoint then
    if coalesce(p_expected_cursor,0)>v_binding.checkpoint then raise exception using errcode='55000',message='BPAY_NEXT_CASE_CANCEL_CURSOR_STALE';end if;
    return pg_catalog.jsonb_build_object('phase',v_request.status,'run_worker_id',v_worker.id,'cursor',v_job.cursor_key,
      'released_line_count',v_request.released_line_count,'rows_visited',0,'holds_released',0,'replay',true);
  end if;
  select * into v_group from private.bpay_next_destination_group where run_worker_id=v_worker.id;
  if v_binding.status='REQUESTED' then
    if v_job.phase<>'NEW' or v_request.status<>'REQUESTED' then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_INTENT_INVALID';end if;
    v_block:=private.bpay_next_case_cancel_block_v1(v_worker.id,false);
    if v_run.status<>'DRAFT' or v_run.confirmed_at_utc is null or v_worker.captured_line_count<>v_binding.expected_work_count then
      v_block:='BPAY_NEXT_CANCEL_DRAFT_CHANGED';end if;
    if v_block is null and v_group.anchor_transfer_id is not null then
      v_group_page:=private.bpay_next_check_destination_cancel_page_v1(p_job_id,p_limit);
      v_block:=v_group_page->>'blocked_code';v_seen:=(v_group_page->>'rows_visited')::integer;
      if v_block is null and (v_group_page->>'complete')::boolean is not true then
        update private.bpay_next_case_cancel_binding set checkpoint=checkpoint+1 where command_id=v_job.command_id
          returning checkpoint into v_checkpoint;
        update private.bpay_next_cancel_request set cursor_line_no=v_checkpoint where command_id=v_job.command_id;
        update private.bpay_next_job set cursor_key=v_checkpoint::text,
          lease_until_utc=pg_catalog.clock_timestamp()+pg_catalog.make_interval(secs=>120) where id=p_job_id;
        return pg_catalog.jsonb_build_object('phase','REQUESTED','run_worker_id',v_worker.id,'cursor',v_checkpoint,
          'released_line_count',0,'rows_visited',v_seen,'holds_released',0,'replay',false);
      end if;
      v_group_started:=true;
    end if;
    if v_block is not null then
      update private.bpay_next_case_cancel_binding set status='BLOCKED',stage='BLOCKED',checkpoint=checkpoint+1,blocked_code=v_block,finished_at_utc=v_now
        where command_id=v_job.command_id returning checkpoint into v_checkpoint;
      update private.bpay_next_cancel_request set status='BLOCKED',blocked_code=v_block,finished_at_utc=v_now,cursor_line_no=v_checkpoint where command_id=v_job.command_id;
      update private.bpay_next_job set status='DONE',phase='BLOCKED',cursor_key=v_checkpoint::text,last_error_code=v_block,lease_nonce=null,lease_until_utc=null where id=p_job_id;
      update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
      return pg_catalog.jsonb_build_object('phase','BLOCKED','run_worker_id',v_worker.id,'cursor',v_checkpoint,
        'released_line_count',0,'blocked_code',v_block,'rows_visited',v_seen,'holds_released',0,'replay',false);
    end if;
    v_binding.accepted_projection_revision:=v_worker.net_projection_revision;
    if v_worker.net_projection_revision>0 then
      select * into strict v_projection from private.bpay_next_net_projection where run_worker_id=v_worker.id
        and projection_no=v_worker.net_projection_revision and retired_at_utc is null;
      select * into strict v_net from private.bpay_next_case_allocation_state where projection_id=v_projection.id and run_worker_id=v_worker.id
        and pass_kind='NET' and status='COMPLETE' and net_stage='COMPLETE';
      if v_net.preparation_revision<>v_binding.preparation_revision or v_net.selection_revision<>v_binding.selection_revision
         or v_projection.input_kind not in ('PAYE_MANUAL','CASE_PAYOUT') then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_NET_BINDING_INVALID';end if;
      v_binding.projection_id:=v_projection.id;v_binding.net_state_id:=v_net.id;
    end if;
    if v_group.anchor_transfer_id is not null then
      v_binding.transfer_id:=v_group.anchor_transfer_id;
    else
      select * into v_transfer from private.bpay_next_transfer where run_worker_id=v_worker.id for update;
      if found then v_binding.transfer_id:=v_transfer.id;update private.bpay_next_transfer set status='CANCELLED' where id=v_transfer.id;end if;
    end if;
    v_binding.expected_case_hold_count:=v_worker.active_case_hold_count;v_binding.started_at_utc:=v_now;
    v_binding.status:='CANCELLING';v_binding.stage:='WORK';
    update private.bpay_next_run_worker set status='CANCELLING' where id=v_worker.id;
    update private.bpay_next_cancel_request set status='CANCELLING',started_at_utc=v_now where command_id=v_job.command_id;
    update private.bpay_next_job set phase='RELEASE' where id=p_job_id;
    v_worker.status:='CANCELLING';v_request.status:='CANCELLING';
  end if;
  if v_binding.status<>'CANCELLING' or v_worker.status<>'CANCELLING' or v_request.status<>'CANCELLING'
     or v_run.status<>'DRAFT' or v_worker.realised_effect_count<>0 or v_worker.financial_resolution_count<>0 or v_control.pending_outcome_count<>0
     or v_worker.net_request_revision<>v_binding.accepted_projection_revision or v_worker.net_projection_revision<>v_binding.accepted_projection_revision then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_FENCE_BROKEN';end if;
  if v_group_started then
    -- This STEP examined only the CHECK page. WORK release starts next STEP;
    -- do not combine two independent <=100-row phases into a 200-row call.
    null;
  elsif v_binding.stage='WORK' then
    for v_line in select * from private.bpay_next_run_line where run_worker_id=v_worker.id and line_no>coalesce(v_binding.work_cursor,0)
      order by line_no,id limit p_limit loop
      select * into v_work_hold from private.bpay_next_hold where run_line_id=v_line.id for update;
      if found then
        if v_work_hold.status<>'ACTIVE' or (v_work_hold.work_id,v_work_hold.component_key,v_work_hold.source_reserved_ex_vat,
          v_work_hold.target_amount_ex_vat,v_work_hold.target_amount_vat,v_work_hold.target_amount_inc_vat)
          is distinct from (v_line.work_id,v_line.component_key,v_line.source_consumed_ex_vat,v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat) then
          raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_HOLD_NOT_EXACT_ACTIVE';end if;
        update private.bpay_next_position set held_source_ex_vat=held_source_ex_vat-v_work_hold.source_reserved_ex_vat,
          held_target_ex_vat=held_target_ex_vat-v_work_hold.target_amount_ex_vat,held_target_vat=held_target_vat-v_work_hold.target_amount_vat,
          held_target_inc_vat=held_target_inc_vat-v_work_hold.target_amount_inc_vat,updated_at_utc=v_now
          where work_id=v_work_hold.work_id and component_key=v_work_hold.component_key and held_source_ex_vat>=v_work_hold.source_reserved_ex_vat
            and held_target_ex_vat>=v_work_hold.target_amount_ex_vat and held_target_vat>=v_work_hold.target_amount_vat and held_target_inc_vat>=v_work_hold.target_amount_inc_vat;
        if not found then raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_POSITION_HOLD_MISMATCH';end if;
        update private.bpay_next_hold set status='RELEASED',finished_at_utc=v_now where id=v_work_hold.id;v_holds:=v_holds+1;
      elsif (v_line.source_consumed_ex_vat,v_line.frozen_ex_vat,v_line.frozen_vat,v_line.frozen_inc_vat) is distinct from (0::numeric,0::numeric,0::numeric,0::numeric) then
        raise exception using errcode='23514',message='BPAY_NEXT_CANCEL_POSITIVE_HOLD_MISSING';end if;
      v_binding.work_cursor:=v_line.line_no;v_binding.released_work_count:=v_binding.released_work_count+1;v_seen:=v_seen+1;
    end loop;
    select exists(select 1 from private.bpay_next_run_line where run_worker_id=v_worker.id and line_no>coalesce(v_binding.work_cursor,0)) into v_more;
    if not v_more then
      if v_binding.released_work_count<>v_binding.expected_work_count then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_WORK_COUNT_INVALID';end if;
      v_binding.stage:='CASE';
    end if;
  elsif v_binding.stage='CASE' then
    for v_hold in select * from private.bpay_next_case_hold where run_worker_id=v_worker.id and status='ACTIVE'
      and (v_binding.case_hold_cursor is null or id>v_binding.case_hold_cursor) order by id limit p_limit loop
      select * into strict v_instruction from private.bpay_next_run_case_instruction where id=v_hold.instruction_id;
      select * into strict v_result from private.bpay_next_case_allocation_result where id=v_hold.allocation_result_id;
      select * into strict v_case from private.bpay_next_finance_case where id=v_hold.case_id for update;
      select * into strict v_component from private.bpay_next_case_component where id=v_hold.case_component_id for update;
      select * into strict v_period from private.bpay_next_case_period where case_component_id=v_hold.case_component_id and pay_week_start=v_hold.pay_week_start for update;
      perform 1 from private.bpay_next_case_hold where id=v_hold.id for update;
      select * into strict v_use from private.bpay_next_case_capacity_use where case_hold_id=v_hold.id for update;
      if v_instruction.run_worker_id<>v_worker.id or v_instruction.preparation_revision<>v_binding.preparation_revision
         or v_instruction.selection_revision<>v_binding.selection_revision or v_result.instruction_id<>v_instruction.id
         or v_result.state_id is distinct from (case when v_instruction.payroll_stage='NET_DEDUCT' and v_binding.accepted_projection_revision>0
           then v_binding.net_state_id else v_binding.draft_state_id end)
         or v_hold.allocation_pass_kind<>v_result.pass_kind or v_use.status<>'ACTIVE' or v_use.realisation_event_id is not null
         or v_use.source_amount_ex_vat<>v_hold.source_reserved_ex_vat
         or (v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat)
           is distinct from (v_result.allocated_source_ex_vat,v_result.allocated_target_ex_vat,v_result.allocated_target_vat,v_result.allocated_target_inc_vat) then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_HOLD_BINDING_INVALID';end if;
      v_amount:=v_hold.source_reserved_ex_vat;
      if v_hold.purpose='PAYOUT' then
        update private.bpay_next_case_component set active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,
          component_revision=component_revision+1,updated_at_utc=v_now where id=v_component.id and active_payout_source_ex_vat>=v_amount;
        if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_COMPONENT_CAPACITY_INVALID';end if;
        update private.bpay_next_finance_case set active_payout_hold_amount=active_payout_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
          case_revision=case_revision+1,updated_at_utc=v_now where id=v_case.id and active_payout_hold_amount>=v_amount and active_hold_amount>=v_amount;
      else
        update private.bpay_next_case_component set active_recovery_source_ex_vat=active_recovery_source_ex_vat-v_amount,
          component_revision=component_revision+1,updated_at_utc=v_now where id=v_component.id and active_recovery_source_ex_vat>=v_amount;
        if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_COMPONENT_CAPACITY_INVALID';end if;
        update private.bpay_next_finance_case set active_recovery_hold_amount=active_recovery_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
          case_revision=case_revision+1,updated_at_utc=v_now where id=v_case.id and active_recovery_hold_amount>=v_amount and active_hold_amount>=v_amount;
      end if;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_CASE_CAPACITY_INVALID';end if;
      if v_hold.purpose='PAYOUT' then
        update private.bpay_next_case_period set active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,period_revision=period_revision+1
          where case_component_id=v_component.id and pay_week_start=v_hold.pay_week_start and active_payout_source_ex_vat>=v_amount;
      else
        update private.bpay_next_case_period set active_unrealised_recovery_source_ex_vat=active_unrealised_recovery_source_ex_vat-v_amount,period_revision=period_revision+1
          where case_component_id=v_component.id and pay_week_start=v_hold.pay_week_start and active_unrealised_recovery_source_ex_vat>=v_amount;
      end if;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_PERIOD_CAPACITY_INVALID';end if;
      update private.bpay_next_case_hold set status='RELEASED',finished_at_utc=v_now where id=v_hold.id;
    perform private.bpay_next_record_case_hold_transition_v1(p_job_id,v_hold.id,'RELEASED');
      update private.bpay_next_case_capacity_use set status='RELEASED' where case_hold_id=v_hold.id;
      update private.bpay_next_run_worker set active_case_hold_count=active_case_hold_count-1 where id=v_worker.id and active_case_hold_count>0;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_WORKER_HOLD_COUNT_INVALID';end if;
      if v_instruction.work_collection_id is not null then
        perform private.bpay_next_reconcile_cancelled_work_collection_v1(p_job_id,v_hold.id);
      end if;
      v_binding.case_hold_cursor:=v_hold.id;v_binding.released_case_hold_count:=v_binding.released_case_hold_count+1;v_seen:=v_seen+1;v_holds:=v_holds+1;
    end loop;
    select exists(select 1 from private.bpay_next_case_hold where run_worker_id=v_worker.id and status='ACTIVE') into v_more;
    if not v_more then
      if v_binding.released_case_hold_count<>v_binding.expected_case_hold_count
         or (select active_case_hold_count from private.bpay_next_run_worker where id=v_worker.id)<>0 then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_CASE_COUNT_INVALID';end if;
      v_binding.stage:='FINAL';
    elsif v_seen=0 then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_CASE_CURSOR_INVALID';end if;
  elsif v_binding.stage='FINAL' then
    if v_binding.released_work_count<>v_binding.expected_work_count or v_binding.released_case_hold_count<>v_binding.expected_case_hold_count
       or v_worker.active_case_hold_count<>0 or exists(select 1 from private.bpay_next_case_hold where run_worker_id=v_worker.id and status='ACTIVE')
       or v_run.cancelled_candidate_count>=v_run.selected_candidate_count then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_FINAL_COUNT_INVALID';end if;
    if v_group.anchor_transfer_id is not null then
      v_group_page:=private.bpay_next_finish_destination_cancel_page_v1(p_job_id,p_limit);
      v_seen:=(v_group_page->>'rows_visited')::integer;
    end if;
    if v_group.anchor_transfer_id is null or (v_group_page->>'complete')::boolean is true then
      if v_binding.projection_id is not null then update private.bpay_next_net_projection set retired_at_utc=v_now where id=v_binding.projection_id and retired_at_utc is null;end if;
      update private.bpay_next_run_worker set status='CANCELLED' where id=v_worker.id;
      update private.bpay_next_pay_run set cancelled_candidate_count=cancelled_candidate_count+1,
        status=case when cancelled_candidate_count+1=selected_candidate_count then 'CANCELLED' else status end where id=v_run.id;
      v_binding.status:='CANCELLED';v_binding.stage:='COMPLETE';v_binding.finished_at_utc:=v_now;
      if v_group.anchor_transfer_id is null then v_seen:=1;end if;
    end if;
  else raise exception using errcode='23514',message='BPAY_NEXT_CASE_CANCEL_STAGE_INVALID';end if;
  update private.bpay_next_case_cancel_binding set status=v_binding.status,stage=v_binding.stage,checkpoint=checkpoint+1,
    released_work_count=v_binding.released_work_count,work_cursor=v_binding.work_cursor,expected_case_hold_count=v_binding.expected_case_hold_count,
    released_case_hold_count=v_binding.released_case_hold_count,case_hold_cursor=v_binding.case_hold_cursor,
    accepted_projection_revision=v_binding.accepted_projection_revision,projection_id=v_binding.projection_id,net_state_id=v_binding.net_state_id,
    transfer_id=v_binding.transfer_id,started_at_utc=v_binding.started_at_utc,finished_at_utc=v_binding.finished_at_utc
    where command_id=v_job.command_id returning checkpoint into v_checkpoint;
  update private.bpay_next_cancel_request set status=v_binding.status,cursor_line_no=v_checkpoint,released_line_count=v_binding.released_work_count,
    finished_at_utc=v_binding.finished_at_utc where command_id=v_job.command_id;
  update private.bpay_next_job set cursor_key=v_checkpoint::text,applied_line_count=v_binding.released_work_count,
    lease_until_utc=pg_catalog.clock_timestamp()+pg_catalog.make_interval(secs=>120) where id=p_job_id;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1,updated_at_utc=v_now where candidate_id=v_candidate;
  if v_binding.status='CANCELLED' then
    perform private.bpay_next_apply_cancelled_week_v1(p_job_id);
    update private.bpay_next_job set status='DONE',phase='CANCELLED',lease_nonce=null,lease_until_utc=null where id=p_job_id;
    update private.bpay_next_command set status='COMPLETE' where id=v_job.command_id;
  end if;
  return pg_catalog.jsonb_build_object('phase',v_binding.status,'run_worker_id',v_worker.id,'cursor',v_checkpoint,
    'released_line_count',v_binding.released_work_count,'rows_visited',v_seen,'holds_released',v_holds,'replay',false);
end
$function$;

create or replace function private.bpay_next_post_case_transfer_member_v1(p_job_id uuid,p_transfer_id uuid,p_member_no bigint)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare
  v_epoch bigint;v_job private.bpay_next_job%rowtype;v_request private.bpay_next_outcome_request%rowtype;
  v_transfer private.bpay_next_transfer%rowtype;v_outcome private.bpay_next_transfer_outcome%rowtype;
  v_internal private.bpay_next_internal_receipt%rowtype;v_occurred_at timestamptz;
  v_member private.bpay_next_transfer_member%rowtype;v_build private.bpay_next_case_transfer_build%rowtype;
  v_instruction private.bpay_next_run_case_instruction%rowtype;v_result private.bpay_next_case_allocation_result%rowtype;
  v_worker private.bpay_next_run_worker%rowtype;v_case private.bpay_next_finance_case%rowtype;
  v_component private.bpay_next_case_component%rowtype;v_pair private.bpay_next_case_component%rowtype;
  v_hold private.bpay_next_case_hold%rowtype;v_use private.bpay_next_case_capacity_use%rowtype;
  v_rule private.bpay_next_case_rule%rowtype;v_period_rule private.bpay_next_case_rule%rowtype;
  v_pair_period private.bpay_next_case_period%rowtype;v_origin private.bpay_next_case_create_request%rowtype;
  v_event_id uuid;v_new_rule_id uuid;v_kind text;v_amount numeric;v_signed numeric;
begin
  if p_job_id is null or p_transfer_id is null or p_member_no is null or p_member_no<1 then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_POST_INPUT_INVALID';end if;
  select owner_epoch into v_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT' for share;
  if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
  select * into strict v_job from private.bpay_next_job where id=p_job_id;
  select * into strict v_request from private.bpay_next_outcome_request where command_id=v_job.command_id and transfer_id=p_transfer_id;
  select * into strict v_transfer from private.bpay_next_transfer where id=p_transfer_id;
  select * into v_build from private.bpay_next_case_transfer_build where transfer_id=p_transfer_id;
  if not found then
    v_build:=private.bpay_next_destination_build_binding_v1(p_transfer_id);
    if v_build.transfer_id is null then raise exception using errcode='23514',message='BPAY_NEXT_CASE_POST_TRANSFER_UNBOUND';end if;
  end if;
  select * into strict v_member from private.bpay_next_transfer_member where transfer_id=p_transfer_id and member_no=p_member_no;
  if v_job.job_kind='INTERNAL_SETTLEMENT' then
    select * into strict v_internal from private.bpay_next_internal_receipt where id=v_request.internal_receipt_id;
    if v_request.outcome_id is not null or v_transfer.execution_kind<>'INTERNAL_ZERO' or v_transfer.cash_amount<>0
       or v_transfer.status<>'INTERNAL_PROCESSING' or v_internal.command_id<>v_job.command_id
       or v_internal.transfer_id<>v_transfer.id or v_internal.run_worker_id<>v_build.run_worker_id
       or v_internal.candidate_id<>v_build.candidate_id or v_internal.projection_id<>v_build.projection_id then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_INTERNAL_POST_SCOPE_INVALID';end if;
    v_occurred_at:=v_internal.occurred_at_utc;
  elsif v_job.job_kind='CSV_SETTLEMENT' then
    select * into strict v_outcome from private.bpay_next_transfer_outcome where id=v_request.outcome_id;
    if v_request.internal_receipt_id is not null or v_transfer.execution_kind<>'BANK'
       or v_transfer.status not in ('SETTLED','RETURNED') or v_outcome.transfer_id<>v_transfer.id
       or v_outcome.outcome_kind<>'SETTLED' or v_outcome.whole_transfer_amount<>v_transfer.cash_amount then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_BANK_POST_SCOPE_INVALID';end if;
    v_occurred_at:=v_outcome.occurred_at_utc;
  else raise exception using errcode='23514',message='BPAY_NEXT_CASE_POST_JOB_KIND_INVALID';end if;
  if v_job.module_epoch<>v_epoch or v_job.status<>'LEASED' or v_job.phase<>'MEMBERS'
     or v_job.lease_until_utc<=pg_catalog.clock_timestamp() or v_job.candidate_id<>v_build.candidate_id
     or v_transfer.original_transfer_id is not null or v_transfer.return_cash_id is not null
     or v_build.stage<>'COMPLETE' or v_member.subject_kind<>'CASE' or v_member.run_worker_id<>v_build.run_worker_id
     or v_occurred_at is null or not pg_catalog.isfinite(v_occurred_at)
     or not exists(select 1 from private.bpay_next_worker_control where candidate_id=v_job.candidate_id and active_owner_epoch=v_job.owner_epoch) then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_POST_SCOPE_INVALID';end if;
  select * into strict v_worker from private.bpay_next_run_worker where id=v_build.run_worker_id for update;
  select * into strict v_instruction from private.bpay_next_run_case_instruction where id=v_member.case_instruction_id;
  select * into strict v_result from private.bpay_next_case_allocation_result where id=v_member.case_allocation_result_id;
  v_signed:=case when v_instruction.direction='DEDUCTION' then -v_result.allocated_target_inc_vat else v_result.allocated_target_inc_vat end;
  if v_instruction.run_worker_id<>v_worker.id or v_instruction.candidate_id<>v_worker.candidate_id
     or v_result.instruction_id<>v_instruction.id or v_member.signed_cash_contribution<>v_signed
     or v_result.state_id is distinct from (case when v_instruction.payroll_stage='NET_DEDUCT' then v_build.net_state_id else v_build.draft_state_id end)
     or v_instruction.source_pay_channel<>'PAYE' or v_instruction.target_pay_channel<>'PAYE' or v_result.allocated_target_vat<>0
     or v_result.allocated_source_ex_vat<>v_result.allocated_target_ex_vat then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_POST_FROZEN_RESULT_MISMATCH';end if;
  select * into strict v_case from private.bpay_next_finance_case where id=v_instruction.case_id for update;
  select * into strict v_component from private.bpay_next_case_component where id=v_instruction.case_component_id for update;
  perform 1 from private.bpay_next_case_period where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start for update;
  v_amount:=v_result.allocated_source_ex_vat;
  if v_amount>0 then
    select * into strict v_hold from private.bpay_next_case_hold where id=v_member.case_hold_id for update;
    select * into strict v_use from private.bpay_next_case_capacity_use where case_hold_id=v_hold.id for update;
    if v_hold.status<>'ACTIVE' or v_use.status<>'ACTIVE' or v_hold.allocation_result_id<>v_result.id
       or v_hold.instruction_id<>v_instruction.id or v_hold.run_worker_id<>v_worker.id
       or (v_hold.source_reserved_ex_vat,v_hold.target_amount_ex_vat,v_hold.target_amount_vat,v_hold.target_amount_inc_vat)
         is distinct from (v_amount,v_result.allocated_target_ex_vat,v_result.allocated_target_vat,v_result.allocated_target_inc_vat)
       or v_use.source_amount_ex_vat<>v_amount or v_worker.active_case_hold_count<1 then
      raise exception using errcode='23514',message='BPAY_NEXT_CASE_POST_HOLD_MISMATCH';end if;
    v_kind:=case when v_instruction.instruction_kind='PAYOUT' then 'FUNDED'
      when v_instruction.instruction_kind='CREDIT' then 'PAID' else 'RECOVERED' end;
    v_event_id:=pg_catalog.gen_random_uuid();
    insert into private.bpay_next_case_event(id,case_id,operation_id,operation_item_id,event_kind,original_transfer_id,occurred_at_utc,
      funded_delta,recovered_delta,paid_credit_delta,instruction_id,allocation_result_id,run_worker_id,candidate_id,case_component_id,
      event_case_kind,hold_purpose,pay_week_start,source_amount_ex_vat,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,shortfall_source_ex_vat)
      values(v_event_id,v_case.id,v_job.command_id,v_hold.id,v_kind,v_transfer.id,v_occurred_at,
        case when v_kind='FUNDED' then v_amount else 0 end,case when v_kind='RECOVERED' then v_amount else 0 end,
        case when v_kind='PAID' then v_amount else 0 end,v_instruction.id,v_result.id,v_worker.id,v_worker.candidate_id,v_component.id,
        v_instruction.case_kind,v_instruction.hold_purpose,v_instruction.pay_week_start,v_amount,v_result.allocated_target_ex_vat,
        v_result.allocated_target_vat,v_result.allocated_target_inc_vat,0);
    if v_kind='FUNDED' then
      select * into strict v_origin from private.bpay_next_case_create_request where case_id=v_case.id
        and primary_component_id=v_component.id and recovery_component_id is not null;
      select * into strict v_pair from private.bpay_next_case_component where id=v_origin.recovery_component_id for update;
      select * into strict v_rule from private.bpay_next_case_rule where id=v_case.current_rule_id;
      if v_pair.case_id<>v_case.id or v_pair.instruction_kind<>'RECOVERY' or v_pair.case_kind<>v_instruction.case_kind
         or v_occurred_at<v_rule.case_created_at_utc then
        raise exception using errcode='23514',message='BPAY_NEXT_CASE_FUNDING_ORIGIN_INVALID';end if;
      v_new_rule_id:=v_rule.id;
      if v_rule.original_funding_event_id is null then
        v_new_rule_id:=pg_catalog.gen_random_uuid();
        insert into private.bpay_next_case_rule(id,case_id,candidate_id,rule_revision,case_kind,case_subtype,tax_treatment,
          case_created_at_utc,original_funded_at_utc,original_funding_event_id,minimum_earnings_threshold,take_home_floor_override,
          weekly_due_source_ex_vat,schedule_start_monday,next_due_monday,schedule_week_count)
          values(v_new_rule_id,v_case.id,v_worker.candidate_id,v_rule.rule_revision+1,v_rule.case_kind,v_rule.case_subtype,v_rule.tax_treatment,
            v_rule.case_created_at_utc,v_occurred_at,v_event_id,v_rule.minimum_earnings_threshold,v_rule.take_home_floor_override,
            v_rule.weekly_due_source_ex_vat,v_rule.schedule_start_monday,v_rule.next_due_monday,v_rule.schedule_week_count);
      end if;
      update private.bpay_next_case_component set funded_source_ex_vat=funded_source_ex_vat+v_amount,
        active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,rule_id=v_new_rule_id,
        component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp()
        where id=v_component.id and active_payout_source_ex_vat>=v_amount;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_FUNDING_COMPONENT_CAPACITY_INVALID';end if;
      -- Mirrored payout/recovery components describe ONE funded principal.
      update private.bpay_next_case_component set funded_source_ex_vat=funded_source_ex_vat+v_amount,
        rule_id=v_new_rule_id,component_revision=component_revision+1,updated_at_utc=pg_catalog.transaction_timestamp() where id=v_pair.id;
      -- Exact creation-owned zero period only; never rewrite consumed opening
      -- facts or scan previously captured pay weeks to manufacture due.
      select * into strict v_pair_period from private.bpay_next_case_period where case_component_id=v_pair.id
        and pay_week_start=v_origin.input_start_monday for update;
      select * into strict v_period_rule from private.bpay_next_case_rule where id=v_pair_period.rule_id;
      if v_period_rule.original_funding_event_id is null
         and (v_period_rule.case_id,v_period_rule.candidate_id,v_period_rule.case_kind,v_period_rule.case_subtype,
           v_period_rule.tax_treatment,v_period_rule.case_created_at_utc,v_period_rule.minimum_earnings_threshold,
           v_period_rule.take_home_floor_override,v_period_rule.weekly_due_source_ex_vat,v_period_rule.schedule_start_monday,
           v_period_rule.next_due_monday,v_period_rule.schedule_week_count)
           is not distinct from (v_rule.case_id,v_rule.candidate_id,v_rule.case_kind,v_rule.case_subtype,
             v_rule.tax_treatment,v_rule.case_created_at_utc,v_rule.minimum_earnings_threshold,v_rule.take_home_floor_override,
             v_rule.weekly_due_source_ex_vat,v_rule.schedule_start_monday,v_rule.next_due_monday,v_rule.schedule_week_count)
         and (v_pair_period.opening_outstanding_source_ex_vat,v_pair_period.opening_due_source_ex_vat,
           v_pair_period.realised_recovery_source_ex_vat,v_pair_period.active_unrealised_recovery_source_ex_vat,v_pair_period.active_payout_source_ex_vat)
           is not distinct from (0::numeric,0::numeric,0::numeric,0::numeric,0::numeric) then
        update private.bpay_next_case_period set rule_id=v_new_rule_id,
          opening_outstanding_source_ex_vat=v_pair.funded_source_ex_vat+v_amount,
          opening_due_source_ex_vat=least(v_rule.weekly_due_source_ex_vat,v_pair.funded_source_ex_vat+v_amount),period_revision=period_revision+1
          where case_component_id=v_pair.id and pay_week_start=v_origin.input_start_monday;
      end if;
      update private.bpay_next_finance_case set principal_funded=principal_funded+v_amount,
        active_payout_hold_amount=active_payout_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
        current_rule_id=v_new_rule_id,current_rule_revision=case when v_new_rule_id=v_rule.id then v_rule.rule_revision else v_rule.rule_revision+1 end,
        case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp()
        where id=v_case.id and active_payout_hold_amount>=v_amount and active_hold_amount>=v_amount;
    elsif v_kind='PAID' then
      update private.bpay_next_case_component set paid_credit_source_ex_vat=paid_credit_source_ex_vat+v_amount,
        active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,component_revision=component_revision+1,
        updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id and active_payout_source_ex_vat>=v_amount;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_CREDIT_COMPONENT_CAPACITY_INVALID';end if;
      update private.bpay_next_finance_case set principal_paid_credit=principal_paid_credit+v_amount,
        active_payout_hold_amount=active_payout_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
        case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp()
        where id=v_case.id and active_payout_hold_amount>=v_amount and active_hold_amount>=v_amount;
    else
      update private.bpay_next_case_component set recovered_source_ex_vat=recovered_source_ex_vat+v_amount,
        active_recovery_source_ex_vat=active_recovery_source_ex_vat-v_amount,component_revision=component_revision+1,
        updated_at_utc=pg_catalog.transaction_timestamp() where id=v_component.id and active_recovery_source_ex_vat>=v_amount;
      if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_RECOVERY_COMPONENT_CAPACITY_INVALID';end if;
      update private.bpay_next_finance_case set principal_recovered=principal_recovered+v_amount,
        active_recovery_hold_amount=active_recovery_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
        case_revision=case_revision+1,updated_at_utc=pg_catalog.transaction_timestamp()
        where id=v_case.id and active_recovery_hold_amount>=v_amount and active_hold_amount>=v_amount;
    end if;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_POST_PRINCIPAL_CAPACITY_INVALID';end if;
    if v_kind in ('FUNDED','PAID') then
      update private.bpay_next_case_period set active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,
        period_revision=period_revision+1 where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start
          and active_payout_source_ex_vat>=v_amount;
    else
      update private.bpay_next_case_period set active_unrealised_recovery_source_ex_vat=active_unrealised_recovery_source_ex_vat-v_amount,
        realised_recovery_source_ex_vat=realised_recovery_source_ex_vat+v_amount,period_revision=period_revision+1
        where case_component_id=v_component.id and pay_week_start=v_instruction.pay_week_start and active_unrealised_recovery_source_ex_vat>=v_amount;
    end if;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_CASE_POST_PERIOD_CAPACITY_INVALID';end if;
    update private.bpay_next_case_hold set status='REALISED',finished_at_utc=pg_catalog.transaction_timestamp(),
      projection_id=case when purpose='NET_RECOVERY_CAPACITY' then v_build.projection_id else projection_id end where id=v_hold.id;
    perform private.bpay_next_record_case_hold_transition_v1(p_job_id,v_hold.id,'REALISED');
    update private.bpay_next_case_capacity_use set status='REALISED',realisation_event_id=v_event_id where case_hold_id=v_hold.id;
    update private.bpay_next_run_worker set active_case_hold_count=active_case_hold_count-1,realised_effect_count=realised_effect_count+1 where id=v_worker.id;
    if v_kind='RECOVERED' and v_instruction.work_collection_id is not null then
      perform private.bpay_next_apply_work_collection_recovery_v1(p_job_id,v_event_id);
    end if;
  elsif v_member.case_hold_id is not null then
    raise exception using errcode='23514',message='BPAY_NEXT_CASE_POST_ZERO_HOLD_FORBIDDEN';
  end if;
  if v_instruction.instruction_kind='RECOVERY' and v_result.shortfall_target_ex_vat>0 then
    insert into private.bpay_next_case_event(case_id,operation_id,operation_item_id,event_kind,original_transfer_id,occurred_at_utc,
      instruction_id,allocation_result_id,run_worker_id,candidate_id,case_component_id,event_case_kind,hold_purpose,pay_week_start,
      source_amount_ex_vat,target_amount_ex_vat,target_amount_vat,target_amount_inc_vat,shortfall_source_ex_vat,cap_reason)
      values(v_case.id,v_job.command_id,v_result.id,'SHORTFALL',v_transfer.id,v_occurred_at,v_instruction.id,v_result.id,
        v_worker.id,v_worker.candidate_id,v_component.id,v_instruction.case_kind,v_instruction.hold_purpose,v_instruction.pay_week_start,
        0,0,0,0,v_instruction.nominal_source_ex_vat-v_amount,v_result.cap_reason);
  end if;
end
$function$;

create or replace function private.bpay_next_release_prepare_case_hold_v1(p_job_id uuid,p_case_hold_id uuid,p_now timestamptz)
returns void language plpgsql security invoker set search_path=pg_catalog,private
as $function$
declare v_case_hold record;v_instruction record;v_result record;v_use record;v_binding record;v_worker record;
  v_candidate uuid;v_kind text;v_amount numeric;v_now timestamptz:=p_now;
begin
  select id,run_worker_id,instruction_id,allocation_result_id,case_id,case_component_id,candidate_id,
    pay_week_start,status,allocation_pass_kind,projection_id,purpose,source_reserved_ex_vat,
    target_amount_ex_vat,target_amount_vat,target_amount_inc_vat
    into strict v_case_hold from private.bpay_next_case_hold where id=p_case_hold_id;
  if v_case_hold.status<>'ACTIVE' or p_now is null or not pg_catalog.isfinite(p_now)
    or not private.bpay_next_prepare_release_scope_v1(p_job_id,v_case_hold.run_worker_id,'CASE') then
    raise exception using errcode='23514',message='BPAY_NEXT_PREPARE_RELEASE_SCOPE_INVALID';end if;
  select job_kind,candidate_id into strict v_kind,v_candidate from private.bpay_next_job where id=p_job_id;
  select id into strict v_worker from private.bpay_next_run_worker where id=v_case_hold.run_worker_id;
  if v_kind='PREPARATION_EXPIRY' then
    select run_worker_id,preparation_revision,selection_revision,draft_state_id into strict v_binding
      from private.bpay_next_preparation_expiry_worker where job_id=p_job_id;
  elsif v_kind='PREPARATION_DISCARD' then
    select run_worker_id,preparation_revision,selection_revision,draft_state_id into strict v_binding
      from private.bpay_next_preparation_discard_worker where job_id=p_job_id;
  else
    select x.run_worker_id,x.captured_preparation_revision as preparation_revision,x.selection_revision,x.draft_state_id
      into strict v_binding from private.bpay_next_worker_reprepare_request x
      join private.bpay_next_run_worker w on w.id=x.run_worker_id and w.preparation_revision=x.target_preparation_revision
      where x.job_id=p_job_id and x.action='RELEASE' and x.status='RUNNING';
  end if;
  select id,run_worker_id,preparation_revision,selection_revision,work_collection_id into strict v_instruction from private.bpay_next_run_case_instruction where id=v_case_hold.instruction_id;
  select pass_kind,instruction_id,state_id,allocated_source_ex_vat,allocated_target_ex_vat,allocated_target_vat,allocated_target_inc_vat into strict v_result from private.bpay_next_case_allocation_result where id=v_case_hold.allocation_result_id;
  perform 1 from private.bpay_next_finance_case where id=v_case_hold.case_id for update;
  perform 1 from private.bpay_next_case_component where id=v_case_hold.case_component_id for update;
  perform 1 from private.bpay_next_case_period where case_component_id=v_case_hold.case_component_id and pay_week_start=v_case_hold.pay_week_start for update;
  perform 1 from private.bpay_next_case_hold where id=v_case_hold.id for update;
  select status,realisation_event_id,source_amount_ex_vat into strict v_use from private.bpay_next_case_capacity_use where case_hold_id=v_case_hold.id for update;
  if v_instruction.run_worker_id<>v_binding.run_worker_id or v_instruction.preparation_revision<>v_binding.preparation_revision
     or v_instruction.selection_revision<>v_binding.selection_revision or v_case_hold.candidate_id<>v_candidate
     or v_case_hold.allocation_pass_kind<>'DRAFT' or v_result.pass_kind<>'DRAFT'
     or v_result.instruction_id<>v_instruction.id or v_result.state_id is distinct from v_binding.draft_state_id
     or v_case_hold.projection_id is not null or v_use.status<>'ACTIVE' or v_use.realisation_event_id is not null
     or v_use.source_amount_ex_vat<>v_case_hold.source_reserved_ex_vat
     or (v_case_hold.source_reserved_ex_vat,v_case_hold.target_amount_ex_vat,v_case_hold.target_amount_vat,v_case_hold.target_amount_inc_vat)
       is distinct from (v_result.allocated_source_ex_vat,v_result.allocated_target_ex_vat,v_result.allocated_target_vat,v_result.allocated_target_inc_vat) then
    raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_HOLD_NOT_EXACT';end if;
  v_amount:=v_case_hold.source_reserved_ex_vat;
  if v_case_hold.purpose='PAYOUT' then
    update private.bpay_next_case_component set active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,
      component_revision=component_revision+1,updated_at_utc=v_now where id=v_case_hold.case_component_id and active_payout_source_ex_vat>=v_amount;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_COMPONENT_CAPACITY_INVALID';end if;
    update private.bpay_next_finance_case set active_payout_hold_amount=active_payout_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
      case_revision=case_revision+1,updated_at_utc=v_now where id=v_case_hold.case_id and active_payout_hold_amount>=v_amount and active_hold_amount>=v_amount;
  else
    update private.bpay_next_case_component set active_recovery_source_ex_vat=active_recovery_source_ex_vat-v_amount,
      component_revision=component_revision+1,updated_at_utc=v_now where id=v_case_hold.case_component_id and active_recovery_source_ex_vat>=v_amount;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_COMPONENT_CAPACITY_INVALID';end if;
    update private.bpay_next_finance_case set active_recovery_hold_amount=active_recovery_hold_amount-v_amount,active_hold_amount=active_hold_amount-v_amount,
      case_revision=case_revision+1,updated_at_utc=v_now where id=v_case_hold.case_id and active_recovery_hold_amount>=v_amount and active_hold_amount>=v_amount;
  end if;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_CAPACITY_INVALID';end if;
  if v_case_hold.purpose='PAYOUT' then
    update private.bpay_next_case_period set active_payout_source_ex_vat=active_payout_source_ex_vat-v_amount,period_revision=period_revision+1
      where case_component_id=v_case_hold.case_component_id and pay_week_start=v_case_hold.pay_week_start and active_payout_source_ex_vat>=v_amount;
  else
    update private.bpay_next_case_period set active_unrealised_recovery_source_ex_vat=active_unrealised_recovery_source_ex_vat-v_amount,period_revision=period_revision+1
      where case_component_id=v_case_hold.case_component_id and pay_week_start=v_case_hold.pay_week_start and active_unrealised_recovery_source_ex_vat>=v_amount;
  end if;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_PERIOD_INVALID';end if;
  update private.bpay_next_case_hold set status='RELEASED',finished_at_utc=v_now where id=v_case_hold.id;
    perform private.bpay_next_record_case_hold_transition_v1(p_job_id,v_case_hold.id,'RELEASED');
  update private.bpay_next_case_capacity_use set status='RELEASED' where case_hold_id=v_case_hold.id;
  update private.bpay_next_run_worker set active_case_hold_count=active_case_hold_count-1 where id=v_worker.id and active_case_hold_count>0;
  if not found then raise exception using errcode='23514',message='BPAY_NEXT_EXPIRY_CASE_HOLD_COUNT_INVALID';end if;
  if v_instruction.work_collection_id is not null then
    if v_kind='PREPARATION_EXPIRY' then
      perform private.bpay_next_reconcile_expired_work_collection_v1(p_job_id,v_case_hold.id);
    elsif v_kind='PREPARATION_DISCARD' then
      perform private.bpay_next_reconcile_discarded_work_collection_v1(p_job_id,v_case_hold.id);
    else
      perform private.bpay_next_reconcile_worker_released_collection_v1(p_job_id,v_case_hold.id);
    end if;
  end if;
end
$function$;
alter function private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_apply_case_paye_net_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_apply_case_paye_net_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_cancel_case_worker_page_v1(uuid,uuid,bigint,bigint,integer) owner to postgres;
revoke all on function private.bpay_next_cancel_case_worker_page_v1(uuid,uuid,bigint,bigint,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_post_case_transfer_member_v1(uuid,uuid,bigint) owner to postgres;
revoke all on function private.bpay_next_post_case_transfer_member_v1(uuid,uuid,bigint) from public,anon,authenticated,service_role;
alter function private.bpay_next_release_prepare_case_hold_v1(uuid,uuid,timestamp with time zone) owner to postgres;
revoke all on function private.bpay_next_release_prepare_case_hold_v1(uuid,uuid,timestamp with time zone) from public,anon,authenticated,service_role;
alter function private.bpay_next_record_case_hold_transition_v1(uuid,uuid,text) owner to postgres;
revoke all on function private.bpay_next_record_case_hold_transition_v1(uuid,uuid,text) from public,anon,authenticated,service_role;

create or replace function public.bpay_next_preparation_page_v1(p_actor_user_id uuid,p_kind text,p_args jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_work uuid;v_revision uuid;v_line uuid;v_shift uuid;v_limit integer;
  v_progress jsonb;v_row jsonb;v_page jsonb[];v_rows jsonb:='[]'::jsonb;v_n integer:=0;v_complete boolean:=true;
begin
  if p_kind in ('CASE_AUDIT','CANDIDATE_CASE_REPORT') then
   if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
   if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
   return private.bpay_next_case_finance_page_v1(p_actor_user_id,p_kind,p_args);
  end if;
  if p_kind in ('CASE_LIST','CASE_SUMMARY') then
   if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
   if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
   return private.bpay_next_case_list_page_v1(p_actor_user_id,p_kind,p_args);
  end if;
  if p_kind='CASE_PREVIEW' then
    -- FC-03 genuinely read-only: no authority helper, receipt, business lock or wake.
    if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
    if p_args ? 'case_id' or p_args ? 'expected_case_revision' then
      if jsonb_typeof(p_args) is distinct from 'object' or p_args-array['input','case_id','expected_case_revision']<>'{}'::jsonb
        or not(p_args ?& array['input','case_id','expected_case_revision'])
        or jsonb_typeof(p_args->'case_id') is distinct from 'string'
        or p_args->>'case_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
        or jsonb_typeof(p_args->'expected_case_revision') is distinct from 'string'
        or p_args->>'expected_case_revision'!~'^[1-9][0-9]{0,18}$'
        or (p_args->>'expected_case_revision')::numeric>9223372036854775807 then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_EDIT_INPUT_INVALID';end if;
      return jsonb_build_object('rows',jsonb_build_array(private.bpay_next_case_edit_preview_v1(p_actor_user_id,
        (p_args->>'case_id')::uuid,(p_args->>'expected_case_revision')::bigint,p_args->'input')),'complete',true);
    end if;
    if p_args is null or pg_catalog.jsonb_typeof(p_args) is distinct from 'object' or (p_args-'input') is distinct from '{}'::jsonb
      or not(p_args ? 'input') then raise exception using errcode='22023',message='BPAY_NEXT_CASE_CREATE_INPUT_INVALID';end if;
    return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(private.bpay_next_case_create_preview_v1(p_actor_user_id,p_args->'input')),'complete',true);
  end if;
  if p_kind='GUARD' then
   -- Read-only guard takes no business locks or mutating authority helper.
   if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
    or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
    raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
   if not exists(select 1 from private.bpay_next_module_control where id=1 and active_owner='NEXT') then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
   v_keys:=array['run_id']||case when p_args ? 'descriptor' then array['descriptor'] else array[]::text[] end;
   perform private.bpay_next_preparation_args_v1(p_args,v_keys);
   return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(private.bpay_next_paye_guard_page_v1(p_actor_user_id,(p_args->>'run_id')::uuid,p_args->'descriptor')),'complete',true);
  end if;
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  v_keys:=case p_kind
    when 'PROGRESS' then array['run_id']
    when 'COMPONENTS' then array['work_id','expected_revision_id','after_line_no','limit']
    when 'WORK_CHOICES' then case when p_args ? 'accepted_inputs' then array['run_id','accepted_inputs','after_member_no','limit']||case when p_args ? 'member_kind' then array['member_kind'] else array[]::text[] end else array['run_id','after_work_id','limit'] end
    when 'SELECTED_COMPONENTS' then array['run_id','work_id','after_selection_no','limit']
    when 'CHANNEL_DRAFTS' then array['run_id']
    when 'CHANNEL_WORKERS' then array['run_id','pay_channel','after_candidate_id','limit']
    when 'CHANNEL_STATUS' then array['run_id','pay_channel','run_worker_id','after_transfer_no','limit']
    when 'WORKERS' then array['run_id','after_candidate_id','limit']
    when 'REVISION_COMPARISON' then array['run_worker_id','after_work_id','limit']
    when 'LINES' then array['run_worker_id','after_line_no','limit']
    when 'SHIFTS' then array['work_id','expected_revision_id','approved_line_id','after_detail_no','limit']
    when 'BREAKS' then array['work_id','expected_revision_id','approved_line_id','shift_detail_id','after_break_no','limit']
    when 'RATES' then array['work_id','expected_revision_id','approved_line_id','after_bucket','limit']
    when 'CURRENT_WORK' then array['candidate_id','after_week','after_work_id','limit']
    when 'CURRENT_CASES' then array['candidate_id','after_case_id','limit']
    when 'CURRENT_STORED_CREDITS' then array['candidate_id','after_legacy_case_id','limit']
    when 'CASE_COMPONENTS' then array['candidate_id','case_id','after_component_ordinal','limit']
      ||case when p_args ? 'edit_command_id' then array['edit_command_id'] else array[]::text[] end
      ||case when p_args ? 'pause_command_id' then array['pause_command_id'] else array[]::text[] end
    when 'STATUS' then array['run_id','run_worker_id','after_transfer_no','limit']
    when 'RETURN_CASH' then array['run_id','run_worker_id','after_transfer_no','limit'] else null end;
  if v_keys is null then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  perform private.bpay_next_preparation_args_v1(case when p_kind='WORK_CHOICES' and p_args ? 'accepted_inputs' then p_args-'member_kind' else p_args end,array_remove(v_keys,'member_kind'));
  if p_kind='WORK_CHOICES' and p_args ? 'accepted_inputs' then
    if p_args->'accepted_inputs' is distinct from 'true'::jsonb or (p_args ? 'member_kind' and (pg_catalog.jsonb_typeof(p_args->'member_kind') is distinct from 'string' or p_args->>'member_kind' not in ('WORK','CASE_COMPONENT'))) then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    perform private.bpay_next_selection_page_authority_v1((p_args->>'run_id')::uuid,p_actor_user_id);
    return private.bpay_next_choice_inputs_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_member_no')::bigint,(p_args->>'limit')::integer,coalesce(p_args->>'member_kind','WORK'));
  end if;
  if p_kind='PROGRESS' then
    v_progress:=private.bpay_next_preparation_progress_v1((p_args->>'run_id')::uuid);
    return pg_catalog.jsonb_build_object('rows',pg_catalog.jsonb_build_array(v_progress),'complete',true);
  end if;
  v_work:=(p_args->>'work_id')::uuid;v_revision:=(p_args->>'expected_revision_id')::uuid;
  v_line:=(p_args->>'approved_line_id')::uuid;v_shift:=(p_args->>'shift_detail_id')::uuid;v_limit:=(p_args->>'limit')::integer;
  if p_kind='CHANNEL_DRAFTS' then
    return private.bpay_next_channel_drafts_page_v1((p_args->>'run_id')::uuid);
  elsif p_kind='CHANNEL_WORKERS' then
    return private.bpay_next_channel_worker_page_v1((p_args->>'run_id')::uuid,
      p_args->>'pay_channel',(p_args->>'after_candidate_id')::uuid,v_limit);
  elsif p_kind='CHANNEL_STATUS' then
    if p_args->'after_transfer_no'<>'null'::jsonb and ((p_args->>'after_transfer_no')::numeric<1
      or (p_args->>'after_transfer_no')::numeric>2147483647) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
    end if;
    return private.bpay_next_channel_status_page_v1((p_args->>'run_id')::uuid,p_args->>'pay_channel',
      (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
  end if;
  -- Only the two exact factual0580 owners; no competing reader or financial
  -- history/calculation path. CURRENT_WORK does not certify payability.
  if p_kind='CURRENT_WORK' then
    return private.bpay_next_current_work_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_week')::date,(p_args->>'after_work_id')::uuid,v_limit);
  elsif p_kind='CURRENT_CASES' then
    return private.bpay_next_current_case_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_case_id')::uuid,v_limit);
  elsif p_kind='CURRENT_STORED_CREDITS' then
    -- Factual, exact-Candidate possible origins only; binding revalidates the
    -- retained creator/component/account facts. This is not pay eligibility.
    -- Closed raw reply includes scan_cursor from the bounded raw window;
    -- filtering can produce an empty nonterminal page without restarting it.
    return private.bpay_next_current_stored_credit_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'after_legacy_case_id')::uuid,v_limit);
  elsif p_kind='CASE_COMPONENTS' then
    v_progress:=private.bpay_next_current_case_component_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'case_id')::uuid,(p_args->>'after_component_ordinal')::bigint,v_limit);
    if p_args ? 'edit_command_id' then
      if jsonb_typeof(p_args->'edit_command_id') is distinct from 'string'
        or p_args->>'edit_command_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_EDIT_INPUT_INVALID';end if;
      v_row:=private.bpay_next_case_edit_receipt_v1((p_args->>'edit_command_id')::uuid,(p_args->>'case_id')::uuid,(p_args->>'candidate_id')::uuid);
      v_progress:=jsonb_set(v_progress,'{rows}',(select coalesce(jsonb_agg(value||jsonb_build_object('edit_receipt',v_row)),'[]'::jsonb)
        from jsonb_array_elements(v_progress->'rows')));
    end if;
    if p_args ? 'pause_command_id' then
      if jsonb_typeof(p_args->'pause_command_id') is distinct from 'string'
         or p_args->>'pause_command_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$' then
        raise exception using errcode='22023',message='BPAY_NEXT_CASE_PAUSE_INPUT_INVALID';end if;
      v_row:=private.bpay_next_case_pause_receipt_v1((p_args->>'pause_command_id')::uuid,(p_args->>'case_id')::uuid,(p_args->>'candidate_id')::uuid);
      v_progress:=jsonb_set(v_progress,'{rows}',(select coalesce(jsonb_agg(value||jsonb_build_object('pause_receipt',v_row)),'[]'::jsonb)
        from jsonb_array_elements(v_progress->'rows')));
    end if;
    if pg_catalog.octet_length(v_progress::text)>120000 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_ROW_TOO_LARGE';end if;
    return v_progress;
  elsif p_kind in ('STATUS','RETURN_CASH') then
    if p_args->'after_transfer_no'<>'null'::jsonb and ((p_args->>'after_transfer_no')::numeric<1
      or (p_args->>'after_transfer_no')::numeric>2147483647) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';
    end if;
    if p_kind='RETURN_CASH' then
      return private.bpay_next_return_cash_page_v1((p_args->>'run_id')::uuid,
        (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
    end if;
    return private.bpay_next_worker_status_page_v1((p_args->>'run_id')::uuid,
      (p_args->>'run_worker_id')::uuid,(p_args->>'after_transfer_no')::integer,v_limit);
  end if;
  if p_kind in ('COMPONENTS','SHIFTS','BREAKS','RATES') then
    if not exists(select 1 from private.bpay_next_work w
      join private.bpay_next_work_revision r on r.work_id=w.id and r.id=v_revision
      where w.id=v_work and w.approval_state='APPROVED' and w.current_revision_id=v_revision
        and w.applied_revision_id=v_revision and r.sealed_at_utc is not null and r.approved_at_utc is not null) then
      raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_REVISION_NOT_CURRENT';
    end if;
    if p_kind<>'COMPONENTS' and not exists(select 1 from private.bpay_next_approved_line l where l.id=v_line and l.revision_id=v_revision) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_LINE_NOT_IN_REVISION';
    end if;
    if p_kind='BREAKS' and not exists(select 1 from private.bpay_next_shift_detail s where s.id=v_shift and s.approved_line_id=v_line) then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_LINE_NOT_IN_REVISION';
    end if;
  elsif p_kind in ('WORK_CHOICES','SELECTED_COMPONENTS') then
    perform 1 from private.bpay_next_pay_run r where r.id=(p_args->>'run_id')::uuid;
    if not found then raise exception using errcode='P0002',message='BPAY_NEXT_PREPARATION_NOT_FOUND';end if;
    if p_kind='SELECTED_COMPONENTS' then
      perform 1 from private.bpay_next_work_choice c where c.run_id=(p_args->>'run_id')::uuid and c.work_id=v_work;
      if not found then raise exception using errcode='P0002',message='BPAY_NEXT_PREPARATION_NOT_FOUND';end if;
    end if;
  end if;
  -- Each branch is a <=100 indexed relational page. A byte boundary ends
  -- before an entire row, never truncates facts; callers resume its last key.
  -- CASE evaluates only its chosen subquery, so unrelated review owners are
  -- never called with NULL scope. Each subquery orders/limits BEFORE packing.
  select case p_kind
    when 'COMPONENTS' then array(
      select pg_catalog.jsonb_build_object(
        'approved_line_id',l.id,'line_no',l.line_no::text,'component_key',l.component_key,'component_kind',l.component_kind,
        'work_date',l.work_date,'source_pay_channel',r.source_pay_channel,'approved_source_ex_vat',l.source_pay_ex_vat::text,
        'unit_label',l.unit_label,'approved_quantity',l.approved_quantity::text,
        'approved_unit_rate',l.approved_unit_rate::text,
        'job_title',r.job_title,'band_label',r.band_label,
        'original_work_ref',parent.reference_number,'original_timesheet_id',parent.timesheet_id,
        'is_adjustment',ts.is_adjustment,'correction_kind',ts.correction_kind,
        'associated_rate_schedule_id',rs.id,'associated_rate_family',rs.rate_family,'associated_rate_code',rs.rate_code,
        'available_paye_rate',rs.paye_rate::text,'available_umbrella_rate',rs.umbrella_rate::text,
        'available_charge_rate',rs.charge_rate::text,'approved_charge_rate',rs.approved_charge_rate::text,
        'standard_rate_schedule',case when l.component_kind in ('WORK','PROTECTED_WORK') then
          (select coalesce(jsonb_agg(snapshot.value order by snapshot.bucket_order),'[]'::jsonb) from (
            select jsonb_build_object('rate_schedule_id',sr.id,'bucket',sr.rate_code,
              'available_paye_rate',sr.paye_rate::text,'available_umbrella_rate',sr.umbrella_rate::text,
              'available_charge_rate',sr.charge_rate::text,'approved_charge_rate',sr.approved_charge_rate::text) value,
              case sr.rate_code when 'DAY' then 1 when 'NIGHT' then 2 when 'SAT' then 3 when 'SUN' then 4 when 'BH' then 5 end bucket_order
            from private.bpay_next_rate_schedule sr where sr.revision_id=v_revision and sr.rate_family='STANDARD'
              and sr.rate_code in ('DAY','NIGHT','SAT','SUN','BH') order by bucket_order limit 5
          ) snapshot) else '[]'::jsonb end,
        'realised_source_ex_vat',p.realised_source_ex_vat::text,'held_source_ex_vat',p.held_source_ex_vat::text,
        'residual_source_ex_vat',(l.source_pay_ex_vat-p.realised_source_ex_vat-p.held_source_ex_vat)::text,
        'position_ready',coalesce(p.applied_revision_id=v_revision and p.approved_source_ex_vat=l.source_pay_ex_vat,false)) as value
        from private.bpay_next_approved_line l join private.bpay_next_work_revision r on r.id=l.revision_id
        join private.bpay_next_work w on w.id=v_work and w.id=r.work_id
        left join public.timesheets ts on ts.timesheet_id=r.physical_timesheet_id and ts.contract_id=w.contract_id
        left join public.timesheets parent on parent.timesheet_id=ts.parent_timesheet_id
          and exists(select 1 from public.contracts pc where pc.id=parent.contract_id and pc.candidate_id=w.candidate_id)
        left join private.bpay_next_rate_schedule rs on rs.revision_id=v_revision
          and ((l.component_kind='ADDITIONAL' and l.component_key like 'ADDITIONAL:%'
            and rs.rate_family='ADDITIONAL' and rs.rate_code=substring(l.component_key from 12))
            or (l.component_kind='MILEAGE' and l.component_key='MILEAGE:MILE' and rs.rate_family='MILEAGE' and rs.rate_code='MILE'))
        left join private.bpay_next_position p on p.work_id=v_work and p.component_key=l.component_key
        where l.revision_id=v_revision and l.line_no>coalesce((p_args->>'after_line_no')::bigint,0)
        order by l.line_no,l.id limit v_limit)
    when 'WORK_CHOICES' then array(
      select pg_catalog.jsonb_build_object('work_id',c.work_id,'expected_revision_id',c.expected_revision_id,
        'selection_mode',c.selection_mode,'selection_state',c.selection_state,'page_count',c.page_count::text,'component_count',c.component_count::text)
        from private.bpay_next_work_choice c where c.run_id=(p_args->>'run_id')::uuid
          and ((p_args->>'after_work_id') is null or c.work_id>(p_args->>'after_work_id')::uuid)
        order by c.work_id limit v_limit)
    when 'SELECTED_COMPONENTS' then array(
      select pg_catalog.jsonb_build_object('selection_no',c.selection_no::text,
        'approved_line_id',c.approved_line_id,'expected_revision_id',c.expected_revision_id,'component_key',c.component_key)
        from private.bpay_next_selected_component c where c.run_id=(p_args->>'run_id')::uuid and c.work_id=v_work
          and c.selection_no>coalesce((p_args->>'after_selection_no')::bigint,0)
        order by c.selection_no limit v_limit)
    when 'WORKERS' then array(
      select pg_catalog.jsonb_build_object('run_worker_id',w.run_worker_id,'candidate_id',w.candidate_id,
        'worker_status',w.worker_status,'frozen_ex_vat',w.frozen_ex_vat::text,'frozen_vat',w.frozen_vat::text,
        'frozen_inc_vat',w.frozen_inc_vat::text,'review_issue_code',w.review_issue_code,
        'discard_outcome',x.outcome,'discard_blocked_code',x.blocked_code)
        from private.bpay_next_simple_review_worker_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_candidate_id')::uuid,v_limit) w
        left join private.bpay_next_preparation_discard_worker x on x.run_id=(p_args->>'run_id')::uuid and x.candidate_id=w.candidate_id
        order by w.candidate_id limit v_limit)
    when 'REVISION_COMPARISON' then array(
      select pg_catalog.jsonb_build_object('run_work_id',w.run_work_id,'work_id',w.work_id,'original_timesheet_id',w.original_timesheet_id,
        'captured_revision_id',w.captured_revision_id,'current_revision_id',w.current_revision_id,'newer_approval_available',w.newer_approval_available)
        from private.bpay_next_simple_revision_comparison_page_v1((p_args->>'run_worker_id')::uuid,(p_args->>'after_work_id')::uuid,v_limit) w
        order by w.work_id limit v_limit)
    when 'LINES' then array(
      select pg_catalog.jsonb_build_object('run_line_id',l.run_line_id,'line_no',l.line_no::text,
        'original_timesheet_id',l.original_timesheet_id,'captured_revision_id',l.captured_revision_id,'component_key',l.component_key,
        'component_kind',l.component_kind,'work_date',l.work_date,'source_pay_channel',l.source_pay_channel,'target_pay_channel',l.target_pay_channel,
        'source_consumed_ex_vat',l.source_consumed_ex_vat::text,'frozen_ex_vat',l.frozen_ex_vat::text,'frozen_vat',l.frozen_vat::text,'frozen_inc_vat',l.frozen_inc_vat::text)
        from private.bpay_next_simple_review_line_page_v1((p_args->>'run_worker_id')::uuid,(p_args->>'after_line_no')::bigint,v_limit) l
        order by l.line_no limit v_limit)
    when 'SHIFTS' then array(
      select pg_catalog.jsonb_build_object('shift_detail_id',s.id,'detail_no',s.detail_no::text,'work_date',s.work_date,
        'shift_start_at',s.shift_start_at,'shift_end_at',s.shift_end_at,'shift_start_local',s.shift_start_local,'shift_end_local',s.shift_end_local,
        'shift_overnight',s.shift_overnight,'submitted_minutes',s.submitted_minutes::text,'approved_minutes',s.approved_minutes::text,
        'approved_hours',s.approved_hours::text,'detail_label',s.detail_label,'segment_pay_ex_vat',s.segment_pay_ex_vat::text,'pay_excluded',s.pay_excluded,
        'hours_day',s.hours_day::text,'hours_night',s.hours_night::text,'hours_sat',s.hours_sat::text,'hours_sun',s.hours_sun::text,'hours_bh',s.hours_bh::text,
        'segment_key',s.segment_key,'approved_segment_id',sg.id,'job_title',wr.job_title,'band_label',wr.band_label)
        from private.bpay_next_shift_detail s
        join private.bpay_next_approved_line al on al.id=s.approved_line_id and al.revision_id=v_revision
        join private.bpay_next_work_revision wr on wr.id=al.revision_id and wr.work_id=v_work
        left join private.bpay_next_approved_segment sg on sg.shift_detail_id=s.id
          and sg.revision_id=v_revision and sg.approved_line_id=v_line and sg.segment_key=s.segment_key
        where s.approved_line_id=v_line and s.detail_no>coalesce((p_args->>'after_detail_no')::bigint,0)
        order by s.detail_no limit v_limit)
    when 'BREAKS' then array(
      select pg_catalog.jsonb_build_object('break_detail_id',b.id,'break_no',b.break_no::text,'break_start_at',b.break_start_at,
        'break_end_at',b.break_end_at,'break_start_local',b.break_start_local,'break_end_local',b.break_end_local,'break_minutes',b.break_minutes::text)
        from private.bpay_next_break_detail b where b.shift_detail_id=v_shift and b.break_no>coalesce((p_args->>'after_break_no')::bigint,0)
        order by b.break_no limit v_limit)
    when 'RATES' then array(
      select pg_catalog.jsonb_build_object('rate_detail_id',r.id,'bucket',r.bucket,'approved_hours',r.approved_hours::text,'source_pay_rate',r.source_pay_rate::text,
        'rate_schedule_id',rs.id,'available_paye_rate',rs.paye_rate::text,'available_umbrella_rate',rs.umbrella_rate::text,
        'available_charge_rate',rs.charge_rate::text,'approved_charge_rate',rs.approved_charge_rate::text)
        from private.bpay_next_rate_detail r
        left join private.bpay_next_rate_schedule rs on rs.revision_id=v_revision
          and rs.rate_family='STANDARD' and rs.rate_code=r.bucket
        where r.approved_line_id=v_line
          and ((p_args->>'after_bucket') is null or r.bucket>p_args->>'after_bucket')
        order by r.bucket limit v_limit)
  end into v_page;
  foreach v_row in array v_page
  loop
    if pg_catalog.octet_length((v_rows||pg_catalog.jsonb_build_array(v_row))::text)>120000 then
      if v_n=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
      v_complete:=false;exit;
    end if;
    v_rows:=v_rows||pg_catalog.jsonb_build_array(v_row);v_n:=v_n+1;
  end loop;
  if v_n=v_limit then v_complete:=false;end if;
  return pg_catalog.jsonb_build_object('rows',v_rows,'complete',v_complete);
end
$function$;
alter function private.bpay_next_case_audit_sources_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_case_audit_sources_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_case_audit_row_v1(text,uuid,smallint,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_case_audit_row_v1(text,uuid,smallint,uuid,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_candidate_case_report_sources_v1(uuid,jsonb,boolean) owner to postgres;
revoke all on function private.bpay_next_candidate_case_report_sources_v1(uuid,jsonb,boolean) from public,anon,authenticated,service_role;
alter function private.bpay_next_case_finance_sort_key_v1(text,text,jsonb) owner to postgres;
revoke all on function private.bpay_next_case_finance_sort_key_v1(text,text,jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_candidate_case_report_row_v1(uuid,jsonb,boolean,text,uuid) owner to postgres;
revoke all on function private.bpay_next_candidate_case_report_row_v1(uuid,jsonb,boolean,text,uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_case_finance_page_v1(uuid,text,jsonb) owner to postgres;
revoke all on function private.bpay_next_case_finance_page_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
alter function public.bpay_next_preparation_page_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) to service_role;
notify pgrst,'reload schema';
commit;
