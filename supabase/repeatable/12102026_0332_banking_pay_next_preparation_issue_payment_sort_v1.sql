-- DR15 displayed Payment label and native whole-list sort use the same operand.
\set ON_ERROR_STOP on
begin;

create or replace function public.bpay_next_preparation_page_v1(p_actor_user_id uuid,p_kind text,p_args jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_work uuid;v_revision uuid;v_line uuid;v_shift uuid;v_limit integer;
  v_progress jsonb;v_row jsonb;v_page jsonb[];v_rows jsonb:='[]'::jsonb;v_n integer:=0;v_complete boolean:=true;
  v_issue_run uuid;v_issue_epoch bigint;v_issue_original_epoch bigint;v_issue_wb bigint;v_issue_adoption bigint;
  v_issue_run_row private.bpay_next_pay_run%rowtype;v_issue_context jsonb;v_issue_after jsonb;v_issue_reply jsonb;v_issue_sort text;v_issue_desc boolean;
  v_issue_reason_labels jsonb;v_issue_fresh_context jsonb;v_issue_final jsonb;
begin
  if p_kind='PREPARE_ISSUES' then
    -- DR15 read-only original selection/diagnostic facts; no economic authority.
    if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role'
      or p_actor_user_id is null or not exists(select 1 from public.tms_users where id=p_actor_user_id and is_active is true and role::text='admin') then
      raise exception using errcode='42501',message='BPAY_NEXT_PREPARATION_FORBIDDEN';end if;
    select owner_epoch into v_issue_epoch from private.bpay_next_module_control where id=1 and active_owner='NEXT';
    if not found then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
    if jsonb_typeof(p_args) is distinct from 'object' or pg_catalog.octet_length(p_args::text)>32768 or p_args-array['run_id','view','sort','after','limit','row_key']<>'{}'::jsonb
      or not(p_args ?& array['run_id','view','sort','after','limit','row_key'])
      or jsonb_typeof(p_args->'run_id') is distinct from 'string' or p_args->>'run_id'!~*'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
      or p_args->>'view' not in ('CANDIDATES','NOT_IN_DRAFT') or jsonb_typeof(p_args->'view') is distinct from 'string'
      or p_args->>'sort' not in ('CANDIDATE_ASC','CANDIDATE_DESC','PAYMENT_ASC','PAYMENT_DESC','REASON_ASC','REASON_DESC','GROSS_ASC','GROSS_DESC','STATE_ASC','STATE_DESC')
      or jsonb_typeof(p_args->'sort') is distinct from 'string' or jsonb_typeof(p_args->'limit') is distinct from 'number'
      or p_args->>'limit'!~'^[1-9][0-9]{0,2}$' or (p_args->>'limit')::integer>100
      or (p_args->'row_key'<>'null'::jsonb and (jsonb_typeof(p_args->'row_key') is distinct from 'string' or length(p_args->>'row_key') not between 1 and 160)) then
      raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    v_issue_run:=(p_args->>'run_id')::uuid;v_limit:=(p_args->>'limit')::integer;v_issue_sort:=p_args->>'sort';v_issue_desc:=right(v_issue_sort,4)='DESC';
    select r.* into v_issue_run_row from private.bpay_next_pay_run r where r.id=v_issue_run;
    if not found then raise exception using errcode='23514',message='BPAY_NEXT_PREPARATION_RUN_MISSING';end if;
    select c.module_epoch into v_issue_original_epoch from private.bpay_next_run_command rc join private.bpay_next_command c on c.id=rc.command_id where rc.run_id=v_issue_run;
    v_issue_original_epoch:=coalesce(v_issue_original_epoch,(v_issue_run_row.accepted_choice_descriptor#>>'{namespace,owner_epoch}')::bigint);
    if v_issue_original_epoch is distinct from v_issue_epoch then raise exception using errcode='55000',message='BPAY_NEXT_PREPARE_ISSUES_CONTEXT_CHANGED';end if;
    select workbench_revision,adoption_revision into v_issue_wb,v_issue_adoption from private.bpay_next_workbench_revision where id=1;
    v_issue_context:=jsonb_build_object('run_id',v_issue_run,'actor_user_id',p_actor_user_id,'owner_epoch',v_issue_epoch::text,
      'review_revision',v_issue_run_row.review_revision::text,'run_status',v_issue_run_row.status,'selection_state',v_issue_run_row.selection_state,
      'selection_pages',v_issue_run_row.selection_page_count::text,'selected_candidates',v_issue_run_row.selected_candidate_count::text,
      'preparing',v_issue_run_row.preparing_candidate_count::text,'ready',v_issue_run_row.ready_candidate_count::text,
      'review',v_issue_run_row.review_candidate_count::text,'removed',v_issue_run_row.cancelled_candidate_count::text,
      'workbench_revision',v_issue_wb::text,'adoption_revision',v_issue_adoption::text,'view',p_args->>'view','sort',v_issue_sort,'row_key',p_args->'row_key');
    v_issue_after:=p_args->'after';
    if v_issue_after<>'null'::jsonb then
      if jsonb_typeof(v_issue_after) is distinct from 'object' or pg_catalog.octet_length(v_issue_after::text)>8192 or v_issue_after-array['context','key']<>'{}'::jsonb or not(v_issue_after ?& array['context','key'])
        or jsonb_typeof(v_issue_after->'key') is distinct from 'array' or jsonb_array_length(v_issue_after->'key')<>3
        or jsonb_typeof(v_issue_after#>'{key,0}') is distinct from 'boolean'
        or jsonb_typeof(v_issue_after#>'{key,2}') is distinct from 'string' or length(v_issue_after#>>'{key,2}') not between 1 and 160
        or (v_issue_after#>'{key,1}'<>'null'::jsonb and (jsonb_typeof(v_issue_after#>'{key,1}') is distinct from 'string' or length(v_issue_after#>>'{key,1}')>8192))
        or ((v_issue_after#>>'{key,0}')::boolean is distinct from (v_issue_after#>'{key,1}'='null'::jsonb))
        or (left(v_issue_sort,5)='GROSS' and v_issue_after#>'{key,1}'<>'null'::jsonb and v_issue_after#>>'{key,1}'!~'^-?(0|[1-9][0-9]{0,15})(\.[0-9]{1,6})?$') then
        raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
      if v_issue_after->'context' is distinct from v_issue_context then raise exception using errcode='55000',message='BPAY_NEXT_PREPARE_ISSUES_CONTEXT_CHANGED';end if;
    end if;
    v_issue_reason_labels:=case when p_args->>'view'='NOT_IN_DRAFT' then '{"SOURCE_PAY_QUERY_OPEN":"Pay query open","SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE":"Could not check the pay query","SELECTED_WORK_NOT_APPROVED":"Changed after Create Draft — next Draft","SELECTED_WORK_REVISION_CHANGED":"Changed after Create Draft — next Draft","SELECTED_WORK_POSITION_PENDING":"Changed after Create Draft — next Draft","CAPTURE_POSITION_CHANGED":"The authorised Timesheet could not be prepared; try again.","POSITION_COMPONENT_EVIDENCE_MISSING":"Part of the authorised Timesheet could not be checked; try again.","POSITION_APPROVAL_AMOUNT_MISMATCH":"The authorised Timesheet amount could not be checked; try again.","UMBRELLA_WORK_EXECUTION_UNSUPPORTED":"This Timesheet cannot be paid this way; check the Timesheet.","SOURCE_BOOK_APPROVAL_UNBOUND":"The authorised Source Timesheet could not be checked; try again.","FINANCIAL_RESOLUTION_REQUIRED":"The payment needs a decision before it can be prepared.","TARGET_UMBRELLA_MISSING":"Choose the Candidate’s Umbrella before trying again.","SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE":"The Timesheet correction could not be checked; contact support.","CASE_SELECTION_REQUIRED":"The selected payments could not be checked; contact support.","CASE_TARGET_UNSUPPORTED":"The selected payment cannot be paid this way; contact support.","CASE_INPUT_UNBOUND":"The selected payment details could not be checked; contact support.","CASE_WEEK_BASIS_UNBOUND":"The payment week could not be checked; contact support.","CASE_RETURN_FLOOR_UNBOUND":"The returned payment details could not be checked; contact support.","CASE_NO_PAYABLE_AMOUNT":"Nothing to pay — deductions stay outstanding.","PF08_NEEDS_ATTENTION":"Could not prepare this Candidate after several tries.","BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE":"Not yet payable","BPAY_NEXT_SELECTION_ON_HOLD":"Pay on hold","BPAY_NEXT_SELECTION_SNOOZED":"Snoozed","BPAY_NEXT_SELECTION_WORK_NOT_APPROVED":"No longer authorised","BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT":"Changed after Create Draft — next Draft","BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE":"This line cannot be paid in Banking Pay; check the Timesheet.","BPAY_NEXT_SIGNED_COMPONENT_NOT_ELIGIBLE":"This line cannot be paid in Banking Pay; check the Timesheet.","PAYE_EXCLUDED_BY_SAME_WEEK_GUARD":"A PAYE Draft already exists for this pay week","ORIGIN_UNAUTHORISED_WAIT":"Recovery paused — waiting for re-authorisation","DRAFT_CANDIDATE_REMOVED":"Removed from Draft — payments are back in Banking Pay.","PREPARATION_ENROLMENT_FAILED":"Could not prepare this Candidate; contact support.","PREPARATION_EXPIRED":"The Draft expired — payments are back in Banking Pay.","DRAFT_DISCARDED":"The Draft was discarded — payments are back in Banking Pay."}'::jsonb else '{"SOURCE_PAY_QUERY_OPEN":"Pay Query open on this Timesheet — resolve it in Weekly Source Office Checks","SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE":"Pay Query evidence could not be confirmed — check the Timesheet''s queries","SELECTED_WORK_NOT_APPROVED":"The Timesheet is no longer authorised.","SELECTED_WORK_REVISION_CHANGED":"The authorised Timesheet changed after Create Draft.","SELECTED_WORK_POSITION_PENDING":"The authorised Timesheet changed after Create Draft.","CAPTURE_POSITION_CHANGED":"The authorised Timesheet could not be prepared; try again.","POSITION_COMPONENT_EVIDENCE_MISSING":"Part of the authorised Timesheet could not be checked; try again.","POSITION_APPROVAL_AMOUNT_MISMATCH":"The authorised Timesheet amount could not be checked; try again.","UMBRELLA_WORK_EXECUTION_UNSUPPORTED":"This Timesheet cannot be paid this way; check the Timesheet.","SOURCE_BOOK_APPROVAL_UNBOUND":"The authorised Source Timesheet could not be checked; try again.","FINANCIAL_RESOLUTION_REQUIRED":"The payment needs a decision before it can be prepared.","TARGET_UMBRELLA_MISSING":"Choose the Candidate’s Umbrella before trying again.","SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE":"The Timesheet correction could not be checked; contact support.","CASE_SELECTION_REQUIRED":"The selected payments could not be checked; contact support.","CASE_TARGET_UNSUPPORTED":"The selected payment cannot be paid this way; contact support.","CASE_INPUT_UNBOUND":"The selected payment details could not be checked; contact support.","CASE_WEEK_BASIS_UNBOUND":"The payment week could not be checked; contact support.","CASE_RETURN_FLOOR_UNBOUND":"The returned payment details could not be checked; contact support.","CASE_NO_PAYABLE_AMOUNT":"Nothing to pay — deductions stay outstanding.","PF08_NEEDS_ATTENTION":"Could not prepare this Candidate after several tries.","BPAY_NEXT_WORK_WEEK_NOT_YET_PAYABLE":"Not yet payable","BPAY_NEXT_SELECTION_ON_HOLD":"Pay on hold","BPAY_NEXT_SELECTION_SNOOZED":"Snoozed","BPAY_NEXT_SELECTION_WORK_NOT_APPROVED":"No longer authorised","BPAY_NEXT_SELECTION_REVISION_NOT_CURRENT":"Changed after Create Draft — next Draft","BPAY_NEXT_SELECTION_SIGNED_LINE_NOT_ELIGIBLE":"This line cannot be paid in Banking Pay; check the Timesheet.","BPAY_NEXT_SIGNED_COMPONENT_NOT_ELIGIBLE":"This line cannot be paid in Banking Pay; check the Timesheet.","PAYE_EXCLUDED_BY_SAME_WEEK_GUARD":"A PAYE Draft already exists for this pay week","ORIGIN_UNAUTHORISED_WAIT":"Recovery paused — waiting for re-authorisation","DRAFT_CANDIDATE_REMOVED":"Removed from Draft — payments are back in Banking Pay.","PREPARATION_ENROLMENT_FAILED":"Could not prepare this Candidate; contact support.","PREPARATION_EXPIRED":"The Draft expired — payments are back in Banking Pay.","DRAFT_DISCARDED":"The Draft was discarded — payments are back in Banking Pay."}'::jsonb end;
    with choice_skips as (
      select 'SKIP:'||m.work_id::text row_key,'SKIPPED_WORK'::text origin,m.candidate_id,null::uuid worker_id,m.work_id,m.expected_revision_id revision_id,
        'NOT_IN_DRAFT'::text state,m.skip_code code,null::numeric gross,null::bigint preparation_revision
      from private.bpay_next_run_choice_member m where m.run_id=v_issue_run and m.member_kind='WORK' and m.selected and m.skip_code is not null
    ), page_skips as (
      select distinct on (w.id) 'PAGE:'||p.page_no::text||':'||(s.value->>'work_id') row_key,'SKIPPED_WORK'::text origin,w.candidate_id,null::uuid worker_id,w.id work_id,
        (ch.value->>'expected_revision_id')::uuid revision_id,'NOT_IN_DRAFT'::text state,s.value->>'code' code,null::numeric gross,null::bigint preparation_revision
      from private.bpay_next_selection_page p cross join lateral jsonb_array_elements(p.skipped_json) s(value)
      join private.bpay_next_work w on w.id=(s.value->>'work_id')::uuid
      left join lateral (select value from jsonb_array_elements(coalesce(p.choices_request_json,'[]'::jsonb)) where value->>'work_id'=w.id::text limit 1) ch on true
      where p.run_id=v_issue_run and not exists(select 1 from private.bpay_next_run_choice_member m where m.run_id=v_issue_run and m.member_kind='WORK' and m.work_id=w.id and m.selected and m.skip_code is not null)
      order by w.id,p.page_no
    ), releases as (
      select 'RELEASE:'||q.run_worker_id::text row_key,case when q.initiator_kind='SYSTEM' then 'SYSTEM_RELEASE' else 'OFFICE_RELEASE' end origin,
        q.candidate_id,q.run_worker_id worker_id,q.original_issue_work_id work_id,coalesce(rw.captured_revision_id,cm.expected_revision_id,wc.expected_revision_id) revision_id,
        'NOT_IN_DRAFT'::text state,case when q.initiator_kind='SYSTEM' then q.original_issue_code else 'DRAFT_CANDIDATE_REMOVED' end code,
        null::numeric gross,q.target_preparation_revision preparation_revision
      from private.bpay_next_worker_reprepare_request q
      left join private.bpay_next_run_work rw on rw.run_worker_id=q.run_worker_id and rw.work_id=q.original_issue_work_id and rw.candidate_id=q.candidate_id
      left join private.bpay_next_run_choice_member cm on cm.run_id=v_issue_run and cm.member_kind='WORK' and cm.work_id=q.original_issue_work_id and cm.candidate_id=q.candidate_id
      left join private.bpay_next_work_choice wc on wc.run_id=v_issue_run and wc.work_id=q.original_issue_work_id
      where q.run_id=v_issue_run and q.action='RELEASE' and q.status='COMPLETE' and q.stage='COMPLETE' and q.finished_at_utc is not null
    ), expired as (
      select 'EXPIRED:'||x.candidate_id::text row_key,'EXPIRED'::text origin,x.candidate_id,x.run_worker_id worker_id,x.original_review_issue_work_id work_id,
        coalesce(rw.captured_revision_id,cm.expected_revision_id,wc.expected_revision_id) revision_id,'NOT_IN_DRAFT'::text state,'PREPARATION_EXPIRED'::text code,null::numeric gross,x.preparation_revision
      from private.bpay_next_preparation_expiry_worker x
      left join private.bpay_next_run_work rw on rw.run_worker_id=x.run_worker_id and rw.work_id=x.original_review_issue_work_id and rw.candidate_id=x.candidate_id
      left join private.bpay_next_run_choice_member cm on cm.run_id=v_issue_run and cm.member_kind='WORK' and cm.work_id=x.original_review_issue_work_id and cm.candidate_id=x.candidate_id
      left join private.bpay_next_work_choice wc on wc.run_id=v_issue_run and wc.work_id=x.original_review_issue_work_id
      where x.run_id=v_issue_run and x.status='EXPIRED' and x.stage='COMPLETE' and x.finished_at_utc is not null
        and not exists(select 1 from releases q where q.candidate_id=x.candidate_id)
    ), discarded as (
      select 'DISCARDED:'||x.candidate_id::text row_key,'DISCARDED'::text origin,x.candidate_id,x.run_worker_id worker_id,x.original_review_issue_work_id work_id,
        coalesce(rw.captured_revision_id,cm.expected_revision_id,wc.expected_revision_id) revision_id,'NOT_IN_DRAFT'::text state,'DRAFT_DISCARDED'::text code,null::numeric gross,x.preparation_revision
      from private.bpay_next_preparation_discard_worker x
      left join private.bpay_next_run_work rw on rw.run_worker_id=x.run_worker_id and rw.work_id=x.original_review_issue_work_id and rw.candidate_id=x.candidate_id
      left join private.bpay_next_run_choice_member cm on cm.run_id=v_issue_run and cm.member_kind='WORK' and cm.work_id=x.original_review_issue_work_id and cm.candidate_id=x.candidate_id
      left join private.bpay_next_work_choice wc on wc.run_id=v_issue_run and wc.work_id=x.original_review_issue_work_id
      where x.run_id=v_issue_run and x.status='DISCARDED' and x.stage='COMPLETE' and x.finished_at_utc is not null and x.outcome in ('REMOVED','ALREADY_REMOVED')
        and not exists(select 1 from releases q where q.candidate_id=x.candidate_id)
    ), candidates as (
      select 'CANDIDATE:'||sc.candidate_id::text row_key,'CANDIDATE'::text origin,sc.candidate_id,rw.id worker_id,rw.review_issue_work_id work_id,
        coalesce(cw.captured_revision_id,cm.expected_revision_id,wc.expected_revision_id) revision_id,
        coalesce(rw.status,case when cmd.status='FAILED' then 'FAILED' else 'PENDING' end) state,coalesce(rw.review_issue_code,case when rw.id is null and cmd.status='FAILED' then 'PREPARATION_ENROLMENT_FAILED' end) code,
        case when rw.status in ('READY','DRAFT','ISSUED','COMPLETE') then rw.gross_ex_vat end gross,rw.preparation_revision
      from private.bpay_next_selection_candidate sc
      left join private.bpay_next_run_worker rw on rw.run_id=sc.run_id and rw.candidate_id=sc.candidate_id
      left join private.bpay_next_run_work cw on cw.run_worker_id=rw.id and cw.work_id=rw.review_issue_work_id and cw.candidate_id=sc.candidate_id
      left join private.bpay_next_run_choice_member cm on cm.run_id=sc.run_id and cm.member_kind='WORK' and cm.work_id=rw.review_issue_work_id and cm.candidate_id=sc.candidate_id
      left join private.bpay_next_work_choice wc on wc.run_id=sc.run_id and wc.work_id=rw.review_issue_work_id
      left join private.bpay_next_run_command rc on rc.run_id=sc.run_id left join private.bpay_next_command cmd on cmd.id=rc.command_id
      where sc.run_id=v_issue_run
    ), facts as (
      select * from choice_skips union all select * from page_skips union all select * from releases union all select * from expired union all select * from discarded union all select * from candidates
    ), labelled as (
      select f.*,wr.physical_timesheet_id,wr.physical_timesheet_version,wr.timesheet_reference,wr.week_ending_date,
        coalesce(wr.candidate_display_name,original_label.candidate_display_name,case when f.work_id is null then coalesce(nullif(btrim(c.display_name),''),nullif(btrim(concat_ws(' ',c.first_name,c.last_name)),'')) end) candidate_name,
        case when wr.id is not null then wr.candidate_reference when original_label.id is not null then original_label.candidate_reference else case when f.work_id is null then c.tms_ref end end candidate_reference,
        case when wr.id is not null or original_label.id is not null then 'CAPTURED_WORK' when f.work_id is null then 'CURRENT_CANDIDATE' else 'UNKNOWN_ORIGINAL' end label_origin,
        x.outcome discard_outcome,x.blocked_code discard_blocked_code
      from facts f left join private.bpay_next_work w on w.id=f.work_id and w.candidate_id=f.candidate_id
      left join private.bpay_next_work_revision wr on wr.work_id=w.id and wr.id=f.revision_id
      left join lateral (
        select r.id,r.candidate_display_name,r.candidate_reference from private.bpay_next_run_choice_member m
        join private.bpay_next_work_revision r on r.work_id=m.work_id and r.id=m.expected_revision_id
        where m.run_id=v_issue_run and m.candidate_id=f.candidate_id and m.member_kind='WORK' and m.selected order by m.member_no limit 1
      ) original_label on f.work_id is null and wr.id is null
      left join public.candidates c on c.id=f.candidate_id
      left join private.bpay_next_preparation_discard_worker x on x.run_id=v_issue_run and x.candidate_id=f.candidate_id and f.origin='CANDIDATE'
    ), scoped as (
      select l.*,case split_part(v_issue_sort,'_',1) when 'CANDIDATE' then lower(l.candidate_name) when 'PAYMENT' then case when l.timesheet_reference is not null then 'Timesheet '||l.timesheet_reference when l.work_id is null then 'Payments' else 'Timesheet' end
        when 'REASON' then coalesce(v_issue_reason_labels->>l.code,case when l.state='PENDING' then 'Waiting to prepare' when l.state in ('PREPARING','CANCELLING') then 'Preparing' else 'Could not prepare this Candidate; contact support.' end)
        when 'STATE' then case l.state when 'FAILED' then 'To review' when 'DRAFT' then 'Confirmed' when 'ISSUED' then 'Issued' when 'COMPLETE' then 'Complete' when 'NOT_IN_DRAFT' then 'Not in this Draft' when 'PENDING' then 'Waiting to prepare' when 'PREPARING' then 'Preparing' when 'REVIEW' then 'To review' when 'READY' then 'Ready' when 'CANCELLING' then 'Removing' when 'CANCELLED' then 'Not in this Draft' else l.state end
        when 'GROSS' then l.gross::text end sort_value
      from labelled l where ((p_args->>'view'='NOT_IN_DRAFT' and l.origin<>'CANDIDATE') or (p_args->>'view'='CANDIDATES' and l.origin='CANDIDATE'))
    ), tail as (
      select sort_value from scoped where row_key=v_issue_after#>>'{key,2}'
    ), counts as (
      select (select count(distinct work_id) from facts where origin='SKIPPED_WORK') skipped_timesheets,
        (select count(distinct candidate_id) from facts where origin in ('SYSTEM_RELEASE','OFFICE_RELEASE','EXPIRED','DISCARDED')) removed_candidates,
        (select count(*) from scoped) scope_count,
        (select count(*) from tail where sort_value is not distinct from v_issue_after#>>'{key,1}') tail_matches
    ), bounded as (
      select n.* from scoped n where (p_args->'row_key'='null'::jsonb or n.row_key=p_args->>'row_key')
        and (v_issue_after='null'::jsonb or case when left(v_issue_sort,5)='GROSS' then
          case when v_issue_desc then (n.sort_value is null,coalesce(n.sort_value::numeric,0),n.row_key collate "C")<((v_issue_after#>>'{key,0}')::boolean,coalesce((v_issue_after#>>'{key,1}')::numeric,0),(v_issue_after#>>'{key,2}') collate "C")
            else (n.sort_value is null,coalesce(n.sort_value::numeric,0),n.row_key collate "C")>((v_issue_after#>>'{key,0}')::boolean,coalesce((v_issue_after#>>'{key,1}')::numeric,0),(v_issue_after#>>'{key,2}') collate "C") end
          else case when v_issue_desc then (n.sort_value is null,coalesce(n.sort_value,'') collate "C",n.row_key collate "C")<((v_issue_after#>>'{key,0}')::boolean,coalesce(v_issue_after#>>'{key,1}','') collate "C",(v_issue_after#>>'{key,2}') collate "C")
            else (n.sort_value is null,coalesce(n.sort_value,'') collate "C",n.row_key collate "C")>((v_issue_after#>>'{key,0}')::boolean,coalesce(v_issue_after#>>'{key,1}','') collate "C",(v_issue_after#>>'{key,2}') collate "C") end end)
      order by case when not v_issue_desc then n.sort_value is null end asc,case when v_issue_desc then n.sort_value is null end desc,
        case when left(v_issue_sort,5)='GROSS' and not v_issue_desc then n.sort_value::numeric end asc,
        case when left(v_issue_sort,5)='GROSS' and v_issue_desc then n.sort_value::numeric end desc,
        case when left(v_issue_sort,5)<>'GROSS' and not v_issue_desc then n.sort_value end collate "C" asc,
        case when left(v_issue_sort,5)<>'GROSS' and v_issue_desc then n.sort_value end collate "C" desc,
        case when not v_issue_desc then n.row_key end collate "C" asc,case when v_issue_desc then n.row_key end collate "C" desc limit v_limit+1
    ) select jsonb_build_object('skipped_timesheets',counts.skipped_timesheets::text,'removed_candidates',counts.removed_candidates::text,'scope_count',counts.scope_count::text,'tail_matches',counts.tail_matches::text,
      'rows',coalesce((select jsonb_agg(jsonb_build_object('row_key',b.row_key,'origin',b.origin,'candidate_id',b.candidate_id,'run_worker_id',b.worker_id,'worker_status',b.state,'issue_code',b.code,
        'work_id',b.work_id,'expected_revision_id',b.revision_id,'physical_timesheet_id',b.physical_timesheet_id,'physical_timesheet_version',b.physical_timesheet_version::text,
        'candidate_display_name',b.candidate_name,'candidate_reference',b.candidate_reference,'label_origin',b.label_origin,'timesheet_reference',b.timesheet_reference,'week_ending_date',b.week_ending_date,
        'frozen_ex_vat',b.gross::text,'preparation_revision',b.preparation_revision::text,'discard_outcome',b.discard_outcome,'discard_blocked_code',b.discard_blocked_code,
        'sort_key',jsonb_build_array(b.sort_value is null,b.sort_value,b.row_key)) order by
        case when not v_issue_desc then b.sort_value is null end asc,case when v_issue_desc then b.sort_value is null end desc,
        case when left(v_issue_sort,5)='GROSS' and not v_issue_desc then b.sort_value::numeric end asc,
        case when left(v_issue_sort,5)='GROSS' and v_issue_desc then b.sort_value::numeric end desc,
        case when left(v_issue_sort,5)<>'GROSS' and not v_issue_desc then b.sort_value end collate "C" asc,
        case when left(v_issue_sort,5)<>'GROSS' and v_issue_desc then b.sort_value end collate "C" desc,
        case when not v_issue_desc then b.row_key end collate "C" asc,case when v_issue_desc then b.row_key end collate "C" desc) from bounded b),'[]'::jsonb)) into v_issue_reply from counts;
    if v_issue_after<>'null'::jsonb and v_issue_reply->>'tail_matches'<>'1' then raise exception using errcode='55000',message='BPAY_NEXT_PREPARE_ISSUES_CONTEXT_CHANGED';end if;
    v_complete:=jsonb_array_length(v_issue_reply->'rows')<=v_limit;v_rows:='[]'::jsonb;v_n:=0;
    for v_row in select value from jsonb_array_elements(v_issue_reply->'rows') limit v_limit loop
      if exists(select 1 from jsonb_each_text(v_row) e where e.key in ('candidate_display_name','candidate_reference','timesheet_reference') and pg_catalog.octet_length(e.value)>8192) then
        raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
      v_issue_final:=jsonb_build_object('context',v_issue_context,'rows',v_rows||jsonb_build_array(v_row),'complete',false,
        'next_cursor',jsonb_build_object('context',v_issue_context,'key',v_row->'sort_key'),'counts',v_issue_reply-array['rows','tail_matches']);
      if pg_catalog.octet_length(v_issue_final::text)>120000 then
        if v_n=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
        v_complete:=false;exit;end if;
      v_rows:=v_rows||jsonb_build_array(v_row);v_n:=v_n+1;
    end loop;
    -- The same native context closes a first-read/concurrent-commit gap.
    select jsonb_build_object('run_id',v_issue_run,'actor_user_id',p_actor_user_id,'owner_epoch',m.owner_epoch::text,
      'review_revision',r.review_revision::text,'run_status',r.status,'selection_state',r.selection_state,
      'selection_pages',r.selection_page_count::text,'selected_candidates',r.selected_candidate_count::text,
      'preparing',r.preparing_candidate_count::text,'ready',r.ready_candidate_count::text,
      'review',r.review_candidate_count::text,'removed',r.cancelled_candidate_count::text,
      'workbench_revision',b.workbench_revision::text,'adoption_revision',b.adoption_revision::text,'view',p_args->>'view','sort',v_issue_sort,'row_key',p_args->'row_key') into v_issue_fresh_context
      from private.bpay_next_pay_run r cross join private.bpay_next_module_control m cross join private.bpay_next_workbench_revision b
      where r.id=v_issue_run and m.id=1 and m.active_owner='NEXT' and b.id=1;
    if v_issue_fresh_context is distinct from v_issue_context then raise exception using errcode='55000',message='BPAY_NEXT_PREPARE_ISSUES_CONTEXT_CHANGED';end if;
    v_issue_final:=jsonb_build_object('context',v_issue_context,'rows',v_rows,'complete',v_complete,
      'next_cursor',case when v_complete then null else jsonb_build_object('context',v_issue_context,'key',v_rows->-1->'sort_key') end,
      'counts',v_issue_reply-array['rows','tail_matches']);
    if pg_catalog.octet_length(v_issue_final::text)>120000 or (not v_complete and pg_catalog.octet_length((v_issue_final->'next_cursor')::text)>8192) then
      raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
    return v_issue_final;
  end if;
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
    when 'WORKERS' then array['run_id','after_candidate_id','limit']||case when p_args ? 'attention_only' then array['attention_only'] else array[]::text[] end
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
  if p_kind='WORKERS' and p_args ? 'attention_only' then
    if p_args->'attention_only' is distinct from 'true'::jsonb then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
    return private.bpay_next_prepare_attention_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_candidate_id')::uuid,(p_args->>'limit')::integer);
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


alter function public.bpay_next_preparation_page_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) from public,anon,authenticated;
grant execute on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) to service_role;
notify pgrst, 'reload schema';
commit;
