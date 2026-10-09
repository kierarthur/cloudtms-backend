-- F-25 bounded immutable accepted identity and factual preparation progress.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_accepted_preparation_header_v1(p_run_id uuid)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare run private.bpay_next_pay_run%rowtype;command private.bpay_next_command%rowtype;
begin
 select r.* into strict run from private.bpay_next_pay_run r where r.id=p_run_id;
 select c.* into strict command from private.bpay_next_run_command rc
   join private.bpay_next_command c on c.id=rc.command_id where rc.run_id=run.id;
 if command.command_kind<>'PREPARE' or command.agency_sequence<1 or command.module_epoch<1
   or command.expected_member_count<1 or command.expected_member_count<>run.selected_candidate_count
   or command.enrolled_member_count<0 or command.enrolled_member_count>command.expected_member_count
   or command.expected_member_count is null or command.enrolled_member_count is null
   or run.selection_state is distinct from 'SEALED' then
   raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_PREPARATION_HEADER_MISMATCH';end if;
 return jsonb_build_object('run_id',run.id,'prepare_command_id',command.id,'accepted_sequence',command.agency_sequence::text,
   'accepted_module_epoch',command.module_epoch::text,'run_status',run.status,'command_status',command.status,
   'expected_candidate_count',command.expected_member_count::text,'enrolled_candidate_count',command.enrolled_member_count::text);
end
$function$;

create or replace function private.bpay_next_accepted_candidate_page_v1(p_run_id uuid,p_after uuid,p_limit integer)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare header jsonb;row record;body jsonb;rows jsonb:='[]'::jsonb;n integer:=0;complete boolean:=true;
 state text;code text;epoch bigint;
begin
 if p_run_id is null or p_limit is null or p_limit not between 1 and 100 then
   raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
 header:=private.bpay_next_accepted_preparation_header_v1(p_run_id);
 select m.owner_epoch into epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT';
 for row in with selection_window as materialized (
   select selected.candidate_id,selected.member_no from private.bpay_next_selection_candidate selected
   where selected.run_id=p_run_id and (p_after is null or selected.candidate_id>p_after)
   order by selected.candidate_id limit p_limit+1
 ) select input.*,selected.candidate_id as selected_candidate_id,member.command_id as member_command,
   j.id as job_id,j.status as job_status,j.phase as job_phase,j.module_epoch as execution_epoch,
   worker.id as worker_id,worker.status as worker_status,worker.review_issue_code,
   waiting.command_id as waiting_command_id,waiting.command_sequence as waiting_sequence,
   exists(select 1 from private.bpay_next_accepted_work_input wi
     where wi.accepted_input_id=input.id and not exists(select 1 from private.bpay_next_effective_work_cut cut
       where cut.accepted_work_input_id=wi.id and cut.accepted_input_id=input.id)) as unsealed
   from selection_window selected
   left join private.bpay_next_command_member member on member.command_id=(header->>'prepare_command_id')::uuid
     and member.candidate_id=selected.candidate_id and member.member_no=selected.member_no
   left join private.bpay_next_accepted_input input on input.run_id=p_run_id and input.candidate_id=selected.candidate_id
     and input.prepare_command_id=(header->>'prepare_command_id')::uuid
     and input.accepted_sequence=(header->>'accepted_sequence')::bigint and input.module_epoch=(header->>'accepted_module_epoch')::bigint
   left join private.bpay_next_job j on j.command_id=input.prepare_command_id and j.candidate_id=input.candidate_id
     and j.job_kind='PREPARE' and j.command_sequence=input.accepted_sequence
   left join private.bpay_next_run_worker worker on worker.run_id=input.run_id and worker.candidate_id=input.candidate_id
   left join lateral (select earlier.command_id,earlier.command_sequence from private.bpay_next_job earlier
     where earlier.candidate_id=input.candidate_id and earlier.command_sequence<input.accepted_sequence and earlier.status<>'DONE'
     order by earlier.command_sequence,earlier.id limit 1) waiting on true
   order by selected.candidate_id
 loop
   if n=p_limit then complete:=false;exit;end if;
   if row.id is null or row.member_command is null then
     raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_PREPARATION_MEMBER_MISSING';end if;
   if exists(select 1 from private.bpay_next_accepted_work_input wi
     join private.bpay_next_effective_work_cut cut on cut.accepted_work_input_id=wi.id
     where wi.accepted_input_id=row.id and ((cut.accepted_input_id,cut.candidate_id,cut.work_id,cut.effective_revision_id,
       cut.publication_command_id,cut.predecessor_sequence,cut.selected_line_count,cut.evidence_sha256)
       is distinct from (wi.accepted_input_id,wi.candidate_id,wi.work_id,wi.effective_revision_id,
         wi.publication_command_id,wi.predecessor_sequence,wi.selected_line_count,wi.evidence_sha256)
       or not exists(select 1 from private.bpay_next_job prepare where prepare.id=cut.prepare_job_id and prepare.job_kind='PREPARE'
         and (prepare.command_id,prepare.candidate_id,prepare.command_sequence,prepare.module_epoch)=
           (row.prepare_command_id,row.candidate_id,row.accepted_sequence,row.module_epoch)))) then
     raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_WORK_CUT_MISMATCH';end if;
   state:=null;code:=null;
   if row.worker_status='CANCELLED' or (row.job_status='DONE' and row.job_phase='EXPIRED') then
     state:='CANCELLED';code:=case when row.job_phase='EXPIRED' then 'PREPARATION_EXPIRED' else null end;
   elsif epoch is null or row.module_epoch<>epoch or (row.job_id is not null and row.execution_epoch<>epoch) then
     state:='EPOCH_PAUSED';code:='OWNER_EPOCH_STALE';
   elsif row.job_status='FAILED' or header->>'command_status'='FAILED' then state:='FAILED';code:='JOB_FAILED';
   elsif row.worker_status='CANCELLING' or header->>'run_status'='CANCELLING' then state:='CANCELLING';code:='CANCELLATION_PENDING';
   elsif row.worker_status='REVIEW' then state:='REVIEW';code:='REVIEW_REQUIRED';
   elsif row.job_status='DONE' and row.job_phase='READY' and row.worker_status in ('READY','DRAFT','ISSUED','COMPLETE')
     and not row.unsealed and header->>'command_status' in ('SEALED','COMPLETE')
     and header->>'expected_candidate_count'=header->>'enrolled_candidate_count' then state:='READY';
   elsif row.job_id is null and header->>'command_status' in ('RECEIVED','ENROLLING') and row.worker_id is null then
     state:='WAITING_ENROLLMENT';code:='ENROLLMENT_PENDING';
   elsif row.waiting_command_id is not null and row.job_status in ('READY','LEASED','BLOCKED') then
     state:='WAITING_EARLIER_COMMAND';code:='EARLIER_COMMAND_PENDING';
   elsif row.job_status in ('READY','LEASED','BLOCKED') and row.job_phase in ('NEW','PINNING','RESERVE','ALLOCATE','FINANCE_ALLOCATE')
     and (row.worker_status is null or row.worker_status='PREPARING') then
     state:='PREPARING';code:=case when row.unsealed then 'CUT_PENDING' else 'PREPARATION_RUNNING' end;
   else raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_PROGRESS_STATE_INVALID';end if;
   body:=jsonb_build_object('candidate_id',row.candidate_id,'accepted_input_id',row.id,'module_epoch',row.module_epoch::text,
     'target_pay_channel',row.target_pay_channel,'target_umbrella_id',row.target_umbrella_id,'pay_week_start',row.pay_week_start,
     'case_scope',row.case_scope,'case_selection_revision',row.case_selection_revision::text,'settings_witness_kind',row.settings_witness_kind,
     'input_evidence_sha256',encode(row.evidence_sha256,'hex'),'prepare_job_id',row.job_id,'prepare_job_status',row.job_status,
     'prepare_job_phase',row.job_phase,'run_worker_id',row.worker_id,'worker_status',row.worker_status,'has_unsealed_work',row.unsealed,
     'waiting_command_id',row.waiting_command_id,'waiting_command_sequence',row.waiting_sequence::text,
     'progress_state',state,'progress_code',code,'review_issue_code',row.review_issue_code);
   if octet_length(jsonb_build_object('header',header,'rows',rows||jsonb_build_array(body),'complete',false)::text)>120000 then
     if n=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
     complete:=false;exit;end if;
   rows:=rows||jsonb_build_array(body);n:=n+1;
 end loop;
 return jsonb_build_object('header',header,'rows',rows,'complete',complete);
end
$function$;

create or replace function private.bpay_next_accepted_work_page_v1(p_run_id uuid,p_candidate_id uuid,p_after uuid,p_limit integer)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare header jsonb;input private.bpay_next_accepted_input%rowtype;row record;
 body jsonb;rows jsonb:='[]'::jsonb;n integer:=0;complete boolean:=true;state text;
begin
 if p_run_id is null or p_candidate_id is null or p_limit is null or p_limit not between 1 and 100 then
   raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
 header:=private.bpay_next_accepted_preparation_header_v1(p_run_id);
 select a.* into strict input from private.bpay_next_accepted_input a where a.run_id=p_run_id and a.candidate_id=p_candidate_id
   and a.prepare_command_id=(header->>'prepare_command_id')::uuid;
 for row in select wi.*,cut.id as cut_id,p.status as publication_status,p.phase as publication_phase,p.applied_at_utc,
   c.status as command_status,c.sealed_at_utc,c.module_epoch as publication_epoch,c.command_kind,
   c.agency_sequence as publication_sequence,c.expected_member_count,c.enrolled_member_count,
   member.candidate_id as original_member,j.status as job_status,j.phase as job_phase,j.lease_nonce,j.lease_until_utc,
   j.command_sequence as job_sequence,j.module_epoch as job_epoch
   from private.bpay_next_accepted_work_input wi
   left join private.bpay_next_effective_work_cut cut on cut.accepted_work_input_id=wi.id and cut.accepted_input_id=input.id
   left join private.bpay_next_publication p on p.command_id=wi.publication_command_id and p.work_id=wi.work_id
     and p.revision_id=wi.effective_revision_id and p.candidate_id=wi.candidate_id
   left join private.bpay_next_command c on c.id=wi.publication_command_id
   left join private.bpay_next_command_member member on member.command_id=c.id and member.candidate_id=wi.candidate_id
   left join private.bpay_next_job j on j.command_id=c.id and j.candidate_id=wi.candidate_id and j.job_kind='POSITION_APPLY'
   where wi.accepted_input_id=input.id and (p_after is null or wi.work_id>p_after)
   order by wi.work_id limit p_limit+1
 loop
   if n=p_limit then complete:=false;exit;end if;
   if row.publication_status is null or row.command_status is null or row.original_member is null
     or row.command_kind<>'POSITION_APPLY' or row.publication_sequence<>row.predecessor_sequence
     or row.predecessor_sequence>=input.accepted_sequence or row.publication_epoch<>input.module_epoch
     or (row.job_status is null and (row.command_status='COMPLETE' or row.enrolled_member_count>0
       or (row.publication_status,row.publication_phase)=('APPLIED','DONE')))
     or (row.job_status is not null and (row.job_sequence,row.job_epoch) is distinct from (row.publication_sequence,row.publication_epoch)) then
     raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_WORK_PUBLICATION_MISMATCH';end if;
   if row.cut_id is not null then
     if not exists(select 1 from private.bpay_next_effective_work_cut cut where cut.id=row.cut_id
       and (cut.work_id,cut.candidate_id,cut.effective_revision_id,cut.publication_command_id,cut.predecessor_sequence,cut.selected_line_count)
         is not distinct from (row.work_id,row.candidate_id,row.effective_revision_id,row.publication_command_id,row.predecessor_sequence,row.selected_line_count)
       and cut.evidence_sha256=row.evidence_sha256
       and exists(select 1 from private.bpay_next_job prepare where prepare.id=cut.prepare_job_id
         and prepare.job_kind='PREPARE' and prepare.command_id=input.prepare_command_id
         and prepare.candidate_id=input.candidate_id and prepare.command_sequence=input.accepted_sequence
         and prepare.module_epoch=input.module_epoch)) then
       raise exception using errcode='23514',message='BPAY_NEXT_ACCEPTED_WORK_CUT_MISMATCH';end if;
     state:='SEALED';
   elsif (row.publication_status,row.publication_phase)=('APPLIED','DONE') and row.applied_at_utc is not null
     and row.command_status='COMPLETE' and row.sealed_at_utc is not null and row.expected_member_count=row.enrolled_member_count
     and (row.job_status,row.job_phase)=('DONE','DONE') and row.lease_nonce is null and row.lease_until_utc is null then state:='WAITING_PIN';
   else state:='WAITING_PREDECESSOR';end if;
   body:=jsonb_build_object('candidate_id',row.candidate_id,'accepted_input_id',row.accepted_input_id,'accepted_work_input_id',row.id,
     'work_id',row.work_id,'original_revision_id',row.original_revision_id,'effective_revision_id',row.effective_revision_id,
     'selection_mode',row.selection_mode,'selected_line_count',row.selected_line_count::text,'publication_command_id',row.publication_command_id,
     'predecessor_sequence',row.predecessor_sequence::text,'source_kind',row.source_kind,'query_disposition',row.query_disposition,
     'query_state_sha256',encode(row.query_state_sha256,'hex'),'input_evidence_sha256',encode(row.evidence_sha256,'hex'),
     'effective_cut_id',row.cut_id,'cut_state',state);
   if octet_length(jsonb_build_object('header',header,'rows',rows||jsonb_build_array(body),'complete',false)::text)>120000 then
     if n=0 then raise exception using errcode='54000',message='BPAY_NEXT_PREPARATION_RESPONSE_TOO_LARGE';end if;
     complete:=false;exit;end if;
   rows:=rows||jsonb_build_array(body);n:=n+1;
 end loop;
 return jsonb_build_object('header',header,'rows',rows,'complete',complete);
end
$function$;

create or replace function public.bpay_next_preparation_page_v1(p_actor_user_id uuid,p_kind text,p_args jsonb)
returns jsonb language plpgsql security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_keys text[];v_work uuid;v_revision uuid;v_line uuid;v_shift uuid;v_limit integer;
  v_row jsonb;v_page jsonb[];v_rows jsonb:='[]'::jsonb;v_n integer:=0;v_complete boolean:=true;
begin
  perform private.bpay_next_preparation_authority_v1(p_actor_user_id);
  v_keys:=case p_kind
    when 'COMPONENTS' then array['work_id','expected_revision_id','after_line_no','limit']
    when 'WORK_CHOICES' then array['run_id','after_work_id','limit']
    when 'SELECTED_COMPONENTS' then array['run_id','work_id','after_selection_no','limit']
    when 'CHANNEL_DRAFTS' then array['run_id']
    when 'CHANNEL_WORKERS' then array['run_id','pay_channel','after_candidate_id','limit']
    when 'CHANNEL_STATUS' then array['run_id','pay_channel','run_worker_id','after_transfer_no','limit']
    when 'ACCEPTED_CANDIDATES' then array['run_id','after_candidate_id','limit']
    when 'ACCEPTED_WORK' then array['run_id','candidate_id','after_work_id','limit']
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
    when 'STATUS' then array['run_id','run_worker_id','after_transfer_no','limit']
    when 'RETURN_CASH' then array['run_id','run_worker_id','after_transfer_no','limit'] else null end;
  if v_keys is null then raise exception using errcode='22023',message='BPAY_NEXT_PREPARATION_REQUEST_INVALID';end if;
  perform private.bpay_next_preparation_args_v1(p_args,v_keys);
  v_work:=(p_args->>'work_id')::uuid;v_revision:=(p_args->>'expected_revision_id')::uuid;
  v_line:=(p_args->>'approved_line_id')::uuid;v_shift:=(p_args->>'shift_detail_id')::uuid;v_limit:=(p_args->>'limit')::integer;
  if p_kind='ACCEPTED_CANDIDATES' then
    return private.bpay_next_accepted_candidate_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_candidate_id')::uuid,v_limit);
  elsif p_kind='ACCEPTED_WORK' then
    return private.bpay_next_accepted_work_page_v1((p_args->>'run_id')::uuid,(p_args->>'candidate_id')::uuid,(p_args->>'after_work_id')::uuid,v_limit);
  elsif p_kind='CHANNEL_DRAFTS' then
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
    return private.bpay_next_current_case_component_page_v1((p_args->>'candidate_id')::uuid,
      (p_args->>'case_id')::uuid,(p_args->>'after_component_ordinal')::bigint,v_limit);
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
        'realised_source_ex_vat',p.realised_source_ex_vat::text,'held_source_ex_vat',p.held_source_ex_vat::text,
        'residual_source_ex_vat',(l.source_pay_ex_vat-p.realised_source_ex_vat-p.held_source_ex_vat)::text,
        'position_ready',coalesce(p.applied_revision_id=v_revision and p.approved_source_ex_vat=l.source_pay_ex_vat,false)) as value
        from private.bpay_next_approved_line l join private.bpay_next_work_revision r on r.id=l.revision_id
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
        'frozen_inc_vat',w.frozen_inc_vat::text,'review_issue_code',w.review_issue_code)
        from private.bpay_next_simple_review_worker_page_v1((p_args->>'run_id')::uuid,(p_args->>'after_candidate_id')::uuid,v_limit) w
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
        'hours_day',s.hours_day::text,'hours_night',s.hours_night::text,'hours_sat',s.hours_sat::text,'hours_sun',s.hours_sun::text,'hours_bh',s.hours_bh::text)
        from private.bpay_next_shift_detail s where s.approved_line_id=v_line and s.detail_no>coalesce((p_args->>'after_detail_no')::bigint,0)
        order by s.detail_no limit v_limit)
    when 'BREAKS' then array(
      select pg_catalog.jsonb_build_object('break_detail_id',b.id,'break_no',b.break_no::text,'break_start_at',b.break_start_at,
        'break_end_at',b.break_end_at,'break_start_local',b.break_start_local,'break_end_local',b.break_end_local,'break_minutes',b.break_minutes::text)
        from private.bpay_next_break_detail b where b.shift_detail_id=v_shift and b.break_no>coalesce((p_args->>'after_break_no')::bigint,0)
        order by b.break_no limit v_limit)
    when 'RATES' then array(
      select pg_catalog.jsonb_build_object('rate_detail_id',r.id,'bucket',r.bucket,'approved_hours',r.approved_hours::text,'source_pay_rate',r.source_pay_rate::text)
        from private.bpay_next_rate_detail r where r.approved_line_id=v_line
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

alter function private.bpay_next_accepted_preparation_header_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_accepted_preparation_header_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_accepted_candidate_page_v1(uuid,uuid,integer) owner to postgres;
revoke all on function private.bpay_next_accepted_candidate_page_v1(uuid,uuid,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_accepted_work_page_v1(uuid,uuid,uuid,integer) owner to postgres;
revoke all on function private.bpay_next_accepted_work_page_v1(uuid,uuid,uuid,integer) from public,anon,authenticated,service_role;
alter function public.bpay_next_preparation_page_v1(uuid,text,jsonb) owner to postgres;
revoke all on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_preparation_page_v1(uuid,text,jsonb) to service_role;
notify pgrst,'reload schema';
commit;
