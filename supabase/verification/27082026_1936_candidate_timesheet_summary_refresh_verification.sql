-- Rollback-contained first-use proof for the Candidate-driven Timesheet Summary refresh cursor.
begin;

do $candidate_timesheet_summary_refresh_verification$
declare
  v_timesheet_id uuid:=gen_random_uuid();
  v_contract_week_id uuid:=gen_random_uuid();
  v_workflow_id uuid:=gen_random_uuid();
  v_cursor_before bigint;
  v_cursor_after bigint;
  v_ping jsonb;
begin
  if has_table_privilege('anon','private.candidate_timesheet_summary_revisions','select')
     or has_table_privilege('authenticated','private.candidate_timesheet_summary_revisions','select')
     or has_table_privilege('service_role','private.candidate_timesheet_summary_revisions','select') then
    raise exception 'CANDIDATE_TIMESHEET_SUMMARY_PRIVATE_TABLE_EXPOSED';
  end if;
  if has_sequence_privilege('anon','private.candidate_timesheet_summary_revision_seq','usage')
     or has_sequence_privilege('authenticated','private.candidate_timesheet_summary_revision_seq','usage')
     or has_sequence_privilege('service_role','private.candidate_timesheet_summary_revision_seq','usage') then
    raise exception 'CANDIDATE_TIMESHEET_SUMMARY_PRIVATE_SEQUENCE_EXPOSED';
  end if;
  if has_function_privilege('anon','public.candidate_timesheet_summary_cursor_v1()','execute')
     or has_function_privilege('authenticated','public.candidate_timesheet_summary_cursor_v1()','execute')
     or not has_function_privilege('service_role','public.candidate_timesheet_summary_cursor_v1()','execute') then
    raise exception 'CANDIDATE_TIMESHEET_SUMMARY_CURSOR_ACL_INVALID';
  end if;

  v_cursor_before:=coalesce((public.candidate_timesheet_summary_cursor_v1()->>'cursor')::bigint,0);
  perform private._candidate_timesheet_summary_revision_upsert_v1(
    'TIMESHEET',v_timesheet_id,v_timesheet_id,v_contract_week_id
  );
  v_cursor_after:=coalesce((public.candidate_timesheet_summary_cursor_v1()->>'cursor')::bigint,0);
  if v_cursor_after<=v_cursor_before then
    raise exception 'CANDIDATE_TIMESHEET_SUMMARY_CURSOR_DID_NOT_ADVANCE';
  end if;

  perform set_config('request.jwt.claim.role','service_role',true);
  v_ping:=public.rpc_changes_ping(jsonb_build_object(
    '__candidate_timesheet_summary_watch',true,
    '__candidate_timesheet_summary_cursor',v_cursor_before
  ));
  if coalesce((v_ping->>'candidate_timesheet_summary_cursor')::bigint,0)<>v_cursor_after
     or jsonb_array_length(coalesce(v_ping->'candidate_timesheet_summary_changed_identities','[]'::jsonb))<>1
     or v_ping#>>'{candidate_timesheet_summary,changed_identities,0,identity_id}'<>v_timesheet_id::text
     or coalesce((v_ping->>'candidate_timesheet_summary_overflow')::boolean,true) then
    raise exception 'CANDIDATE_TIMESHEET_SUMMARY_HEARTBEAT_FIRST_USE_FAILED: %',v_ping;
  end if;

  create temporary table candidate_summary_workflow_trigger_fixture(
    anchor_timesheet_id uuid,
    target_timesheet_id uuid,
    contract_week_id uuid
  ) on commit drop;
  create trigger candidate_summary_workflow_trigger_fixture_trg
  after insert or update or delete on candidate_summary_workflow_trigger_fixture
  for each row execute function private._candidate_timesheet_summary_workflow_revision_trg_v1();
  insert into candidate_summary_workflow_trigger_fixture(
    anchor_timesheet_id,target_timesheet_id,contract_week_id
  ) values(v_timesheet_id,v_timesheet_id,v_contract_week_id);
  update candidate_summary_workflow_trigger_fixture
  set target_timesheet_id=gen_random_uuid();
  delete from candidate_summary_workflow_trigger_fixture;

  create temporary table candidate_summary_approval_trigger_fixture(workflow_id uuid) on commit drop;
  create trigger candidate_summary_approval_trigger_fixture_trg
  after insert or update or delete on candidate_summary_approval_trigger_fixture
  for each row execute function private._candidate_timesheet_summary_approval_revision_trg_v1();
  insert into candidate_summary_approval_trigger_fixture(workflow_id) values(v_workflow_id);
  update candidate_summary_approval_trigger_fixture set workflow_id=gen_random_uuid();
  delete from candidate_summary_approval_trigger_fixture;

  create temporary table candidate_summary_mail_trigger_fixture(payment_scope_json jsonb) on commit drop;
  create trigger candidate_summary_mail_trigger_fixture_trg
  after insert or update or delete on candidate_summary_mail_trigger_fixture
  for each row execute function private._candidate_timesheet_summary_mail_revision_trg_v1();
  insert into candidate_summary_mail_trigger_fixture(payment_scope_json)
  values(jsonb_build_object('candidate_workflow_id',v_workflow_id));
  update candidate_summary_mail_trigger_fixture
  set payment_scope_json=jsonb_build_object('candidate_manager_workflow_id',v_workflow_id);
  delete from candidate_summary_mail_trigger_fixture;

  if not exists(
    select 1 from private.candidate_timesheet_summary_revisions
    where identity_kind='TIMESHEET' and identity_id=v_timesheet_id
  ) or not exists(
    select 1 from private.candidate_timesheet_summary_revisions
    where identity_kind='CONTRACT_WEEK' and identity_id=v_contract_week_id
  ) then
    raise exception 'CANDIDATE_TIMESHEET_SUMMARY_TRIGGER_FIRST_USE_FAILED';
  end if;
end;
$candidate_timesheet_summary_refresh_verification$;

rollback;
