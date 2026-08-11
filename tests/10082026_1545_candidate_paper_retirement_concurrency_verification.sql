\set ON_ERROR_STOP on
-- Disposable PostgreSQL-only two-session proof. Synthetic fixtures are
-- committed only so dblink sessions can see them, then every exact fixture is
-- removed before the suite returns. Never run this file against TEST or LIVE.
create extension if not exists dblink;

begin;
update public.settings_defaults
set candidate_app_feature_flags_json=candidate_app_feature_flags_json
  ||'{"candidate_app_writes":true,"candidate_paper_qr":true,"candidate_notifications":true}'::jsonb,
    candidate_app_environment='TEST'
where id=1;

insert into public.candidates(id,email,active)
values('b5200000-0000-4000-8000-000000000001','paper-race@example.test',true);
insert into public.clients(id,name)
values('b5200000-0000-4000-8000-000000000002','Paper race client');
insert into public.contracts(id,candidate_id,client_id,start_date,end_date)
values(
  'b5200000-0000-4000-8000-000000000003',
  'b5200000-0000-4000-8000-000000000001',
  'b5200000-0000-4000-8000-000000000002','2026-01-01','2026-12-31'
);
insert into public.timesheets(
  timesheet_id,submission_mode,sheet_scope,contract_id,booking_id,week_ending_date,
  qr_status,qr_token,qr_payload_json,qr_generated_at,document_state
) values(
  'b5200000-0000-4000-8000-000000000004','MANUAL','WEEKLY',
  'b5200000-0000-4000-8000-000000000003','PAPER-RELEASE-RACE','2026-08-16',
  'PENDING','paper-race-token',jsonb_build_object('v',1,'tok','paper-race-token'),now(),'READY'
);
insert into public.contract_weeks(
  id,contract_id,week_ending_date,status,submission_mode_snapshot,timesheet_id
) values(
  'b5200000-0000-4000-8000-000000000005',
  'b5200000-0000-4000-8000-000000000003','2026-08-16','SUBMITTED','MANUAL',
  'b5200000-0000-4000-8000-000000000004'
);
insert into public.timesheets_financials(
  timesheet_id,candidate_id,client_id,is_current,processing_status,total_hours
) values(
  'b5200000-0000-4000-8000-000000000004',
  'b5200000-0000-4000-8000-000000000001',
  'b5200000-0000-4000-8000-000000000002',true,'AWAITING_MANUAL_SIGNATURE',8
);
insert into public.candidate_app_accounts(id,environment,email_normalized,status)
values(
  'b5200000-0000-4000-8000-000000000006','TEST','paper-race@example.test','ACTIVE'
);
insert into public.candidate_app_sessions(
  id,account_id,environment,selected_candidate_id,status,refresh_token_hash,
  expires_at_utc,absolute_expires_at_utc
) values(
  'b5200000-0000-4000-8000-000000000007',
  'b5200000-0000-4000-8000-000000000006','TEST',
  'b5200000-0000-4000-8000-000000000001','ACTIVE',decode(repeat('52',32),'hex'),
  now()+interval '1 day',now()+interval '7 days'
);

with manifest as (
  select jsonb_build_object(
    'workflow_id','b5200000-0000-4000-8000-000000000008'::uuid,
    'workflow_generation',1,'immutable_submission_sha256',repeat('a',64),
    'pages',jsonb_build_array(jsonb_build_object(
      'page_key','HOURS_TIMESHEET','component_kind','HOURS_TIMESHEET'))
  ) value
)
insert into public.candidate_submission_workflows(
  id,environment,account_id,candidate_id,workflow_kind,scope,route,state,generation,
  contract_id,contract_week_id,anchor_timesheet_id,target_timesheet_id,week_ending_date,
  idempotency_key,immutable_submission_json,immutable_submission_sha256,
  paper_return_manifest_json,paper_return_manifest_sha256,renderer_contract_version
)
select
  'b5200000-0000-4000-8000-000000000008','TEST',
  'b5200000-0000-4000-8000-000000000006','b5200000-0000-4000-8000-000000000001',
  'CONTRACT_HOURS','WEEKLY','PAPER','AWAITING_PAPER_RETURN',1,
  'b5200000-0000-4000-8000-000000000003','b5200000-0000-4000-8000-000000000005',
  'b5200000-0000-4000-8000-000000000004','b5200000-0000-4000-8000-000000000004',
  '2026-08-16','paper-release-race-create',
  jsonb_build_object('official_presentation',jsonb_build_object(
    'renderer_contract_version','CANDIDATE_REVIEW_DOCUMENTS_V1',
    'branding',jsonb_build_object('branding_contract_sha256',repeat('c',64)))),
  decode(repeat('a',64),'hex'),manifest.value,
  private._candidate_sha256_jsonb_v1(manifest.value),'CANDIDATE_REVIEW_DOCUMENTS_V1'
from manifest;

insert into public.mail_outbox(
  id,type,"to",subject,attachments,status,created_at_utc,context_kind,context_id,
  scheduled_for_utc,next_attempt_at_utc,reference,payment_scope_json
)
select
  'b5200000-0000-4000-8000-000000000009','TIMESHEET_QR','paper-race@example.test',
  'Paper race pack','[]'::jsonb,'QUEUED',now(),'timesheets',
  'b5200000-0000-4000-8000-000000000004','infinity','infinity','paper-release-race',
  jsonb_build_object(
    'candidate_mail_authority','CANDIDATE_PAPER_V1',
    'candidate_workflow_id','b5200000-0000-4000-8000-000000000008'::uuid,
    'candidate_workflow_generation',1,
    'paper_return_manifest_sha256',encode(paper_return_manifest_sha256,'hex'),
    'candidate_paper_pack_ready',false,'mail_held_until_pdf_rendered',true,
    'mail_hold_reason','CANDIDATE_PAPER_PACK_PENDING',
    'qr_token_hash',encode(extensions.digest(
      convert_to('paper-race-token','UTF8'),'sha256'),'hex')
  )
from public.candidate_submission_workflows
where id='b5200000-0000-4000-8000-000000000008';

create function public._candidate_paper_release_race_probe_v1()
returns jsonb language plpgsql as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_manifest text;
  v_key text;
  v_result jsonb;
begin
  select * into v_workflow from public.candidate_submission_workflows
  where id='b5200000-0000-4000-8000-000000000008';
  v_manifest:=encode(v_workflow.paper_return_manifest_sha256,'hex');
  v_key:='candidate-app/test/'||v_workflow.id::text||'/1/paper-pack/'
    ||v_manifest||'-'||repeat('b',64)||'-'||repeat('c',64)
    ||'-CANDIDATE_REVIEW_DOCUMENTS_V1.pdf';
  v_result:=public.candidate_workflow_transition_atomic_v1(
    null,'TEST',v_workflow.id,'PAPER_PACK_RELEASE',1,
    jsonb_build_object(
      'service_paper_pack_release',true,
      'mail_outbox_id','b5200000-0000-4000-8000-000000000009'::uuid,
      'paper_return_manifest_sha256',v_manifest,
      'complete_pack_storage_key',v_key,'complete_pack_sha256',repeat('d',64),
      'complete_pack_byte_size',500,'complete_pack_page_count',1,
      'complete_pack_media_type','application/pdf','base_document_sha256',repeat('b',64),
      'branding_contract_sha256',repeat('c',64),
      'renderer_contract_version','CANDIDATE_REVIEW_DOCUMENTS_V1'
    ),'paper-release-race',now()
  );
  perform pg_sleep(1);
  return v_result;
end;
$function$;

create function public._candidate_paper_amend_race_probe_v1()
returns jsonb language sql as $function$
  select public.candidate_workflow_transition_atomic_v1(
    'b5200000-0000-4000-8000-000000000007','TEST',
    'b5200000-0000-4000-8000-000000000008','AMEND',1,
    jsonb_build_object('input_snapshot',jsonb_build_object('race',true)),
    'paper-amend-race',now()
  );
$function$;
commit;

do $run_release_amend_race$
declare
  v_connection text:='host=127.0.0.1 port='||inet_server_port()::text
    ||' dbname='||current_database()||' user='||current_user;
  v_release text;
  v_amend_conflict boolean:=false;
  v_snapshot text;
begin
  perform dblink_connect('candidate_paper_release',v_connection);
  perform dblink_connect('candidate_paper_amend',v_connection);
  perform dblink_exec('candidate_paper_amend','begin isolation level repeatable read');
  select result into v_snapshot
  from dblink('candidate_paper_amend',$query$
    select updated_at_utc::text
    from public.candidate_submission_workflows
    where id='b5200000-0000-4000-8000-000000000008'
  $query$) as snapshot_result(result text);
  if v_snapshot is null then raise exception 'PAPER amendment race snapshot was not established'; end if;
  perform dblink_send_query('candidate_paper_release',
    'select public._candidate_paper_release_race_probe_v1()::text');
  select result into v_release
  from dblink_get_result('candidate_paper_release') as release_result(result text);
  if not coalesce((v_release::jsonb->>'ok')::boolean,false) then
    raise exception 'Concurrent PAPER release did not succeed: %',v_release;
  end if;
  begin
    perform result
    from dblink('candidate_paper_amend',
      'select public._candidate_paper_amend_race_probe_v1()::text') as amend_result(result text);
  exception when others then
    v_amend_conflict:=sqlerrm like '%CANDIDATE_WORKFLOW_CONTEXT_CONFLICT%'
      or sqlerrm like '%could not serialize access%';
  end;
  if not v_amend_conflict then
    raise exception 'Concurrent PAPER amendment did not receive the controlled context conflict';
  end if;
  perform dblink_exec('candidate_paper_amend','rollback');
  perform dblink_disconnect('candidate_paper_release');
  perform dblink_disconnect('candidate_paper_amend');
end;
$run_release_amend_race$;

do $verify_and_retire_after_race$
declare
  v_result jsonb;
  v_claimed uuid[];
begin
  if (select generation from public.candidate_submission_workflows
      where id='b5200000-0000-4000-8000-000000000008')<>1
     or (select state from public.candidate_submission_workflows
         where id='b5200000-0000-4000-8000-000000000008')<>'AWAITING_PAPER_RETURN'
     or lower((select payment_scope_json->>'candidate_paper_pack_ready'
               from public.mail_outbox
               where id='b5200000-0000-4000-8000-000000000009'))<>'true' then
    raise exception 'Concurrent loser partially changed the PAPER workflow';
  end if;
  v_result:=public.candidate_workflow_transition_atomic_v1(
    'b5200000-0000-4000-8000-000000000007','TEST',
    'b5200000-0000-4000-8000-000000000008','AMEND',1,
    jsonb_build_object('input_snapshot',jsonb_build_object('retry',true)),
    'paper-amend-race-retry',now()
  );
  if (v_result->>'generation')::integer<>2 or v_result->>'state'<>'WORKER_DRAFT' then
    raise exception 'Post-conflict PAPER amendment retry did not succeed: %',v_result;
  end if;
  select coalesce(array_agg(claimed.id),'{}'::uuid[]) into v_claimed
  from public.email_outbox_claim_ready_batch(10,'paper-race-retired-claim',5) claimed;
  if v_claimed @> array['b5200000-0000-4000-8000-000000000009'::uuid] then
    raise exception 'Retired race-generation mail remained claimable';
  end if;
end;
$verify_and_retire_after_race$;

begin;
delete from public.candidate_notifications
where workflow_id='b5200000-0000-4000-8000-000000000008';
delete from public.audit_events
where object_type='candidate_submission_workflow'
  and object_id_text='b5200000-0000-4000-8000-000000000008';
delete from public.mail_outbox
where id='b5200000-0000-4000-8000-000000000009';
delete from public.candidate_submission_components
where workflow_id='b5200000-0000-4000-8000-000000000008';
delete from public.candidate_approval_requests
where workflow_id='b5200000-0000-4000-8000-000000000008';
delete from public.candidate_submission_workflows
where id='b5200000-0000-4000-8000-000000000008';
delete from public.candidate_app_sessions
where id='b5200000-0000-4000-8000-000000000007';
delete from public.candidate_app_accounts
where id='b5200000-0000-4000-8000-000000000006';
delete from public.timesheets_financials
where timesheet_id='b5200000-0000-4000-8000-000000000004';
delete from public.contract_weeks
where id='b5200000-0000-4000-8000-000000000005';
delete from public.timesheets
where timesheet_id='b5200000-0000-4000-8000-000000000004';
delete from public.contracts
where id='b5200000-0000-4000-8000-000000000003';
delete from public.clients
where id='b5200000-0000-4000-8000-000000000002';
delete from public.candidates
where id='b5200000-0000-4000-8000-000000000001';
drop function public._candidate_paper_amend_race_probe_v1();
drop function public._candidate_paper_release_race_probe_v1();
commit;
