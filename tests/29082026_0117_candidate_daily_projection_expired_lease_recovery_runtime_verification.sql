\set ON_ERROR_STOP on

begin;

do $test$
declare
  v_candidate uuid := '00000000-0000-4000-8000-00000000f901';
  v_command uuid := '00000000-0000-4000-8000-00000000f902';
  v_retry_outbox uuid := '00000000-0000-4000-8000-00000000f903';
  v_terminal_outbox uuid := '00000000-0000-4000-8000-00000000f904';
  v_active_outbox uuid := '00000000-0000-4000-8000-00000000f905';
  v_source_hmac text := repeat('9',64);
  v_system jsonb := jsonb_build_object(
    'policy','SIGNED_SYSTEM_SYNC','environment','TEST','system_auth_verified',true,
    'nonce_consumed',true,'environment_trusted',true,'stable_operation_identity',true,
    'approved_source_mapping',true,'source_scope_ready',true,'authority_mode_compatible',true,
    'transition_ready',true
  );
  v_claim jsonb;
  v_item jsonb;
  v_result jsonb;
  v_new_token text;
begin
  insert into public.candidates(id,email,display_name,first_name,last_name,active,key_norm)
  values(v_candidate,'projection-lease-runtime@example.invalid','Projection Lease Runtime',
    'Projection','Lease Runtime',true,'CID1-PROJECTIONLEASETEST');
  insert into private.candidate_daily_authority_scopes(environment,candidate_id,authority_mode)
  values('TEST',v_candidate,'GOOGLE_PRIMARY');
  insert into private.candidate_daily_entitlements(environment,candidate_id,enabled,reason,evidence_sha256)
  values('TEST',v_candidate,false,'Projection lease runtime fixture',repeat('8',64));
  insert into private.candidate_daily_source_links(environment,candidate_id,source_system,
    canonicalization_version,link_group_id,identifier_hmac,hmac_key_version,state,evidence_sha256)
  values('TEST',v_candidate,'GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',
    '00000000-0000-4000-8000-00000000f906',v_source_hmac,1,'PRIMARY',repeat('7',64));
  insert into public.candidate_daily_command_receipts(command_id,environment,candidate_id,actor_class,
    command_class,idempotency_key,request_sha256,canonical_version_before,canonical_version_after,state,
    terminal_http_status,terminal_body_json,terminal_body_sha256,correlation_id,completed_at_utc)
  values(v_command,'TEST',v_candidate,'CANDIDATE','AVAILABILITY_APPLY',
    'projection-lease-runtime-command-0001',repeat('6',64),0,3,'COMPLETED',200,
    '{}'::jsonb,repeat('5',64),'01K2ABCDEF0123456789ABCDE1',now());

  insert into public.candidate_daily_sheet_projection_outbox(outbox_id,environment,candidate_id,
    availability_date,availability_version,preference,command_id,state,delivery_attempt_count,
    lease_owner,lease_token,lease_expires_at_utc,correlation_id)
  values
    (v_retry_outbox,'TEST',v_candidate,date '2026-09-01',1,'LONG_DAY',v_command,'CLAIMED',0,
      'expired-runtime-owner','expired-old-token-0001',now()-interval '1 second','01K2ABCDEF0123456789ABCDE1'),
    (v_terminal_outbox,'TEST',v_candidate,date '2026-09-02',2,'NIGHT',v_command,'CLAIMED',11,
      'expired-terminal-owner','expired-old-token-0002',now()-interval '1 second','01K2ABCDEF0123456789ABCDE1'),
    (v_active_outbox,'TEST',v_candidate,date '2026-09-03',3,'LONG_DAY_OR_NIGHT',v_command,'CLAIMED',0,
      'availability-runtime-active','active-runtime-token-0003',now()+interval '10 minutes','01K2ABCDEF0123456789ABCDE1');

  v_claim:=public.candidate_daily_projection_claim_v1(
    v_system,'00000000-0000-4000-8000-00000000f907','projection-lease-runtime-claim-0001',
    'MASTER_AVAILABILITY_SHEET','availability-runtime-recovery',20,600,
    '01K2ABCDEF0123456789ABCDE2');
  if jsonb_array_length(v_claim->'items')<>0 then
    raise exception 'PROJECTION_LEASE_RUNTIME: recovered rows ignored backoff';
  end if;
  if not exists(select 1 from public.candidate_daily_sheet_projection_outbox
      where outbox_id=v_retry_outbox and state='RETRY' and delivery_attempt_count=1
        and lease_owner is null and lease_token is null and lease_expires_at_utc is null
        and safe_error_code='LEASE_EXPIRED' and next_available_at_utc>now()) then
    raise exception 'PROJECTION_LEASE_RUNTIME: expired row was not safely retried';
  end if;
  if not exists(select 1 from public.candidate_daily_sheet_projection_outbox
      where outbox_id=v_terminal_outbox and state='TERMINAL' and delivery_attempt_count=12
        and completed_at_utc is not null and safe_error_code='LEASE_EXPIRED') then
    raise exception 'PROJECTION_LEASE_RUNTIME: exhausted row was not terminal';
  end if;
  if not exists(select 1 from public.candidate_daily_sheet_projection_outbox
      where outbox_id=v_active_outbox and state='CLAIMED'
        and lease_token='active-runtime-token-0003' and delivery_attempt_count=0) then
    raise exception 'PROJECTION_LEASE_RUNTIME: active lease was changed';
  end if;

  update public.candidate_daily_sheet_projection_outbox
  set next_available_at_utc=now()-interval '1 second'
  where outbox_id=v_retry_outbox;
  v_claim:=public.candidate_daily_projection_claim_v1(
    v_system,'00000000-0000-4000-8000-00000000f908','projection-lease-runtime-claim-0002',
    'MASTER_AVAILABILITY_SHEET','availability-runtime-retry',20,600,
    '01K2ABCDEF0123456789ABCDE3');
  v_item:=v_claim#>'{items,0}';
  v_new_token:=v_item->>'lease_token';
  if (v_item->>'outbox_id')::uuid<>v_retry_outbox or length(v_new_token)<16
     or (v_item->>'lease_expires_at')::timestamptz<now()+interval '9 minutes 50 seconds' then
    raise exception 'PROJECTION_LEASE_RUNTIME: recovered retry did not receive a fresh full lease';
  end if;

  v_result:=public.candidate_daily_projection_complete_atomic_v1(
    v_system,'00000000-0000-4000-8000-00000000f909','projection-lease-runtime-complete-old',
    jsonb_build_array(jsonb_build_object('outbox_id',v_retry_outbox,
      'lease_token','expired-old-token-0001','outcome','DELIVERED',
      'observed_sheet_revision','stale-runtime-revision')),
    '01K2ABCDEF0123456789ABCDE4');
  if v_result#>>'{outcomes,0,accepted}'<>'false'
     or v_result#>>'{outcomes,0,state}'<>'LEASE_CONFLICT' then
    raise exception 'PROJECTION_LEASE_RUNTIME: delayed old completion was accepted';
  end if;
  if not exists(select 1 from public.candidate_daily_sheet_projection_outbox
      where outbox_id=v_retry_outbox and state='CLAIMED' and lease_token=v_new_token) then
    raise exception 'PROJECTION_LEASE_RUNTIME: delayed old completion changed the new lease';
  end if;

  v_result:=public.candidate_daily_projection_complete_atomic_v1(
    v_system,'00000000-0000-4000-8000-00000000f910','projection-lease-runtime-complete-new',
    jsonb_build_array(jsonb_build_object('outbox_id',v_retry_outbox,
      'lease_token',v_new_token,'outcome','DELIVERED',
      'observed_sheet_revision','current-runtime-revision')),
    '01K2ABCDEF0123456789ABCDE5');
  if v_result#>>'{outcomes,0,accepted}'<>'true'
     or v_result#>>'{outcomes,0,state}'<>'DELIVERED' then
    raise exception 'PROJECTION_LEASE_RUNTIME: current completion was not accepted';
  end if;
end;
$test$;

rollback;

select 'candidate daily projection expired lease recovery runtime verification passed' as result;
