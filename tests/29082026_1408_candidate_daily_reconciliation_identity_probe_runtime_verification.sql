\set ON_ERROR_STOP on

begin;

do $test$
declare
  v_candidate_a uuid:=gen_random_uuid();
  v_candidate_b uuid:=gen_random_uuid();
  v_source_a text:=encode(extensions.digest(gen_random_uuid()::text,'sha256'),'hex');
  v_source_b text:=encode(extensions.digest(gen_random_uuid()::text,'sha256'),'hex');
  v_system jsonb:=jsonb_build_object(
    'policy','SIGNED_SYSTEM_SYNC','environment','TEST','system_auth_verified',true,
    'nonce_consumed',true,'environment_trusted',true,'stable_operation_identity',true,
    'approved_source_mapping',true,'source_scope_ready',true,'authority_mode_compatible',true,
    'transition_ready',true,'route_operation','RECONCILIATION'
  );
  v_batch_id uuid:=gen_random_uuid();
  v_observations jsonb;
  v_result jsonb;
  v_replay jsonb;
  v_conflict_caught boolean:=false;
begin
  insert into public.candidates(id,email,display_name,first_name,last_name,active,key_norm)
  values
    (v_candidate_a,'probe-'||v_candidate_a||'@example.invalid','Probe Candidate A','Probe','Candidate A',true,
      'CID1-'||upper(replace(v_candidate_a::text,'-',''))),
    (v_candidate_b,'probe-'||v_candidate_b||'@example.invalid','Probe Candidate B','Probe','Candidate B',true,
      'CID1-'||upper(replace(v_candidate_b::text,'-','')));

  insert into private.candidate_daily_authority_scopes(environment,candidate_id,authority_mode)
  values ('TEST',v_candidate_a,'GOOGLE_PRIMARY'),('TEST',v_candidate_b,'GOOGLE_PRIMARY');
  insert into private.candidate_daily_source_links(environment,candidate_id,source_system,
    canonicalization_version,link_group_id,identifier_hmac,hmac_key_version,state,evidence_sha256)
  values
    ('TEST',v_candidate_a,'GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',gen_random_uuid(),
      v_source_a,1,'PRIMARY',encode(extensions.digest('probe-a-'||v_candidate_a,'sha256'),'hex')),
    ('TEST',v_candidate_b,'GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',gen_random_uuid(),
      v_source_b,1,'PRIMARY',encode(extensions.digest('probe-b-'||v_candidate_b,'sha256'),'hex'));

  select jsonb_agg(jsonb_build_object(
    'candidate_source_hmac',case n when 1 then v_source_a when 2 then v_source_b
      else encode(extensions.digest('unlinked-probe-'||v_batch_id||'-'||n,'sha256'),'hex') end,
    'probe_only',true,
    'source_revision','availability-window.runtime-v1',
    'source_event_time','2026-08-29T14:08:00.000Z',
    'source_hash',encode(extensions.digest('probe-source-'||v_batch_id||'-'||n,'sha256'),'hex'),
    'item_key','identity-probe-'||lpad(n::text,4,'0')
  ) order by n) into v_observations from generate_series(1,50)n;

  v_result:=public.candidate_daily_reconciliation_apply_atomic_v1(
    v_system,v_batch_id,'identity-probe-runtime-'||replace(v_batch_id::text,'-',''),
    v_observations,'01M00000000000000000000008'
  );
  if jsonb_array_length(v_result->'outcomes')<>50
     or (select count(*) from jsonb_array_elements(v_result->'outcomes')o
          where o->>'classification'='LINKED')<>2
     or (select count(*) from jsonb_array_elements(v_result->'outcomes')o
          where o->>'classification'='NOT_ENROLLED')<>48 then
    raise exception 'Identity probe did not classify exactly 2 linked and 48 unlinked Candidates: %',v_result;
  end if;
  if exists(select 1 from public.candidate_daily_availability_days
      where environment='TEST' and candidate_id in (v_candidate_a,v_candidate_b))
     or exists(select 1 from public.candidate_daily_sheet_projection_outbox
      where environment='TEST' and candidate_id in (v_candidate_a,v_candidate_b))
     or exists(select 1 from private.candidate_daily_sync_state
      where environment='TEST' and candidate_id in (v_candidate_a,v_candidate_b))
     or exists(select 1 from public.candidate_daily_command_receipts
      where environment='TEST' and candidate_id in (v_candidate_a,v_candidate_b)) then
    raise exception 'Read-only identity probe changed Candidate availability/projection/sync/command state';
  end if;

  v_replay:=public.candidate_daily_reconciliation_apply_atomic_v1(
    v_system,v_batch_id,'identity-probe-runtime-'||replace(v_batch_id::text,'-',''),
    v_observations,'01M00000000000000000000008'
  );
  if v_replay->>'_idempotent_replay'<>'true'
     or v_replay->'outcomes'<>v_result->'outcomes' then
    raise exception 'Exact identity-probe replay changed its terminal receipt: %',v_replay;
  end if;

  begin
    perform public.candidate_daily_reconciliation_apply_atomic_v1(
      v_system,v_batch_id,'identity-probe-runtime-'||replace(v_batch_id::text,'-',''),
      jsonb_set(v_observations,'{0,source_revision}','"changed-revision"'::jsonb),
      '01M00000000000000000000008'
    );
  exception when unique_violation then
    v_conflict_caught:=sqlerrm='IDEMPOTENCY_KEY_REUSED';
  end;
  if not v_conflict_caught then
    raise exception 'Changed identity-probe payload reused an accepted idempotency key';
  end if;
end;
$test$;

rollback;
