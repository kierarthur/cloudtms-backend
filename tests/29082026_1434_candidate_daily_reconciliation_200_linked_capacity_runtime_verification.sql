\set ON_ERROR_STOP on

begin;

create temporary table capacity_candidate_fixtures (
  sequence_number integer primary key,
  candidate_id uuid not null,
  candidate_source_hmac text not null,
  source_command_id uuid not null
) on commit drop;

with fixture_rows as (
  select n as sequence_number,gen_random_uuid() as candidate_id,gen_random_uuid() as source_command_id
  from generate_series(1,200)n
)
insert into capacity_candidate_fixtures(sequence_number,candidate_id,candidate_source_hmac,source_command_id)
select sequence_number,candidate_id,
  encode(extensions.digest('capacity-source-'||candidate_id::text,'sha256'),'hex'),source_command_id
from fixture_rows;

insert into public.candidates(id,email,display_name,first_name,last_name,active,key_norm)
select candidate_id,'capacity-'||candidate_id||'@example.invalid',
  'Capacity Candidate '||sequence_number,'Capacity','Candidate '||sequence_number,true,
  'CID1-'||upper(replace(candidate_id::text,'-',''))
from capacity_candidate_fixtures;

insert into private.candidate_daily_authority_scopes(environment,candidate_id,authority_mode,canonical_version)
select 'TEST',candidate_id,'GOOGLE_PRIMARY',1
from capacity_candidate_fixtures;

insert into private.candidate_daily_source_links(environment,candidate_id,source_system,
  canonicalization_version,link_group_id,identifier_hmac,hmac_key_version,state,evidence_sha256)
select 'TEST',candidate_id,'GOOGLE_CREDENTIALLY_PUBLIC_ID','SOURCE_IDENTITY_V1',gen_random_uuid(),
  candidate_source_hmac,1,'PRIMARY',
  encode(extensions.digest('capacity-link-evidence-'||candidate_id::text,'sha256'),'hex')
from capacity_candidate_fixtures;

insert into public.candidate_daily_command_receipts(command_id,environment,candidate_id,actor_class,
  command_class,idempotency_key,request_sha256,canonical_version_before,canonical_version_after,state,
  terminal_http_status,terminal_body_json,terminal_body_sha256,correlation_id,completed_at_utc)
select source_command_id,'TEST',candidate_id,'SIGNED_SYSTEM','CAPACITY_FIXTURE',
  'capacity-command-'||lpad(sequence_number::text,3,'0')||'-20260829',
  encode(extensions.digest('capacity-command-request-'||candidate_id::text,'sha256'),'hex'),0,1,'COMPLETED',200,
  jsonb_build_object('fixture','capacity'),
  private._candidate_daily_json_sha256_v1(jsonb_build_object('fixture','capacity')),
  '01M00000000000000000000034',now()
from capacity_candidate_fixtures;

insert into public.candidate_daily_availability_days(environment,candidate_id,availability_date,preference,
  availability_version,source_class,source_command_id,changed_by_class,row_hash)
select 'TEST',fixture.candidate_id,date '2026-08-29'+day_number,
  case day_number%5
    when 0 then 'PENDING'
    when 1 then 'NOT_AVAILABLE'
    when 2 then 'LONG_DAY'
    when 3 then 'NIGHT'
    else 'LONG_DAY_OR_NIGHT'
  end,
  1,'SIGNED_SYSTEM',fixture.source_command_id,'SIGNED_SYSTEM',
  encode(extensions.digest('capacity-availability-'||fixture.candidate_id::text||'-'||day_number,'sha256'),'hex')
from capacity_candidate_fixtures fixture
cross join generate_series(0,13)day_number;

do $test$
declare
  v_system jsonb:=jsonb_build_object(
    'policy','SIGNED_SYSTEM_SYNC','environment','TEST','system_auth_verified',true,
    'nonce_consumed',true,'environment_trusted',true,'stable_operation_identity',true,
    'approved_source_mapping',true,'source_scope_ready',true,'authority_mode_compatible',true,
    'transition_ready',true,'route_operation','RECONCILIATION'
  );
  v_run_tag text:=replace(gen_random_uuid()::text,'-','');
  v_batch_number integer;
  v_batch_request_id uuid;
  v_idempotency_key text;
  v_observations jsonb;
  v_result jsonb;
  v_probe_total integer:=0;
  v_full_total integer:=0;
  v_first_probe_batch_id uuid;
  v_first_probe_idempotency text;
  v_first_probe_observations jsonb;
  v_first_probe_result jsonb;
  v_first_full_batch_id uuid;
  v_first_full_idempotency text;
  v_first_full_observations jsonb;
  v_first_full_result jsonb;
  v_replay jsonb;
begin
  for v_batch_number in 0..3 loop
    select jsonb_agg(jsonb_build_object(
      'candidate_source_hmac',candidate_source_hmac,
      'probe_only',true,
      'source_revision','availability-window.capacity-v1',
      'source_event_time','2026-08-29T14:34:00.000Z',
      'source_hash',encode(extensions.digest(
        'capacity-probe-hash-'||v_run_tag||'-'||sequence_number,'sha256'),'hex'),
      'item_key','capacity-probe-'||lpad(sequence_number::text,3,'0')
    ) order by sequence_number)
    into v_observations
    from capacity_candidate_fixtures
    where sequence_number between v_batch_number*50+1 and (v_batch_number+1)*50;

    v_batch_request_id:=gen_random_uuid();
    v_idempotency_key:='capacity-probe-'||v_run_tag||'-'||lpad(v_batch_number::text,2,'0');
    v_result:=public.candidate_daily_reconciliation_apply_atomic_v1(
      v_system,v_batch_request_id,v_idempotency_key,v_observations,'01M00000000000000000000034'
    );
    if jsonb_array_length(v_result->'outcomes')<>50
       or (select count(*) from jsonb_array_elements(v_result->'outcomes')outcome
            where outcome->>'classification'='LINKED')<>50 then
      raise exception 'Capacity probe batch % did not classify all 50 linked Candidates: %',
        v_batch_number,v_result;
    end if;
    v_probe_total:=v_probe_total+jsonb_array_length(v_result->'outcomes');
    if v_batch_number=0 then
      v_first_probe_batch_id:=v_batch_request_id;
      v_first_probe_idempotency:=v_idempotency_key;
      v_first_probe_observations:=v_observations;
      v_first_probe_result:=v_result;
    end if;
  end loop;

  if v_probe_total<>200 then
    raise exception 'Capacity proof did not probe exactly 200 linked Candidates: %',v_probe_total;
  end if;

  for v_batch_number in 0..55 loop
    with expanded as (
      select fixture.sequence_number,fixture.candidate_source_hmac,day_number,
        ((fixture.sequence_number-1)*14)+day_number+1 as item_number,
        case day_number%5
          when 0 then ''
          when 1 then 'N/A'
          when 2 then 'LD'
          when 3 then 'N'
          else 'LD/N'
        end as observed_value
      from capacity_candidate_fixtures fixture
      cross join generate_series(0,13)day_number
    )
    select jsonb_agg(jsonb_build_object(
      'candidate_source_hmac',candidate_source_hmac,
      'date',(date '2026-08-29'+day_number)::text,
      'observed_value',observed_value,
      'observed_sheet_revision','capacity-sheet-revision-1',
      'source_event_id','capacity-event-'||lpad(item_number::text,4,'0'),
      'source_revision','availability-window.capacity-v1',
      'source_event_time','2026-08-29T14:34:00.000Z',
      'source_hash',encode(extensions.digest(
        'capacity-full-hash-'||v_run_tag||'-'||item_number,'sha256'),'hex'),
      'item_key','capacity-full-'||lpad(item_number::text,4,'0')
    ) order by item_number)
    into v_observations
    from expanded
    where item_number between v_batch_number*50+1 and (v_batch_number+1)*50;

    v_batch_request_id:=gen_random_uuid();
    v_idempotency_key:='capacity-full-'||v_run_tag||'-'||lpad(v_batch_number::text,2,'0');
    v_result:=public.candidate_daily_reconciliation_apply_atomic_v1(
      v_system,v_batch_request_id,v_idempotency_key,v_observations,'01M00000000000000000000034'
    );
    if jsonb_array_length(v_result->'outcomes')<>50
       or (select count(*) from jsonb_array_elements(v_result->'outcomes')outcome
            where outcome->>'classification'='MATCH')<>50 then
      raise exception 'Capacity full-window batch % did not reconcile all 50 observations: %',
        v_batch_number,v_result;
    end if;
    v_full_total:=v_full_total+jsonb_array_length(v_result->'outcomes');
    if v_batch_number=0 then
      v_first_full_batch_id:=v_batch_request_id;
      v_first_full_idempotency:=v_idempotency_key;
      v_first_full_observations:=v_observations;
      v_first_full_result:=v_result;
    end if;
  end loop;

  if v_full_total<>2800 then
    raise exception 'Capacity proof did not reconcile exactly 2,800 Candidate/date observations: %',v_full_total;
  end if;

  v_replay:=public.candidate_daily_reconciliation_apply_atomic_v1(
    v_system,v_first_probe_batch_id,v_first_probe_idempotency,v_first_probe_observations,
    '01M00000000000000000000034'
  );
  if v_replay->>'_idempotent_replay'<>'true'
     or v_replay->'outcomes'<>v_first_probe_result->'outcomes' then
    raise exception 'Capacity identity-probe replay was not stable: %',v_replay;
  end if;

  v_replay:=public.candidate_daily_reconciliation_apply_atomic_v1(
    v_system,v_first_full_batch_id,v_first_full_idempotency,v_first_full_observations,
    '01M00000000000000000000034'
  );
  if v_replay->>'_idempotent_replay'<>'true'
     or v_replay->'outcomes'<>v_first_full_result->'outcomes' then
    raise exception 'Capacity full-window replay was not stable: %',v_replay;
  end if;

  if (select count(*) from private.candidate_daily_batch_receipts
      where environment='TEST' and idempotency_key like 'capacity-%-'||v_run_tag||'-%')<>60 then
    raise exception 'Capacity proof did not create exactly 60 accepted batch receipts';
  end if;
  if (select coalesce(sum(item_count),0) from private.candidate_daily_batch_receipts
      where environment='TEST' and idempotency_key like 'capacity-%-'||v_run_tag||'-%')<>3000 then
    raise exception 'Capacity proof batch receipts did not cover exactly 200 probes and 2,800 observations';
  end if;
  if (select count(*) from private.candidate_daily_sync_state sync_state
      join capacity_candidate_fixtures fixture on fixture.candidate_id=sync_state.candidate_id
      where sync_state.environment='TEST' and sync_state.target='MASTER_AVAILABILITY_SHEET'
        and sync_state.observed_source_revision='capacity-sheet-revision-1')<>200 then
    raise exception 'Capacity proof did not reconcile all 200 Candidate sync states';
  end if;
  if exists(
    select 1
    from public.candidate_daily_sheet_projection_outbox outbox
    join capacity_candidate_fixtures fixture on fixture.candidate_id=outbox.candidate_id
  ) then
    raise exception 'Matching capacity observations unexpectedly queued projection repairs';
  end if;
end;
$test$;

rollback;
