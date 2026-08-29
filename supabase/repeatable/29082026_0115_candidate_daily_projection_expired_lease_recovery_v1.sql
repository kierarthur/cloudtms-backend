begin;

create or replace function public.candidate_daily_projection_claim_v1(
  p_internal_context jsonb,
  p_claim_request_id uuid,
  p_idempotency_key text,
  p_target text,
  p_claimant text,
  p_max_items integer default 50,
  p_lease_seconds integer default 120,
  p_correlation_id text default null
)
returns jsonb
language plpgsql
security definer
set search_path=''
as $function$
declare v_context jsonb; v_environment text; v_request_hash text;
  v_batch private.candidate_daily_batch_receipts%rowtype; v_batch_id uuid:=gen_random_uuid();
  v_items jsonb:='[]'::jsonb; v_row public.candidate_daily_sheet_projection_outbox%rowtype;
  v_link text; v_token text; v_expiry timestamptz; v_terminal jsonb;
begin
  v_context:=private._candidate_daily_context_v1(p_internal_context,'SIGNED_SYSTEM_SYNC',false);
  v_environment:=v_context->>'environment';
  if p_claim_request_id is null or p_idempotency_key !~ '^[A-Za-z0-9._~:+/-]{16,128}$'
     or p_target<>'MASTER_AVAILABILITY_SHEET' or length(btrim(p_claimant)) not between 8 and 128
     or p_max_items not between 1 and 100 or p_lease_seconds not between 30 and 600
     or p_correlation_id !~ '^[0-7][0-9A-HJKMNP-TV-Z]{25}$' then
    raise exception using errcode='22023',message='VALIDATION_FAILED';
  end if;
  v_request_hash:=private._candidate_daily_json_sha256_v1(jsonb_build_object(
    'operation','PROJECTION_CLAIM','claim_request_id',p_claim_request_id,'target',p_target,
    'claimant',p_claimant,'max_items',p_max_items,'lease_seconds',p_lease_seconds));
  insert into private.candidate_daily_batch_receipts(batch_receipt_id,environment,actor_class,operation_class,
    idempotency_key,request_hash,item_keys_json,item_count,state,claim_request_id,claim_target,claim_limit,
    correlation_id)
  values(v_batch_id,v_environment,'SIGNED_SYSTEM','PROJECTION_CLAIM',p_idempotency_key,v_request_hash,
    jsonb_build_array('claim:'||p_claim_request_id::text),1,'IN_PROGRESS',p_claim_request_id,p_target,p_max_items,
    p_correlation_id)
  on conflict(environment,actor_class,operation_class,idempotency_key) do nothing;
  select * into v_batch from private.candidate_daily_batch_receipts
    where environment=v_environment and actor_class='SIGNED_SYSTEM' and operation_class='PROJECTION_CLAIM'
      and idempotency_key=p_idempotency_key for update;
  if v_batch.request_hash<>v_request_hash or v_batch.claim_request_id<>p_claim_request_id then
    raise exception using errcode='23505',message='IDEMPOTENCY_KEY_REUSED';
  end if;
  if v_batch.state<>'IN_PROGRESS' then
    if exists(select 1 from jsonb_array_elements(coalesce(v_batch.claimed_items_json,'[]'::jsonb))i
      where (i->>'lease_expires_at')::timestamptz<=now()) then
      raise exception using errcode='55000',message='LEASE_EXPIRED_STATUS_REQUIRED';
    end if;
    return v_batch.terminal_response_body||jsonb_build_object('_idempotent_replay',true);
  end if;
  if exists(select 1 from public.candidate_daily_sheet_projection_outbox o
    where o.environment=v_environment and o.target=p_target and o.state='CLAIMED'
      and o.lease_owner=p_claimant and o.lease_expires_at_utc>now()) then
    raise exception using errcode='55000',message='LEASE_CONFLICT';
  end if;

  -- A completed claim receipt remains immutable: replaying its old key above still
  -- requires a status check. A genuinely new claim may recover only leases whose
  -- database deadline has passed. The old token is cleared before a later claim,
  -- so a delayed completion can never acknowledge or overwrite the retry.
  for v_row in
    with expired as (
      select candidate_daily_sheet_projection_outbox.outbox_id
      from public.candidate_daily_sheet_projection_outbox
      where environment=v_environment and target=p_target and state='CLAIMED'
        and lease_expires_at_utc<=now()
      order by lease_expires_at_utc,outbox_id
      for update skip locked
      limit p_max_items
    )
    update public.candidate_daily_sheet_projection_outbox o
    set state=case when o.delivery_attempt_count+1>=12 then 'TERMINAL' else 'RETRY' end,
      delivery_attempt_count=o.delivery_attempt_count+1,
      next_available_at_utc=now()+least(
        interval '24 hours',
        make_interval(secs=>power(2,least(o.delivery_attempt_count+1,16))::integer)
      ),
      safe_error_code='LEASE_EXPIRED',
      completed_at_utc=case when o.delivery_attempt_count+1>=12 then now() else null end,
      lease_owner=null,lease_token=null,lease_expires_at_utc=null,updated_at_utc=now()
    from expired
    where o.outbox_id=expired.outbox_id
    returning o.*
  loop
    perform private._candidate_daily_refresh_sync_state_v1(
      v_environment,v_row.candidate_id,v_row.target,now());
  end loop;

  v_expiry:=now()+make_interval(secs=>p_lease_seconds);
  for v_row in
    select o.* from public.candidate_daily_sheet_projection_outbox o
    where o.environment=v_environment and o.target=p_target
      and o.state in ('PENDING','RETRY') and o.next_available_at_utc<=now()
    order by o.created_at_utc,o.outbox_id
    for update skip locked limit p_max_items
  loop
    select l.identifier_hmac into v_link from private.candidate_daily_source_links l
      where l.environment=v_environment and l.candidate_id=v_row.candidate_id
        and l.source_system='GOOGLE_CREDENTIALLY_PUBLIC_ID' and l.state='PRIMARY'
        and l.valid_from_utc<=now() and (l.valid_to_utc is null or l.valid_to_utc>now())
      order by l.hmac_key_version desc limit 1;
    if v_link is null then continue; end if;
    v_token:=replace(gen_random_uuid()::text,'-','')||replace(gen_random_uuid()::text,'-','');
    update public.candidate_daily_sheet_projection_outbox set state='CLAIMED',lease_owner=p_claimant,
      lease_token=v_token,lease_expires_at_utc=v_expiry,updated_at_utc=now() where outbox_id=v_row.outbox_id;
    v_items:=v_items||jsonb_build_array(jsonb_build_object('outbox_id',v_row.outbox_id,
      'lease_token',v_token,'lease_expires_at',v_expiry,'candidate_source_hmac',v_link,
      'date',v_row.availability_date,'availability_version',v_row.availability_version,
      'availability',private._candidate_daily_legacy_value_v1(v_row.preference)));
  end loop;
  v_terminal:=jsonb_build_object('claim_request_id',p_claim_request_id,'batch_receipt_id',v_batch.batch_receipt_id,
    'lease_set_expires_at',v_expiry,'items',v_items);
  update private.candidate_daily_batch_receipts set state='COMPLETED',lease_owner=p_claimant,
    lease_expires_at_utc=v_expiry,claimed_items_json=v_items,terminal_http_status=200,
    terminal_response_body=v_terminal,terminal_response_sha256=private._candidate_daily_json_sha256_v1(v_terminal),
    completed_at_utc=now(),updated_at_utc=now() where batch_receipt_id=v_batch.batch_receipt_id;
  return v_terminal;
end;
$function$;

revoke all on function public.candidate_daily_projection_claim_v1(
  jsonb,uuid,text,text,text,integer,integer,text
) from public;

do $grants$
declare v_function regprocedure :=
  'public.candidate_daily_projection_claim_v1(jsonb,uuid,text,text,text,integer,integer,text)'::regprocedure;
begin
  if exists(select 1 from pg_roles where rolname='anon') then
    execute format('revoke all on function %s from anon',v_function);
  end if;
  if exists(select 1 from pg_roles where rolname='authenticated') then
    execute format('revoke all on function %s from authenticated',v_function);
  end if;
  if exists(select 1 from pg_roles where rolname='service_role') then
    execute format('grant execute on function %s to service_role',v_function);
  end if;
end;
$grants$;

commit;
