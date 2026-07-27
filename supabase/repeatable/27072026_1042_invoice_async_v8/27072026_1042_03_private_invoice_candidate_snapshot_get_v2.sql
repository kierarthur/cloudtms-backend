create or replace function private._invoice_candidate_snapshot_get_v2(
  p_action text,
  p_now_utc timestamptz default now()
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','vault','pg_temp'
as $function$
declare
  v_action text := upper(btrim(coalesce(p_action, '')));
  v_now timestamptz := date_trunc(
    'milliseconds',
    coalesce(p_now_utc, statement_timestamp())
  );
  v_expires timestamptz;
  v_revision bigint;
  v_key_id text;
  v_secret text;
  v_payload jsonb;
  v_payload_b64 text;
  v_signing_input text;
  v_signature text;
begin
  if v_action not in ('GENERATE', 'ISSUE') then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_ACTION_INVALID';
  end if;

  -- Linearise snapshot issuance with candidate-visible revision changes.
  perform pg_advisory_xact_lock_shared(
    hashtextextended('invoice-candidate-snapshot:' || v_action, 0)
  );

  if v_action = 'GENERATE' then
    select coalesce(pg_sequence_last_value(
      'private.invoice_generate_candidate_change_seq'::regclass
    ), 0)
    into v_revision;
  else
    select coalesce(pg_sequence_last_value(
      'private.invoice_issue_candidate_change_seq'::regclass
    ), 0)
    into v_revision;
  end if;

  select
    k.key_id,
    s.decrypted_secret
  into
    v_key_id,
    v_secret
  from private.invoice_async_snapshot_hmac_keys k
  join vault.decrypted_secrets s on s.id = k.vault_secret_id
  where k.is_current
    and k.active_from_utc <= v_now
    and (k.active_to_utc is null or k.active_to_utc > v_now)
  order by k.active_from_utc desc, k.key_id
  limit 1;

  if v_key_id is null or v_secret is null then
    raise exception using
      errcode = '55000',
      message = 'BATCH_SNAPSHOT_SIGNING_KEY_UNAVAILABLE';
  end if;

  v_expires := v_now + interval '30 minutes';
  v_payload := jsonb_build_object(
    'action', v_action,
    'at_utc', to_char(
      v_now at time zone 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
    ),
    'contract_version', 'INVOICE_BATCH_SNAPSHOT_V2',
    'expires_at_utc', to_char(
      v_expires at time zone 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
    ),
    'revision', v_revision::text
  );
  v_payload_b64 := rtrim(
    replace(replace(replace(replace(
      encode(convert_to(v_payload::text, 'UTF8'), 'base64'),
      E'\n',
      ''
    ), E'\r', ''), '+', '-'), '/', '_'),
    '='
  );
  v_signing_input := 'v2.' || v_key_id || '.' || v_payload_b64;
  v_signature := encode(
    extensions.hmac(
      convert_to(v_signing_input, 'UTF8'),
      convert_to(v_secret, 'UTF8'),
      'sha256'
    ),
    'hex'
  );

  return v_payload || jsonb_build_object(
    'token', v_signing_input || '.' || v_signature
  );
end;
$function$;

alter function private._invoice_candidate_snapshot_get_v2(text,timestamptz)
  owner to postgres;
revoke all on function private._invoice_candidate_snapshot_get_v2(text,timestamptz)
  from public, anon, authenticated;
grant execute on function private._invoice_candidate_snapshot_get_v2(text,timestamptz)
  to service_role;
