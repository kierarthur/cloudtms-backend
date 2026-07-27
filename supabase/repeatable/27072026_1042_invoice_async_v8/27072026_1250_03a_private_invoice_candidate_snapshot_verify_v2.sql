create or replace function private._invoice_candidate_snapshot_verify_v2(
  p_action text,
  p_snapshot jsonb,
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
  v_at timestamptz;
  v_expires timestamptz;
  v_revision bigint;
  v_current_revision bigint;
  v_token text;
  v_parts text[];
  v_key_id text;
  v_secret text;
  v_payload jsonb;
  v_payload_b64 text;
  v_signing_input text;
  v_expected_signature text;
begin
  if v_action not in ('GENERATE', 'ISSUE') then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_ACTION_INVALID';
  end if;

  if jsonb_typeof(coalesce(p_snapshot, 'null'::jsonb)) is distinct from 'object'
     or exists (
       select 1
       from jsonb_object_keys(p_snapshot) key_name
       where key_name not in (
         'contract_version',
         'action',
         'at_utc',
         'revision',
         'expires_at_utc',
         'token'
       )
     )
     or coalesce(p_snapshot->>'contract_version', '') <>
        'INVOICE_BATCH_SNAPSHOT_V2'
     or upper(coalesce(p_snapshot->>'action', '')) <> v_action
     or coalesce(p_snapshot->>'revision', '') !~ '^[0-9]+$'
     or not pg_input_is_valid(
       coalesce(p_snapshot->>'at_utc', ''),
       'timestamp with time zone'
     )
     or not pg_input_is_valid(
       coalesce(p_snapshot->>'expires_at_utc', ''),
       'timestamp with time zone'
     ) then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_INVALID';
  end if;

  v_at := date_trunc(
    'milliseconds',
    (p_snapshot->>'at_utc')::timestamptz
  );
  v_expires := date_trunc(
    'milliseconds',
    (p_snapshot->>'expires_at_utc')::timestamptz
  );
  v_revision := (p_snapshot->>'revision')::bigint;
  v_token := coalesce(p_snapshot->>'token', '');
  v_parts := string_to_array(v_token, '.');

  if cardinality(v_parts) <> 4
     or v_parts[1] <> 'v2'
     or v_parts[2] !~ '^[a-z0-9][a-z0-9._-]{0,63}$'
     or v_parts[3] !~ '^[A-Za-z0-9_-]+$'
     or v_parts[4] !~ '^[0-9a-f]{64}$'
     or v_expires is distinct from v_at + interval '30 minutes' then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_INVALID';
  end if;

  if v_expires <= v_now then
    raise exception using
      errcode = '40001',
      message = 'BATCH_SNAPSHOT_EXPIRED';
  end if;

  v_key_id := v_parts[2];
  select s.decrypted_secret
  into v_secret
  from private.invoice_async_snapshot_hmac_keys k
  join vault.decrypted_secrets s on s.id = k.vault_secret_id
  where k.key_id = v_key_id
    and k.active_from_utc <= v_at
    and (k.active_to_utc is null or v_at < k.active_to_utc)
    and (
      k.is_current
      or (
        k.verify_until_utc is not null
        and v_now <= k.verify_until_utc
      )
    )
  limit 1;

  if v_secret is null then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_SIGNATURE_INVALID';
  end if;

  v_payload := jsonb_build_object(
    'action', v_action,
    'at_utc', to_char(
      v_at at time zone 'UTC',
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
  v_expected_signature := encode(
    extensions.hmac(
      convert_to(v_signing_input, 'UTF8'),
      convert_to(v_secret, 'UTF8'),
      'sha256'
    ),
    'hex'
  );

  if v_parts[3] is distinct from v_payload_b64
     or extensions.digest(convert_to(v_parts[4], 'UTF8'), 'sha256')
        is distinct from extensions.digest(
          convert_to(v_expected_signature, 'UTF8'),
          'sha256'
        ) then
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_SIGNATURE_INVALID';
  end if;

  perform pg_advisory_xact_lock_shared(
    hashtextextended('invoice-candidate-snapshot:' || v_action, 0)
  );

  if v_action = 'GENERATE' then
    select coalesce(pg_sequence_last_value(
      'private.invoice_generate_candidate_change_seq'::regclass
    ), 0)
    into v_current_revision;
  else
    select coalesce(pg_sequence_last_value(
      'private.invoice_issue_candidate_change_seq'::regclass
    ), 0)
    into v_current_revision;
  end if;

  if v_current_revision is distinct from v_revision then
    raise exception using
      errcode = '40001',
      message = 'BATCH_SNAPSHOT_CHANGED';
  end if;

  return v_payload || jsonb_build_object('token', v_token);
end;
$function$;

alter function private._invoice_candidate_snapshot_verify_v2(
  text,jsonb,timestamptz
) owner to postgres;
revoke all on function private._invoice_candidate_snapshot_verify_v2(
  text,jsonb,timestamptz
) from public, anon, authenticated;
grant execute on function private._invoice_candidate_snapshot_verify_v2(
  text,jsonb,timestamptz
) to service_role;
