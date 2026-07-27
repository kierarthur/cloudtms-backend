create or replace function private._invoice_candidate_snapshot_get_v2(
  p_action text,
  p_now_utc timestamptz default now()
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_action text := upper(btrim(coalesce(p_action, '')));
  v_now timestamptz := coalesce(p_now_utc, now());
  v_revision bigint;
begin
  if v_action = 'GENERATE' then
    select coalesce(pg_sequence_last_value(
      'private.invoice_generate_candidate_change_seq'::regclass
    ), 0)
    into v_revision;
  elsif v_action = 'ISSUE' then
    select coalesce(pg_sequence_last_value(
      'private.invoice_issue_candidate_change_seq'::regclass
    ), 0)
    into v_revision;
  else
    raise exception using
      errcode = '22023',
      message = 'BATCH_SNAPSHOT_ACTION_INVALID';
  end if;

  return jsonb_build_object(
    'at_utc', to_char(v_now at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
    'revision', v_revision::text,
    'expires_at_utc', to_char(
      (v_now + interval '30 minutes') at time zone 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
    )
  );
end;
$function$;

alter function private._invoice_candidate_snapshot_get_v2(text,timestamptz)
  owner to postgres;
revoke all on function private._invoice_candidate_snapshot_get_v2(text,timestamptz)
  from public, anon, authenticated;
grant execute on function private._invoice_candidate_snapshot_get_v2(text,timestamptz)
  to service_role;
