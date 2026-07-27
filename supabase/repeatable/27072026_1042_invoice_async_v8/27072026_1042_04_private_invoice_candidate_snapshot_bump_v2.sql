create or replace function private._invoice_candidate_snapshot_bump_v2(
  p_generate boolean,
  p_issue boolean,
  p_reason text,
  p_source text,
  p_now_utc timestamptz default now()
) returns jsonb
language plpgsql
volatile
security definer
set search_path to 'public','private','extensions','pg_temp'
as $function$
declare
  v_generate_revision bigint;
  v_issue_revision bigint;
begin
  if coalesce(p_generate, false) then
    v_generate_revision :=
      nextval('private.invoice_generate_candidate_change_seq'::regclass);
  else
    v_generate_revision := coalesce(pg_sequence_last_value(
      'private.invoice_generate_candidate_change_seq'::regclass
    ), 0);
  end if;

  if coalesce(p_issue, false) then
    v_issue_revision :=
      nextval('private.invoice_issue_candidate_change_seq'::regclass);
  else
    v_issue_revision := coalesce(pg_sequence_last_value(
      'private.invoice_issue_candidate_change_seq'::regclass
    ), 0);
  end if;

  return jsonb_build_object(
    'generate_revision', v_generate_revision::text,
    'issue_revision', v_issue_revision::text,
    'reason', left(coalesce(nullif(btrim(p_reason), ''), 'CANDIDATE_VISIBLE_CHANGE'), 120),
    'source', left(coalesce(nullif(btrim(p_source), ''), 'UNKNOWN'), 120),
    'at_utc', to_char(
      coalesce(p_now_utc, now()) at time zone 'UTC',
      'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
    )
  );
end;
$function$;

alter function private._invoice_candidate_snapshot_bump_v2(
  boolean,boolean,text,text,timestamptz
) owner to postgres;
revoke all on function private._invoice_candidate_snapshot_bump_v2(
  boolean,boolean,text,text,timestamptz
) from public, anon, authenticated;
grant execute on function private._invoice_candidate_snapshot_bump_v2(
  boolean,boolean,text,text,timestamptz
) to service_role;
