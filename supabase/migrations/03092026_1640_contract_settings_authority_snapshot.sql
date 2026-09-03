\set ON_ERROR_STOP on

begin;

-- Global Bank Holiday dates have two deliberately separate owners. bh_list
-- remains the manually maintained list; the official feed is stored here so a
-- refresh can never overwrite a date that an Office user added deliberately.
alter table public.settings_defaults
  add column if not exists bh_feed_list jsonb not null default '[]'::jsonb,
  add column if not exists bh_feed_last_checked_at timestamptz,
  add column if not exists bh_feed_last_success_at timestamptz,
  add column if not exists bh_feed_next_refresh_at timestamptz,
  add column if not exists bh_feed_claim_token uuid,
  add column if not exists bh_feed_claimed_at timestamptz,
  add column if not exists bh_feed_content_sha256 text,
  add column if not exists bh_feed_last_error_code text;

alter table public.settings_defaults
  drop constraint if exists settings_defaults_bh_feed_list_array_ck,
  add constraint settings_defaults_bh_feed_list_array_ck
    check (jsonb_typeof(bh_feed_list) = 'array'),
  drop constraint if exists settings_defaults_bh_feed_hash_ck,
  add constraint settings_defaults_bh_feed_hash_ck
    check (bh_feed_content_sha256 is null or bh_feed_content_sha256 ~ '^[0-9a-f]{64}$');

comment on column public.settings_defaults.bh_feed_list is
  'Official GOV.UK England and Wales Bank Holiday date strings. Resolver merges these with manual bh_list; neither source overwrites the other.';

-- A Contract week is still planned only while it has no real Timesheet. Its
-- authority snapshot may therefore be refreshed when its governing settings
-- change. Once linked to a Timesheet the week snapshot is retained unchanged.
alter table public.contract_weeks
  add column if not exists settings_authority_json jsonb not null default '{}'::jsonb,
  add column if not exists settings_authority_version text,
  add column if not exists settings_authority_fingerprint text,
  add column if not exists settings_authority_resolved_at timestamptz;

alter table public.contract_weeks
  drop constraint if exists contract_weeks_settings_authority_object_ck,
  add constraint contract_weeks_settings_authority_object_ck
    check (jsonb_typeof(settings_authority_json) = 'object'),
  drop constraint if exists contract_weeks_settings_authority_identity_ck,
  add constraint contract_weeks_settings_authority_identity_ck check (
    (settings_authority_json = '{}'::jsonb
      and settings_authority_version is null
      and settings_authority_fingerprint is null
      and settings_authority_resolved_at is null)
    or
    (settings_authority_json <> '{}'::jsonb
      and settings_authority_version = 'CONTRACT_SETTINGS_AUTHORITY_V1'
      and settings_authority_fingerprint ~ '^[0-9a-f]{64}$'
      and settings_authority_resolved_at is not null)
  );

create index if not exists idx_contract_weeks_planned_settings_authority_v1
  on public.contract_weeks(contract_id, week_ending_date, id)
  where timesheet_id is null;

-- Weekly rows are real from creation. Daily rows become real only when Office
-- processing records processed_at_utc; the repeatable trigger freezes this
-- snapshot at the appropriate boundary.
alter table public.timesheets
  add column if not exists settings_authority_json jsonb not null default '{}'::jsonb,
  add column if not exists settings_authority_version text,
  add column if not exists settings_authority_fingerprint text,
  add column if not exists settings_authority_resolved_at timestamptz;

alter table public.timesheets
  drop constraint if exists timesheets_settings_authority_object_ck,
  add constraint timesheets_settings_authority_object_ck
    check (jsonb_typeof(settings_authority_json) = 'object'),
  drop constraint if exists timesheets_settings_authority_identity_ck,
  add constraint timesheets_settings_authority_identity_ck check (
    (settings_authority_json = '{}'::jsonb
      and settings_authority_version is null
      and settings_authority_fingerprint is null
      and settings_authority_resolved_at is null)
    or
    (settings_authority_json <> '{}'::jsonb
      and settings_authority_version = 'CONTRACT_SETTINGS_AUTHORITY_V1'
      and settings_authority_fingerprint ~ '^[0-9a-f]{64}$'
      and settings_authority_resolved_at is not null)
  );

comment on column public.timesheets.settings_authority_json is
  'Immutable effective Client/Contract settings authority frozen when the Timesheet becomes real; ordinary later settings edits must not rewrite it.';

-- These fail-closed compatibility definitions let earlier repeatables compile
-- during a clean installation.  The authoritative repeatable at the end of
-- the release replaces both before any verification or application use.
create or replace function private._contract_settings_effective_core_v1(
  p_client_id uuid,
  p_contract_id uuid,
  p_relevant_date date,
  p_workflow text,
  p_timesheet_id uuid default null
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare v_snapshot jsonb;
begin
  if p_timesheet_id is not null then
    select t.settings_authority_json into v_snapshot
    from public.timesheets t where t.timesheet_id=p_timesheet_id;
    if found and coalesce(v_snapshot,'{}'::jsonb)<>'{}'::jsonb then
      return v_snapshot;
    end if;
  end if;
  raise exception 'CONTRACT_SETTINGS_AUTHORITY_INSTALLING' using errcode='55000';
end
$function$;

alter function private._contract_settings_effective_core_v1(uuid,uuid,date,text,uuid)
  owner to postgres;
revoke all on function private._contract_settings_effective_core_v1(uuid,uuid,date,text,uuid)
  from public,anon,authenticated,service_role;

create or replace function public.import_auto_authorise_policy_resolve_v2(
  p_source_system public.hr_source_enum,
  p_client_id uuid,
  p_contract_id uuid,
  p_timesheet_id uuid,
  p_relevant_date date,
  p_validation_context boolean default false
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
begin
  if p_relevant_date is null then
    raise exception 'IMPORT_AUTO_AUTHORISE_RELEVANT_DATE_REQUIRED' using errcode='22023';
  end if;
  return public.import_auto_authorise_policy_resolve_v1(
    p_source_system,p_client_id,p_contract_id,p_validation_context
  )||jsonb_build_object('relevant_date',p_relevant_date,'timesheet_id',p_timesheet_id);
end
$function$;

alter function public.import_auto_authorise_policy_resolve_v2(
  public.hr_source_enum,uuid,uuid,uuid,date,boolean
) owner to postgres;
revoke all on function public.import_auto_authorise_policy_resolve_v2(
  public.hr_source_enum,uuid,uuid,uuid,date,boolean
) from public,anon,authenticated;
grant execute on function public.import_auto_authorise_policy_resolve_v2(
  public.hr_source_enum,uuid,uuid,uuid,date,boolean
) to service_role;

-- Later invoice repeatables consume only a real Timesheet's frozen authority.
-- Define this guard in the migration so it exists before repeatables are
-- installed on a clean database.
create or replace function private._timesheet_settings_authority_frozen_v1(
  p_timesheet_id uuid
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare v_snapshot jsonb; v_version text; v_fingerprint text; v_expected text;
begin
  select t.settings_authority_json,t.settings_authority_version,
    t.settings_authority_fingerprint
  into v_snapshot,v_version,v_fingerprint
  from public.timesheets t
  where t.timesheet_id=p_timesheet_id and t.is_current;
  if not found then
    raise exception 'CONTRACT_SETTINGS_CURRENT_TIMESHEET_NOT_FOUND' using errcode='P0002';
  end if;
  if coalesce(v_snapshot,'{}'::jsonb)='{}'::jsonb
     or v_version is distinct from 'CONTRACT_SETTINGS_AUTHORITY_V1'
     or coalesce(v_fingerprint,'') !~ '^[0-9a-f]{64}$' then
    raise exception 'CONTRACT_SETTINGS_TIMESHEET_AUTHORITY_NOT_FROZEN' using errcode='55000';
  end if;
  v_expected:=encode(digest(convert_to(
    (v_snapshot-'authority_fingerprint'-'resolved_at_utc')::text,'UTF8'
  ),'sha256'),'hex');
  if v_expected is distinct from v_fingerprint
     or v_snapshot->>'authority_fingerprint' is distinct from v_fingerprint then
    raise exception 'CONTRACT_SETTINGS_TIMESHEET_AUTHORITY_INVALID' using errcode='22023';
  end if;
  return v_snapshot;
end
$function$;

alter function private._timesheet_settings_authority_frozen_v1(uuid) owner to postgres;
revoke all on function private._timesheet_settings_authority_frozen_v1(uuid)
  from public,anon,authenticated,service_role;

-- TEST contains legacy Clients whose first effective-dated settings row begins
-- after their earliest Contract.  Backdate only that first row, preserving all
-- of its values and every later settings-history row.  A NULL effective_from
-- already applies to the Client's full history and therefore needs no change.
with earliest_contract as (
  select c.client_id,min(c.start_date) earliest_contract_date
  from public.contracts c
  where c.client_id is not null and c.start_date is not null
  group by c.client_id
), first_settings as (
  select ranked.id,ranked.client_id,ranked.effective_from
  from (
    select cs.id,cs.client_id,cs.effective_from,
      row_number() over(
        partition by cs.client_id
        order by cs.effective_from asc nulls first,cs.created_at asc,cs.id asc
      ) rn
    from public.client_settings cs
  ) ranked
  where ranked.rn=1
)
update public.client_settings cs
set effective_from=ec.earliest_contract_date
from first_settings fs
join earliest_contract ec on ec.client_id=fs.client_id
where cs.id=fs.id
  and fs.effective_from is not null
  and fs.effective_from>ec.earliest_contract_date;

commit;
