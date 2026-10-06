-- A factual route discriminator for the approval owners. It neither calculates
-- pay nor publishes a revision. Source HOURS must be published by the Source
-- owner (including its first-authorisation and later-head paths); a separate
-- expense/mileage Timesheet on the same contract uses its own ordinary owner.
\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_approval_route_v1(p_timesheet_id uuid)
returns text
language plpgsql
stable
security definer
set search_path = pg_catalog, public, private
as $function$
declare
  v_self_bill boolean;
  v_source_root boolean;
  v_line_type text;
  v_is_current boolean;
  v_archived_at timestamptz;
  v_revoked_at timestamptz;
  v_adjustment_origin text;
  v_correction_kind text;
  v_is_adjustment boolean;
  v_parent_timesheet_id uuid;
  v_correction_id text;
  v_import_classification jsonb;
begin
  if p_timesheet_id is null then
    raise exception using errcode='22023', message='BPAY_NEXT_APPROVAL_ROUTE_TIMESHEET_REQUIRED';
  end if;
  select c.self_bill,t.line_type::text,t.is_current,t.archived_at_utc,t.revoked_at,
         upper(btrim(coalesce(t.adjustment_origin,''))),
         upper(btrim(coalesce(t.correction_kind,''))),
         t.is_adjustment,t.parent_timesheet_id,t.correction_id
    into v_self_bill,v_line_type,v_is_current,v_archived_at,v_revoked_at,
         v_adjustment_origin,v_correction_kind,v_is_adjustment,
         v_parent_timesheet_id,v_correction_id
    from public.timesheets t
    join public.contracts c on c.id=t.contract_id
    where t.timesheet_id=p_timesheet_id;
  if not found or v_is_current is distinct from true
     or v_archived_at is not null or v_revoked_at is not null then
    raise exception using errcode='23514', message='BPAY_NEXT_APPROVAL_ROUTE_NOT_CURRENT';
  end if;
  if v_line_type in ('EXPENSES','MILEAGE') then
    return 'EXPENSE_CARRIER';
  end if;
  if v_line_type='HOURS' then
    -- Imported reversal/replacement children remain physical/invoice facts;
    -- NEXT realises the approved complete Source base, never the retired pair.
    -- Either typed marker requires genuine provenance: a malformed child must
    -- not fall through to the ordinary or self-bill Source financial producer.
    if v_adjustment_origin in (
         'IMPORT_CORRECTION','IMPORT_CANCELLATION',
         'HEALTHROSTER_CHANGED_HOURS','NHSP_CHANGED_HOURS',
         'HEALTHROSTER_CANCELLATION','NHSP_CANCELLATION'
       ) or v_correction_kind in (
         'CHANGED_HOURS_REVERSAL','CHANGED_HOURS_REPLACEMENT',
         'CANCELLATION_REVERSAL','CANCELLATION_REPLACEMENT'
       ) then
      v_import_classification:=public._ctms_import_correction_classify_v1(p_timesheet_id);
      if v_is_adjustment is distinct from true
         or v_parent_timesheet_id is null or v_correction_id is null
         or jsonb_typeof(v_import_classification) is distinct from 'object'
         or (v_import_classification->>'timesheet_id') is distinct from p_timesheet_id::text
         or (v_import_classification->>'correction_id') is distinct from v_correction_id
         or (v_import_classification->>'adjustment_origin') is distinct from v_adjustment_origin
         or (v_import_classification->>'correction_kind') is distinct from v_correction_kind
         or (v_import_classification->'is_import_authoritative_correction')
              is distinct from 'true'::jsonb then
        raise exception using errcode='23514', message='BPAY_NEXT_IMPORT_CORRECTION_PROVENANCE_INVALID';
      end if;
      return 'FACTUAL_IMPORT_CORRECTION';
    end if;
    -- Self-bill is import-authoritative by the product decision. During the
    -- real first-authorisation owner the financial row is stamped BEFORE its
    -- Source authorisation record is inserted. Do not misroute that temporary
    -- state to the ordinary producer or refuse the legitimate Source call.
    -- The Source stage still requires the completed authorisation and exact
    -- inventory before it can publish any pay revision.
    if v_self_bill is true then
      return 'SOURCE_HOURS';
    end if;
    -- Non-self-bill imported HOURS (including Magnit) use the installed
    -- resolved family classifier, not an untrusted import label.
    v_source_root:=private._candidate_expense_source_family_v1(p_timesheet_id);
    if v_source_root then
      return 'SOURCE_HOURS';
    end if;
    return 'ORDINARY_HOURS';
  end if;
  raise exception using errcode='23514', message='BPAY_NEXT_APPROVAL_ROUTE_UNSUPPORTED';
end
$function$;

alter function private.bpay_next_approval_route_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_approval_route_v1(uuid)
  from public, anon, authenticated, service_role;

commit;
