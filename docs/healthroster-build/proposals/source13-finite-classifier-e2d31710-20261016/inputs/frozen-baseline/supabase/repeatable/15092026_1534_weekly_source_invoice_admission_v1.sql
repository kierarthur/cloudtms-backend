-- Repeatable CloudTMS function/trigger authority: weekly_source_invoice_admission_v1
-- Self-bill invoice membership is movement-only. Timesheet authorisation,
-- query state and protected-pay state are deliberately outside this owner.

\set ON_ERROR_STOP on

begin;

-- Gate 7 item G7-5 (24 section 14; proof/34 section 9; ROT-008).
-- Invoice lineage resolves through the Timesheet FAMILY, never through a bare
-- physical id.
--
-- proof/34 section 10 (rule 12), word for word: "The only identity mechanism is
-- the existing Workbench resolver and its family normaliser ... no second
-- identity table, no parallel version counter."  proof/34 section 7 (rule 6)
-- requires every path to resolve through public._pay_timesheet_rotation_scope
-- before acting.  This helper is therefore a READ-ONLY adapter over that one
-- resolver: it calls it and returns its family_timesheet_id set.
--
-- WP-51, closing WP-48's combined-review finding F1 (the two family resolvers
-- disagreed about what counts as ONE family).
--
-- public._pay_timesheet_rotation_scope is the Banking Pay scope resolver.  It is
-- call-only for Weekly Source and it is RAW-keyed by design: its family is
-- `requested_bookings.booking_id = family_rows.booking_id`, plain equality on
-- the stored text.  The Weekly Source managed-root guard
-- (private.weekly_source_managed_root_guard_v1) and the root-identity resolver
-- beneath it (private.weekly_source_resolve_root_identity_v1) key the family on
-- pg_catalog.btrim(booking_id) and FAIL CLOSED on a raw/canonical split with
-- BOOKING_REFERENCE_CANONICAL_COLLISION.
--
-- EXECUTED on a build from empty (wp51_before, 18 September 2026): a
-- whitespace-padded sibling of a lineage-bound root was accepted by the RAW
-- unique index `timesheets_booking_id_current_uidx` - which is unique on the
-- stored booking_id among current rows, so ' bk_x' and 'bk_x' are two different
-- keys to it and it can never be the thing that prevents the split - resolved
-- here to a SINGLETON family, was OFFERED to the ordinary invoice batch as one
-- group, and was ACCEPTED by the invoice-line owner guard, while the managed-
-- root guard called the same pair ONE MANAGED FAMILY and refused it.  Two
-- invoicing systems disagreeing about the same rows is exactly what the
-- isolation predicate exists to prevent.
--
-- This adapter is the PERMISSIVE side, so this adapter is what changes.  The
-- ruled and proved behaviour is the guard's, and it is not touched.  The
-- reconciliation is a SEED WIDENING, not a second identity mechanism: the only
-- family resolution is still public._pay_timesheet_rotation_scope, called once,
-- and the only thing added is WHICH ids it is asked about.  When - and only
-- when - a raw/canonical split exists, the split siblings are seeded alongside
-- the requested id and the resolver's own families are unioned, so the adapter
-- reports the one family the guard already sees.  With no split the seed set is
-- exactly `{p_timesheet_id}` and the returned array is byte-identical to the
-- previous revision, which is why ordinary invoicing is unchanged.
--
-- The split predicate is `btrim(sibling.booking_id)=btrim(requested.booking_id)
-- and sibling.booking_id<>requested.booking_id`, copied from
-- private.weekly_source_resolve_root_identity_v1's own `split_family` CTE so the
-- two cannot drift apart.  The non-empty trimmed key is the same restriction
-- private.weekly_source_managed_root_guard_v1 puts on its two family probes:
-- without it one blank-booking row would inherit another blank-booking row's
-- family, which ruling B3 forbids, and the guard PERMITS a blank-booking split
-- (bound=false), so the adapter must not widen there either.  Membership is a
-- set union computed explicitly; nothing here is decided by a `limit`, an
-- `order by` or by what an index happens to accept.
--
-- Fallback: the resolver returns no row for a Timesheet that exists but carries
-- no booking identity, and the singleton family is then the physical id itself.
-- Nothing here writes; proof/34 section 9 is explicit that a newer version
-- never creates, supersedes or re-points a binding.
create or replace function private.weekly_source_invoice_family_timesheet_ids_v1(
  p_timesheet_id uuid
) returns uuid[]
language sql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  with canonical_split_siblings as (
    select sibling.timesheet_id
    from public.timesheets sibling
    join public.timesheets requested
      on requested.timesheet_id=p_timesheet_id
     and pg_catalog.btrim(requested.booking_id)<>''
    where pg_catalog.btrim(sibling.booking_id)
          =pg_catalog.btrim(requested.booking_id)
      and sibling.booking_id<>requested.booking_id
  ),
  resolver_seeds as (
    select p_timesheet_id as timesheet_id
    union
    select canonical_split_siblings.timesheet_id from canonical_split_siblings
  )
  select coalesce(
    (
      select pg_catalog.array_agg(distinct scope.family_timesheet_id)
      from public._pay_timesheet_rotation_scope(
        (
          select pg_catalog.array_agg(resolver_seeds.timesheet_id)
          from resolver_seeds
        )
      ) scope
      where scope.family_timesheet_id is not null
    ),
    array[p_timesheet_id]::uuid[]
  );
$function$;

-- The family is resolved ONCE, in the materialised CTE below, and every limb
-- reads that one array.  Nine separate calls to the resolver per evaluation was
-- the other half of the finding proof/34 section 10 raised.
create or replace function private.weekly_source_invoice_movement_only_integrity_v1(
  p_timesheet_id uuid
) returns boolean
language sql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
  with resolved_family as materialized (
    select private.weekly_source_invoice_family_timesheet_ids_v1(p_timesheet_id) timesheet_ids
  )
  select p_timesheet_id is not null
    and (
      exists(
        select 1 from public.weekly_source_row_timesheet_lineages lineage
        where lineage.timesheet_id=any(resolved_family.timesheet_ids)
      )
      or exists(
        select 1
        from public.weekly_exceptional_pay_target_families family
        join public.timesheets root
          on root.timesheet_id=family.root_timesheet_id
        join public.contracts contract
          on contract.id=family.contract_id
        join public.contract_weeks contract_week
          on contract_week.contract_id=family.contract_id
         and contract_week.week_ending_date=family.week_ending_date
         and contract_week.additional_seq=0
         and contract_week.timesheet_id=family.root_timesheet_id
        -- Gate 13 hostile review F5 / standing rule 3: after S8 a physical root
        -- id is no longer a family identity.  A protected family recorded
        -- against ANY member of this family is this family's protected state,
        -- so the same one resolved_family array that every other limb reads
        -- decides this limb too.  Keyed on the bare physical id, a family
        -- recorded against a rotated sibling was invisible here.
        where family.root_timesheet_id=any(resolved_family.timesheet_ids)
          and family.ownership_state='TARGET_MANAGED'
          and contract.candidate_id=family.candidate_id
          and root.contract_id=family.contract_id
          and root.week_ending_date=family.week_ending_date
          and root.sheet_scope='WEEKLY'::public.timesheet_scope_enum
          and root.line_type='HOURS'::public.timesheet_line_type_enum
          and not root.is_adjustment
          and root.is_current
          and root.revoked_at is null
          and root.archived_at_utc is null
          and not contract_week.is_adjustment
          and contract_week.status<>'CANCELLED'::public.contract_week_status_enum
          and not exists(
            select 1 from public.weekly_source_billing_movements movement
            where movement.invoice_timesheet_id=any(resolved_family.timesheet_ids)
          )
          and not exists(
            select 1 from public.invoice_lines invoice_line
            where invoice_line.timesheet_id=any(resolved_family.timesheet_ids)
          )
      )
    )
    and not exists(
      select 1
      from public.invoice_lines invoice_line
      where invoice_line.timesheet_id=any(resolved_family.timesheet_ids)
        and not exists(
          select 1
          from public.weekly_source_invoice_line_bindings binding
          join public.weekly_source_billing_movements movement
            on movement.id=binding.billing_movement_id
          where binding.invoice_line_id=invoice_line.id
            and binding.invoice_id=invoice_line.invoice_id
            and binding.state='CURRENT'
            and movement.invoice_timesheet_id=any(resolved_family.timesheet_ids)
            and movement.placement_state in ('PLACED','ISSUED')
        )
    )
    and not exists(
      select 1
      from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_billing_movements movement
        on movement.id=binding.billing_movement_id
      join public.invoice_lines invoice_line on invoice_line.id=binding.invoice_line_id
      where movement.invoice_timesheet_id=any(resolved_family.timesheet_ids)
        and binding.state='CURRENT'
        and (
          not (invoice_line.timesheet_id=any(resolved_family.timesheet_ids))
          or invoice_line.invoice_id is distinct from binding.invoice_id
        )
    )
    and (
      exists(
        select 1 from public.weekly_source_billing_movements movement
        where movement.invoice_timesheet_id=any(resolved_family.timesheet_ids)
      )
      or not exists(
        select 1 from public.invoice_lines invoice_line
        where invoice_line.timesheet_id=any(resolved_family.timesheet_ids)
      )
    )
  from resolved_family;
$function$;

create or replace function private.weekly_source_invoice_line_owner_guard_v1()
returns trigger
language plpgsql security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_timesheet_id uuid:=case when tg_op='DELETE' then old.timesheet_id
    else new.timesheet_id end;
  v_owner text:=nullif(pg_catalog.current_setting(
    'cloudtms.weekly_source_invoice_owner',true
  ),'');
  v_family uuid[];
begin
  -- Gate 13 hostile review F4 / standing rule 3.  This guard is the last thing
  -- standing between the two invoicing systems, so it must be keyed on the
  -- Timesheet FAMILY, not on the physical row being written.  A lineage-bound
  -- root that rotates keeps its lineage on the OLD physical id; the new current
  -- version carries no lineage row of its own, so a guard keyed on
  -- new.timesheet_id let an ordinary invoice line be written for worked time
  -- that is already on a source self-bill - a double invoice.
  --
  -- The owner test is evaluated first only to avoid resolving the family on the
  -- two owners that are allowed to write these lines anyway; the conjunction is
  -- identical, neither side has an effect, and the refusal is unchanged.  A
  -- final source row is never removed merely because pay authorisation is
  -- withdrawn.  The only allocation exception is the explicit same-client move
  -- between two unissued invoices.
  if v_timesheet_id is not null and coalesce(v_owner,'') not in (
    'ADMIT_SOURCE_INVOICE','MOVE_SOURCE_INVOICE'
  ) then
    -- One family resolution, through the one installed resolver adapter.  No
    -- second identity mechanism and no inline re-derivation (proof/34 s10
    -- rule 12).
    v_family:=private.weekly_source_invoice_family_timesheet_ids_v1(v_timesheet_id);
    -- Standing rule 3: fail closed when the family identity cannot be
    -- established.  This is an explicit cardinality test, never a limit.
    if v_family is null or pg_catalog.cardinality(v_family)=0 then
      raise exception 'WEEKLY_SOURCE_INVOICE_FAMILY_UNRESOLVED'
        using errcode='55000';
    end if;
    if exists(
      select 1 from public.weekly_source_row_timesheet_lineages lineage
      where lineage.timesheet_id=any(v_family)
    ) or exists(
      select 1 from public.weekly_exceptional_pay_target_families family
      where family.root_timesheet_id=any(v_family)
        and family.ownership_state='TARGET_MANAGED'
    ) then
      raise exception 'WEEKLY_SOURCE_INVOICE_MOVEMENT_OWNER_REQUIRED'
        using errcode='55000';
    end if;
  end if;
  if tg_op='DELETE' then return old; end if;
  return new;
end;
$function$;

drop trigger if exists weekly_source_invoice_line_owner_guard on public.invoice_lines;
create trigger weekly_source_invoice_line_owner_guard
before insert or update or delete on public.invoice_lines
for each row execute function private.weekly_source_invoice_line_owner_guard_v1();

create or replace function private.weekly_source_invoice_allocation_assert_v1(
  p_invoice_id uuid
) returns jsonb
language plpgsql stable security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_invoice public.invoices%rowtype;
  v_line_count integer;
  v_binding_count integer;
  v_has_source_header boolean;
begin
  select * into v_invoice from public.invoices where id=p_invoice_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_INVOICE_NOT_FOUND' using errcode='22023';
  end if;
  select pg_catalog.count(*)::integer into v_binding_count
  from public.weekly_source_invoice_line_bindings binding
  where binding.invoice_id=p_invoice_id and binding.state='CURRENT';
  v_has_source_header:=coalesce(
    v_invoice.header_snapshot_json#>>'{meta,source}'='WEEKLY_FINAL_SOURCE'
    and v_invoice.header_snapshot_json#>>'{meta,self_bill}'='true',false
  );
  if v_binding_count=0 then
    if not v_has_source_header then
      return pg_catalog.jsonb_build_object(
        'ok',true,'is_source_invoice',false,'invoice_id',p_invoice_id
      );
    end if;
    select pg_catalog.count(*)::integer into v_line_count
    from public.invoice_lines invoice_line where invoice_line.invoice_id=p_invoice_id;
    if v_line_count<>0 then
      raise exception 'WEEKLY_SOURCE_INVOICE_EXTRA_LINE' using errcode='55000';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'is_source_invoice',true,'invoice_id',p_invoice_id,
      'line_count',0,'binding_count',0,'empty',true
    );
  end if;
  if not v_has_source_header then
    raise exception 'WEEKLY_SOURCE_INVOICE_HEADER_INVALID' using errcode='55000';
  end if;
  select pg_catalog.count(*)::integer into v_line_count
  from public.invoice_lines invoice_line where invoice_line.invoice_id=p_invoice_id;

  if exists(
    select 1 from public.invoice_lines invoice_line
    where invoice_line.invoice_id=p_invoice_id
      and not exists(
        select 1 from public.weekly_source_invoice_line_bindings binding
        where binding.invoice_line_id=invoice_line.id
          and binding.invoice_id=p_invoice_id and binding.state='CURRENT'
      )
  ) then
    raise exception 'WEEKLY_SOURCE_INVOICE_EXTRA_LINE' using errcode='55000';
  end if;

  if exists(
    select 1
    from public.weekly_source_invoice_line_bindings binding
    join public.weekly_source_billing_movements movement
      on movement.id=binding.billing_movement_id
    join public.weekly_source_invoice_presentation_lines presentation
      on presentation.id=binding.presentation_line_id
    join public.invoice_lines invoice_line on invoice_line.id=binding.invoice_line_id
    join public.weekly_source_invoice_placements placement
      on placement.billing_movement_id=movement.id and placement.is_current
    join public.weekly_source_manifest_movements manifest_movement
      on manifest_movement.billing_movement_id=movement.id
    join public.weekly_source_client_manifests manifest
      on manifest.id=manifest_movement.client_manifest_id
    where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
      and (
        invoice_line.invoice_id is distinct from p_invoice_id
        or placement.invoice_id is distinct from p_invoice_id
        or placement.invoice_line_id is distinct from invoice_line.id
        or placement.placement_state<>'PLACED'
        or binding.client_id is distinct from v_invoice.client_id
        or movement.actual_client_id is distinct from v_invoice.client_id
        or presentation.client_id is distinct from v_invoice.client_id
        or manifest.client_id is distinct from v_invoice.client_id
        or manifest_movement.movement_hash is distinct from movement.movement_economic_hash
        or binding.manifest_hash is distinct from manifest.manifest_hash
        or presentation.client_manifest_id is distinct from manifest.id
        or presentation.final_revision_id is distinct from movement.final_revision_id
        or presentation.original_finalisation_cycle_id is distinct from movement.finalisation_cycle_id
        or invoice_line.timesheet_id is distinct from movement.invoice_timesheet_id
        or invoice_line.total_pay_ex_vat is distinct from presentation.total_pay_ex_vat
        or invoice_line.total_charge_ex_vat is distinct from presentation.total_charge_ex_vat
        or invoice_line.vat_rate_pct is distinct from presentation.vat_rate_pct
        or invoice_line.vat_amount is distinct from presentation.vat_amount
        or invoice_line.total_inc_vat is distinct from presentation.total_inc_vat
      )
  ) then
    raise exception 'WEEKLY_SOURCE_INVOICE_ALLOCATION_MISMATCH' using errcode='55000';
  end if;

  if exists(
    select 1
    from (
      select binding.invoice_line_id,
             pg_catalog.count(*)::integer as member_count,
             pg_catalog.count(distinct movement.correction_unit_id) as correction_count,
             pg_catalog.count(distinct movement.work_event_id) as event_count,
             pg_catalog.count(distinct movement.final_revision_id) as revision_count,
             pg_catalog.count(distinct movement.finalisation_cycle_id) as cycle_count,
             pg_catalog.count(distinct movement.actual_client_id) as client_count,
             pg_catalog.count(*) filter(where movement.source_profile_kind='NHSP_TRUST_BACKING_REPORT') as nhsp_count,
             pg_catalog.array_agg(movement.movement_role order by movement.movement_role) as roles,
             pg_catalog.min(presentation.correction_role) as correction_role
      from public.weekly_source_invoice_line_bindings binding
      join public.weekly_source_billing_movements movement
        on movement.id=binding.billing_movement_id
      join public.weekly_source_invoice_presentation_lines presentation
        on presentation.id=binding.presentation_line_id
      where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
      group by binding.invoice_line_id
    ) grouped
    where grouped.member_count not in (1,2)
       or (grouped.member_count=2 and (
         grouped.correction_count<>1 or grouped.event_count<>1
         or grouped.revision_count<>1 or grouped.cycle_count<>1
         or grouped.client_count<>1 or grouped.nhsp_count<>0
         or grouped.correction_role is distinct from 'NET_DIFFERENCE'
         or grouped.roles not in (
           array['REPLACEMENT','REVERSAL']::text[],
           array['EXPENSE_REPLACEMENT','EXPENSE_REVERSAL']::text[]
         )
       ))
  ) then
    raise exception 'WEEKLY_SOURCE_INVOICE_NET_CARDINALITY_INVALID' using errcode='55000';
  end if;

  if v_invoice.subtotal_ex_vat is distinct from (
       select pg_catalog.round(coalesce(pg_catalog.sum(line.total_charge_ex_vat),0),2)
       from public.invoice_lines line where line.invoice_id=p_invoice_id
     )
     or v_invoice.vat_amount is distinct from (
       select pg_catalog.round(coalesce(pg_catalog.sum(line.vat_amount),0),2)
       from public.invoice_lines line where line.invoice_id=p_invoice_id
     )
     or v_invoice.total_inc_vat is distinct from (
       select pg_catalog.round(coalesce(pg_catalog.sum(line.total_inc_vat),0),2)
       from public.invoice_lines line where line.invoice_id=p_invoice_id
     ) then
    raise exception 'WEEKLY_SOURCE_INVOICE_TOTAL_MISMATCH' using errcode='55000';
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'is_source_invoice',true,'invoice_id',p_invoice_id,
    'line_count',v_line_count,'binding_count',v_binding_count
  );
end;
$function$;

create or replace function private.weekly_source_invoice_issue_guard_v1()
returns trigger
language plpgsql security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
begin
  if coalesce(
    new.header_snapshot_json#>>'{meta,source}'='WEEKLY_FINAL_SOURCE'
    and new.header_snapshot_json#>>'{meta,self_bill}'='true',false
  ) then
    if (old.status::text<>'ISSUED' and new.status::text='ISSUED')
       or (old.issue_state<>'ISSUED' and new.issue_state='ISSUED') then
      if not exists(
        select 1 from public.weekly_source_invoice_line_bindings binding
        where binding.invoice_id=new.id and binding.state='CURRENT'
      ) then
        raise exception 'WEEKLY_SOURCE_INVOICE_EMPTY' using errcode='55000';
      end if;
      perform private.weekly_source_invoice_allocation_assert_v1(new.id);
      update public.weekly_source_billing_movements movement
      set placement_state='ISSUED'
      from public.weekly_source_invoice_line_bindings binding
      where binding.invoice_id=new.id and binding.state='CURRENT'
        and binding.billing_movement_id=movement.id
        and movement.placement_state='PLACED';
    elsif old.status::text='ISSUED' and new.status::text='DRAFT' then
      update public.weekly_source_billing_movements movement
      set placement_state='PLACED'
      from public.weekly_source_invoice_line_bindings binding
      where binding.invoice_id=new.id and binding.state='CURRENT'
        and binding.billing_movement_id=movement.id
        and movement.placement_state='ISSUED';
    end if;
  end if;
  return new;
end;
$function$;

create or replace function private.weekly_source_invoice_expected_version_fingerprint_v1(
  p_invoice_id uuid,
  p_document_revision bigint
) returns bytea
language sql immutable
set search_path to 'private','pg_catalog','pg_temp'
as $function$
  select private.weekly_source_sha256_jsonb_v1(
    'WEEKLY_SOURCE_INVOICE_EXPECTED_VERSION_V1',
    pg_catalog.jsonb_build_object(
      'invoice_id',p_invoice_id,
      'document_revision',p_document_revision
    )
  );
$function$;

create or replace function private.weekly_source_invoice_header_refresh_v1(
  p_invoice_id uuid
) returns void
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_manifest_ids jsonb;
  v_cycle_ids jsonb;
  v_week_endings jsonb;
  v_backing_reports jsonb;
begin
  with facts as materialized (
    select distinct manifest.id as manifest_id,manifest.source_cycle_id,
           manifest.finalisation_week_ending,manifest.backing_report_number
    from public.weekly_source_invoice_line_bindings binding
    join public.weekly_source_manifest_movements manifest_movement
      on manifest_movement.billing_movement_id=binding.billing_movement_id
    join public.weekly_source_client_manifests manifest
      on manifest.id=manifest_movement.client_manifest_id
    where binding.invoice_id=p_invoice_id and binding.state='CURRENT'
  )
  select
    coalesce((select pg_catalog.jsonb_agg(f.manifest_id order by f.manifest_id)
              from facts f),'[]'::jsonb),
    coalesce((select pg_catalog.jsonb_agg(c.source_cycle_id order by c.source_cycle_id)
              from (select distinct source_cycle_id from facts) c),'[]'::jsonb),
    coalesce((select pg_catalog.jsonb_agg(w.finalisation_week_ending order by w.finalisation_week_ending)
              from (select distinct finalisation_week_ending from facts) w),'[]'::jsonb),
    coalesce((select pg_catalog.jsonb_agg(r.backing_report_number order by r.backing_report_number)
              from (select distinct backing_report_number from facts
                    where backing_report_number is not null) r),'[]'::jsonb)
  into v_manifest_ids,v_cycle_ids,v_week_endings,v_backing_reports;

  update public.invoices invoice
  set header_snapshot_json=pg_catalog.jsonb_set(
        invoice.header_snapshot_json,'{meta}',
        coalesce(invoice.header_snapshot_json->'meta','{}'::jsonb)
          ||pg_catalog.jsonb_build_object(
            'contained_client_manifest_ids',v_manifest_ids,
            'contained_source_cycle_ids',v_cycle_ids,
            'contained_finalisation_week_endings',v_week_endings,
            'backing_report_numbers',v_backing_reports
          ),true
      ),
      updated_at=pg_catalog.statement_timestamp()
  where invoice.id=p_invoice_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_INVOICE_NOT_FOUND' using errcode='22023';
  end if;
end;
$function$;

drop trigger if exists weekly_source_invoice_issue_guard on public.invoices;
create trigger weekly_source_invoice_issue_guard
before update of status,issue_state on public.invoices
for each row execute function private.weekly_source_invoice_issue_guard_v1();

create or replace function public.weekly_source_invoice_admit_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_allowed_keys constant text[]:=array[
    'actor_user_id','client_manifest_id','expected_manifest_hash'
  ]::text[];
  v_unknown text;
  v_actor uuid;
  v_manifest_id uuid;
  v_expected_hash bytea;
  v_manifest public.weekly_source_client_manifests%rowtype;
  v_revision public.weekly_source_final_revisions%rowtype;
  v_cycle public.weekly_source_cycles%rowtype;
  v_client public.clients%rowtype;
  v_defaults public.settings_defaults%rowtype;
  v_invoice_id uuid;
  v_header jsonb;
  v_group_row record;
  v_movement public.weekly_source_billing_movements%rowtype;
  v_snapshot public.weekly_source_final_snapshot_lines%rowtype;
  v_source_row public.weekly_source_upload_rows%rowtype;
  v_contract public.contracts%rowtype;
  v_candidate public.candidates%rowtype;
  v_ids uuid[];
  v_member_id uuid;
  v_member public.weekly_source_billing_movements%rowtype;
  v_presentation_id uuid;
  v_invoice_line_id uuid;
  v_binding_id uuid;
  v_line_kind text;
  v_origin_kind text;
  v_correction_role text;
  v_old_id uuid;
  v_new_id uuid;
  v_work_date date;
  v_start timestamp without time zone;
  v_end timestamp without time zone;
  v_break integer;
  v_reference text;
  v_description text;
  v_hours_day numeric;
  v_hours_night numeric;
  v_hours_sat numeric;
  v_hours_sun numeric;
  v_hours_bh numeric;
  v_pay_day numeric;
  v_pay_night numeric;
  v_pay_sat numeric;
  v_pay_sun numeric;
  v_pay_bh numeric;
  v_charge_day numeric;
  v_charge_night numeric;
  v_charge_sat numeric;
  v_charge_sun numeric;
  v_charge_bh numeric;
  v_total_pay numeric(12,2);
  v_total_charge numeric(12,2);
  v_calc_pence bigint;
  v_source_pence bigint;
  v_vat_rate numeric(5,2);
  v_vat numeric(12,2);
  v_total numeric(12,2);
  v_price_result text;
  v_price_fingerprint bytea;
  v_mapping_fingerprint bytea;
  v_source_hash bytea;
  v_presentation_hash bytea;
  v_materialised_hash bytea;
  v_placement_hash bytea;
  v_candidate_display text;
  v_client_display text;
  v_is_net boolean;
  v_is_expense boolean;
  v_count integer;
begin
  perform pg_catalog.set_config('lock_timeout','5s',true);
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')
       <>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_INVOICE_ADMIT_REQUEST_INVALID' using errcode='22023';
  end if;
  select key into v_unknown from pg_catalog.jsonb_object_keys(p_request) key
  where not (key=any(v_allowed_keys)) order by key limit 1;
  if v_unknown is not null then
    raise exception 'WEEKLY_SOURCE_INVOICE_ADMIT_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown;
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_manifest_id:=(p_request->>'client_manifest_id')::uuid;
    v_expected_hash:=pg_catalog.decode(p_request->>'expected_manifest_hash','hex');
  exception when others then
    raise exception 'WEEKLY_SOURCE_INVOICE_ADMIT_VALUE_INVALID' using errcode='22023';
  end;
  if pg_catalog.octet_length(v_expected_hash)<>32 then
    raise exception 'WEEKLY_SOURCE_INVOICE_ADMIT_HASH_INVALID' using errcode='22023';
  end if;

  select * into v_manifest from public.weekly_source_client_manifests
  where id=v_manifest_id for update;
  if not found or v_manifest.manifest_hash is distinct from v_expected_hash then
    raise exception 'WEEKLY_SOURCE_INVOICE_MANIFEST_CHANGED' using errcode='40001';
  end if;
  select * into strict v_revision from public.weekly_source_final_revisions
  where id=v_manifest.final_revision_id for share;
  select * into strict v_cycle from public.weekly_source_cycles
  where id=v_manifest.source_cycle_id for share;
  if v_revision.state<>'CURRENT'
     or v_revision.source_cycle_id is distinct from v_cycle.id
     or v_cycle.source_group_id is distinct from v_manifest.source_group_id
     or not exists(
       select 1 from public.weekly_source_client_cycle_completions completion
       where completion.source_cycle_id=v_cycle.id
         and completion.client_id=v_manifest.client_id
         and completion.final_revision_id=v_revision.id
         and completion.completion_kind='FINAL_SOURCE' and completion.state='CURRENT'
     ) then
    raise exception 'WEEKLY_SOURCE_INVOICE_FINAL_AUTHORITY_STALE' using errcode='40001';
  end if;
  perform private.weekly_source_office_authority_v1(
    v_actor,'ADMIT_SOURCE_INVOICE',v_manifest.source_group_id,
    v_manifest.client_id,v_manifest.finalisation_week_ending
  );

  if (select pg_catalog.count(*) from public.weekly_source_manifest_movements mm
      where mm.client_manifest_id=v_manifest.id)<>v_manifest.movement_count
     or exists(
       select 1 from public.weekly_source_manifest_movements mm
       join public.weekly_source_billing_movements movement
         on movement.id=mm.billing_movement_id
       where mm.client_manifest_id=v_manifest.id
         and (mm.movement_hash is distinct from movement.movement_economic_hash
           or movement.final_revision_id is distinct from v_revision.id
           or movement.finalisation_cycle_id is distinct from v_cycle.id
           or movement.actual_client_id is distinct from v_manifest.client_id)
     ) then
    raise exception 'WEEKLY_SOURCE_INVOICE_MANIFEST_INVALID' using errcode='55000';
  end if;

  if v_manifest.invoice_state='ADMITTED' then
    if exists(
      select 1 from public.weekly_source_manifest_movements mm
      where mm.client_manifest_id=v_manifest.id
        and not exists(
          select 1 from public.weekly_source_invoice_line_bindings binding
          where binding.billing_movement_id=mm.billing_movement_id
            and binding.state='CURRENT'
        )
    ) then
      raise exception 'WEEKLY_SOURCE_INVOICE_ADMITTED_INCOMPLETE' using errcode='55000';
    end if;
    return pg_catalog.jsonb_build_object(
      'ok',true,'status',case when v_manifest.movement_count=0 then 'NO_MOVEMENTS' else 'ADMITTED' end,
      'idempotent',true,'client_manifest_id',v_manifest.id,
      'invoice_ids',coalesce((
        select pg_catalog.jsonb_agg(distinct binding.invoice_id)
        from public.weekly_source_manifest_movements mm
        join public.weekly_source_invoice_line_bindings binding
          on binding.billing_movement_id=mm.billing_movement_id and binding.state='CURRENT'
        where mm.client_manifest_id=v_manifest.id
      ),'[]'::jsonb)
    );
  end if;
  if v_manifest.invoice_state<>'READY' then
    raise exception 'WEEKLY_SOURCE_INVOICE_MANIFEST_NOT_READY' using errcode='55000';
  end if;
  if v_manifest.movement_count=0 then
    update public.weekly_source_client_manifests set invoice_state='ADMITTED'
    where id=v_manifest.id and invoice_state='READY';
    return pg_catalog.jsonb_build_object(
      'ok',true,'status','NO_MOVEMENTS','idempotent',false,
      'client_manifest_id',v_manifest.id,'invoice_ids','[]'::jsonb
    );
  end if;
  if exists(
    select 1 from public.weekly_source_manifest_movements mm
    join public.weekly_source_billing_movements movement on movement.id=mm.billing_movement_id
    where mm.client_manifest_id=v_manifest.id
      and (movement.placement_state<>'UNPLACED'
        or exists(select 1 from public.weekly_source_invoice_line_bindings binding
                  where binding.billing_movement_id=movement.id and binding.state='CURRENT'))
  ) then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVEMENT_ALREADY_ADMITTED' using errcode='55000';
  end if;

  select * into strict v_client from public.clients where id=v_manifest.client_id for share;
  select * into strict v_defaults from public.settings_defaults where id=1 for share;
  v_header:=pg_catalog.jsonb_build_object(
    'schema_version','WEEKLY_SOURCE_SELF_BILL_INVOICE_V1',
    'client_id',v_client.id,'client_name',v_client.name,
    'client_invoice_address',v_client.invoice_address,
    'client_primary_invoice_email',v_client.primary_invoice_email,
    'payment_terms_days',coalesce(v_client.payment_terms_days,0),
    'agency_name',v_defaults.agency_name,'registered_address',v_defaults.registered_address,
    'company_reg_number',v_defaults.company_reg_number,
    'vat_registration_number',v_defaults.vat_registration_number,
    'meta',pg_catalog.jsonb_build_object(
      'source','WEEKLY_FINAL_SOURCE','self_bill',true,
      'source_group_id',v_manifest.source_group_id,
      'source_cycle_id',v_manifest.source_cycle_id,
      'final_revision_id',v_manifest.final_revision_id,
      'client_manifest_id',v_manifest.id,
      'finalisation_week_ending',v_manifest.finalisation_week_ending,
      'backing_report_numbers',case when v_manifest.backing_report_number is null
        then '[]'::jsonb else pg_catalog.jsonb_build_array(v_manifest.backing_report_number) end,
      'automatic_consolidation','ONE_CLIENT_ONE_FINALISED_CYCLE'
    )
  );
  insert into public.invoices(
    type,client_id,status,status_date_utc,subtotal_ex_vat,vat_amount,total_inc_vat,
    notes,header_snapshot_json,do_not_send,document_revision,document_state,issue_state
  ) values (
    'INVOICE',v_manifest.client_id,'DRAFT',pg_catalog.transaction_timestamp(),0,0,0,
    case when v_manifest.backing_report_number is null then null
      else 'Backing report '||v_manifest.backing_report_number end,
    v_header,false,1,'STALE','NOT_STARTED'
  ) returning id into v_invoice_id;

  perform pg_catalog.set_config(
    'cloudtms.weekly_source_invoice_owner','ADMIT_SOURCE_INVOICE',true
  );

  for v_group_row in
    with member as (
      select movement.*,
             movement.source_facts_json->>'correction_presentation' as presentation_policy,
             case
               when movement.source_profile_kind<>'NHSP_TRUST_BACKING_REPORT'
                and movement.correction_unit_id is not null
                and movement.source_facts_json->>'correction_presentation'='NET_DIFFERENCE_PRESENTATION'
               then movement.correction_unit_id else movement.id
             end as presentation_group
      from public.weekly_source_manifest_movements mm
      join public.weekly_source_billing_movements movement
        on movement.id=mm.billing_movement_id
      where mm.client_manifest_id=v_manifest.id
    )
    select presentation_group,
           pg_catalog.array_agg(id order by movement_role,id) as movement_ids,
           pg_catalog.count(*)::integer as member_count,
           pg_catalog.bool_and(source_line_kind='SOURCE_FIXED_EXPENSE') as is_expense,
           pg_catalog.bool_and(presentation_policy='NET_DIFFERENCE_PRESENTATION') as requested_net
    from member
    group by presentation_group
    -- 24 section 12 requires a source-fixed expense presentation to declare the
    -- shift presentation it follows.  weekly_source_invoice_presentation_lines
    -- is IMMUTABLE_APPEND_ONLY, so that declaration has to be written at birth;
    -- every non-expense group is therefore materialised first and the expense
    -- group can then name its companion.  Within each of the two blocks the
    -- established creation order is unchanged.
    order by pg_catalog.bool_and(source_line_kind='SOURCE_FIXED_EXPENSE'),
             pg_catalog.min(created_at_utc),presentation_group
  loop
    v_ids:=v_group_row.movement_ids;
    v_count:=v_group_row.member_count;
    v_is_expense:=v_group_row.is_expense;
    v_is_net:=v_group_row.requested_net and v_count=2;
    if v_count>2
       or (v_count=2 and not v_is_net)
       or (v_is_net and exists(
         select 1 from public.weekly_source_billing_movements movement
         where movement.id=any(v_ids)
           and movement.source_profile_kind='NHSP_TRUST_BACKING_REPORT'
       ))
       or (v_is_net and (
         select pg_catalog.array_agg(movement.movement_role order by movement.movement_role)
         from public.weekly_source_billing_movements movement where movement.id=any(v_ids)
       ) not in (
         array['REPLACEMENT','REVERSAL']::text[],
         array['EXPENSE_REPLACEMENT','EXPENSE_REVERSAL']::text[]
       ))
       or (v_count=2 and (select pg_catalog.count(distinct movement.correction_unit_id)
           from public.weekly_source_billing_movements movement where movement.id=any(v_ids))<>1)
       or (select pg_catalog.count(distinct movement.work_event_id)
           from public.weekly_source_billing_movements movement where movement.id=any(v_ids))<>1
       or (select pg_catalog.count(distinct movement.final_revision_id)
           from public.weekly_source_billing_movements movement where movement.id=any(v_ids))<>1
       or (select pg_catalog.count(distinct movement.actual_client_id)
           from public.weekly_source_billing_movements movement where movement.id=any(v_ids))<>1 then
      raise exception 'WEEKLY_SOURCE_INVOICE_PRESENTATION_GROUP_INVALID' using errcode='55000';
    end if;

    select * into strict v_movement
    from public.weekly_source_billing_movements movement
    where movement.id=case when v_is_net then (
      select candidate.id from public.weekly_source_billing_movements candidate
      where candidate.id=any(v_ids)
        and candidate.movement_role in ('REPLACEMENT','EXPENSE_REPLACEMENT')
      limit 1
    ) else v_ids[1] end;
    select * into strict v_contract from public.contracts
    where id=v_movement.contract_id and client_id=v_manifest.client_id
      and candidate_id=v_movement.candidate_id for share;
    select * into strict v_candidate from public.candidates
    where id=v_movement.candidate_id for share;
    v_candidate_display:=coalesce(nullif(pg_catalog.btrim(v_candidate.display_name),''),
      nullif(pg_catalog.btrim(pg_catalog.concat_ws(' ',v_candidate.first_name,v_candidate.last_name)),''),
      v_candidate.tms_ref,v_candidate.id::text);
    v_client_display:=coalesce(nullif(pg_catalog.btrim(v_client.name),''),v_client.id::text);

    if v_is_expense then
      select source_row.* into strict v_source_row
      from public.weekly_expense_authority_generations expense_authority
      join public.weekly_source_row_expense_policy_snapshots expense_policy
        on expense_policy.id=expense_authority.row_expense_policy_snapshot_id
      join public.weekly_source_upload_rows source_row
        on source_row.id=expense_policy.upload_row_id
      where expense_authority.id=case
        when v_movement.movement_role='EXPENSE_REVERSAL' then (
          select prior_movement.expense_authority_generation_id
          from public.weekly_source_billing_movements prior_movement
          where prior_movement.id=v_movement.prior_movement_id
        ) else v_movement.expense_authority_generation_id end;
      v_work_date:=v_source_row.work_date;
      v_start:=v_source_row.start_at_local;
      v_end:=v_source_row.end_at_local;
      v_break:=coalesce(v_source_row.break_minutes,0);
      v_reference:=v_source_row.external_source_key;
    elsif v_movement.source_profile_kind='NHSP_TRUST_BACKING_REPORT' then
      select * into strict v_source_row from public.weekly_source_upload_rows
      where id=v_movement.nhsp_upload_row_id;
      v_work_date:=v_source_row.work_date;
      v_start:=v_source_row.start_at_local;
      v_end:=v_source_row.end_at_local;
      v_break:=v_source_row.break_minutes;
      v_reference:=v_source_row.external_source_key;
    else
      select snapshot.* into strict v_snapshot
      from public.weekly_source_state_transitions transition_row
      join public.weekly_source_final_snapshot_lines snapshot
        on snapshot.id=case
          when v_is_net or v_movement.movement_role in (
            'POSITIVE','REPLACEMENT','EXPENSE_POSITIVE','EXPENSE_REPLACEMENT'
          ) then transition_row.new_snapshot_line_id
          else transition_row.previous_snapshot_line_id end
      where transition_row.final_revision_id=v_manifest.final_revision_id
        and transition_row.work_event_id=v_movement.work_event_id;
      v_work_date:=v_snapshot.work_date;
      v_start:=v_snapshot.start_at_local;
      v_end:=v_snapshot.end_at_local;
      v_break:=v_snapshot.break_minutes;
      v_reference:=v_snapshot.external_event_identity;
    end if;

    v_mapping_fingerprint:=v_movement.mapping_rate_policy_fingerprint;
    v_price_fingerprint:=case when v_count=1 then v_movement.price_check_fingerprint end;

    select
      coalesce(pg_catalog.sum((movement.canonical_pay_vector_json#>>'{hours,day}')::numeric),0),
      coalesce(pg_catalog.sum((movement.canonical_pay_vector_json#>>'{hours,night}')::numeric),0),
      coalesce(pg_catalog.sum((movement.canonical_pay_vector_json#>>'{hours,sat}')::numeric),0),
      coalesce(pg_catalog.sum((movement.canonical_pay_vector_json#>>'{hours,sun}')::numeric),0),
      coalesce(pg_catalog.sum((movement.canonical_pay_vector_json#>>'{hours,bh}')::numeric),0),
      pg_catalog.sum(movement.total_pay_ex_vat),
      pg_catalog.sum(movement.invoice_presentation_charge_pence)::numeric/100,
      pg_catalog.sum(movement.calculated_comparison_charge_pence),
      case when pg_catalog.count(movement.source_validation_charge_pence)=pg_catalog.count(*)
        then pg_catalog.sum(movement.source_validation_charge_pence) end,
      pg_catalog.sum(movement.vat_amount),pg_catalog.sum(movement.total_inc_vat),
      pg_catalog.min(movement.vat_rate_pct),
      case when pg_catalog.count(distinct movement.price_check_result)=1
        then pg_catalog.min(movement.price_check_result) else 'NOT_APPLICABLE' end,
      private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_INVOICE_PRESENTATION_SOURCE_V1',
        pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
          'movement_id',movement.id,
          'movement_economic_hash',pg_catalog.encode(movement.movement_economic_hash,'hex')
        ) order by movement.id)
      )
    into v_hours_day,v_hours_night,v_hours_sat,v_hours_sun,v_hours_bh,
         v_total_pay,v_total_charge,v_calc_pence,v_source_pence,v_vat,v_total,
         v_vat_rate,v_price_result,v_source_hash
    from public.weekly_source_billing_movements movement where movement.id=any(v_ids);
    if v_is_expense then
      v_hours_day:=0;v_hours_night:=0;v_hours_sat:=0;v_hours_sun:=0;v_hours_bh:=0;
      v_pay_day:=null;v_pay_night:=null;v_pay_sat:=null;v_pay_sun:=null;v_pay_bh:=null;
      v_charge_day:=null;v_charge_night:=null;v_charge_sat:=null;v_charge_sun:=null;v_charge_bh:=null;
    else
      v_pay_day:=(v_movement.canonical_pay_vector_json#>>'{rates,day}')::numeric;
      v_pay_night:=(v_movement.canonical_pay_vector_json#>>'{rates,night}')::numeric;
      v_pay_sat:=(v_movement.canonical_pay_vector_json#>>'{rates,sat}')::numeric;
      v_pay_sun:=(v_movement.canonical_pay_vector_json#>>'{rates,sun}')::numeric;
      v_pay_bh:=(v_movement.canonical_pay_vector_json#>>'{rates,bh}')::numeric;
      v_charge_day:=(v_movement.canonical_charge_vector_json#>>'{rates,day}')::numeric;
      v_charge_night:=(v_movement.canonical_charge_vector_json#>>'{rates,night}')::numeric;
      v_charge_sat:=(v_movement.canonical_charge_vector_json#>>'{rates,sat}')::numeric;
      v_charge_sun:=(v_movement.canonical_charge_vector_json#>>'{rates,sun}')::numeric;
      v_charge_bh:=(v_movement.canonical_charge_vector_json#>>'{rates,bh}')::numeric;
    end if;
    v_line_kind:=case
      when v_is_expense then 'SOURCE_FIXED_EXPENSE'
      when v_is_net then 'NON_NHSP_DIFFERENCE'
      when v_movement.movement_role='REPLACEMENT' then 'SOURCE_REPLACEMENT'
      when v_movement.movement_role='REVERSAL' then 'GENERATED_HISTORICAL_REVERSAL'
      else 'SOURCE_ORDINARY' end;
    v_origin_kind:=case
      when v_is_expense then 'SOURCE_FIXED_EXPENSE'
      when v_movement.source_profile_kind='NHSP_TRUST_BACKING_REPORT' then 'NHSP_PHYSICAL_ROW'
      when v_movement.source_profile_kind='HEALTHROSTER_ACTUAL_ROWS' then 'HEALTHROSTER_TRANSITION'
      else 'GENERIC_TRANSITION' end;
    v_correction_role:=case
      when v_is_net then 'NET_DIFFERENCE'
      when v_movement.movement_role in ('REVERSAL','EXPENSE_REVERSAL') then 'REVERSAL'
      when v_movement.movement_role in ('REPLACEMENT','EXPENSE_REPLACEMENT') then 'REPLACEMENT'
      else null end;
    v_old_id:=null;v_new_id:=null;
    select id into v_old_id from public.weekly_source_billing_movements
      where id=any(v_ids) and movement_role in ('REVERSAL','EXPENSE_REVERSAL') limit 1;
    select id into v_new_id from public.weekly_source_billing_movements
      where id=any(v_ids) and movement_role in ('REPLACEMENT','EXPENSE_REPLACEMENT') limit 1;
    v_description:=case when v_is_expense then 'Source expense'
      else pg_catalog.concat_ws(' · ',coalesce(v_contract.role,'Shift'),
        pg_catalog.to_char(v_work_date,'Dy DD Mon YYYY'),
        pg_catalog.to_char(v_start,'HH24:MI')||'–'||pg_catalog.to_char(v_end,'HH24:MI'),
        case when v_break>0 then v_break::text||' min break' end) end;
    v_presentation_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_INVOICE_PRESENTATION_V1',
      pg_catalog.jsonb_build_object(
        'client_manifest_id',v_manifest.id,'movement_ids',pg_catalog.to_jsonb(v_ids),
        'line_kind',v_line_kind,'source_shift_group_id',v_movement.work_event_id,
        'work_date',v_work_date,'start_at_local',v_start,'end_at_local',v_end,
        'break_minutes',v_break,'hours',pg_catalog.jsonb_build_array(
          v_hours_day,v_hours_night,v_hours_sat,v_hours_sun,v_hours_bh),
        'total_pay',v_total_pay,'total_charge',v_total_charge,
        'vat_rate',v_vat_rate,'vat',v_vat,'total',v_total,
        'charge_acceptance_id',v_movement.charge_acceptance_id,
        'source_hash',pg_catalog.encode(v_source_hash,'hex')
      )
    );
    insert into public.weekly_source_invoice_presentation_lines(
      billing_movement_id,client_manifest_id,final_revision_id,
      original_finalisation_cycle_id,line_kind,origin_kind,correction_root_id,
      correction_role,old_internal_movement_id,new_internal_movement_id,
      client_id,candidate_id,contract_id,work_event_id,source_shift_group_id,
      role_snapshot,band_snapshot,candidate_display_snapshot,client_display_snapshot,
      work_date,start_at_local,end_at_local,break_minutes,booking_reference_snapshot,
      description_snapshot,hours_day,hours_night,hours_sat,hours_sun,hours_bh,
      pay_day,pay_night,pay_sat,pay_sun,pay_bh,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      total_pay_ex_vat,total_charge_ex_vat,calculated_comparison_charge_pence,
      source_validation_charge_pence,invoice_presentation_charge_pence,
      margin_ex_vat,vat_rate_pct,vat_amount,total_inc_vat,price_check_result,
      price_check_profile,charge_acceptance_id,mapping_rate_policy_fingerprint,source_hash,
      amount_authority,presentation_hash,companion_presentation_line_id
    ) values (
      v_movement.id,v_manifest.id,v_manifest.final_revision_id,v_manifest.source_cycle_id,
      v_line_kind,v_origin_kind,v_movement.correction_unit_id,v_correction_role,
      v_old_id,v_new_id,v_manifest.client_id,v_movement.candidate_id,v_movement.contract_id,
      v_movement.work_event_id,v_movement.work_event_id,v_contract.role,v_contract.band,
      v_candidate_display,v_client_display,v_work_date,v_start,v_end,v_break,v_reference,
      v_description,v_hours_day,v_hours_night,v_hours_sat,v_hours_sun,v_hours_bh,
      v_pay_day,v_pay_night,v_pay_sat,v_pay_sun,v_pay_bh,
      v_charge_day,v_charge_night,v_charge_sat,v_charge_sun,v_charge_bh,
      v_total_pay,v_total_charge,v_calc_pence,v_source_pence,(v_total_charge*100)::bigint,
      v_total_charge-v_total_pay,v_vat_rate,v_vat,v_total,v_price_result,
      case when v_movement.source_profile_kind='NHSP_TRUST_BACKING_REPORT'
        then 'NHSP_TWO_COMPONENT_PENCE_V1' else null end,
      v_movement.charge_acceptance_id,
      v_mapping_fingerprint,v_source_hash,
      case when v_movement.source_profile_kind='NHSP_TRUST_BACKING_REPORT' or v_is_expense
        then 'VALIDATED_SOURCE_PENCE' else 'CLOUDTMS_CALCULATION' end,
      v_presentation_hash,
      -- 24 section 12: "a source-fixed expense declared as part of the same
      -- source presentation follows its defined companion relationship; no
      -- unrelated expense or work-event line moves accidentally."  Declared
      -- only when exactly one shift presentation of the same work event and the
      -- same correction role exists in this manifest, so the relationship is
      -- never guessed; null otherwise and the expense then moves on its own.
      -- PostgreSQL has no min(uuid), so the text form is aggregated and cast
      -- back after the having clause has proved the single-value cardinality.
      case when v_is_expense then (
        select pg_catalog.min(shift_line.id::text)::uuid
        from public.weekly_source_invoice_presentation_lines shift_line
        where shift_line.client_manifest_id=v_manifest.id
          and shift_line.work_event_id=v_movement.work_event_id
          and shift_line.line_kind<>'SOURCE_FIXED_EXPENSE'
          and shift_line.correction_role is not distinct from v_correction_role
        having pg_catalog.count(*)=1
      ) end
    ) returning id into v_presentation_id;

    insert into public.invoice_lines(
      invoice_id,timesheet_id,booking_id,description,
      hours_day,hours_night,hours_sat,hours_sun,hours_bh,
      pay_day,pay_night,pay_sat,pay_sun,pay_bh,
      charge_day,charge_night,charge_sat,charge_sun,charge_bh,
      total_pay_ex_vat,total_charge_ex_vat,margin_ex_vat,vat_rate_pct,
      vat_amount,total_inc_vat,meta_json,source_key
    ) values (
      v_invoice_id,v_movement.invoice_timesheet_id,v_reference,v_description,
      v_hours_day,v_hours_night,v_hours_sat,v_hours_sun,v_hours_bh,
      v_pay_day,v_pay_night,v_pay_sat,v_pay_sun,v_pay_bh,
      v_charge_day,v_charge_night,v_charge_sat,v_charge_sun,v_charge_bh,
      v_total_pay,v_total_charge,v_total_charge-v_total_pay,v_vat_rate,v_vat,v_total,
      pg_catalog.jsonb_build_object(
        'line_type','WEEKLY_FINAL_SOURCE','presentation_line_id',v_presentation_id,
        'source_shift_group_id',v_movement.work_event_id,
        'client_manifest_id',v_manifest.id,'movement_ids',pg_catalog.to_jsonb(v_ids)
      ),'WEEKLY_SOURCE:'||v_presentation_id::text
    ) returning id into v_invoice_line_id;

    foreach v_member_id in array v_ids loop
      select * into strict v_member from public.weekly_source_billing_movements
      where id=v_member_id;
      v_materialised_hash:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_INVOICE_BINDING_V1',pg_catalog.jsonb_build_object(
          'movement_id',v_member.id,'presentation_line_id',v_presentation_id,
          'invoice_line_id',v_invoice_line_id,'invoice_id',v_invoice_id,
          'manifest_hash',pg_catalog.encode(v_manifest.manifest_hash,'hex'),'binding_version',1
        )
      );
      insert into public.weekly_source_invoice_line_bindings(
        billing_movement_id,presentation_line_id,invoice_line_id,invoice_id,
        original_final_revision_id,original_cycle_id,client_id,correction_root_id,
        correction_role,manifest_hash,materialised_line_hash,binding_version,state
      ) values (
        v_member.id,v_presentation_id,v_invoice_line_id,v_invoice_id,
        v_manifest.final_revision_id,v_manifest.source_cycle_id,v_manifest.client_id,
        v_member.correction_unit_id,v_correction_role,v_manifest.manifest_hash,
        v_materialised_hash,1,'CURRENT'
      ) returning id into v_binding_id;
      v_placement_hash:=private.weekly_source_sha256_jsonb_v1(
        'WEEKLY_SOURCE_INVOICE_PLACEMENT_V1',pg_catalog.jsonb_build_object(
          'movement_id',v_member.id,'invoice_id',v_invoice_id,
          'invoice_line_id',v_invoice_line_id,'placement_revision',1,
          'placement_reason','AUTOMATIC_SOURCE_BATCH'
        )
      );
      insert into public.weekly_source_invoice_placements(
        billing_movement_id,source_shift_group_id,invoice_id,invoice_line_id,
        placement_revision,placement_state,is_current,original_automatic_cycle_id,
        current_target_cycle_id,placement_reason,actor_user_id,placement_hash
      ) values (
        v_member.id,v_member.work_event_id,v_invoice_id,v_invoice_line_id,1,
        'PLACED',true,v_manifest.source_cycle_id,v_manifest.source_cycle_id,
        'AUTOMATIC_SOURCE_BATCH',v_actor,v_placement_hash
      );
      update public.weekly_source_billing_movements
      set placement_state='PLACED' where id=v_member.id and placement_state='UNPLACED';
      if not found then
        raise exception 'WEEKLY_SOURCE_INVOICE_MOVEMENT_CAS_LOST' using errcode='40001';
      end if;
    end loop;
  end loop;

  insert into public.weekly_source_expense_materialisations(
    expense_authority_generation_id,billing_movement_id,
    invoice_presentation_line_id,expense_lineage_hash,state
  )
  select movement.expense_authority_generation_id,
         (pg_catalog.array_agg(
           movement.id
           order by (movement.movement_role not in (
             'EXPENSE_POSITIVE','EXPENSE_REPLACEMENT'
           )),movement.id
         ))[1],
         (pg_catalog.array_agg(
           binding.presentation_line_id
           order by (movement.movement_role not in (
             'EXPENSE_POSITIVE','EXPENSE_REPLACEMENT'
           )),movement.id
         ))[1],
         private.weekly_source_sha256_jsonb_v1(
           'WEEKLY_SOURCE_EXPENSE_INVOICE_MATERIALISATION_V1',
           pg_catalog.jsonb_build_object(
             'expense_authority_generation_id',movement.expense_authority_generation_id,
             'movement_ids',pg_catalog.jsonb_agg(movement.id order by movement.id),
             'presentation_ids',pg_catalog.jsonb_agg(binding.presentation_line_id order by movement.id)
           )
         ),'MATERIALISED'
  from public.weekly_source_manifest_movements mm
  join public.weekly_source_billing_movements movement on movement.id=mm.billing_movement_id
  join public.weekly_source_invoice_line_bindings binding
    on binding.billing_movement_id=movement.id and binding.state='CURRENT'
  where mm.client_manifest_id=v_manifest.id
    and movement.expense_authority_generation_id is not null
  group by movement.expense_authority_generation_id;

  perform private.weekly_source_invoice_header_refresh_v1(v_invoice_id);
  perform public.invoice_recompute_totals(v_invoice_id);
  perform private.weekly_source_invoice_allocation_assert_v1(v_invoice_id);
  if exists(
    select 1 from public.weekly_source_manifest_movements mm
    where mm.client_manifest_id=v_manifest.id
      and not exists(
        select 1 from public.weekly_source_invoice_line_bindings binding
        where binding.billing_movement_id=mm.billing_movement_id and binding.state='CURRENT'
      )
  ) then
    raise exception 'WEEKLY_SOURCE_INVOICE_MANIFEST_NOT_FULLY_ADMITTED' using errcode='55000';
  end if;
  update public.weekly_source_client_manifests set invoice_state='ADMITTED'
  where id=v_manifest.id and invoice_state='READY';
  if not found then
    raise exception 'WEEKLY_SOURCE_INVOICE_MANIFEST_CAS_LOST' using errcode='40001';
  end if;
  perform pg_catalog.set_config('cloudtms.weekly_source_invoice_owner','',true);

  insert into public.audit_events(
    ts_utc,actor_user_id,actor_display,actor_role_at_time,object_type,
    object_id_text,action,before_json,after_json,reason
  )
  select pg_catalog.statement_timestamp(),v_actor,actor.display_name,actor.role,
         'invoices',v_invoice_id::text,'WEEKLY_SOURCE_INVOICE_ADMITTED',null,
         pg_catalog.jsonb_build_object(
           'client_manifest_id',v_manifest.id,'source_cycle_id',v_manifest.source_cycle_id,
           'client_id',v_manifest.client_id,'movement_count',v_manifest.movement_count,
           'backing_report_number',v_manifest.backing_report_number
         ),'FINAL_SOURCE_MANIFEST'
  from public.tms_users actor where actor.id=v_actor;
  if not found then
    raise exception 'WEEKLY_SOURCE_ACTOR_NOT_FOUND' using errcode='55000';
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'status','ADMITTED','idempotent',false,
    'client_manifest_id',v_manifest.id,'invoice_ids',pg_catalog.jsonb_build_array(v_invoice_id),
    'movement_count',v_manifest.movement_count
  );
end;
$function$;

-- 24 section 12 "Moving one source line between invoices", word for word:
--
--   "The selected unit is one immutable source presentation line, identified by
--    presentation-line ID or by invoice-line ID plus its expected presentation
--    hash.
--    - An NHSP physical row moves independently.
--    - A non-NHSP net presentation line moves as one indivisible presentation
--      even if it represents more than one underlying movement.
--    - A source-fixed expense declared as part of the same source presentation
--      follows its defined companion relationship; no unrelated expense or
--      work-event line moves accidentally.
--    ...
--    Moving by whole work-event ID is prohibited because it can move more than
--    the Office selected."
--
-- The move set is therefore exactly: the selected presentation line, plus every
-- source-fixed expense presentation whose declared companion is that line.
-- Nothing is ever selected by work_event_id.
create or replace function public.weekly_source_invoice_move_atomic_v1(
  p_request jsonb
) returns jsonb
language plpgsql volatile security definer
set search_path to 'public','private','pg_catalog','pg_temp'
as $function$
declare
  v_allowed_keys constant text[]:=array[
    'actor_user_id','source_invoice_id','destination_invoice_id',
    'presentation_line_id','invoice_line_id','expected_presentation_hash',
    'expected_source_document_revision',
    'expected_destination_document_revision','reason'
  ]::text[];
  v_required_keys constant text[]:=array[
    'actor_user_id','source_invoice_id','destination_invoice_id',
    'expected_presentation_hash','expected_source_document_revision',
    'expected_destination_document_revision','reason'
  ]::text[];
  v_unknown text;
  v_actor uuid;
  v_source_invoice_id uuid;
  v_destination_invoice_id uuid;
  v_presentation_line_id uuid;
  v_invoice_line_id uuid;
  v_expected_presentation_hash bytea;
  v_expected_source_revision bigint;
  v_expected_destination_revision bigint;
  v_reason text;
  v_selected public.weekly_source_invoice_presentation_lines%rowtype;
  v_presentation_ids uuid[];
  v_companion_count integer;
  v_source public.invoices%rowtype;
  v_destination public.invoices%rowtype;
  v_source_manifest public.weekly_source_client_manifests%rowtype;
  v_destination_manifest public.weekly_source_client_manifests%rowtype;
  v_event public.weekly_work_events%rowtype;
  v_source_fingerprint bytea;
  v_destination_fingerprint bytea;
  v_line_ids uuid[];
  v_line_count integer;
  v_updated_line_count integer;
  v_movement_count integer;
  v_binding_invoice_ids uuid[];
  v_binding record;
  v_placement public.weekly_source_invoice_placements%rowtype;
  v_new_binding_version integer;
  v_new_placement_revision integer;
  v_binding_hash bytea;
  v_placement_hash bytea;
  v_replay_count integer;
begin
  perform pg_catalog.set_config('lock_timeout','5s',true);
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')
       <>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object' then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_REQUEST_INVALID' using errcode='22023';
  end if;
  select key into v_unknown from pg_catalog.jsonb_object_keys(p_request) key
  where not (key=any(v_allowed_keys)) order by key limit 1;
  if v_unknown is not null then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_UNKNOWN_FIELD'
      using errcode='22023',detail=v_unknown;
  end if;
  if not (p_request ?& v_required_keys) then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_REQUEST_INVALID' using errcode='22023';
  end if;
  -- 24 section 12 is a contract over VALUES; ?& above proves only that a key
  -- EXISTS.  A key present with a JSON null passes ?&, and ->> on a JSON null
  -- yields SQL NULL, which no cast rejects, so a null would reach a predicate
  -- and be swallowed by three-valued logic.  Every required value is therefore
  -- type-gated here, before anything is parsed.  The two optional identity keys
  -- may be JSON null, which is how "not supplied" has always been expressed,
  -- but may not be any other non-string type.
  if pg_catalog.jsonb_typeof(p_request->'actor_user_id') is distinct from 'string'
     or pg_catalog.jsonb_typeof(p_request->'source_invoice_id') is distinct from 'string'
     or pg_catalog.jsonb_typeof(p_request->'destination_invoice_id') is distinct from 'string'
     or pg_catalog.jsonb_typeof(p_request->'expected_presentation_hash') is distinct from 'string'
     or pg_catalog.jsonb_typeof(p_request->'reason') is distinct from 'string'
     or pg_catalog.jsonb_typeof(p_request->'expected_source_document_revision')
          is distinct from 'number'
     or pg_catalog.jsonb_typeof(p_request->'expected_destination_document_revision')
          is distinct from 'number'
     or coalesce(pg_catalog.jsonb_typeof(p_request->'presentation_line_id'),'null')
          not in ('string','null')
     or coalesce(pg_catalog.jsonb_typeof(p_request->'invoice_line_id'),'null')
          not in ('string','null') then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_VALUE_INVALID' using errcode='22023';
  end if;
  -- Exactly one of the two identities 24 section 12 allows.
  if ((nullif(p_request->>'presentation_line_id','') is not null)::integer
     +(nullif(p_request->>'invoice_line_id','') is not null)::integer)<>1 then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_LINE_IDENTITY_REQUIRED' using errcode='22023';
  end if;
  begin
    v_actor:=(p_request->>'actor_user_id')::uuid;
    v_source_invoice_id:=(p_request->>'source_invoice_id')::uuid;
    v_destination_invoice_id:=(p_request->>'destination_invoice_id')::uuid;
    v_presentation_line_id:=nullif(p_request->>'presentation_line_id','')::uuid;
    v_invoice_line_id:=nullif(p_request->>'invoice_line_id','')::uuid;
    v_expected_source_revision:=(p_request->>'expected_source_document_revision')::bigint;
    v_expected_destination_revision:=(p_request->>'expected_destination_document_revision')::bigint;
    if coalesce(p_request->>'expected_presentation_hash','') !~ '^[0-9a-f]{64}$' then
      raise exception 'invalid presentation hash';
    end if;
    v_expected_presentation_hash:=pg_catalog.decode(
      p_request->>'expected_presentation_hash','hex'
    );
  exception when others then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_VALUE_INVALID' using errcode='22023';
  end;
  v_reason:=pg_catalog.btrim(coalesce(p_request->>'reason',''));
  if v_source_invoice_id=v_destination_invoice_id
     or coalesce(v_expected_source_revision,0)<1
     or coalesce(v_expected_destination_revision,0)<1
     or pg_catalog.char_length(v_reason) not between 1 and 1000 then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_VALUE_INVALID' using errcode='22023';
  end if;
  v_source_fingerprint:=private.weekly_source_invoice_expected_version_fingerprint_v1(
    v_source_invoice_id,v_expected_source_revision
  );
  v_destination_fingerprint:=private.weekly_source_invoice_expected_version_fingerprint_v1(
    v_destination_invoice_id,v_expected_destination_revision
  );

  -- Resolve the selected presentation line.  The invoice-line identity is the
  -- alternative 24 section 12 allows; it resolves to exactly one presentation,
  -- otherwise the selection is ambiguous and is refused.
  if v_presentation_line_id is null then
    select pg_catalog.count(distinct binding.presentation_line_id)::integer,
           pg_catalog.min(binding.presentation_line_id::text)::uuid
      into v_companion_count,v_presentation_line_id
    from public.weekly_source_invoice_line_bindings binding
    where binding.invoice_line_id=v_invoice_line_id and binding.state='CURRENT';
    if coalesce(v_companion_count,0)<>1 then
      raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_LINE_NOT_FOUND' using errcode='22023';
    end if;
  end if;
  select * into v_selected from public.weekly_source_invoice_presentation_lines
  where id=v_presentation_line_id;
  if not found then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_LINE_NOT_FOUND' using errcode='22023';
  end if;
  -- The expected presentation hash is the Office's proof that it is moving the
  -- immutable line it was shown.
  if v_selected.presentation_hash is distinct from v_expected_presentation_hash then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_PRESENTATION_HASH_MISMATCH'
      using errcode='40001';
  end if;
  if v_invoice_line_id is not null and not exists(
    select 1 from public.weekly_source_invoice_line_bindings binding
    where binding.presentation_line_id=v_selected.id and binding.state='CURRENT'
      and binding.invoice_line_id=v_invoice_line_id
  ) then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_LINE_NOT_FOUND' using errcode='22023';
  end if;
  -- A source-fixed expense follows its companion; it is never the unit of
  -- selection while it has one.  The Office moves the shift presentation and
  -- the expense goes with it.
  if v_selected.companion_presentation_line_id is not null then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_COMPANION_REQUIRED'
      using errcode='55000',detail=v_selected.companion_presentation_line_id::text;
  end if;
  select pg_catalog.array_agg(distinct member.id)
    into v_presentation_ids
  from public.weekly_source_invoice_presentation_lines member
  where member.id=v_selected.id
     or member.companion_presentation_line_id=v_selected.id;
  -- Nothing unrelated: every member is the selected line or an expense that
  -- declared it as its companion, on the same work event.
  if exists(
    select 1 from public.weekly_source_invoice_presentation_lines member
    where member.id=any(v_presentation_ids)
      and (member.work_event_id is distinct from v_selected.work_event_id
        or member.client_manifest_id is distinct from v_selected.client_manifest_id
        or (member.id<>v_selected.id and member.line_kind<>'SOURCE_FIXED_EXPENSE'))
  ) then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_COMPANION_INVALID' using errcode='55000';
  end if;

  perform 1 from public.invoices invoice
  where invoice.id=any(array[v_source_invoice_id,v_destination_invoice_id])
  order by invoice.id for update;
  if (select pg_catalog.count(*) from public.invoices invoice
      where invoice.id=any(array[v_source_invoice_id,v_destination_invoice_id]))<>2 then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_INVOICE_NOT_FOUND' using errcode='22023';
  end if;
  select * into strict v_source from public.invoices where id=v_source_invoice_id;
  select * into strict v_destination from public.invoices where id=v_destination_invoice_id;

  -- Gate 13 finding F6.  `->>` and `#>>` yield SQL NULL for an ABSENT key, and
  -- `NULL <> 'X'` is NULL, not true, so each of these four limbs evaluated to
  -- NULL and the whole `if` was not taken.  Executed on a build from empty
  -- before the change: with `schema_version` removed from either header the
  -- move was ACCEPTED and COMMITTED (`status=MOVED`); with `{meta,source}`
  -- removed it was refused only by the neighbouring G7-4 header trigger
  -- (`WEEKLY_SOURCE_INVOICE_HEADER_INVALID`), never by this guard.
  -- `is distinct from` is null-safe, so absent, JSON null and wrong value are
  -- all unsafe, exactly as the self_bill and manifest-id limbs beside them
  -- already treat them. PHD-003 intentionally has no cross-week confirmation
  -- gate: same Client plus two idle, unissued Draft invoices is the complete
  -- business compatibility rule.
  if v_source.header_snapshot_json->>'schema_version'
       is distinct from 'WEEKLY_SOURCE_SELF_BILL_INVOICE_V1'
     or v_destination.header_snapshot_json->>'schema_version'
       is distinct from 'WEEKLY_SOURCE_SELF_BILL_INVOICE_V1'
     or v_source.header_snapshot_json#>>'{meta,source}'
       is distinct from 'WEEKLY_FINAL_SOURCE'
     or v_destination.header_snapshot_json#>>'{meta,source}'
       is distinct from 'WEEKLY_FINAL_SOURCE'
     or v_source.header_snapshot_json#>>'{meta,self_bill}' is distinct from 'true'
     or v_destination.header_snapshot_json#>>'{meta,self_bill}' is distinct from 'true'
     or not pg_catalog.pg_input_is_valid(
          coalesce(v_source.header_snapshot_json#>>'{meta,client_manifest_id}',''),'uuid')
     or not pg_catalog.pg_input_is_valid(
          coalesce(v_destination.header_snapshot_json#>>'{meta,client_manifest_id}',''),'uuid') then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_SOURCE_INVOICE_REQUIRED' using errcode='55000';
  end if;
  select * into strict v_source_manifest
  from public.weekly_source_client_manifests
  where id=(v_source.header_snapshot_json#>>'{meta,client_manifest_id}')::uuid;
  select * into strict v_destination_manifest
  from public.weekly_source_client_manifests
  where id=(v_destination.header_snapshot_json#>>'{meta,client_manifest_id}')::uuid;
  select * into strict v_event from public.weekly_work_events
  where id=v_selected.work_event_id;
  if v_source.client_id is distinct from v_destination.client_id
     or v_source.client_id is distinct from v_source_manifest.client_id
     or v_destination.client_id is distinct from v_destination_manifest.client_id
     or v_source.client_id is distinct from v_event.client_id
     or v_source.client_id is distinct from v_selected.client_id
     or v_source.type::text<>'INVOICE' or v_destination.type::text<>'INVOICE' then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_SCOPE_INVALID' using errcode='55000';
  end if;

  perform private.weekly_source_office_authority_v1(
    v_actor,'MOVE_SOURCE_INVOICE',v_source_manifest.source_group_id,
    v_source.client_id,v_event.work_date
  );

  -- Where do the selected presentations currently live?  The presentation
  -- identity is immutable, so this answers the replay question without needing
  -- the bindings to still be on the source invoice.
  select pg_catalog.array_agg(distinct binding.invoice_id)
    into v_binding_invoice_ids
  from public.weekly_source_invoice_line_bindings binding
  where binding.presentation_line_id=any(v_presentation_ids)
    and binding.state='CURRENT';
  if v_binding_invoice_ids is null then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_LINE_NOT_FOUND' using errcode='22023';
  end if;

  if v_binding_invoice_ids=array[v_destination_invoice_id]::uuid[] then
    select pg_catalog.count(*)::integer into v_replay_count
    from public.weekly_source_invoice_placements placement
    join public.weekly_source_invoice_placements prior
      on prior.id=placement.prior_placement_id
    join public.weekly_source_invoice_line_bindings binding
      on binding.billing_movement_id=placement.billing_movement_id
     and binding.state='CURRENT'
    where placement.is_current
      and binding.presentation_line_id=any(v_presentation_ids)
      and placement.invoice_id=v_destination_invoice_id
      and prior.invoice_id=v_source_invoice_id
      and placement.source_invoice_version_fingerprint=v_source_fingerprint
      and placement.destination_invoice_version_fingerprint=v_destination_fingerprint
      and placement.placement_reason='OFFICE_MOVE_BETWEEN_UNISSUED_INVOICES';
    if v_replay_count>0 then
      return pg_catalog.jsonb_build_object(
        'ok',true,'status','MOVED','idempotent',true,
        'source_invoice_id',v_source_invoice_id,
        'destination_invoice_id',v_destination_invoice_id,
        'presentation_line_id',v_selected.id,
        'presentation_line_ids',pg_catalog.to_jsonb(v_presentation_ids),
        'movement_count',v_replay_count
      );
    end if;
  end if;
  if v_binding_invoice_ids<>array[v_source_invoice_id]::uuid[] then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_LINE_NOT_ON_SOURCE_INVOICE'
      using errcode='55000';
  end if;

  if v_source.document_revision is distinct from v_expected_source_revision
     or v_destination.document_revision is distinct from v_expected_destination_revision then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_STALE_REVISION' using errcode='40001';
  end if;
  if v_source.status::text<>'DRAFT' or v_destination.status::text<>'DRAFT'
     or v_source.issued_at_utc is not null or v_destination.issued_at_utc is not null
     or v_source.paid_at_utc is not null or v_destination.paid_at_utc is not null
     or v_source.active_document_operation_id is not null
     or v_destination.active_document_operation_id is not null
     or v_source.active_issue_operation_id is not null
     or v_destination.active_issue_operation_id is not null
     or pg_catalog.upper(coalesce(v_source.issue_state,'')) in (
       'VALIDATING','PREPARING_DOCUMENT','READY_TO_FINALISE'
     )
     or pg_catalog.upper(coalesce(v_destination.issue_state,'')) in (
       'VALIDATING','PREPARING_DOCUMENT','READY_TO_FINALISE'
     )
     or exists(
       select 1 from public.invoice_operation_chunks chunk
       where chunk.entity_type='INVOICE'
         and chunk.entity_id=any(array[v_source_invoice_id,v_destination_invoice_id])
         and chunk.status in ('QUEUED','RUNNING','WAITING','RETRY_WAIT','BLOCKED')
     ) then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_REQUIRES_IDLE_DRAFTS' using errcode='55000';
  end if;
  -- PHD-003: same Client and both invoices unissued are the only business
  -- compatibility restrictions. Source group, cycle, week, report, profile
  -- and original batch remain immutable lineage on the presentation but never
  -- prevent its deliberate Office placement on another same-Client Draft.

  perform private.weekly_source_invoice_allocation_assert_v1(v_source_invoice_id);
  perform private.weekly_source_invoice_allocation_assert_v1(v_destination_invoice_id);

  perform 1
  from public.weekly_source_invoice_line_bindings binding
  join public.weekly_source_billing_movements movement
    on movement.id=binding.billing_movement_id
  where binding.invoice_id=v_source_invoice_id and binding.state='CURRENT'
    and binding.presentation_line_id=any(v_presentation_ids)
  order by movement.id
  for update of binding,movement;
  select pg_catalog.count(*)::integer into v_movement_count
  from public.weekly_source_invoice_line_bindings binding
  join public.weekly_source_billing_movements movement
    on movement.id=binding.billing_movement_id
  where binding.invoice_id=v_source_invoice_id and binding.state='CURRENT'
    and binding.presentation_line_id=any(v_presentation_ids)
    and movement.placement_state='PLACED';
  if v_movement_count=0 then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_LINE_NOT_FOUND' using errcode='22023';
  end if;
  if exists(
    select 1
    from public.weekly_source_invoice_line_bindings binding
    join public.weekly_source_billing_movements movement
      on movement.id=binding.billing_movement_id
    where binding.invoice_id=v_source_invoice_id and binding.state='CURRENT'
      and binding.presentation_line_id=any(v_presentation_ids)
      and movement.placement_state<>'PLACED'
  ) then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_PLACEMENT_INVALID' using errcode='55000';
  end if;
  select pg_catalog.array_agg(lines.invoice_line_id order by lines.invoice_line_id),
         pg_catalog.count(*)::integer
    into v_line_ids,v_line_count
  from (
    select distinct binding.invoice_line_id
    from public.weekly_source_invoice_line_bindings binding
    where binding.invoice_id=v_source_invoice_id and binding.state='CURRENT'
      and binding.presentation_line_id=any(v_presentation_ids)
  ) lines;
  -- An indivisible presentation moves whole: no visible line in the move set
  -- may keep a CURRENT binding that is not part of the move set, and no member
  -- of the move set may sit on a line outside it.
  if exists(
    select 1
    from public.weekly_source_invoice_line_bindings binding
    where binding.invoice_line_id=any(v_line_ids) and binding.state='CURRENT'
      and (binding.invoice_id<>v_source_invoice_id
        or not (binding.presentation_line_id=any(v_presentation_ids)))
  ) then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_PARTIAL_LINE_REFUSED' using errcode='55000';
  end if;
  perform 1 from public.weekly_source_invoice_placements placement
  where placement.billing_movement_id in (
    select binding.billing_movement_id
    from public.weekly_source_invoice_line_bindings binding
    where binding.invoice_id=v_source_invoice_id and binding.state='CURRENT'
      and binding.invoice_line_id=any(v_line_ids)
  ) and placement.is_current
  order by placement.billing_movement_id for update;

  perform pg_catalog.set_config(
    'cloudtms.weekly_source_invoice_owner','MOVE_SOURCE_INVOICE',true
  );
  update public.invoice_lines
  set invoice_id=v_destination_invoice_id
  where id=any(v_line_ids) and invoice_id=v_source_invoice_id;
  get diagnostics v_updated_line_count=row_count;
  if v_updated_line_count is distinct from v_line_count then
    raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_LINE_CAS_LOST' using errcode='40001';
  end if;

  for v_binding in
    select binding.*,presentation.source_shift_group_id
    from public.weekly_source_invoice_line_bindings binding
    join public.weekly_source_invoice_presentation_lines presentation
      on presentation.id=binding.presentation_line_id
    where binding.invoice_id=v_source_invoice_id and binding.state='CURRENT'
      and binding.presentation_line_id=any(v_presentation_ids)
    order by binding.billing_movement_id
  loop
    update public.weekly_source_invoice_line_bindings
    set state='SUPERSEDED',superseded_at_utc=pg_catalog.statement_timestamp()
    where id=v_binding.id and state='CURRENT';
    if not found then
      raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_BINDING_CAS_LOST' using errcode='40001';
    end if;
    v_new_binding_version:=v_binding.binding_version+1;
    v_binding_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_INVOICE_BINDING_V1',pg_catalog.jsonb_build_object(
        'movement_id',v_binding.billing_movement_id,
        'presentation_line_id',v_binding.presentation_line_id,
        'invoice_line_id',v_binding.invoice_line_id,
        'invoice_id',v_destination_invoice_id,
        'manifest_hash',pg_catalog.encode(v_binding.manifest_hash,'hex'),
        'binding_version',v_new_binding_version,
        'prior_binding_id',v_binding.id
      )
    );
    insert into public.weekly_source_invoice_line_bindings(
      billing_movement_id,presentation_line_id,invoice_line_id,invoice_id,
      original_final_revision_id,original_cycle_id,client_id,correction_root_id,
      correction_role,manifest_hash,materialised_line_hash,binding_version,
      prior_binding_id,state
    ) values (
      v_binding.billing_movement_id,v_binding.presentation_line_id,
      v_binding.invoice_line_id,v_destination_invoice_id,
      v_binding.original_final_revision_id,v_binding.original_cycle_id,
      v_binding.client_id,v_binding.correction_root_id,v_binding.correction_role,
      v_binding.manifest_hash,v_binding_hash,v_new_binding_version,v_binding.id,'CURRENT'
    );

    select * into strict v_placement
    from public.weekly_source_invoice_placements placement
    where placement.billing_movement_id=v_binding.billing_movement_id
      and placement.is_current;
    if v_placement.invoice_id is distinct from v_source_invoice_id
       or v_placement.invoice_line_id is distinct from v_binding.invoice_line_id
       or v_placement.placement_state<>'PLACED' then
      raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_PLACEMENT_INVALID' using errcode='55000';
    end if;
    update public.weekly_source_invoice_placements
    set is_current=false where id=v_placement.id and is_current;
    if not found then
      raise exception 'WEEKLY_SOURCE_INVOICE_MOVE_PLACEMENT_CAS_LOST' using errcode='40001';
    end if;
    v_new_placement_revision:=v_placement.placement_revision+1;
    v_placement_hash:=private.weekly_source_sha256_jsonb_v1(
      'WEEKLY_SOURCE_INVOICE_PLACEMENT_V1',pg_catalog.jsonb_build_object(
        'movement_id',v_binding.billing_movement_id,
        'invoice_id',v_destination_invoice_id,
        'invoice_line_id',v_binding.invoice_line_id,
        'placement_revision',v_new_placement_revision,
        'placement_reason','OFFICE_MOVE_BETWEEN_UNISSUED_INVOICES',
        'prior_placement_id',v_placement.id,
        'source_invoice_version_fingerprint',pg_catalog.encode(v_source_fingerprint,'hex'),
        'destination_invoice_version_fingerprint',pg_catalog.encode(v_destination_fingerprint,'hex')
      )
    );
    insert into public.weekly_source_invoice_placements(
      billing_movement_id,source_shift_group_id,invoice_id,invoice_line_id,
      placement_revision,placement_state,is_current,original_automatic_cycle_id,
      current_target_cycle_id,placement_reason,actor_user_id,prior_placement_id,
      source_invoice_version_fingerprint,destination_invoice_version_fingerprint,
      placement_hash
    ) values (
      v_binding.billing_movement_id,v_binding.source_shift_group_id,
      v_destination_invoice_id,v_binding.invoice_line_id,
      v_new_placement_revision,'PLACED',true,v_placement.original_automatic_cycle_id,
      v_destination_manifest.source_cycle_id,'OFFICE_MOVE_BETWEEN_UNISSUED_INVOICES',
      v_actor,v_placement.id,v_source_fingerprint,v_destination_fingerprint,
      v_placement_hash
    );
  end loop;

  perform private.weekly_source_invoice_header_refresh_v1(v_source_invoice_id);
  perform private.weekly_source_invoice_header_refresh_v1(v_destination_invoice_id);
  perform public.invoice_recompute_totals(v_source_invoice_id);
  perform public.invoice_recompute_totals(v_destination_invoice_id);
  perform private.weekly_source_invoice_allocation_assert_v1(v_source_invoice_id);
  perform private.weekly_source_invoice_allocation_assert_v1(v_destination_invoice_id);
  perform pg_catalog.set_config('cloudtms.weekly_source_invoice_owner','',true);

  insert into public.audit_events(
    ts_utc,actor_user_id,actor_display,actor_role_at_time,object_type,
    object_id_text,action,before_json,after_json,reason
  )
  select pg_catalog.statement_timestamp(),v_actor,actor.display_name,actor.role,
         'weekly_source_invoice_presentation_lines',v_selected.id::text,
         'WEEKLY_SOURCE_PRESENTATION_LINE_MOVED_BETWEEN_DRAFT_INVOICES',
         pg_catalog.jsonb_build_object(
           'source_invoice_id',v_source_invoice_id,
           'source_document_revision',v_expected_source_revision,
           'presentation_line_id',v_selected.id,
           'expected_presentation_hash',
             pg_catalog.encode(v_expected_presentation_hash,'hex')
         ),
         pg_catalog.jsonb_build_object(
           'destination_invoice_id',v_destination_invoice_id,
           'destination_document_revision',v_expected_destination_revision,
           'presentation_line_ids',pg_catalog.to_jsonb(v_presentation_ids),
           'work_event_id',v_selected.work_event_id,
           'line_count',v_line_count,'movement_count',v_movement_count,
           'different_finalised_week',
             v_source_manifest.source_cycle_id is distinct from v_destination_manifest.source_cycle_id
         ),v_reason
  from public.tms_users actor where actor.id=v_actor;
  if not found then
    raise exception 'WEEKLY_SOURCE_ACTOR_NOT_FOUND' using errcode='55000';
  end if;

  return pg_catalog.jsonb_build_object(
    'ok',true,'status','MOVED','idempotent',false,
    'source_invoice_id',v_source_invoice_id,
    'destination_invoice_id',v_destination_invoice_id,
    'presentation_line_id',v_selected.id,
    'presentation_line_ids',pg_catalog.to_jsonb(v_presentation_ids),
    'line_count',v_line_count,'movement_count',v_movement_count
  );
end;
$function$;

alter function private.weekly_source_invoice_family_timesheet_ids_v1(uuid) owner to postgres;
alter function private.weekly_source_invoice_movement_only_integrity_v1(uuid) owner to postgres;
alter function private.weekly_source_invoice_line_owner_guard_v1() owner to postgres;
alter function private.weekly_source_invoice_allocation_assert_v1(uuid) owner to postgres;
alter function private.weekly_source_invoice_issue_guard_v1() owner to postgres;
alter function private.weekly_source_invoice_expected_version_fingerprint_v1(uuid,bigint) owner to postgres;
alter function private.weekly_source_invoice_header_refresh_v1(uuid) owner to postgres;
alter function public.weekly_source_invoice_admit_atomic_v1(jsonb) owner to postgres;
alter function public.weekly_source_invoice_move_atomic_v1(jsonb) owner to postgres;

revoke all on function private.weekly_source_invoice_family_timesheet_ids_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invoice_movement_only_integrity_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invoice_line_owner_guard_v1()
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invoice_allocation_assert_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invoice_issue_guard_v1()
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invoice_expected_version_fingerprint_v1(uuid,bigint)
  from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invoice_header_refresh_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_invoice_admit_atomic_v1(jsonb)
  from public,anon,authenticated;
revoke all on function public.weekly_source_invoice_move_atomic_v1(jsonb)
  from public,anon,authenticated;
grant execute on function public.weekly_source_invoice_admit_atomic_v1(jsonb)
  to service_role;
grant execute on function public.weekly_source_invoice_move_atomic_v1(jsonb)
  to service_role;

comment on function public.weekly_source_invoice_admit_atomic_v1(jsonb) is
  'Creates one DRAFT self-bill invoice for exactly one current client/finalised-cycle manifest. Only immutable source billing movements are eligible; protected pay, authorisation and query state cannot add or block a line.';

comment on function public.weekly_source_invoice_move_atomic_v1(jsonb) is
  'Moves ONE immutable source presentation line, identified by presentation-line id or by invoice-line id plus its expected presentation hash, between two idle, unissued DRAFT self-bill invoices for the same Client. An NHSP physical row moves alone; a non-NHSP net presentation moves as one indivisible presentation; a source-fixed expense follows its declared companion and nothing else moves. Moving by whole work-event id is prohibited. Source economics and lineage remain immutable. Source group, cycle, week, report, profile and original batch never prevent deliberate Office placement on another same-Client DRAFT, and no special cross-week confirmation applies.';

notify pgrst, 'reload schema';

commit;
