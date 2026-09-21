-- Rollback-only proof for the Plan 6.2 Gate 1 entitlement data contract.
--
-- Covers, in order: structure of the common current-head relation and its
-- component relation (24 section 4.3; WB-005, WB-007, WB-009), the decision
-- bundle and its post-decision component index (24 section 4.5 step 3; H2-024),
-- the pending bundle record (proof/32 section 2, section 8 step 5, section 10),
-- the frozen publication receipt with every constraint and its negatives
-- (proof/32 section 9), the per-source-row binding and the per-ROOT authorisation
-- generations it no longer carries (decision D8; proof/34 section 4;
-- proof/36 section 5.6), the approval withdrawal columns (proof/36 section 5
-- step 7), the invoice companion column (24 section 12), the RLS and ACL
-- contract for all of it, the symmetric one-penny source-charge predicate
-- (25 section 6, 03 line 417) and the comparison classes the established
-- validation-only engine emits (WP-04 handoff N8).
--
-- The write order this file follows is the order the head-publication
-- coordinator (WP-02) must use, because two of the rules are DEFERRABLE
-- INITIALLY DEFERRED constraint triggers:
--   1. insert the decision bundle revision;
--   2. insert the heads as STAGED, tagged with that bundle revision;
--   3. insert every component, tagged with the same bundle revision;
--   4. append the publication receipt;
--   5. supersede the outgoing current head, THEN activate the incoming one
--      (the committed-current indexes are bare unique indexes and cannot be
--      deferred, so activate-before-supersede is refused);
--   6. commit, at which point the inventory and receipt assertions run.
--
-- Prerequisites: supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql
-- and supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql.
--
-- The fixture rows below are the minimum legal rows each relation accepts. They
-- exist only to exercise constraints, indexes and triggers; they are not a
-- business scenario and nothing here calls a Banking Pay, Draft, execution,
-- cancellation, settlement, provider or remittance owner.

\set ON_ERROR_STOP on

\if :{?weekly_source_verification_outer_transaction}
\else
begin;
\endif
set local request.jwt.claim.role='service_role';

create function pg_temp.assert_true(p_condition boolean,p_message text)
returns void language plpgsql as $function$
begin
  if p_condition is distinct from true then
    raise exception 'ASSERTION_FAILED: %',p_message;
  end if;
end;
$function$;

-- Runs one statement, requires it to fail, and requires the exact SQLSTATE and
-- (optionally) a message fragment. A statement that succeeds is a failure of
-- the proof, not of the statement.
create function pg_temp.expect_failure(
  p_sql text,p_sqlstate text,p_needle text,p_label text
) returns void language plpgsql as $function$
declare
  v_state text;
  v_message text;
begin
  begin
    execute p_sql;
  exception when others then
    get stacked diagnostics v_state=returned_sqlstate,v_message=message_text;
    if v_state is distinct from p_sqlstate then
      raise exception 'EXPECTED_FAILURE_WRONG_SQLSTATE: % expected=% actual=% message=%',
        p_label,p_sqlstate,v_state,v_message;
    end if;
    if p_needle<>'' and pg_catalog.strpos(v_message,p_needle)=0 then
      raise exception 'EXPECTED_FAILURE_WRONG_REASON: % needle=% message=%',
        p_label,p_needle,v_message;
    end if;
    return;
  end;
  raise exception 'EXPECTED_FAILURE_DID_NOT_OCCUR: %',p_label;
end;
$function$;

-- The same, for the three DEFERRABLE INITIALLY DEFERRED constraint triggers
-- this file owns: the statement is legal on its own and only the
-- end-of-transaction assertion refuses it, so the assertion is forced early
-- inside the sub-transaction that is then rolled back, by naming exactly those
-- three constraints and restoring exactly those three.
--
-- SET CONSTRAINTS ALL IMMEDIATE is PROHIBITED in this programme and must never
-- be reintroduced here.  `ALL` fires every pending deferred event in the
-- transaction, and this build carries 22 deferrable constraints.  One of them is
-- public.trg_pay_workbench_scope_change_finalize_v1 on
-- public.banking_pay_scope_change_transactions, a Banking Pay finalisation owner
-- that takes FOR UPDATE locks, transitions the scope-change transaction's state,
-- sets the session marker cloudtms.scope_generation_finalising and drives queue
-- and counter finalisation.  Section 3 of this file seeds real Timesheets, which
-- wakes the ordinary Workbench invalidation machinery and always leaves a
-- pending event on that trigger, so `ALL` fired real money machinery early.
-- The mirror statement `SET CONSTRAINTS ALL DEFERRED` is equally prohibited: it
-- would additionally defer three initially-IMMEDIATE Banking Pay Workbench
-- foreign keys for the rest of the transaction.  Name the constraints.
create function pg_temp.expect_deferred_failure(
  p_sql text,p_needle text,p_label text
) returns void language plpgsql as $function$
declare
  v_state text;
  v_message text;
begin
  begin
    execute p_sql;
    -- The generic helper is used for both assert functions, so it names all
    -- three of this file's constraint triggers: an inventory mutation fires the
    -- head and component inventory asserts, a head mutation fires the inventory
    -- and receipt asserts.
    set constraints
      public.weekly_source_entitlement_head_inventory_assert,
      public.weekly_source_entitlement_head_component_inventory_assert,
      public.weekly_source_entitlement_head_receipt_assert immediate;
    set constraints
      public.weekly_source_entitlement_head_inventory_assert,
      public.weekly_source_entitlement_head_component_inventory_assert,
      public.weekly_source_entitlement_head_receipt_assert deferred;
  exception when others then
    get stacked diagnostics v_state=returned_sqlstate,v_message=message_text;
    if v_state is distinct from '55000' then
      raise exception 'EXPECTED_DEFERRED_FAILURE_WRONG_SQLSTATE: % actual=% message=%',
        p_label,v_state,v_message;
    end if;
    if p_needle<>'' and pg_catalog.strpos(v_message,p_needle)=0 then
      raise exception 'EXPECTED_DEFERRED_FAILURE_WRONG_REASON: % needle=% message=%',
        p_label,p_needle,v_message;
    end if;
    return;
  end;
  raise exception 'EXPECTED_DEFERRED_FAILURE_DID_NOT_OCCUR: %',p_label;
end;
$function$;

-- One entitlement component of a stored publication request (section 6's
-- `request_json`).  The canonical encoder requires the complete 27-key component
-- object and refuses an unknown key, so every key is written here and the ones
-- this minimum legal fixture does not model are explicitly null rather than
-- omitted.  Money fields travel as decimal STRINGS, not JSON numbers, because
-- the encoder refuses a number for a DEC field: a float is not a penny.
create function pg_temp.entitlement_component(
  p_ordinal integer,p_component_id text,p_kind text,p_event text,
  p_member_identity text,p_pay_ex_vat text,p_movement_id text,p_movement_group_id text
) returns jsonb language sql immutable as $function$
  select pg_catalog.jsonb_build_object(
    'component_ordinal',p_ordinal,
    'component_id',p_component_id,
    'component_kind',p_kind,
    'economic_key_type','WORK_EVENT',
    'economic_key_value',p_event,
    'component_member_identity',p_member_identity,
    'segment_id',null::text,
    'segment_key',null::text,
    'segment_stable_key',null::text,
    'work_date','2026-03-08',
    'reference_number',null::text,
    'hours_day',null::text,
    'hours_night',null::text,
    'hours_sat',null::text,
    'hours_sun',null::text,
    'hours_bh',null::text,
    'additional_code_raw',null::text,
    'unit_count',null::text,
    'unit_pay_rate',null::text,
    'unit_charge_rate',null::text,
    'expense_code',null::text,
    'pay_ex_vat',p_pay_ex_vat,
    'charge_ex_vat',null::text,
    'exclude_from_pay',false,
    'origin','SOURCE',
    'movement_id',p_movement_id,
    'movement_group_id',p_movement_group_id);
$function$;

-- ---------------------------------------------------------------------------
-- 1. Structure
-- ---------------------------------------------------------------------------
do $verify_entitlement_structure$
declare
  v_missing text;
  v_index oid;
begin
  foreach v_missing in array array[
    'public.weekly_source_entitlement_decision_bundles',
    'public.weekly_source_entitlement_heads',
    'public.weekly_source_entitlement_head_components',
    'public.weekly_source_pending_entitlement_bundles',
    'public.weekly_source_root_authorisations',
    'private.weekly_source_entitlement_publication_receipts'
  ] loop
    if pg_catalog.to_regclass(v_missing) is null then
      raise exception 'missing relation %',v_missing;
    end if;
  end loop;

  -- Every column the contract and the proofs name, with its exact type and
  -- nullability. A later package that quietly relaxes one of these fails here.
  select pg_catalog.string_agg(expected.relation||'.'||expected.column_name,',' order by expected.relation,expected.column_name)
    into v_missing
  from (values
    -- S1 head (24 section 4.3; 27 section 5.2)
    ('public.weekly_source_entitlement_heads','authority_kind','text',true),
    ('public.weekly_source_entitlement_heads','candidate_id','uuid',true),
    ('public.weekly_source_entitlement_heads','contract_id','uuid',true),
    ('public.weekly_source_entitlement_heads','week_ending_date','date',true),
    ('public.weekly_source_entitlement_heads','root_timesheet_id','uuid',true),
    ('public.weekly_source_entitlement_heads','root_family_booking_id','text',true),
    ('public.weekly_source_entitlement_heads','root_timesheet_version','integer',true),
    ('public.weekly_source_entitlement_heads','head_revision','bigint',true),
    ('public.weekly_source_entitlement_heads','prior_head_id','uuid',false),
    ('public.weekly_source_entitlement_heads','state','text',true),
    ('public.weekly_source_entitlement_heads','certified_zero','boolean',true),
    ('public.weekly_source_entitlement_heads','component_count','integer',true),
    ('public.weekly_source_entitlement_heads','entitlement_digest','bytea',true),
    ('public.weekly_source_entitlement_heads','inventory_digest','bytea',true),
    ('public.weekly_source_entitlement_heads','source_generation_digest','bytea',true),
    ('public.weekly_source_entitlement_heads','publication_receipt_digest','bytea',false),
    ('public.weekly_source_entitlement_heads','scope_change_tx_token','uuid',false),
    -- F2: the bundle tag is mandatory on a head, so the H2-024 index below can
    -- bind on its components.
    ('public.weekly_source_entitlement_heads','decision_bundle_id','uuid',true),
    ('public.weekly_source_entitlement_heads','bundle_revision','bigint',true),
    ('public.weekly_source_entitlement_heads','superseded_by_head_id','uuid',false),
    -- S1 components (WB-005)
    ('public.weekly_source_entitlement_head_components','head_id','uuid',true),
    ('public.weekly_source_entitlement_head_components','component_ordinal','integer',true),
    ('public.weekly_source_entitlement_head_components','component_id','uuid',true),
    ('public.weekly_source_entitlement_head_components','pay_ex_vat','numeric',true),
    ('public.weekly_source_entitlement_head_components','exclude_from_pay','boolean',true),
    ('public.weekly_source_entitlement_head_components','component_sha256','bytea',true),
    -- S2 movement identity (24 section 4.5 step 3)
    ('public.weekly_source_entitlement_head_components','decision_bundle_id','uuid',true),
    ('public.weekly_source_entitlement_head_components','bundle_revision','bigint',true),
    ('public.weekly_source_entitlement_head_components','movement_id','uuid',false),
    ('public.weekly_source_entitlement_head_components','movement_group_id','uuid',false),
    ('public.weekly_source_entitlement_decision_bundles','decision_bundle_id','uuid',true),
    ('public.weekly_source_entitlement_decision_bundles','bundle_revision','bigint',true),
    ('public.weekly_source_entitlement_decision_bundles','request_digest','bytea',true),
    ('public.weekly_source_entitlement_decision_bundles','proposed_head_ids','uuid[]',true),
    ('public.weekly_source_entitlement_decision_bundles','state','text',true),
    -- S3 pending bundle (proof/32 section 2, section 8 step 5)
    ('public.weekly_source_pending_entitlement_bundles','candidate_id','uuid',true),
    ('public.weekly_source_pending_entitlement_bundles','member_root_ids','uuid[]',true),
    ('public.weekly_source_pending_entitlement_bundles','member_family_booking_ids','text[]',true),
    ('public.weekly_source_pending_entitlement_bundles','member_root_versions','integer[]',true),
    ('public.weekly_source_pending_entitlement_bundles','request_digest','bytea',true),
    ('public.weekly_source_pending_entitlement_bundles','decision_id','uuid',true),
    ('public.weekly_source_pending_entitlement_bundles','decided_by_user_id','uuid',true),
    ('public.weekly_source_pending_entitlement_bundles','proposed_head_ids','uuid[]',true),
    ('public.weekly_source_pending_entitlement_bundles','pending_revision','bigint',true),
    ('public.weekly_source_pending_entitlement_bundles','state','text',true),
    ('public.weekly_source_pending_entitlement_bundles','lease_owner','text',false),
    ('public.weekly_source_pending_entitlement_bundles','lease_token','uuid',false),
    ('public.weekly_source_pending_entitlement_bundles','lease_worker_run_id','uuid',false),
    ('public.weekly_source_pending_entitlement_bundles','lease_expires_at_utc','timestamp with time zone',false),
    ('public.weekly_source_pending_entitlement_bundles','next_check_at_utc','timestamp with time zone',false),
    ('public.weekly_source_pending_entitlement_bundles','technical_failure_count','integer',true),
    ('public.weekly_source_pending_entitlement_bundles','last_census_json','jsonb',false),
    ('public.weekly_source_pending_entitlement_bundles','released_receipt_id','uuid',false),
    ('public.weekly_source_pending_entitlement_bundles','released_receipt_digest','bytea',false),
    ('public.weekly_source_pending_entitlement_bundles','released_by_worker_id','text',false),
    ('public.weekly_source_pending_entitlement_bundles','released_by_worker_run_id','uuid',false),
    ('public.weekly_source_pending_entitlement_bundles','released_at_utc','timestamp with time zone',false),
    -- S4 receipt (proof/32 section 9)
    ('private.weekly_source_entitlement_publication_receipts','decision_bundle_id','uuid',true),
    ('private.weekly_source_entitlement_publication_receipts','pending_bundle_id','uuid',false),
    ('private.weekly_source_entitlement_publication_receipts','bundle_revision','bigint',true),
    ('private.weekly_source_entitlement_publication_receipts','request_digest','bytea',true),
    ('private.weekly_source_entitlement_publication_receipts','publication_mode','text',true),
    ('private.weekly_source_entitlement_publication_receipts','candidate_id','uuid',true),
    ('private.weekly_source_entitlement_publication_receipts','member_root_ids','uuid[]',true),
    ('private.weekly_source_entitlement_publication_receipts','member_family_booking_ids','text[]',true),
    ('private.weekly_source_entitlement_publication_receipts','member_root_versions','integer[]',true),
    ('private.weekly_source_entitlement_publication_receipts','head_ids','uuid[]',true),
    ('private.weekly_source_entitlement_publication_receipts','scope_change_tx_token','uuid',true),
    ('private.weekly_source_entitlement_publication_receipts','decision_id','uuid',true),
    ('private.weekly_source_entitlement_publication_receipts','decided_by_user_id','uuid',true),
    ('private.weekly_source_entitlement_publication_receipts','released_by_worker_id','text',false),
    ('private.weekly_source_entitlement_publication_receipts','released_by_worker_run_id','uuid',false),
    ('private.weekly_source_entitlement_publication_receipts','census_json','jsonb',true),
    ('private.weekly_source_entitlement_publication_receipts','proof_json','jsonb',true),
    -- S5 lineage (proof/34 section 4)
    -- S5 after decision D8: the lineage relation keeps only the binding-time
    -- facts; the authorisation record is per root, below.
    ('public.weekly_source_row_timesheet_lineages','family_booking_id','text',true),
    ('public.weekly_source_row_timesheet_lineages','timesheet_version','integer',true),
    -- D8 root authorisation (proof/34 section 4 write-set table)
    ('public.weekly_source_root_authorisations','root_timesheet_id','uuid',true),
    ('public.weekly_source_root_authorisations','family_booking_id','text',true),
    ('public.weekly_source_root_authorisations','timesheet_version','integer',true),
    ('public.weekly_source_root_authorisations','authorisation_generation','integer',true),
    ('public.weekly_source_root_authorisations','authorised_row_signature','text',true),
    ('public.weekly_source_root_authorisations','current_entitlement_head_id','uuid',false),
    ('public.weekly_source_root_authorisations','authorised_by_user_id','uuid',true),
    ('public.weekly_source_root_authorisations','authorised_at_utc','timestamp with time zone',true),
    ('public.weekly_source_root_authorisations','withdrawn_at_utc','timestamp with time zone',false),
    ('public.weekly_source_root_authorisations','withdrawn_by_user_id','uuid',false),
    -- S6 approval withdrawal (proof/36 section 5 step 7)
    ('public.weekly_exceptional_payment_approvals','withdrawn_at_utc','timestamp with time zone',false),
    ('public.weekly_exceptional_payment_approvals','withdrawn_by_user_id','uuid',false),
    ('public.weekly_exceptional_payment_approvals','withdrawal_kind','text',false),
    -- S10 invoice companion (24 section 12)
    ('public.weekly_source_invoice_presentation_lines','companion_presentation_line_id','uuid',false),
    -- WP-04 handoff N8: exactly these three become nullable, nothing else.
    ('public.weekly_timesheet_source_comparisons','work_event_id','uuid',false),
    ('public.weekly_timesheet_source_comparisons','candidate_start_at_local','timestamp without time zone',false),
    ('public.weekly_timesheet_source_comparisons','candidate_end_at_local','timestamp without time zone',false),
    ('public.weekly_timesheet_source_comparisons','timesheet_id','uuid',true),
    ('public.weekly_timesheet_source_comparisons','candidate_break_minutes','integer',true)
  ) as expected(relation,column_name,type_name,is_not_null)
  where not exists(
    select 1
    from pg_catalog.pg_attribute a
    where a.attrelid=pg_catalog.to_regclass(expected.relation)
      and a.attname=expected.column_name
      and a.attnum>0
      and not a.attisdropped
      and pg_catalog.format_type(a.atttypid,null)=expected.type_name
      and a.attnotnull=expected.is_not_null
  );
  if v_missing is not null then
    raise exception 'entitlement column contract differs: %',v_missing;
  end if;

  -- WB-007 and WB-013: adjustments are never copied into a head, so the
  -- component relation must not carry an adjustment identity at all.
  if exists(
    select 1 from pg_catalog.pg_attribute a
    where a.attrelid='public.weekly_source_entitlement_head_components'::pg_catalog.regclass
      and a.attname in ('adjustment_id','ts_pay_adjustment_id')
      and a.attnum>0 and not a.attisdropped
  ) then
    raise exception 'head components must not carry an adjustment identity';
  end if;

  -- No stored generated column may exist on a relation the ACL closure guards
  -- with the IMMUTABLE_FACTS_WITH_LIFECYCLE allowlist: NEW carries NULL for a
  -- generated column inside a BEFORE trigger, so the allowlist comparison would
  -- refuse every legitimate lifecycle update.
  if exists(
    select 1
    from private._weekly_source_acl_table_contract_v1() c
    join pg_catalog.pg_attribute a
      on a.attrelid=pg_catalog.to_regclass(pg_catalog.format('public.%I',c.table_name))
    where c.record_class='IMMUTABLE_FACTS_WITH_LIFECYCLE'
      and a.attnum>0 and not a.attisdropped and a.attgenerated<>''
  ) then
    raise exception 'a lifecycle-guarded relation carries a generated column';
  end if;

  -- The complete CHECK inventory of every new relation, compared as a set of
  -- normalised definitions. Dropping, weakening or adding any one of them fails
  -- here even when no negative test happens to exercise it.
  select pg_catalog.string_agg(missing.definition,' | ' order by missing.definition)
    into v_missing
  from (
    select expected.relation,expected.definition
    from (values
      ('public.weekly_source_entitlement_heads','CHECK ((certified_zero = (component_count = 0)))'),
      ('public.weekly_source_entitlement_heads','CHECK (((head_revision = 1) = (prior_head_id IS NULL)))'),
      ('public.weekly_source_entitlement_heads','CHECK (((state = ''STAGED''::text) = ((committed_at_utc IS NULL) AND (superseded_at_utc IS NULL))))'),
      ('public.weekly_source_entitlement_heads','CHECK (((state = ''COMMITTED_CURRENT''::text) = ((committed_at_utc IS NOT NULL) AND (superseded_at_utc IS NULL))))'),
      ('public.weekly_source_entitlement_heads','CHECK (((state = ''SUPERSEDED''::text) = (superseded_at_utc IS NOT NULL)))'),
      -- SUPERSEDED BY HANDOVER 2 round-5 ruling A3 (package WP-07c), which
      -- rejects the permanent refusal and requires the Office change-of-mind
      -- withdrawal to supersede a committed head atomically "with an explicit
      -- withdrawal reason and immutable predecessor link".  A withdrawal has no
      -- successor head, so the old
      --     CHECK (((superseded_at_utc IS NULL) = (superseded_by_head_id IS NULL)))
      -- which made a successor head mandatory is replaced by the constraint
      -- below.  It is STRICTER, not weaker: it still forbids a supersession
      -- with no authority, it forbids BOTH authorities at once, and it forces
      -- the withdrawal branch to state its reason.  Migration
      -- 18092026_0900_weekly_source_withdrawal_supersession.sql.
      ('public.weekly_source_entitlement_heads','CHECK ((((superseded_at_utc IS NULL) = ((superseded_by_head_id IS NULL) AND (superseded_by_withdrawal_id IS NULL))) AND (NOT ((superseded_by_head_id IS NOT NULL) AND (superseded_by_withdrawal_id IS NOT NULL))) AND ((superseded_reason IS NULL) OR (superseded_reason = ANY (ARRAY[''ENTITLEMENT_HEAD_PUBLICATION''::text, ''FIRST_AUTHORISATION_WITHDRAWN''::text]))) AND ((superseded_by_withdrawal_id IS NULL) OR (superseded_reason = ''FIRST_AUTHORISATION_WITHDRAWN''::text)) AND ((superseded_by_head_id IS NULL) OR (superseded_reason IS DISTINCT FROM ''FIRST_AUTHORISATION_WITHDRAWN''::text)) AND ((superseded_at_utc IS NOT NULL) OR (superseded_reason IS NULL))))'),
      ('public.weekly_source_entitlement_heads','CHECK ((superseded_by_head_id IS DISTINCT FROM id))'),
      ('public.weekly_source_entitlement_heads','CHECK (((publication_receipt_digest IS NOT NULL) = (committed_at_utc IS NOT NULL)))'),
      ('public.weekly_source_entitlement_heads','CHECK (((scope_change_tx_token IS NOT NULL) = (committed_at_utc IS NOT NULL)))'),
      ('private.weekly_source_entitlement_publication_receipts','CHECK (((publication_mode = ''DEFERRED''::text) = (pending_bundle_id IS NOT NULL)))'),
      ('private.weekly_source_entitlement_publication_receipts','CHECK (((publication_mode = ''DEFERRED''::text) = ((released_by_worker_id IS NOT NULL) AND (released_by_worker_run_id IS NOT NULL))))'),
      ('private.weekly_source_entitlement_publication_receipts','CHECK ((cardinality(member_root_ids) >= 1))'),
      ('private.weekly_source_entitlement_publication_receipts','CHECK ((cardinality(member_root_ids) = cardinality(member_family_booking_ids)))'),
      ('private.weekly_source_entitlement_publication_receipts','CHECK ((cardinality(member_root_ids) = cardinality(member_root_versions)))'),
      ('private.weekly_source_entitlement_publication_receipts','CHECK ((cardinality(member_root_ids) = cardinality(head_ids)))'),
      ('private.weekly_source_entitlement_publication_receipts','CHECK ((array_position(member_root_ids, NULL::uuid) IS NULL))'),
      ('private.weekly_source_entitlement_publication_receipts','CHECK ((array_position(member_family_booking_ids, NULL::text) IS NULL))'),
      ('private.weekly_source_entitlement_publication_receipts','CHECK ((array_position(member_root_versions, NULL::integer) IS NULL))'),
      ('private.weekly_source_entitlement_publication_receipts','CHECK ((array_position(head_ids, NULL::uuid) IS NULL))'),
      ('private.weekly_source_entitlement_publication_receipts','CHECK (weekly_source_uuid_array_is_distinct_v1(member_root_ids))'),
      ('private.weekly_source_entitlement_publication_receipts','CHECK (weekly_source_uuid_array_is_distinct_v1(head_ids))'),
      ('private.weekly_source_entitlement_publication_receipts','CHECK (((publication_mode <> ''DEFERRED''::text) OR ((census_json <> ''{}''::jsonb) AND (proof_json <> ''{}''::jsonb))))'),
      -- Decision D8, the per-root authorisation record (proof/34 section 4).
      ('public.weekly_source_root_authorisations','CHECK (((withdrawn_at_utc IS NULL) = (withdrawn_by_user_id IS NULL)))'),
      ('public.weekly_source_root_authorisations','CHECK (((withdrawn_at_utc IS NULL) OR (current_entitlement_head_id IS NULL)))'),
      ('public.weekly_source_root_authorisations','CHECK (((withdrawn_at_utc IS NULL) OR (withdrawn_at_utc >= authorised_at_utc)))'),
      ('public.weekly_source_root_authorisations','CHECK ((authorisation_generation >= 1))'),
      ('public.weekly_source_root_authorisations','CHECK ((timesheet_version >= 1))')
    ) as expected(relation,definition)
    where not exists(
      select 1
      from pg_catalog.pg_constraint c
      where c.conrelid=pg_catalog.to_regclass(expected.relation)
        and c.contype='c'
        and pg_catalog.replace(pg_catalog.pg_get_constraintdef(c.oid),'private.','')=expected.definition
    )
  ) as missing;
  if v_missing is not null then
    raise exception 'a required CHECK on the Gate 1 relations is missing or reworded: %',v_missing;
  end if;

  -- 24 section 4.3: at most one committed current head per root, ACROSS BOTH
  -- authority kinds, and the rule binds on BOTH root identities: the trimmed
  -- family booking id and the physical root Timesheet id.
  v_index:='public.weekly_source_entitlement_heads_committed_current_uq'::pg_catalog.regclass;
  perform pg_temp.assert_true(
    (select i.indisunique from pg_catalog.pg_index i where i.indexrelid=v_index),
    'committed-current family index must be unique');
  perform pg_temp.assert_true(
    (select pg_catalog.pg_get_indexdef(v_index)) like '%(btrim(root_family_booking_id))%',
    'committed-current family index must key on the trimmed family booking id only');
  perform pg_temp.assert_true(
    pg_catalog.strpos((select pg_catalog.pg_get_indexdef(v_index)),'authority_kind')=0,
    'committed-current family index must not include authority_kind');
  perform pg_temp.assert_true(
    (select pg_catalog.pg_get_indexdef(v_index)) like '%WHERE (state = ''COMMITTED_CURRENT''::text)%',
    'committed-current family index must be restricted to COMMITTED_CURRENT');

  v_index:='public.weekly_source_entitlement_heads_committed_root_uq'::pg_catalog.regclass;
  perform pg_temp.assert_true(
    (select i.indisunique from pg_catalog.pg_index i where i.indexrelid=v_index),
    'committed-current physical-root index must be unique');
  perform pg_temp.assert_true(
    (select pg_catalog.pg_get_indexdef(v_index)) like '%(root_timesheet_id)%',
    'committed-current physical-root index must key on root_timesheet_id');
  perform pg_temp.assert_true(
    pg_catalog.strpos((select pg_catalog.pg_get_indexdef(v_index)),'authority_kind')=0,
    'committed-current physical-root index must not include authority_kind');
  perform pg_temp.assert_true(
    (select pg_catalog.pg_get_indexdef(v_index)) like '%WHERE (state = ''COMMITTED_CURRENT''::text)%',
    'committed-current physical-root index must be restricted to COMMITTED_CURRENT');

  perform pg_temp.assert_true(
    (select i.indisunique from pg_catalog.pg_index i
      where i.indexrelid='public.weekly_source_entitlement_heads_family_revision_uq'::pg_catalog.regclass),
    'the monotonic (family, revision) index must be unique');

  -- Decision D8: the per-root authorisation keys.
  v_index:='public.weekly_source_root_authorisations_live_uq'::pg_catalog.regclass;
  perform pg_temp.assert_true(
    (select i.indisunique from pg_catalog.pg_index i where i.indexrelid=v_index),
    'the live-generation index must be unique');
  perform pg_temp.assert_true(
    (select pg_catalog.pg_get_indexdef(v_index)) like '%(root_timesheet_id)%',
    'the live-generation index must key on root_timesheet_id');
  perform pg_temp.assert_true(
    (select pg_catalog.pg_get_indexdef(v_index)) like '%WHERE (withdrawn_at_utc IS NULL)%',
    'the live-generation index must be restricted to generations that are not withdrawn');
  if not exists(
    select 1 from pg_catalog.pg_constraint c
    where c.conrelid='public.weekly_source_root_authorisations'::pg_catalog.regclass
      and c.contype='u'
      and pg_catalog.pg_get_constraintdef(c.oid)='UNIQUE (root_timesheet_id, authorisation_generation)'
  ) then
    raise exception 'the per-root generation uniqueness rule is missing';
  end if;
  if not exists(
    select 1 from pg_catalog.pg_constraint c
    where c.conrelid='public.weekly_source_row_timesheet_lineages'::pg_catalog.regclass
      and c.contype='u'
      and pg_catalog.pg_get_constraintdef(c.oid)='UNIQUE (row_resolution_id)'
  ) then
    raise exception 'the per-source-row binding uniqueness rule is missing';
  end if;
  foreach v_missing in array array[
    'weekly_source_root_authorisation_identity',
    'weekly_source_root_authorisation_withdrawal_once'
  ] loop
    if not exists(
      select 1 from pg_catalog.pg_trigger t
      where t.tgrelid='public.weekly_source_root_authorisations'::pg_catalog.regclass
        and t.tgname=v_missing and not t.tgisinternal
    ) then
      raise exception 'root-authorisation guard % is missing',v_missing;
    end if;
  end loop;

  -- 24 section 4.5 step 3 / H2-024: a unique INDEX over
  -- (decision_bundle_id,bundle_revision,component_id), never a CHECK, and it
  -- must stay PLAIN and NON-PARTIAL. indpred is the assertion the first version
  -- of this file was missing.
  v_index:='public.weekly_source_entitlement_head_components_bundle_component_uq'::pg_catalog.regclass;
  perform pg_temp.assert_true(
    (select i.indisunique from pg_catalog.pg_index i where i.indexrelid=v_index),
    'the H2-024 index must be unique');
  perform pg_temp.assert_true(
    (select i.indpred is null from pg_catalog.pg_index i where i.indexrelid=v_index),
    'the H2-024 index must be NON-PARTIAL: a predicate narrows the approved rule');
  perform pg_temp.assert_true(
    (select i.indexprs is null from pg_catalog.pg_index i where i.indexrelid=v_index),
    'the H2-024 index must key on plain columns');
  perform pg_temp.assert_true(
    (select i.indnullsnotdistinct from pg_catalog.pg_index i where i.indexrelid=v_index),
    'the H2-024 index must treat NULL keys as equal so the rule survives a relaxed NOT NULL');
  perform pg_temp.assert_true(
    (select pg_catalog.string_agg(a.attname,',' order by k.ordinality)
     from pg_catalog.pg_index i
     cross join lateral pg_catalog.unnest(i.indkey::smallint[]) with ordinality as k(attnum,ordinality)
     join pg_catalog.pg_attribute a on a.attrelid=i.indrelid and a.attnum=k.attnum
     where i.indexrelid=v_index)='decision_bundle_id,bundle_revision,component_id',
    'the H2-024 index must key on exactly decision_bundle_id, bundle_revision, component_id in that order');
  if exists(
    select 1 from pg_catalog.pg_constraint c
    where c.conrelid='public.weekly_source_entitlement_head_components'::pg_catalog.regclass
      and c.contype='c'
      and pg_catalog.pg_get_constraintdef(c.oid) like '%component_id%'
      and pg_catalog.pg_get_constraintdef(c.oid) like '%decision_bundle_id%'
  ) then
    raise exception 'the post-decision component rule must not be expressed as a CHECK';
  end if;

  -- The component's bundle tag must be its own head's bundle tag.
  if not exists(
    select 1 from pg_catalog.pg_constraint c
    where c.conrelid='public.weekly_source_entitlement_head_components'::pg_catalog.regclass
      and c.contype='f'
      and c.conname='weekly_source_entitlement_head_components_head_bundle_fk'
  ) then
    raise exception 'the component-to-head bundle identity foreign key is missing';
  end if;

  -- movement_group_id groups components that move together and is NON-unique.
  if exists(
    select 1
    from pg_catalog.pg_index i
    join pg_catalog.pg_attribute a
      on a.attrelid=i.indrelid and a.attnum=any(i.indkey::smallint[])
    where i.indrelid='public.weekly_source_entitlement_head_components'::pg_catalog.regclass
      and i.indisunique
      and a.attname='movement_group_id'
  ) then
    raise exception 'movement_group_id must never carry a unique index';
  end if;

  -- The helpers, the receipt guards and the four constraint/row guards.
  foreach v_missing in array array[
    'private.weekly_source_uuid_array_is_distinct_v1(uuid[])',
    'private.weekly_source_entitlement_publication_receipt_immutable_v1()',
    'private.weekly_source_entitlement_head_root_identity_v1()',
    'private.weekly_source_entitlement_head_inventory_assert_v1()',
    'private.weekly_source_entitlement_head_receipt_assert_v1()',
    'private.weekly_source_root_authorisation_identity_v1()',
    'private.weekly_source_root_authorisation_withdrawal_once_v1()'
  ] loop
    if pg_catalog.to_regprocedure(v_missing) is null then
      raise exception 'missing routine %',v_missing;
    end if;
  end loop;
  foreach v_missing in array array[
    'weekly_source_entitlement_publication_receipt_immutable',
    'weekly_source_entitlement_publication_receipt_truncate_guard'
  ] loop
    if not exists(
      select 1 from pg_catalog.pg_trigger t
      where t.tgrelid='private.weekly_source_entitlement_publication_receipts'::pg_catalog.regclass
        and t.tgname=v_missing and not t.tgisinternal
    ) then
      raise exception 'receipt trigger % is missing',v_missing;
    end if;
  end loop;
  if not exists(
    select 1 from pg_catalog.pg_trigger t
    where t.tgrelid='public.weekly_source_entitlement_heads'::pg_catalog.regclass
      and t.tgname='weekly_source_entitlement_head_root_identity' and not t.tgisinternal
  ) then
    raise exception 'the head root-identity guard is missing';
  end if;
  -- Both inventory/receipt assertions must be DEFERRABLE INITIALLY DEFERRED
  -- constraint triggers, because the receipt is appended after the heads.
  if (
    select pg_catalog.count(*)
    from pg_catalog.pg_trigger t
    where t.tgname in (
      'weekly_source_entitlement_head_inventory_assert',
      'weekly_source_entitlement_head_component_inventory_assert',
      'weekly_source_entitlement_head_receipt_assert')
      and not t.tgisinternal
      and t.tgconstraint<>0
      and t.tgdeferrable
      and t.tginitdeferred
  )<>3 then
    raise exception 'the Gate 1 deferred constraint triggers are not all installed as deferred';
  end if;

  -- proof/32 section 9 uniqueness, both rules.
  if not exists(
    select 1 from pg_catalog.pg_constraint c
    where c.conrelid='private.weekly_source_entitlement_publication_receipts'::pg_catalog.regclass
      and c.contype='u'
      and pg_catalog.pg_get_constraintdef(c.oid)='UNIQUE (decision_bundle_id, bundle_revision, request_digest)'
  ) or not exists(
    select 1 from pg_catalog.pg_constraint c
    where c.conrelid='private.weekly_source_entitlement_publication_receipts'::pg_catalog.regclass
      and c.contype='u'
      and pg_catalog.pg_get_constraintdef(c.oid)='UNIQUE (request_digest)'
  ) then
    raise exception 'the receipt uniqueness rules are not both installed';
  end if;

  -- 24 section 12: the companion link is directional and self-exclusive.
  if not exists(
    select 1 from pg_catalog.pg_constraint c
    where c.conrelid='public.weekly_source_invoice_presentation_lines'::pg_catalog.regclass
      and c.contype='c'
      and pg_catalog.pg_get_constraintdef(c.oid) like '%companion_presentation_line_id IS NULL%'
      and pg_catalog.pg_get_constraintdef(c.oid) like '%SOURCE_FIXED_EXPENSE%'
  ) or not exists(
    select 1 from pg_catalog.pg_constraint c
    where c.conrelid='public.weekly_source_invoice_presentation_lines'::pg_catalog.regclass
      and c.contype='c'
      and pg_catalog.pg_get_constraintdef(c.oid) like '%companion_presentation_line_id IS DISTINCT FROM id%'
  ) then
    raise exception 'the invoice companion constraints are not installed';
  end if;
  if not exists(
    select 1 from pg_catalog.pg_constraint c
    where c.conrelid='public.weekly_source_invoice_presentation_lines'::pg_catalog.regclass
      and c.contype='f'
      and pg_catalog.pg_get_constraintdef(c.oid)
          like 'FOREIGN KEY (companion_presentation_line_id) REFERENCES weekly_source_invoice_presentation_lines(id)%'
  ) then
    raise exception 'the invoice companion self reference is missing';
  end if;

  -- proof/36 section 5 step 7: the withdrawal kind is exactly one value.
  if not exists(
    select 1 from pg_catalog.pg_constraint c
    where c.conrelid='public.weekly_exceptional_payment_approvals'::pg_catalog.regclass
      and c.contype='c'
      and pg_catalog.pg_get_constraintdef(c.oid) like '%FIRST_AUTHORISATION_WITHDRAWN%'
  ) then
    raise exception 'the approval withdrawal_kind constraint is missing';
  end if;
end
$verify_entitlement_structure$;

-- ---------------------------------------------------------------------------
-- 2. RLS and ACL
-- ---------------------------------------------------------------------------
do $verify_entitlement_acl$
declare
  v_relation text;
  v_function regprocedure;
  v_owner oid:=(current_user::pg_catalog.regrole)::oid;
begin
  foreach v_relation in array array[
    'public.weekly_source_entitlement_decision_bundles',
    'public.weekly_source_entitlement_heads',
    'public.weekly_source_entitlement_head_components',
    'public.weekly_source_pending_entitlement_bundles',
    'public.weekly_source_root_authorisations',
    'private.weekly_source_entitlement_publication_receipts'
  ] loop
    if not exists(
      select 1 from pg_catalog.pg_class c
      where c.oid=pg_catalog.to_regclass(v_relation)
        and c.relrowsecurity and c.relforcerowsecurity
    ) then
      raise exception 'RLS is not enabled and forced for %',v_relation;
    end if;
    if not exists(
      select 1 from pg_catalog.pg_class c
      where c.oid=pg_catalog.to_regclass(v_relation) and c.relowner=v_owner
    ) then
      raise exception 'unexpected owner for %',v_relation;
    end if;
    if not exists(
      select 1 from pg_catalog.pg_policy p
      where p.polrelid=pg_catalog.to_regclass(v_relation)
        and p.polname='cloudtms_miget_service_owner_all'
    ) then
      raise exception 'the Miget owner policy is missing on %',v_relation;
    end if;
    if exists(
      select 1 from pg_catalog.pg_policy p
      where p.polrelid=pg_catalog.to_regclass(v_relation)
        and p.polname<>'cloudtms_miget_service_owner_all'
    ) then
      raise exception 'an unexpected policy exists on %',v_relation;
    end if;
    if pg_catalog.has_table_privilege('anon',v_relation,'select')
       or pg_catalog.has_table_privilege('anon',v_relation,'insert')
       or pg_catalog.has_table_privilege('anon',v_relation,'update')
       or pg_catalog.has_table_privilege('anon',v_relation,'delete')
       or pg_catalog.has_table_privilege('authenticated',v_relation,'select')
       or pg_catalog.has_table_privilege('authenticated',v_relation,'insert')
       or pg_catalog.has_table_privilege('authenticated',v_relation,'update')
       or pg_catalog.has_table_privilege('authenticated',v_relation,'delete') then
      raise exception 'a browser role can reach % directly',v_relation;
    end if;
    if pg_catalog.has_table_privilege('service_role',v_relation,'select')
       or pg_catalog.has_table_privilege('service_role',v_relation,'insert')
       or pg_catalog.has_table_privilege('service_role',v_relation,'update')
       or pg_catalog.has_table_privilege('service_role',v_relation,'delete') then
      raise exception 'service_role retains direct table access to %',v_relation;
    end if;
  end loop;

  -- proof/32 section 9: service_role is not even named in the receipt policy.
  if exists(
    select 1
    from pg_catalog.pg_policy p
    cross join lateral pg_catalog.unnest(p.polroles) as r(roleoid)
    where p.polrelid='private.weekly_source_entitlement_publication_receipts'::pg_catalog.regclass
      and r.roleoid<>v_owner
  ) then
    raise exception 'the receipt policy must name the owner role only';
  end if;

  foreach v_function in array array[
    'private.weekly_source_uuid_array_is_distinct_v1(uuid[])'::pg_catalog.regprocedure,
    'private.weekly_source_entitlement_publication_receipt_immutable_v1()'::pg_catalog.regprocedure,
    'private.weekly_source_entitlement_head_root_identity_v1()'::pg_catalog.regprocedure,
    'private.weekly_source_entitlement_head_inventory_assert_v1()'::pg_catalog.regprocedure,
    'private.weekly_source_entitlement_head_receipt_assert_v1()'::pg_catalog.regprocedure,
    'private.weekly_source_root_authorisation_identity_v1()'::pg_catalog.regprocedure,
    'private.weekly_source_root_authorisation_withdrawal_once_v1()'::pg_catalog.regprocedure
  ] loop
    if pg_catalog.has_function_privilege('anon',v_function,'execute')
       or pg_catalog.has_function_privilege('authenticated',v_function,'execute')
       or pg_catalog.has_function_privilege('service_role',v_function,'execute') then
      raise exception 'a non-owner role can execute %',v_function;
    end if;
    if not exists(
      select 1 from pg_catalog.pg_proc p where p.oid=v_function and p.proowner=v_owner
    ) then
      raise exception 'unexpected owner for %',v_function;
    end if;
  end loop;

  -- ACL record classes and lifecycle allowlists for everything this package
  -- registered or reclassified.
  if not exists(
    select 1 from private._weekly_source_acl_table_contract_v1() c
    where c.table_name='weekly_source_entitlement_heads'
      and c.record_class='IMMUTABLE_FACTS_WITH_LIFECYCLE'
  ) or not exists(
    select 1 from private._weekly_source_acl_table_contract_v1() c
    where c.table_name='weekly_source_entitlement_decision_bundles'
      and c.record_class='IMMUTABLE_FACTS_WITH_LIFECYCLE'
  ) or not exists(
    select 1 from private._weekly_source_acl_table_contract_v1() c
    where c.table_name='weekly_source_entitlement_head_components'
      and c.record_class='IMMUTABLE_APPEND_ONLY'
  ) or not exists(
    select 1 from private._weekly_source_acl_table_contract_v1() c
    where c.table_name='weekly_source_pending_entitlement_bundles'
      and c.record_class='IMMUTABLE_FACTS_WITH_LIFECYCLE'
  ) or not exists(
    select 1 from private._weekly_source_acl_table_contract_v1() c
    where c.table_name='weekly_source_row_timesheet_lineages'
      and c.record_class='IMMUTABLE_APPEND_ONLY'
  ) or not exists(
    select 1 from private._weekly_source_acl_table_contract_v1() c
    where c.table_name='weekly_source_root_authorisations'
      and c.record_class='IMMUTABLE_FACTS_WITH_LIFECYCLE'
  ) or not exists(
    select 1 from private._weekly_source_acl_table_contract_v1() c
    where c.table_name='weekly_exceptional_payment_approvals'
      and c.record_class='IMMUTABLE_FACTS_WITH_LIFECYCLE'
  ) then
    raise exception 'the ACL record classes for Gate 1 are not registered';
  end if;

  -- Decision D8: the per-source-row binding is append-only again and carries no
  -- lifecycle column at all; the authorisation lifecycle lives on the root.
  if exists(
    select 1
    from private._weekly_source_acl_lifecycle_column_contract_v1() l
    where l.table_name='weekly_source_row_timesheet_lineages'
  ) then
    raise exception 'the per-source-row binding must carry no lifecycle column';
  end if;

  if (
    select pg_catalog.count(*)
    from private._weekly_source_acl_lifecycle_column_contract_v1() l
    where l.table_name='weekly_source_root_authorisations'
      and l.column_name in ('current_entitlement_head_id','withdrawn_at_utc',
                            'withdrawn_by_user_id','updated_at_utc')
  )<>4 or (
    select pg_catalog.count(*)
    from private._weekly_source_acl_lifecycle_column_contract_v1() l
    where l.table_name='weekly_source_root_authorisations'
  )<>4 then
    raise exception 'root-authorisation lifecycle columns are not exactly the proof/34 set';
  end if;

  if (
    select pg_catalog.count(*)
    from private._weekly_source_acl_lifecycle_column_contract_v1() l
    where l.table_name='weekly_exceptional_payment_approvals'
      and l.column_name in ('withdrawn_at_utc','withdrawn_by_user_id','withdrawal_kind')
  )<>3 or (
    select pg_catalog.count(*)
    from private._weekly_source_acl_lifecycle_column_contract_v1() l
    where l.table_name='weekly_exceptional_payment_approvals'
  )<>3 then
    raise exception 'approval lifecycle columns are not exactly the three withdrawal columns';
  end if;

  if (
    select pg_catalog.count(*)
    from private._weekly_source_acl_lifecycle_column_contract_v1() l
    where l.table_name='weekly_source_entitlement_heads'
      -- The last two are HANDOVER 2 round-5 ruling A3 step 3 (package WP-07c):
      -- the withdrawal supersession's explicit reason and its immutable
      -- predecessor link.  They are activation lifecycle exactly as the four
      -- before them; no economic fact, digest, inventory count or
      -- certified-zero flag became movable.
      and l.column_name in ('state','committed_at_utc','superseded_at_utc',
                            'superseded_by_head_id','publication_receipt_digest','scope_change_tx_token',
                            'superseded_reason','superseded_by_withdrawal_id')
  )<>8 or (
    select pg_catalog.count(*)
    from private._weekly_source_acl_lifecycle_column_contract_v1() l
    where l.table_name='weekly_source_entitlement_heads'
  )<>8 then
    raise exception 'head lifecycle columns must be the activation lifecycle only';
  end if;

  -- proof/32 section 2: decision_id and decided_by_user_id are copied once from
  -- the immutable accepted decision, and section 7 revalidates the stored member
  -- set and digest, so none of those may be a lifecycle column.
  if exists(
    select 1
    from private._weekly_source_acl_lifecycle_column_contract_v1() l
    where l.table_name='weekly_source_pending_entitlement_bundles'
      and l.column_name in ('decision_bundle_id','bundle_revision','candidate_id','member_root_ids',
                            'member_family_booking_ids','member_root_versions','request_digest',
                            'source_revision_digest','contract_choice_digest','decision_id',
                            'decided_by_user_id','proposed_head_ids','created_at_utc')
  ) then
    raise exception 'a pending-bundle money identity column is registered as a lifecycle column';
  end if;
end
$verify_entitlement_acl$;

-- ---------------------------------------------------------------------------
-- 3. Minimum legal fixture
-- ---------------------------------------------------------------------------
insert into public.settings_defaults(
  id,candidate_manager_email_templates_sha256,candidate_home_announcement_sha256
) values (
  1,pg_catalog.decode(pg_catalog.repeat('01',32),'hex'),
  pg_catalog.decode(pg_catalog.repeat('02',32),'hex')
) on conflict (id) do update set
  candidate_manager_email_templates_sha256=excluded.candidate_manager_email_templates_sha256,
  candidate_home_announcement_sha256=excluded.candidate_home_announcement_sha256;
insert into public.tms_users(id,email,role,is_active,password_hash)
values ('b0000000-0000-4000-8000-000000000001','entitlement@example.test','admin',true,'not-a-login');
insert into public.clients(id,name)
values ('b0000000-0000-4000-8000-000000000002','Entitlement Client');
insert into public.client_settings(client_id,vat_rate_pct,effective_from)
values ('b0000000-0000-4000-8000-000000000002',20,'2026-01-01');
insert into public.candidates(id,display_name)
values ('b0000000-0000-4000-8000-000000000003','Entitlement Candidate');
insert into public.contracts(
  id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
  weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr
) values (
  'b0000000-0000-4000-8000-000000000004',
  'b0000000-0000-4000-8000-000000000003',
  'b0000000-0000-4000-8000-000000000002',
  '2026-01-01','2026-12-31','PAYE','{}'::jsonb,'HEALTHROSTER',true,true,true,true
),(
  'b0000000-0000-4000-8000-000000000008',
  'b0000000-0000-4000-8000-000000000003',
  'b0000000-0000-4000-8000-000000000002',
  '2026-01-01','2026-12-31','PAYE','{}'::jsonb,'HEALTHROSTER',true,true,true,true
);
insert into public.contract_weeks(id,contract_id,week_ending_date)
values (
  'b0000000-0000-4000-8000-000000000005',
  'b0000000-0000-4000-8000-000000000004','2026-03-08'
);
insert into public.timesheets(
  timesheet_id,booking_id,version,is_current,status,sheet_scope,submission_mode,line_type,
  occupant_key_norm,hospital_norm,ward_norm,job_title_norm,shift_label_norm,
  week_ending_date,contract_id,actual_schedule_json,qr_payload_json,
  is_adjustment,created_at,updated_at
) values (
  'b0000000-0000-4000-8000-000000000006','WSENT-0001',1,true,
  'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
  'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
  'entitlement-occupant','entitlement-hospital','entitlement-ward','entitlement-role','weekly-0',
  '2026-03-08','b0000000-0000-4000-8000-000000000004','[]'::jsonb,'{}'::jsonb,false,
  pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()
),(
  'b0000000-0000-4000-8000-000000000007','WSENT-0002',1,true,
  'RECEIVED'::public.timesheet_status_enum,'WEEKLY'::public.timesheet_scope_enum,
  'MANUAL'::public.submission_mode_enum,'HOURS'::public.timesheet_line_type_enum,
  'entitlement-occupant-b','entitlement-hospital','entitlement-ward','entitlement-role','weekly-0',
  '2026-03-08','b0000000-0000-4000-8000-000000000004','[]'::jsonb,'{}'::jsonb,false,
  pg_catalog.statement_timestamp(),pg_catalog.statement_timestamp()
);

-- ---------------------------------------------------------------------------
-- 4. One committed current head per root, on both root identities
-- ---------------------------------------------------------------------------
do $verify_entitlement_heads$
declare
  v_bundle uuid:='b0000000-0000-4000-8000-0000000000b1';
  v_head uuid:='b0000000-0000-4000-8000-0000000000e1';
  v_token uuid:='b0000000-0000-4000-8000-0000000000f1';
  v_digest bytea:=pg_catalog.decode(pg_catalog.repeat('14',32),'hex');
begin
  -- Step 1: the accepted Office decision bundle revision.
  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,bundle_kind,
    source_root_family_booking_id,source_root_timesheet_id,source_contract_id,
    decision_id,decided_by_user_id,publication_mode,request_digest,
    source_revision_digest,contract_choice_digest,before_inventory_digest,
    proposed_head_ids,state
  ) values (
    v_bundle,1,'b0000000-0000-4000-8000-0000000000aa',
    'b0000000-0000-4000-8000-000000000003','2026-03-08','SINGLE_ROOT',
    'WSENT-0001','b0000000-0000-4000-8000-000000000006','b0000000-0000-4000-8000-000000000004',
    'b0000000-0000-4000-8000-0000000000d1','b0000000-0000-4000-8000-000000000001','IMMEDIATE',
    pg_catalog.decode(pg_catalog.repeat('11',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('12',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('13',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('15',32),'hex'),
    array[v_head]::uuid[],'PROPOSED'
  );

  -- Step 2: the head, staged.
  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,state,
    certified_zero,component_count,entitlement_digest,inventory_digest,
    source_generation_digest,decision_bundle_id,bundle_revision,decision_id,decided_by_user_id
  ) values (
    v_head,'LOCKED_FINAL_SOURCE','b0000000-0000-4000-8000-0000000000aa',
    'b0000000-0000-4000-8000-000000000003','b0000000-0000-4000-8000-000000000004','2026-03-08',
    'b0000000-0000-4000-8000-000000000006','WSENT-0001',1,1,'STAGED',
    false,2,pg_catalog.decode(pg_catalog.repeat('16',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('17',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('18',32),'hex'),
    v_bundle,1,'b0000000-0000-4000-8000-0000000000d1','b0000000-0000-4000-8000-000000000001'
  );

  -- F1: the stored family identity and version must be the physical root's own.
  perform pg_temp.expect_failure($sql$
    insert into public.weekly_source_entitlement_heads(
      authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
      root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,state,
      certified_zero,component_count,entitlement_digest,inventory_digest,
      source_generation_digest,decision_bundle_id,bundle_revision,decision_id,decided_by_user_id
    ) values (
      'PROTECTED','b0000000-0000-4000-8000-0000000000aa',
      'b0000000-0000-4000-8000-000000000003','b0000000-0000-4000-8000-000000000004','2026-03-08',
      'b0000000-0000-4000-8000-000000000006','wsent-0001',1,9,'STAGED',
      true,0,pg_catalog.decode(pg_catalog.repeat('19',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('1a',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('1b',32),'hex'),
      'b0000000-0000-4000-8000-0000000000b1',1,
      'b0000000-0000-4000-8000-0000000000d1','b0000000-0000-4000-8000-000000000001'
    );
  $sql$,'55000','WEEKLY_SOURCE_HEAD_ROOT_IDENTITY_MISMATCH',
  'a differently-cased family string on the real physical root');

  perform pg_temp.expect_failure($sql$
    insert into public.weekly_source_entitlement_heads(
      authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
      root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,state,
      certified_zero,component_count,entitlement_digest,inventory_digest,
      source_generation_digest,decision_bundle_id,bundle_revision,decision_id,decided_by_user_id
    ) values (
      'PROTECTED','b0000000-0000-4000-8000-0000000000aa',
      'b0000000-0000-4000-8000-000000000003','b0000000-0000-4000-8000-000000000004','2026-03-08',
      'b0000000-0000-4000-8000-000000000006','NOT-THE-BOOKING-ID',99,9,'STAGED',
      true,0,pg_catalog.decode(pg_catalog.repeat('1c',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('1d',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('1e',32),'hex'),
      'b0000000-0000-4000-8000-0000000000b1',1,
      'b0000000-0000-4000-8000-0000000000d1','b0000000-0000-4000-8000-000000000001'
    );
  $sql$,'55000','WEEKLY_SOURCE_HEAD_ROOT_IDENTITY_MISMATCH',
  'an unrelated family string and version on the real physical root');

  perform pg_temp.expect_failure($sql$
    insert into public.weekly_source_entitlement_heads(
      authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
      root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,state,
      certified_zero,component_count,entitlement_digest,inventory_digest,
      source_generation_digest,decision_bundle_id,bundle_revision,decision_id,decided_by_user_id
    ) values (
      'PROTECTED','b0000000-0000-4000-8000-0000000000aa',
      'b0000000-0000-4000-8000-000000000003','b0000000-0000-4000-8000-000000000004','2026-03-08',
      'b0000000-0000-4000-8000-000000000006','  WSENT-0001  ',1,9,'STAGED',
      true,0,pg_catalog.decode(pg_catalog.repeat('1f',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('20',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('21',32),'hex'),
      'b0000000-0000-4000-8000-0000000000b1',1,
      'b0000000-0000-4000-8000-0000000000d1','b0000000-0000-4000-8000-000000000001'
    );
  $sql$,'55000','WEEKLY_SOURCE_HEAD_ROOT_IDENTITY_MISMATCH',
  'a whitespace-padded family string on the real physical root');

  -- F2: a head must carry its bundle identity.
  perform pg_temp.expect_failure($sql$
    insert into public.weekly_source_entitlement_heads(
      authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
      root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,state,
      certified_zero,component_count,entitlement_digest,inventory_digest,
      source_generation_digest,decision_id,decided_by_user_id
    ) values (
      'PROTECTED','b0000000-0000-4000-8000-0000000000aa',
      'b0000000-0000-4000-8000-000000000003','b0000000-0000-4000-8000-000000000004','2026-03-08',
      'b0000000-0000-4000-8000-000000000007','WSENT-0002',1,9,'STAGED',
      true,0,pg_catalog.decode(pg_catalog.repeat('22',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('23',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('24',32),'hex'),
      'b0000000-0000-4000-8000-0000000000d1','b0000000-0000-4000-8000-000000000001'
    );
  $sql$,'23502','','a head with no decision bundle identity');

  -- Monotonic revision: revision 1 has no prior head, a later revision must.
  perform pg_temp.expect_failure($sql$
    insert into public.weekly_source_entitlement_heads(
      authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
      root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,state,
      certified_zero,component_count,entitlement_digest,inventory_digest,
      source_generation_digest,decision_bundle_id,bundle_revision,decision_id,decided_by_user_id
    ) values (
      'PROTECTED','b0000000-0000-4000-8000-0000000000aa',
      'b0000000-0000-4000-8000-000000000003','b0000000-0000-4000-8000-000000000004','2026-03-08',
      'b0000000-0000-4000-8000-000000000007','WSENT-0002',1,7,'STAGED',
      true,0,pg_catalog.decode(pg_catalog.repeat('25',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('26',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('27',32),'hex'),
      'b0000000-0000-4000-8000-0000000000b1',1,
      'b0000000-0000-4000-8000-0000000000d1','b0000000-0000-4000-8000-000000000001'
    );
  $sql$,'23514','','a later head revision with no prior head');

  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into public.weekly_source_entitlement_heads(
      authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
      root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,state,
      prior_head_id,certified_zero,component_count,entitlement_digest,inventory_digest,
      source_generation_digest,decision_bundle_id,bundle_revision,decision_id,decided_by_user_id
    ) values (
      'PROTECTED','b0000000-0000-4000-8000-0000000000aa',
      'b0000000-0000-4000-8000-000000000003','b0000000-0000-4000-8000-000000000004','2026-03-08',
      'b0000000-0000-4000-8000-000000000007','WSENT-0002',1,1,'STAGED',
      %L,true,0,pg_catalog.decode(pg_catalog.repeat('28',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('29',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('2a',32),'hex'),
      'b0000000-0000-4000-8000-0000000000b1',1,
      'b0000000-0000-4000-8000-0000000000d1','b0000000-0000-4000-8000-000000000001'
    );
  $sql$,v_head),'23514','','head revision 1 naming a prior head');

  -- One head per (family, revision).
  perform pg_temp.expect_failure($sql$
    insert into public.weekly_source_entitlement_heads(
      authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
      root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,state,
      certified_zero,component_count,entitlement_digest,inventory_digest,
      source_generation_digest,decision_bundle_id,bundle_revision,decision_id,decided_by_user_id
    ) values (
      'PROTECTED','b0000000-0000-4000-8000-0000000000aa',
      'b0000000-0000-4000-8000-000000000003','b0000000-0000-4000-8000-000000000004','2026-03-08',
      'b0000000-0000-4000-8000-000000000006','WSENT-0001',1,1,'STAGED',
      true,0,pg_catalog.decode(pg_catalog.repeat('2b',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('2c',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('2d',32),'hex'),
      'b0000000-0000-4000-8000-0000000000b1',1,
      'b0000000-0000-4000-8000-0000000000d1','b0000000-0000-4000-8000-000000000001'
    );
  $sql$,'23505','weekly_source_entitlement_heads_family_revision_uq',
  'a second head at the same family revision');

  -- A committed head always carries its receipt digest and its one
  -- invalidation token, and certified zero is exactly "no component remains".
  perform pg_temp.expect_failure($sql$
    insert into public.weekly_source_entitlement_heads(
      authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
      root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,state,
      certified_zero,component_count,entitlement_digest,inventory_digest,
      source_generation_digest,decision_bundle_id,bundle_revision,decision_id,decided_by_user_id,
      committed_at_utc,scope_change_tx_token
    ) values (
      'PROTECTED','b0000000-0000-4000-8000-0000000000aa',
      'b0000000-0000-4000-8000-000000000003','b0000000-0000-4000-8000-000000000004','2026-03-08',
      'b0000000-0000-4000-8000-000000000007','WSENT-0002',1,1,'COMMITTED_CURRENT',
      true,0,pg_catalog.decode(pg_catalog.repeat('2e',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('2f',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('30',32),'hex'),
      'b0000000-0000-4000-8000-0000000000b1',1,
      'b0000000-0000-4000-8000-0000000000d1','b0000000-0000-4000-8000-000000000001',
      pg_catalog.clock_timestamp(),'b0000000-0000-4000-8000-0000000000f9'
    );
  $sql$,'23514','','a committed head without its publication receipt digest');

  perform pg_temp.expect_failure($sql$
    insert into public.weekly_source_entitlement_heads(
      authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
      root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,state,
      certified_zero,component_count,entitlement_digest,inventory_digest,
      source_generation_digest,publication_receipt_digest,decision_bundle_id,bundle_revision,
      decision_id,decided_by_user_id,committed_at_utc
    ) values (
      'PROTECTED','b0000000-0000-4000-8000-0000000000aa',
      'b0000000-0000-4000-8000-000000000003','b0000000-0000-4000-8000-000000000004','2026-03-08',
      'b0000000-0000-4000-8000-000000000007','WSENT-0002',1,1,'COMMITTED_CURRENT',
      true,0,pg_catalog.decode(pg_catalog.repeat('31',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('32',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('33',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('34',32),'hex'),
      'b0000000-0000-4000-8000-0000000000b1',1,
      'b0000000-0000-4000-8000-0000000000d1','b0000000-0000-4000-8000-000000000001',
      pg_catalog.clock_timestamp()
    );
  $sql$,'23514','','a committed head with no aligned invalidation token');

  perform pg_temp.expect_failure($sql$
    insert into public.weekly_source_entitlement_heads(
      authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
      root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,state,
      certified_zero,component_count,entitlement_digest,inventory_digest,
      source_generation_digest,decision_bundle_id,bundle_revision,decision_id,decided_by_user_id
    ) values (
      'PROTECTED','b0000000-0000-4000-8000-0000000000aa',
      'b0000000-0000-4000-8000-000000000003','b0000000-0000-4000-8000-000000000004','2026-03-08',
      'b0000000-0000-4000-8000-000000000007','WSENT-0002',1,1,'STAGED',
      true,3,pg_catalog.decode(pg_catalog.repeat('35',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('36',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('37',32),'hex'),
      'b0000000-0000-4000-8000-0000000000b1',1,
      'b0000000-0000-4000-8000-0000000000d1','b0000000-0000-4000-8000-000000000001'
    );
  $sql$,'23514','','certified zero declared while components remain');

  -- Step 3: the complete component inventory, tagged with the same bundle.
  insert into public.weekly_source_entitlement_head_components(
    head_id,component_ordinal,component_id,component_kind,economic_key_type,
    economic_key_value,component_member_identity,pay_ex_vat,exclude_from_pay,origin,
    decision_bundle_id,bundle_revision,component_sha256
  ) values (
    v_head,1,'b0000000-0000-4000-8000-0000000000c1','SHIFT','WORK_EVENT','we-1','member-1',
    120.00,false,'SOURCE',v_bundle,1,pg_catalog.decode(pg_catalog.repeat('41',32),'hex')
  ),(
    v_head,2,'b0000000-0000-4000-8000-0000000000c2','EXPENSE','WORK_EVENT','we-1','member-2',
    7.50,false,'SOURCE',v_bundle,1,pg_catalog.decode(pg_catalog.repeat('42',32),'hex')
  );

  -- Step 4: the receipt, appended before the heads activate.
  insert into private.weekly_source_entitlement_publication_receipts(
    decision_bundle_id,pending_bundle_id,bundle_revision,request_digest,publication_mode,
    candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
    scope_change_tx_token,decision_id,decided_by_user_id,census_json,proof_json
  ) values (
    v_bundle,null,1,v_digest,'IMMEDIATE','b0000000-0000-4000-8000-000000000003',
    array['b0000000-0000-4000-8000-000000000006']::uuid[],array['WSENT-0001']::text[],
    array[1]::integer[],array[v_head]::uuid[],v_token,
    'b0000000-0000-4000-8000-0000000000d1','b0000000-0000-4000-8000-000000000001',
    '{}'::jsonb,'{}'::jsonb
  );

  -- Step 5: activate.
  update public.weekly_source_entitlement_heads
     set state='COMMITTED_CURRENT',committed_at_utc=pg_catalog.clock_timestamp(),
         publication_receipt_digest=v_digest,scope_change_tx_token=v_token
   where id=v_head;
  update public.weekly_source_entitlement_decision_bundles
     set state='COMMITTED',committed_at_utc=pg_catalog.clock_timestamp()
   where decision_bundle_id=v_bundle and bundle_revision=1;

  -- The state is now internally consistent, so all three deferred assertions
  -- pass.  Forcing them here also drains their pending events, so the two
  -- expect_deferred_failure calls below fail for their own mutation and not for
  -- something this block left behind.  Named, never ALL: see the prohibition on
  -- pg_temp.expect_deferred_failure above.
  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert immediate;
  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert deferred;

  -- F3: the inventory is no longer a writer assertion.
  perform pg_temp.expect_deferred_failure(pg_catalog.format($sql$
    insert into public.weekly_source_entitlement_head_components(
      head_id,component_ordinal,component_id,component_kind,economic_key_type,
      economic_key_value,component_member_identity,pay_ex_vat,exclude_from_pay,origin,
      decision_bundle_id,bundle_revision,component_sha256
    ) values (
      %L,3,'b0000000-0000-4000-8000-0000000000c8','SHIFT','WORK_EVENT','we-8','member-8',
      1.00,false,'SOURCE',%L,1,pg_catalog.decode(pg_catalog.repeat('43',32),'hex')
    );
  $sql$,v_head,v_bundle),
  'WEEKLY_SOURCE_HEAD_INVENTORY_MISMATCH','a component beyond the head''s declared inventory');

  -- F4: a committed head must be the head a real receipt published.
  perform pg_temp.expect_deferred_failure(pg_catalog.format($sql$
    update public.weekly_source_entitlement_heads
       set publication_receipt_digest=pg_catalog.decode(pg_catalog.repeat('ff',32),'hex')
     where id=%L;
  $sql$,v_head),
  'WEEKLY_SOURCE_HEAD_RECEIPT_MISSING','a committed head whose receipt digest matches no receipt');

  -- 24 section 4.3, both root identities. The guard makes the family string on a
  -- second head for this physical root necessarily identical, so the family
  -- index is what refuses it; the physical index is the second, independent key.
  perform pg_temp.expect_failure($sql$
    insert into public.weekly_source_entitlement_heads(
      authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
      root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,state,
      prior_head_id,certified_zero,component_count,entitlement_digest,inventory_digest,
      source_generation_digest,publication_receipt_digest,decision_bundle_id,bundle_revision,
      decision_id,decided_by_user_id,committed_at_utc,scope_change_tx_token
    ) values (
      'PROTECTED','b0000000-0000-4000-8000-0000000000aa',
      'b0000000-0000-4000-8000-000000000003','b0000000-0000-4000-8000-000000000004','2026-03-08',
      'b0000000-0000-4000-8000-000000000006','WSENT-0001',1,5,'COMMITTED_CURRENT',
      'b0000000-0000-4000-8000-0000000000e1',false,1,
      pg_catalog.decode(pg_catalog.repeat('44',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('45',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('46',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('47',32),'hex'),
      'b0000000-0000-4000-8000-0000000000b1',1,
      'b0000000-0000-4000-8000-0000000000d1','b0000000-0000-4000-8000-000000000001',
      pg_catalog.clock_timestamp(),'b0000000-0000-4000-8000-0000000000f8'
    );
  $sql$,'23505','weekly_source_entitlement_heads_committed_current_uq',
  'a second committed current head for one root, across authority kinds');

  -- WB-005: economic facts on a head are frozen; only the activation lifecycle
  -- may move.
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_entitlement_heads set component_count=99
     where state='COMMITTED_CURRENT';
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_FACT','rewriting a head component count');
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_entitlement_heads
       set entitlement_digest=pg_catalog.decode(pg_catalog.repeat('99',32),'hex')
     where state='COMMITTED_CURRENT';
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_FACT','rewriting a head entitlement digest');
  perform pg_temp.expect_failure($sql$
    delete from public.weekly_source_entitlement_heads;
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_RECORD','deleting a head');
  perform pg_temp.expect_failure(pg_catalog.format($sql$
    update public.weekly_source_entitlement_heads
       set state='SUPERSEDED',superseded_at_utc=pg_catalog.clock_timestamp(),
           superseded_by_head_id=%L
     where id=%L;
  $sql$,v_head,v_head),'23514','','a head superseding itself');

  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_entitlement_heads
      where state='COMMITTED_CURRENT')=1,
    'exactly one committed current head must exist for the root');
end
$verify_entitlement_heads$;

-- ---------------------------------------------------------------------------
-- 5. The decision bundle and its post-decision component index
-- ---------------------------------------------------------------------------
do $verify_entitlement_bundle$
declare
  v_bundle uuid:='b0000000-0000-4000-8000-0000000000b2';
  v_head_a uuid:='b0000000-0000-4000-8000-0000000000e2';
  v_head_b uuid:='b0000000-0000-4000-8000-0000000000e3';
  v_prior uuid:='b0000000-0000-4000-8000-0000000000e1';
  v_component uuid:='b0000000-0000-4000-8000-0000000000c3';
begin
  insert into public.weekly_source_entitlement_decision_bundles(
    decision_bundle_id,bundle_revision,agency_id,candidate_id,week_ending_date,bundle_kind,
    source_root_family_booking_id,source_root_timesheet_id,source_contract_id,
    target_root_family_booking_id,target_root_timesheet_id,target_contract_id,
    decision_id,decided_by_user_id,publication_mode,request_digest,
    source_revision_digest,contract_choice_digest,before_inventory_digest,
    proposed_head_ids,state
  ) values (
    v_bundle,1,'b0000000-0000-4000-8000-0000000000aa',
    'b0000000-0000-4000-8000-000000000003','2026-03-08','CROSS_CONTRACT_A_B',
    'WSENT-0001','b0000000-0000-4000-8000-000000000006','b0000000-0000-4000-8000-000000000004',
    'WSENT-0002','b0000000-0000-4000-8000-000000000007','b0000000-0000-4000-8000-000000000008',
    'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001','DEFERRED',
    pg_catalog.decode(pg_catalog.repeat('51',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('52',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('53',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('54',32),'hex'),
    array[v_head_a,v_head_b]::uuid[],'PROPOSED'
  );

  insert into public.weekly_source_entitlement_heads(
    id,authority_kind,agency_id,candidate_id,contract_id,week_ending_date,
    root_timesheet_id,root_family_booking_id,root_timesheet_version,head_revision,state,
    prior_head_id,certified_zero,component_count,entitlement_digest,inventory_digest,
    source_generation_digest,decision_bundle_id,bundle_revision,decision_id,decided_by_user_id
  ) values (
    v_head_a,'LOCKED_FINAL_SOURCE','b0000000-0000-4000-8000-0000000000aa',
    'b0000000-0000-4000-8000-000000000003','b0000000-0000-4000-8000-000000000004','2026-03-08',
    'b0000000-0000-4000-8000-000000000006','WSENT-0001',1,2,'STAGED',
    v_prior,false,2,pg_catalog.decode(pg_catalog.repeat('55',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('56',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('57',32),'hex'),
    v_bundle,1,'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001'
  ),(
    v_head_b,'LOCKED_FINAL_SOURCE','b0000000-0000-4000-8000-0000000000aa',
    'b0000000-0000-4000-8000-000000000003','b0000000-0000-4000-8000-000000000008','2026-03-08',
    'b0000000-0000-4000-8000-000000000007','WSENT-0002',1,1,'STAGED',
    null,false,1,pg_catalog.decode(pg_catalog.repeat('58',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('59',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('5a',32),'hex'),
    v_bundle,1,'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001'
  );

  insert into public.weekly_source_entitlement_head_components(
    head_id,component_ordinal,component_id,component_kind,economic_key_type,
    economic_key_value,component_member_identity,pay_ex_vat,exclude_from_pay,origin,
    decision_bundle_id,bundle_revision,movement_id,movement_group_id,component_sha256
  ) values (
    v_head_a,1,v_component,'SHIFT','WORK_EVENT','we-1','member-1',120.00,false,'SOURCE',
    v_bundle,1,'b0000000-0000-4000-8000-0000000000a1','b0000000-0000-4000-8000-0000000000a9',
    pg_catalog.decode(pg_catalog.repeat('61',32),'hex')
  );

  -- 24 section 4.5 step 3: the same component can never be retained in A and
  -- added to B within one bundle revision.
  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into public.weekly_source_entitlement_head_components(
      head_id,component_ordinal,component_id,component_kind,economic_key_type,
      economic_key_value,component_member_identity,pay_ex_vat,exclude_from_pay,origin,
      decision_bundle_id,bundle_revision,movement_id,component_sha256
    ) values (
      %L,1,%L,'SHIFT','WORK_EVENT','we-1','member-1',120.00,false,'SOURCE',
      %L,1,'b0000000-0000-4000-8000-0000000000a2',
      pg_catalog.decode(pg_catalog.repeat('62',32),'hex')
    );
  $sql$,v_head_b,v_component,v_bundle),
  '23505','weekly_source_entitlement_head_components_bundle_component_uq',
  'the same component retained in A and added to B');

  -- F2: and it cannot be evaded by omitting or falsifying the bundle tag.
  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into public.weekly_source_entitlement_head_components(
      head_id,component_ordinal,component_id,component_kind,economic_key_type,
      economic_key_value,component_member_identity,pay_ex_vat,exclude_from_pay,origin,
      component_sha256
    ) values (
      %L,1,%L,'SHIFT','WORK_EVENT','we-1','member-1',120.00,false,'SOURCE',
      pg_catalog.decode(pg_catalog.repeat('63',32),'hex')
    );
  $sql$,v_head_b,v_component),
  '23502','','the same component added to B with no bundle tag at all');

  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into public.weekly_source_entitlement_head_components(
      head_id,component_ordinal,component_id,component_kind,economic_key_type,
      economic_key_value,component_member_identity,pay_ex_vat,exclude_from_pay,origin,
      decision_bundle_id,bundle_revision,component_sha256
    ) values (
      %L,1,%L,'SHIFT','WORK_EVENT','we-1','member-1',120.00,false,'SOURCE',
      'b0000000-0000-4000-8000-0000000000b1',1,
      pg_catalog.decode(pg_catalog.repeat('64',32),'hex')
    );
  $sql$,v_head_b,v_component),
  '23503','weekly_source_entitlement_head_components_head_bundle_fk',
  'the same component added to B under another bundle tag than its head''s');

  -- One movement identity is one moved component.
  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into public.weekly_source_entitlement_head_components(
      head_id,component_ordinal,component_id,component_kind,economic_key_type,
      economic_key_value,component_member_identity,pay_ex_vat,exclude_from_pay,origin,
      decision_bundle_id,bundle_revision,movement_id,component_sha256
    ) values (
      %L,1,'b0000000-0000-4000-8000-0000000000c9','SHIFT','WORK_EVENT','we-9','member-9',
      10.00,false,'SOURCE',%L,1,'b0000000-0000-4000-8000-0000000000a1',
      pg_catalog.decode(pg_catalog.repeat('65',32),'hex')
    );
  $sql$,v_head_b,v_bundle),
  '23505','weekly_source_entitlement_head_components_bundle_movement_uq',
  'one movement identity used twice');

  -- One component appears once in one head.
  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into public.weekly_source_entitlement_head_components(
      head_id,component_ordinal,component_id,component_kind,economic_key_type,
      economic_key_value,component_member_identity,pay_ex_vat,exclude_from_pay,origin,
      decision_bundle_id,bundle_revision,component_sha256
    ) values (
      %L,9,%L,'SHIFT','WORK_EVENT','we-1','member-1',120.00,false,'SOURCE',
      %L,1,pg_catalog.decode(pg_catalog.repeat('66',32),'hex')
    );
  $sql$,v_head_a,v_component,v_bundle),
  '23505','weekly_source_entitlement_head_compone_head_id_component_id_key',
  'the same component twice in one head');

  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into public.weekly_source_entitlement_head_components(
      head_id,component_ordinal,component_id,component_kind,economic_key_type,
      economic_key_value,component_member_identity,pay_ex_vat,exclude_from_pay,origin,
      decision_bundle_id,bundle_revision,movement_group_id,component_sha256
    ) values (
      %L,10,'b0000000-0000-4000-8000-0000000000c4','SHIFT','WORK_EVENT','we-4','member-4',
      1.00,false,'SOURCE',%L,1,'b0000000-0000-4000-8000-0000000000ab',
      pg_catalog.decode(pg_catalog.repeat('67',32),'hex')
    );
  $sql$,v_head_a,v_bundle),
  '23514','','a movement group without a movement identity');

  -- movement_group_id is deliberately non-unique: components that move together
  -- share it.
  insert into public.weekly_source_entitlement_head_components(
    head_id,component_ordinal,component_id,component_kind,economic_key_type,
    economic_key_value,component_member_identity,pay_ex_vat,exclude_from_pay,origin,
    decision_bundle_id,bundle_revision,movement_id,movement_group_id,component_sha256
  ) values (
    v_head_a,2,'b0000000-0000-4000-8000-0000000000c5','EXPENSE','WORK_EVENT','we-1','member-5',
    7.50,false,'SOURCE',v_bundle,1,'b0000000-0000-4000-8000-0000000000a3',
    'b0000000-0000-4000-8000-0000000000a9',
    pg_catalog.decode(pg_catalog.repeat('68',32),'hex')
  ),(
    v_head_b,1,'b0000000-0000-4000-8000-0000000000c6','SHIFT','WORK_EVENT','we-6','member-6',
    60.00,false,'SOURCE',v_bundle,1,null,null,
    pg_catalog.decode(pg_catalog.repeat('69',32),'hex')
  );

  -- Head components are append-only evidence.
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_entitlement_head_components set pay_ex_vat=1.00;
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_RECORD','rewriting a head component');
  perform pg_temp.expect_failure($sql$
    delete from public.weekly_source_entitlement_head_components;
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_RECORD','deleting a head component');

  -- Section 5 built two STAGED heads and their components, so it leaves pending
  -- events on all three of this file's constraint triggers.  Force and drain
  -- exactly those, so this block's inventory is proved here rather than being
  -- carried into section 6.  Named, never ALL: see the prohibition above.
  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert immediate;
  set constraints
    public.weekly_source_entitlement_head_inventory_assert,
    public.weekly_source_entitlement_head_component_inventory_assert,
    public.weekly_source_entitlement_head_receipt_assert deferred;
end
$verify_entitlement_bundle$;

-- ---------------------------------------------------------------------------
-- 6. The publication receipt and the pending bundle
-- ---------------------------------------------------------------------------
do $verify_entitlement_receipt$
declare
  v_bundle uuid:='b0000000-0000-4000-8000-0000000000b2';
  v_pending uuid;
  v_root uuid:='b0000000-0000-4000-8000-000000000006';
  v_root_b uuid:='b0000000-0000-4000-8000-000000000007';
  v_head uuid:='b0000000-0000-4000-8000-0000000000e2';
  v_head_b uuid:='b0000000-0000-4000-8000-0000000000e3';
  -- The stored publication request (WP-08b's `request_json`, decision D10).
  -- It is an IDENTITY column: NOT NULL, no default, CHECK jsonb_typeof='object',
  -- and deliberately absent from the sixteen lifecycle columns so the fact guard
  -- refuses every rewrite of it (WP-08b report, Addendum 1).  D10 requires the
  -- stored value to be byte-for-byte the request the save owner was handed, so
  -- this is the real caller-side object for the very bundle section 5 built -
  -- the same two roots, the same family strings, the same versions, the same two
  -- heads, and a financial_request whose contract choices and entitlement
  -- vectors match the heads and components this file actually wrote - and not a
  -- placeholder.  publication_mode and pending_bundle_id are deliberately absent
  -- because the coordinator supplies them to the canonicaliser, never the caller
  -- (WP-08b report, Addendum 2 section B2).
  v_request jsonb:=pg_catalog.jsonb_build_object(
    'decision_bundle_id','b0000000-0000-4000-8000-0000000000b2',
    'bundle_revision',1,
    'candidate_id','b0000000-0000-4000-8000-000000000003',
    'member_root_ids',pg_catalog.jsonb_build_array(
      'b0000000-0000-4000-8000-000000000006','b0000000-0000-4000-8000-000000000007'),
    'member_family_booking_ids',pg_catalog.jsonb_build_array('WSENT-0001','WSENT-0002'),
    'member_root_versions',pg_catalog.jsonb_build_array(1,1),
    'head_ids',pg_catalog.jsonb_build_array(
      'b0000000-0000-4000-8000-0000000000e2','b0000000-0000-4000-8000-0000000000e3'),
    'decision_id','b0000000-0000-4000-8000-0000000000d2',
    'financial_request',pg_catalog.jsonb_build_object(
      'source_revision',pg_catalog.jsonb_build_object(
        'final_revision_id','b0000000-0000-4000-8000-0000000000f2',
        'source_cycle_id','b0000000-0000-4000-8000-0000000000f3',
        'revision_number',1,
        'manifest_hash',pg_catalog.repeat('74',32),
        'policy_fingerprint',pg_catalog.repeat('75',32)),
      'contract_choices',pg_catalog.jsonb_build_array(
        -- Root 1 keeps its own Contract; root 2 is the B side of the
        -- CROSS_CONTRACT_A_B bundle section 5 wrote, so Office chose it.
        pg_catalog.jsonb_build_object(
          'root_ordinal',1,'contract_id','b0000000-0000-4000-8000-000000000004',
          'week_ending_date','2026-03-08','selection_method','UNCHANGED'),
        pg_catalog.jsonb_build_object(
          'root_ordinal',2,'contract_id','b0000000-0000-4000-8000-000000000008',
          'week_ending_date','2026-03-08','selection_method','OFFICE_SELECTED')),
      'member_entitlements',pg_catalog.jsonb_build_array(
        -- Head A: LOCKED_FINAL_SOURCE, two components, the exact rows section 5
        -- wrote (the SHIFT that moved as part of group a9 and its EXPENSE).
        pg_catalog.jsonb_build_object(
          'root_ordinal',1,'authority_kind','LOCKED_FINAL_SOURCE',
          'certified_zero',false,'component_count',2,
          'components',pg_catalog.jsonb_build_array(
            pg_temp.entitlement_component(
              1,'b0000000-0000-4000-8000-0000000000c3','SHIFT','we-1','member-1','120.00',
              'b0000000-0000-4000-8000-0000000000a1','b0000000-0000-4000-8000-0000000000a9'),
            pg_temp.entitlement_component(
              2,'b0000000-0000-4000-8000-0000000000c5','EXPENSE','we-1','member-5','7.50',
              'b0000000-0000-4000-8000-0000000000a3','b0000000-0000-4000-8000-0000000000a9'))),
        -- Head B: one component, no movement identity, as section 5 wrote it.
        pg_catalog.jsonb_build_object(
          'root_ordinal',2,'authority_kind','LOCKED_FINAL_SOURCE',
          'certified_zero',false,'component_count',1,
          'components',pg_catalog.jsonb_build_array(
            pg_temp.entitlement_component(
              1,'b0000000-0000-4000-8000-0000000000c6','SHIFT','we-6','member-6','60.00',
              null,null))))));
begin
  -- proof/32 section 2: the pending record of a decision frozen by a Draft.
  insert into public.weekly_source_pending_entitlement_bundles(
    decision_bundle_id,bundle_revision,candidate_id,member_root_ids,
    member_family_booking_ids,member_root_versions,request_digest,
    source_revision_digest,contract_choice_digest,decision_id,decided_by_user_id,
    proposed_head_ids,request_json,pending_revision,state,next_check_at_utc
  ) values (
    v_bundle,1,'b0000000-0000-4000-8000-000000000003',
    array[v_root,v_root_b]::uuid[],array['WSENT-0001','WSENT-0002']::text[],
    array[1,1]::integer[],pg_catalog.decode(pg_catalog.repeat('71',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('72',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('73',32),'hex'),
    'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
    array[v_head,v_head_b]::uuid[],v_request,1,'PENDING',pg_catalog.clock_timestamp()
  ) returning id into v_pending;

  -- WP-08b's structural negatives for this column live in its own verifier, but
  -- a fixture that merely satisfies `jsonb_typeof='object'` would rot silently.
  -- When WP-01b's canonical encoder is installed - it is NOT one of this file's
  -- declared prerequisites, so this is guarded rather than assumed - require it
  -- to accept this exact stored request under the same ('DEFERRED', pending id)
  -- pairing the release owner recomputes under the lock.  If the request ever
  -- drifts from the shape the real coordinator stores, this fails here.
  if pg_catalog.to_regprocedure(
       'private.weekly_source_publication_request_canonical_v1(jsonb,text,uuid)') is not null then
    perform pg_temp.assert_true(
      (select private.weekly_source_publication_request_canonical_v1(
                bundle_row.request_json,'DEFERRED',bundle_row.id)
              ->>'decision_bundle_id'
         from public.weekly_source_pending_entitlement_bundles bundle_row
        where bundle_row.id=v_pending)=v_bundle::text,
      'the stored request must be accepted by the canonical publication encoder');
  else
    raise notice 'NOT CHECKED: private.weekly_source_publication_request_canonical_v1 is not installed';
  end if;

  -- request_json is an identity column, not a lifecycle one: the fact guard must
  -- refuse a rewrite of the stored request (WP-08b report, Addendum 1).
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_pending_entitlement_bundles
       set request_json='{"tampered":true}'::jsonb;
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_FACT','rewriting a stored publication request');

  -- And the shape CHECK is a real refusal, not decoration.
  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into public.weekly_source_pending_entitlement_bundles(
      decision_bundle_id,bundle_revision,candidate_id,member_root_ids,
      member_family_booking_ids,member_root_versions,request_digest,
      source_revision_digest,contract_choice_digest,decision_id,decided_by_user_id,
      proposed_head_ids,request_json,pending_revision,state
    ) values (
      'b0000000-0000-4000-8000-0000000000b9',1,'b0000000-0000-4000-8000-000000000003',
      array['b0000000-0000-4000-8000-000000000006']::uuid[],array['WSENT-0001']::text[],
      array[1]::integer[],pg_catalog.decode(pg_catalog.repeat('76',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('77',32),'hex'),
      pg_catalog.decode(pg_catalog.repeat('78',32),'hex'),
      'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
      array['b0000000-0000-4000-8000-0000000000e2']::uuid[],
      %L::jsonb,1,'PENDING'
    );
  $sql$,'[]'),
  '23514','','a stored publication request that is a JSON array rather than an object');

  -- proof/32 section 2: a RELEASING bundle carries the complete lease,
  -- including the Worker run id, so the right name and token with the wrong run
  -- id cannot release.
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_pending_entitlement_bundles
       set state='RELEASING',lease_owner='worker-1',
           lease_token='b0000000-0000-4000-8000-0000000000bb',
           lease_expires_at_utc=pg_catalog.clock_timestamp()
     where state='PENDING';
  $sql$,'23514','','a RELEASING lease without its Worker run id');

  -- The lease itself is a legal tick update.
  update public.weekly_source_pending_entitlement_bundles
     set state='RELEASING',lease_owner='worker-1',
         lease_token='b0000000-0000-4000-8000-0000000000bb',
         lease_worker_run_id='b0000000-0000-4000-8000-0000000000fa',
         lease_expires_at_utc=pg_catalog.clock_timestamp(),
         technical_failure_count=0,updated_at_utc=pg_catalog.clock_timestamp()
   where id=v_pending;

  -- F8: the money identity of a pending bundle is frozen and the row cannot be
  -- deleted (proof/32 section 2, section 7).
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_pending_entitlement_bundles
       set member_root_ids=array['b0000000-0000-4000-8000-000000000007']::uuid[];
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_FACT','rewriting a pending bundle member set');
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_pending_entitlement_bundles
       set request_digest=pg_catalog.decode(pg_catalog.repeat('ee',32),'hex');
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_FACT','rewriting a pending bundle request digest');
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_pending_entitlement_bundles
       set decision_id='b0000000-0000-4000-8000-0000000000dd';
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_FACT','rewriting a pending bundle decision id');
  perform pg_temp.expect_failure($sql$
    delete from public.weekly_source_pending_entitlement_bundles;
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_RECORD','deleting a pending bundle');

  -- F6: proof/32 section 8 step 5 names four facts to record on release.
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_pending_entitlement_bundles
       set state='RELEASED',released_at_utc=pg_catalog.clock_timestamp(),
           released_receipt_digest=pg_catalog.decode(pg_catalog.repeat('83',32),'hex');
  $sql$,'23514','','a RELEASED bundle without the receipt id and Worker identity');

  -- A DEFERRED receipt: pending bundle and both Worker fields required, and a
  -- real census and proof (proof/32 section 9).
  insert into private.weekly_source_entitlement_publication_receipts(
    decision_bundle_id,pending_bundle_id,bundle_revision,request_digest,publication_mode,
    candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
    scope_change_tx_token,decision_id,decided_by_user_id,
    released_by_worker_id,released_by_worker_run_id,census_json,proof_json
  ) values (
    v_bundle,v_pending,1,pg_catalog.decode(pg_catalog.repeat('83',32),'hex'),'DEFERRED',
    'b0000000-0000-4000-8000-000000000003',
    array[v_root,v_root_b]::uuid[],array['WSENT-0001','WSENT-0002']::text[],
    array[1,1]::integer[],array[v_head,v_head_b]::uuid[],
    'b0000000-0000-4000-8000-0000000000f2',
    'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
    'worker-1','b0000000-0000-4000-8000-0000000000fa',
    '{"roots":[]}'::jsonb,'{"tuples":[]}'::jsonb
  );

  -- proof/32 section 8 step 1: exact replay is an index lookup, so the same
  -- request digest can never be recorded twice.
  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into private.weekly_source_entitlement_publication_receipts(
      decision_bundle_id,pending_bundle_id,bundle_revision,request_digest,publication_mode,
      candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
      scope_change_tx_token,decision_id,decided_by_user_id,
      released_by_worker_id,released_by_worker_run_id,census_json,proof_json
    ) values (
      %L,%L,1,pg_catalog.decode(pg_catalog.repeat('83',32),'hex'),'DEFERRED',
      'b0000000-0000-4000-8000-000000000003',
      array[%L]::uuid[],array['WSENT-0001']::text[],array[1]::integer[],array[%L]::uuid[],
      'b0000000-0000-4000-8000-0000000000f3',
      'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
      'worker-1','b0000000-0000-4000-8000-0000000000fa',
      '{"roots":[]}'::jsonb,'{"tuples":[]}'::jsonb
    );
  $sql$,v_bundle,v_pending,v_root,v_head),'23505','','the same publication recorded twice');

  -- Mode consistency, both directions.
  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into private.weekly_source_entitlement_publication_receipts(
      decision_bundle_id,bundle_revision,request_digest,publication_mode,candidate_id,
      member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
      scope_change_tx_token,decision_id,decided_by_user_id,
      released_by_worker_id,released_by_worker_run_id,census_json,proof_json
    ) values (
      %L,1,pg_catalog.decode(pg_catalog.repeat('84',32),'hex'),'IMMEDIATE',
      'b0000000-0000-4000-8000-000000000003',
      array[%L]::uuid[],array['WSENT-0001']::text[],array[1]::integer[],array[%L]::uuid[],
      'b0000000-0000-4000-8000-0000000000f3',
      'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
      'worker-1','b0000000-0000-4000-8000-0000000000fa','{}'::jsonb,'{}'::jsonb
    );
  $sql$,v_bundle,v_root,v_head),'23514','','an IMMEDIATE receipt carrying Worker fields');

  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into private.weekly_source_entitlement_publication_receipts(
      decision_bundle_id,pending_bundle_id,bundle_revision,request_digest,publication_mode,
      candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
      scope_change_tx_token,decision_id,decided_by_user_id,census_json,proof_json
    ) values (
      %L,%L,1,pg_catalog.decode(pg_catalog.repeat('85',32),'hex'),'DEFERRED',
      'b0000000-0000-4000-8000-000000000003',
      array[%L]::uuid[],array['WSENT-0001']::text[],array[1]::integer[],array[%L]::uuid[],
      'b0000000-0000-4000-8000-0000000000f3',
      'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
      '{"roots":[]}'::jsonb,'{"tuples":[]}'::jsonb
    );
  $sql$,v_bundle,v_pending,v_root,v_head),
  '23514','','a DEFERRED receipt without its Worker identity');

  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into private.weekly_source_entitlement_publication_receipts(
      decision_bundle_id,bundle_revision,request_digest,publication_mode,
      candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
      scope_change_tx_token,decision_id,decided_by_user_id,
      released_by_worker_id,released_by_worker_run_id,census_json,proof_json
    ) values (
      %L,1,pg_catalog.decode(pg_catalog.repeat('86',32),'hex'),'DEFERRED',
      'b0000000-0000-4000-8000-000000000003',
      array[%L]::uuid[],array['WSENT-0001']::text[],array[1]::integer[],array[%L]::uuid[],
      'b0000000-0000-4000-8000-0000000000f3',
      'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
      'worker-1','b0000000-0000-4000-8000-0000000000fa',
      '{"roots":[]}'::jsonb,'{"tuples":[]}'::jsonb
    );
  $sql$,v_bundle,v_root,v_head),
  '23514','','a DEFERRED receipt without its pending bundle');

  -- F9: a deferred release proved a freeze census, so it may not record one.
  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into private.weekly_source_entitlement_publication_receipts(
      decision_bundle_id,pending_bundle_id,bundle_revision,request_digest,publication_mode,
      candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
      scope_change_tx_token,decision_id,decided_by_user_id,
      released_by_worker_id,released_by_worker_run_id,census_json,proof_json
    ) values (
      %L,%L,1,pg_catalog.decode(pg_catalog.repeat('87',32),'hex'),'DEFERRED',
      'b0000000-0000-4000-8000-000000000003',
      array[%L]::uuid[],array['WSENT-0001']::text[],array[1]::integer[],array[%L]::uuid[],
      'b0000000-0000-4000-8000-0000000000f3',
      'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
      'worker-1','b0000000-0000-4000-8000-0000000000fa','{}'::jsonb,'{}'::jsonb
    );
  $sql$,v_bundle,v_pending,v_root,v_head),
  '23514','','a DEFERRED receipt recording an empty census and proof');

  -- The four proof/32 section 9 array rules, each with its own negative.
  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into private.weekly_source_entitlement_publication_receipts(
      decision_bundle_id,bundle_revision,request_digest,publication_mode,
      candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
      scope_change_tx_token,decision_id,decided_by_user_id,census_json,proof_json
    ) values (
      %L,1,pg_catalog.decode(pg_catalog.repeat('88',32),'hex'),'IMMEDIATE',
      'b0000000-0000-4000-8000-000000000003',
      array[%L,%L]::uuid[],array['WSENT-0001','WSENT-0002']::text[],
      array[1,1]::integer[],array[%L]::uuid[],
      'b0000000-0000-4000-8000-0000000000f3',
      'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
      '{}'::jsonb,'{}'::jsonb
    );
  $sql$,v_bundle,v_root,v_root_b,v_head),
  '23514','','head_ids shorter than the member roots');

  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into private.weekly_source_entitlement_publication_receipts(
      decision_bundle_id,bundle_revision,request_digest,publication_mode,
      candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
      scope_change_tx_token,decision_id,decided_by_user_id,census_json,proof_json
    ) values (
      %L,1,pg_catalog.decode(pg_catalog.repeat('89',32),'hex'),'IMMEDIATE',
      'b0000000-0000-4000-8000-000000000003',
      array[%L,%L]::uuid[],array['WSENT-0001']::text[],
      array[1,1]::integer[],array[%L,%L]::uuid[],
      'b0000000-0000-4000-8000-0000000000f3',
      'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
      '{}'::jsonb,'{}'::jsonb
    );
  $sql$,v_bundle,v_root,v_root_b,v_head,v_head_b),
  '23514','','member_family_booking_ids shorter than the member roots');

  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into private.weekly_source_entitlement_publication_receipts(
      decision_bundle_id,bundle_revision,request_digest,publication_mode,
      candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
      scope_change_tx_token,decision_id,decided_by_user_id,census_json,proof_json
    ) values (
      %L,1,pg_catalog.decode(pg_catalog.repeat('8a',32),'hex'),'IMMEDIATE',
      'b0000000-0000-4000-8000-000000000003',
      array[%L,%L]::uuid[],array['WSENT-0001','WSENT-0002']::text[],
      array[1]::integer[],array[%L,%L]::uuid[],
      'b0000000-0000-4000-8000-0000000000f3',
      'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
      '{}'::jsonb,'{}'::jsonb
    );
  $sql$,v_bundle,v_root,v_root_b,v_head,v_head_b),
  '23514','','member_root_versions shorter than the member roots');

  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into private.weekly_source_entitlement_publication_receipts(
      decision_bundle_id,bundle_revision,request_digest,publication_mode,
      candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
      scope_change_tx_token,decision_id,decided_by_user_id,census_json,proof_json
    ) values (
      %L,1,pg_catalog.decode(pg_catalog.repeat('8b',32),'hex'),'IMMEDIATE',
      'b0000000-0000-4000-8000-000000000003',
      array[%L,null]::uuid[],array['WSENT-0001','WSENT-0002']::text[],
      array[1,1]::integer[],array[%L,%L]::uuid[],
      'b0000000-0000-4000-8000-0000000000f3',
      'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
      '{}'::jsonb,'{}'::jsonb
    );
  $sql$,v_bundle,v_root,v_head,v_head_b),
  '23514','','a null root element');

  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into private.weekly_source_entitlement_publication_receipts(
      decision_bundle_id,bundle_revision,request_digest,publication_mode,
      candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
      scope_change_tx_token,decision_id,decided_by_user_id,census_json,proof_json
    ) values (
      %L,1,pg_catalog.decode(pg_catalog.repeat('8c',32),'hex'),'IMMEDIATE',
      'b0000000-0000-4000-8000-000000000003',
      array[%L,%L]::uuid[],array['WSENT-0001',null]::text[],
      array[1,1]::integer[],array[%L,%L]::uuid[],
      'b0000000-0000-4000-8000-0000000000f3',
      'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
      '{}'::jsonb,'{}'::jsonb
    );
  $sql$,v_bundle,v_root,v_root_b,v_head,v_head_b),
  '23514','','a null family element');

  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into private.weekly_source_entitlement_publication_receipts(
      decision_bundle_id,bundle_revision,request_digest,publication_mode,
      candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
      scope_change_tx_token,decision_id,decided_by_user_id,census_json,proof_json
    ) values (
      %L,1,pg_catalog.decode(pg_catalog.repeat('8d',32),'hex'),'IMMEDIATE',
      'b0000000-0000-4000-8000-000000000003',
      array[%L,%L]::uuid[],array['WSENT-0001','WSENT-0002']::text[],
      array[1,null]::integer[],array[%L,%L]::uuid[],
      'b0000000-0000-4000-8000-0000000000f3',
      'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
      '{}'::jsonb,'{}'::jsonb
    );
  $sql$,v_bundle,v_root,v_root_b,v_head,v_head_b),
  '23514','','a null version element');

  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into private.weekly_source_entitlement_publication_receipts(
      decision_bundle_id,bundle_revision,request_digest,publication_mode,
      candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
      scope_change_tx_token,decision_id,decided_by_user_id,census_json,proof_json
    ) values (
      %L,1,pg_catalog.decode(pg_catalog.repeat('8e',32),'hex'),'IMMEDIATE',
      'b0000000-0000-4000-8000-000000000003',
      array[%L,%L]::uuid[],array['WSENT-0001','WSENT-0002']::text[],
      array[1,1]::integer[],array[%L,null]::uuid[],
      'b0000000-0000-4000-8000-0000000000f3',
      'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
      '{}'::jsonb,'{}'::jsonb
    );
  $sql$,v_bundle,v_root,v_root_b,v_head),
  '23514','','a null head element');

  -- Duplicates, on both arrays.
  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into private.weekly_source_entitlement_publication_receipts(
      decision_bundle_id,bundle_revision,request_digest,publication_mode,
      candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
      scope_change_tx_token,decision_id,decided_by_user_id,census_json,proof_json
    ) values (
      %L,1,pg_catalog.decode(pg_catalog.repeat('8f',32),'hex'),'IMMEDIATE',
      'b0000000-0000-4000-8000-000000000003',
      array[%L,%L]::uuid[],array['WSENT-0001','WSENT-0001']::text[],
      array[1,1]::integer[],array[%L,%L]::uuid[],
      'b0000000-0000-4000-8000-0000000000f3',
      'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
      '{}'::jsonb,'{}'::jsonb
    );
  $sql$,v_bundle,v_root,v_root,v_head,v_head_b),
  '23514','','a duplicate root in one receipt');

  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into private.weekly_source_entitlement_publication_receipts(
      decision_bundle_id,bundle_revision,request_digest,publication_mode,
      candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
      scope_change_tx_token,decision_id,decided_by_user_id,census_json,proof_json
    ) values (
      %L,1,pg_catalog.decode(pg_catalog.repeat('90',32),'hex'),'IMMEDIATE',
      'b0000000-0000-4000-8000-000000000003',
      array[%L,%L]::uuid[],array['WSENT-0001','WSENT-0002']::text[],
      array[1,1]::integer[],array[%L,%L]::uuid[],
      'b0000000-0000-4000-8000-0000000000f3',
      'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
      '{}'::jsonb,'{}'::jsonb
    );
  $sql$,v_bundle,v_root,v_root_b,v_head,v_head),
  '23514','','a duplicate head id in one receipt');

  -- An empty member set, a non-object census and a short digest.
  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into private.weekly_source_entitlement_publication_receipts(
      decision_bundle_id,bundle_revision,request_digest,publication_mode,
      candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
      scope_change_tx_token,decision_id,decided_by_user_id,census_json,proof_json
    ) values (
      %L,1,pg_catalog.decode(pg_catalog.repeat('91',32),'hex'),'IMMEDIATE',
      'b0000000-0000-4000-8000-000000000003',
      array[]::uuid[],array[]::text[],array[]::integer[],array[]::uuid[],
      'b0000000-0000-4000-8000-0000000000f3',
      'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
      '{}'::jsonb,'{}'::jsonb
    );
  $sql$,v_bundle),'23514','','an empty receipt member set');

  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into private.weekly_source_entitlement_publication_receipts(
      decision_bundle_id,bundle_revision,request_digest,publication_mode,
      candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
      scope_change_tx_token,decision_id,decided_by_user_id,census_json,proof_json
    ) values (
      %L,1,pg_catalog.decode(pg_catalog.repeat('92',32),'hex'),'IMMEDIATE',
      'b0000000-0000-4000-8000-000000000003',
      array[%L]::uuid[],array['WSENT-0001']::text[],array[1]::integer[],array[%L]::uuid[],
      'b0000000-0000-4000-8000-0000000000f3',
      'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
      '[]'::jsonb,'{}'::jsonb
    );
  $sql$,v_bundle,v_root,v_head),'23514','','a receipt census that is not an object');

  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into private.weekly_source_entitlement_publication_receipts(
      decision_bundle_id,bundle_revision,request_digest,publication_mode,
      candidate_id,member_root_ids,member_family_booking_ids,member_root_versions,head_ids,
      scope_change_tx_token,decision_id,decided_by_user_id,census_json,proof_json
    ) values (
      %L,1,pg_catalog.decode('00','hex'),'IMMEDIATE',
      'b0000000-0000-4000-8000-000000000003',
      array[%L]::uuid[],array['WSENT-0001']::text[],array[1]::integer[],array[%L]::uuid[],
      'b0000000-0000-4000-8000-0000000000f3',
      'b0000000-0000-4000-8000-0000000000d2','b0000000-0000-4000-8000-000000000001',
      '{}'::jsonb,'{}'::jsonb
    );
  $sql$,v_bundle,v_root,v_head),'23514','','a request digest that is not 32 bytes');

  -- proof/32 section 9: a BEFORE UPDATE OR DELETE trigger raises
  -- WEEKLY_SOURCE_PUBLICATION_RECEIPT_IMMUTABLE.
  perform pg_temp.expect_failure($sql$
    update private.weekly_source_entitlement_publication_receipts
       set census_json='{"tampered":true}'::jsonb;
  $sql$,'55000','WEEKLY_SOURCE_PUBLICATION_RECEIPT_IMMUTABLE','updating a receipt');
  perform pg_temp.expect_failure($sql$
    delete from private.weekly_source_entitlement_publication_receipts;
  $sql$,'55000','WEEKLY_SOURCE_PUBLICATION_RECEIPT_IMMUTABLE','deleting a receipt');
  perform pg_temp.expect_failure($sql$
    truncate private.weekly_source_entitlement_publication_receipts;
  $sql$,'55000','WEEKLY_SOURCE_PUBLICATION_RECEIPT_IMMUTABLE','truncating the receipt relation');

  -- A complete, legal release record (proof/32 section 8 step 5).
  update public.weekly_source_pending_entitlement_bundles
     set state='RELEASED',released_at_utc=pg_catalog.clock_timestamp(),
         released_receipt_id=(
           select receipt_row.id
           from private.weekly_source_entitlement_publication_receipts receipt_row
           where receipt_row.request_digest=pg_catalog.decode(pg_catalog.repeat('83',32),'hex')),
         released_receipt_digest=pg_catalog.decode(pg_catalog.repeat('83',32),'hex'),
         released_by_worker_id='worker-1',
         released_by_worker_run_id='b0000000-0000-4000-8000-0000000000fa',
         updated_at_utc=pg_catalog.clock_timestamp()
   where id=v_pending;
  perform pg_temp.assert_true(
    (select released_receipt_id is not null and released_by_worker_run_id is not null
       from public.weekly_source_pending_entitlement_bundles where id=v_pending),
    'the release record must carry the receipt id and the Worker run id');
end
$verify_entitlement_receipt$;

-- ---------------------------------------------------------------------------
-- 7. Lineage generations (proof/34 section 4; proof/36 section 5.6)
-- ---------------------------------------------------------------------------
do $verify_entitlement_lineage$
declare
  v_profile uuid:='b0000000-0000-4000-8000-0000000000f5';
  v_group uuid:='b0000000-0000-4000-8000-0000000000f6';
  v_cycle uuid:='b0000000-0000-4000-8000-0000000000f7';
  v_upload uuid:='b0000000-0000-4000-8000-0000000000e5';
  v_upload_row uuid:='b0000000-0000-4000-8000-0000000000e6';
  v_event uuid:='b0000000-0000-4000-8000-0000000000e7';
  v_resolution uuid:='b0000000-0000-4000-8000-0000000000e8';
  v_head uuid:='b0000000-0000-4000-8000-0000000000e1';
  v_authorisation uuid:='b0000000-0000-4000-8000-0000000000e9';
begin
  insert into public.weekly_source_format_profiles(
    id,profile_code,version,final_authority_kind,container_kind,omission_meaning,
    row_finalisation_capability,worked_duration_authority,profile_json,profile_sha256
  ) values (
    v_profile,'ENTITLEMENT_PROOF',1,'GENERIC_COMPLETE_SNAPSHOT','XLSX',
    'CANCEL_INSIDE_CONFIRMED_COVERAGE','NONE','SOURCE_ACTUAL','{}'::jsonb,
    pg_catalog.decode(pg_catalog.repeat('a1',32),'hex')
  );
  insert into public.weekly_source_groups(
    id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time
  ) values (
    v_group,'TEST','b0000000-0000-4000-8000-0000000000aa','ENTITLEMENT_GROUP',
    'Entitlement Group','ROSTER',3,'15:00'
  );
  insert into public.weekly_source_cycles(
    id,source_group_id,finalisation_week_ending,cutoff_at_utc
  ) values (v_cycle,v_group,'2026-03-08',pg_catalog.clock_timestamp());
  insert into public.weekly_source_uploads(
    id,source_cycle_id,original_filename,content_sha256,byte_count,source_format_profile_id,
    parser_version,normaliser_version,header_coordinate_map_hash,declared_scope_fingerprint,
    coverage_proof_kind,physical_row_count,uploaded_by_user_id
  ) values (
    v_upload,v_cycle,'entitlement.xlsx',pg_catalog.decode(pg_catalog.repeat('a2',32),'hex'),
    1024,v_profile,'p1','n1',pg_catalog.decode(pg_catalog.repeat('a3',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('a4',32),'hex'),
    'HEALTHROSTER_COMPLETE_EXPORT_ATTESTATION',0,'b0000000-0000-4000-8000-000000000001'
  );
  insert into public.weekly_source_upload_rows(
    id,upload_id,source_row_ordinal,source_candidate_identity,source_client_identity,
    work_date,start_at_local,end_at_local,break_minutes,actual_net_minutes,normalised_row_hash
  ) values (
    v_upload_row,v_upload,1,'cand-1','client-1','2026-03-02',
    '2026-03-02 08:00','2026-03-02 16:00',30,450,
    pg_catalog.decode(pg_catalog.repeat('a5',32),'hex')
  );
  insert into public.weekly_work_events(
    id,candidate_id,client_id,work_date,identity_kind,durable_identity_hash,
    source_format_profile_id,profile_external_key
  ) values (
    v_event,'b0000000-0000-4000-8000-000000000003','b0000000-0000-4000-8000-000000000002',
    '2026-03-02','PROFILE_EXTERNAL_KEY',pg_catalog.decode(pg_catalog.repeat('a6',32),'hex'),
    v_profile,'entitlement-external-key'
  );
  insert into public.weekly_source_row_resolutions(
    id,upload_row_id,generation,mapping_state,qualification_profile_fingerprint,
    qualifying_contract_set_hash,source_row_fingerprint,work_event_id,
    candidate_id,client_id,contract_id,contract_selection_method,
    work_event_match_kind,work_event_match_fingerprint
  ) values (
    v_resolution,v_upload_row,1,'RESOLVED',
    pg_catalog.decode(pg_catalog.repeat('a7',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('a8',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('a9',32),'hex'),v_event,
    'b0000000-0000-4000-8000-000000000003','b0000000-0000-4000-8000-000000000002',
    'b0000000-0000-4000-8000-000000000004','AUTO_UNIQUE','NEW_PROFILE_KEY',
    pg_catalog.decode(pg_catalog.repeat('aa',32),'hex')
  );


  -- Decision D8. The per-source-row BINDING is written first, by the ensure
  -- owner, long before any Office decision, and it now carries no authorisation
  -- fact at all.
  insert into public.weekly_source_row_timesheet_lineages(
    row_resolution_id,source_cycle_id,work_event_id,candidate_id,client_id,contract_id,
    contract_week_id,timesheet_id,family_booking_id,timesheet_version,
    week_ending_date,lineage_fingerprint
  ) values (
    v_resolution,v_cycle,v_event,'b0000000-0000-4000-8000-000000000003',
    'b0000000-0000-4000-8000-000000000002','b0000000-0000-4000-8000-000000000004',
    'b0000000-0000-4000-8000-000000000005','b0000000-0000-4000-8000-000000000006',
    'WSENT-0001',1,'2026-03-08',
    pg_catalog.decode(pg_catalog.repeat('b1',32),'hex')
  );

  -- The binding relation carries no authorisation column, so a guard reading it
  -- can never mistake a bound-but-never-authorised root for a managed one
  -- (WP-03 review F3).
  if exists(
    select 1 from pg_catalog.pg_attribute a
    where a.attrelid='public.weekly_source_row_timesheet_lineages'::pg_catalog.regclass
      and a.attnum>0 and not a.attisdropped
      and a.attname in ('authorisation_generation','authorised_row_signature',
                        'current_entitlement_head_id','withdrawn_at_utc','withdrawn_by_user_id')
  ) then
    raise exception 'the per-source-row binding must carry no authorisation column';
  end if;

  -- It is still append-only evidence, and one binding per resolved source row.
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_row_timesheet_lineages set timesheet_version=2;
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_RECORD','rewriting the bound version');
  perform pg_temp.expect_failure($sql$
    delete from public.weekly_source_row_timesheet_lineages;
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_RECORD','deleting a source-row binding');
  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into public.weekly_source_row_timesheet_lineages(
      row_resolution_id,source_cycle_id,work_event_id,candidate_id,client_id,contract_id,
      contract_week_id,timesheet_id,family_booking_id,timesheet_version,
      week_ending_date,lineage_fingerprint
    ) values (
      %L,%L,%L,'b0000000-0000-4000-8000-000000000003',
      'b0000000-0000-4000-8000-000000000002','b0000000-0000-4000-8000-000000000004',
      'b0000000-0000-4000-8000-000000000005','b0000000-0000-4000-8000-000000000006',
      'WSENT-0001',1,'2026-03-08',
      pg_catalog.decode(pg_catalog.repeat('b9',32),'hex')
    );
  $sql$,v_resolution,v_cycle,v_event),
  '23505','','a second binding for one resolved source row');

  -- ---------------------------------------------------------------------
  -- The ROOT authorisation record (proof/34 section 4 write-set table).
  -- ---------------------------------------------------------------------

  -- The identity guard: family and version must be the physical root's own.
  perform pg_temp.expect_failure($sql$
    insert into public.weekly_source_root_authorisations(
      root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
      authorised_row_signature,authorised_by_user_id
    ) values (
      'b0000000-0000-4000-8000-000000000006','wsent-0001',1,1,
      'signature-generation-1','b0000000-0000-4000-8000-000000000001'
    );
  $sql$,'55000','WEEKLY_SOURCE_ROOT_AUTHORISATION_IDENTITY_MISMATCH',
  'a differently-cased family string on the real physical root');
  perform pg_temp.expect_failure($sql$
    insert into public.weekly_source_root_authorisations(
      root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
      authorised_row_signature,authorised_by_user_id
    ) values (
      'b0000000-0000-4000-8000-000000000006','WSENT-0001',99,1,
      'signature-generation-1','b0000000-0000-4000-8000-000000000001'
    );
  $sql$,'55000','WEEKLY_SOURCE_ROOT_AUTHORISATION_IDENTITY_MISMATCH',
  'a version that disagrees with the physical root');
  perform pg_temp.expect_failure($sql$
    insert into public.weekly_source_root_authorisations(
      root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
      authorised_row_signature,authorised_by_user_id
    ) values (
      'b0000000-0000-4000-8000-000000000006','WSENT-0002',1,1,
      'signature-generation-1','b0000000-0000-4000-8000-000000000001'
    );
  $sql$,'55000','WEEKLY_SOURCE_ROOT_AUTHORISATION_IDENTITY_MISMATCH',
  'another family''s booking id on this physical root');

  -- Generation 1, written by the first-authorisation owner after the ordinary
  -- Authorise succeeds.
  insert into public.weekly_source_root_authorisations(
    id,root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
    authorised_row_signature,authorised_by_user_id
  ) values (
    v_authorisation,'b0000000-0000-4000-8000-000000000006','WSENT-0001',1,1,
    'signature-generation-1','b0000000-0000-4000-8000-000000000001'
  );

  -- Only the head-publication coordinator's column may move.
  update public.weekly_source_root_authorisations
     set current_entitlement_head_id=v_head,updated_at_utc=pg_catalog.clock_timestamp()
   where id=v_authorisation;
  perform pg_temp.assert_true(
    (select current_entitlement_head_id from public.weekly_source_root_authorisations
      where id=v_authorisation)=v_head,
    'the head-publication coordinator must be able to set current_entitlement_head_id');

  -- Every other column is immutable (proof/34 section 4; ROT-011).
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_root_authorisations set timesheet_version=2;
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_FACT','rewriting the authorised version');
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_root_authorisations set family_booking_id='WSENT-9999';
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_FACT','rewriting the family booking id');
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_root_authorisations
       set root_timesheet_id='b0000000-0000-4000-8000-000000000007';
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_FACT','rewriting the physical root id');
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_root_authorisations set authorised_row_signature='tampered';
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_FACT','rewriting the authorised row signature');
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_root_authorisations set authorisation_generation=7;
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_FACT','renumbering a generation');
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_root_authorisations
       set authorised_by_user_id='b0000000-0000-4000-8000-000000000001',
           authorised_at_utc=pg_catalog.clock_timestamp();
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_FACT','rewriting the authorisation actor and time');
  perform pg_temp.expect_failure($sql$
    delete from public.weekly_source_root_authorisations;
  $sql$,'55000','WEEKLY_SOURCE_IMMUTABLE_RECORD','deleting a root authorisation generation');

  -- A second LIVE generation for one root is impossible.
  perform pg_temp.expect_failure($sql$
    insert into public.weekly_source_root_authorisations(
      root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
      authorised_row_signature,authorised_by_user_id
    ) values (
      'b0000000-0000-4000-8000-000000000006','WSENT-0001',1,2,
      'signature-generation-2','b0000000-0000-4000-8000-000000000001'
    );
  $sql$,'23505','weekly_source_root_authorisations_live_uq',
  'a second live authorisation generation for one root');

  -- proof/36 section 5.6: withdrawal sets the two columns and clears the head
  -- in the same statement.
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_root_authorisations
       set withdrawn_at_utc=pg_catalog.clock_timestamp(),
           withdrawn_by_user_id='b0000000-0000-4000-8000-000000000001';
  $sql$,'23514','','a withdrawn generation that still points at a head');

  update public.weekly_source_root_authorisations
     set withdrawn_at_utc=pg_catalog.clock_timestamp(),
         withdrawn_by_user_id='b0000000-0000-4000-8000-000000000001',
         current_entitlement_head_id=null,
         updated_at_utc=pg_catalog.clock_timestamp()
   where id=v_authorisation;

  -- A withdrawal is permanent (proof/36 section 6; WP-03 review F12).
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_root_authorisations
       set withdrawn_at_utc=null,withdrawn_by_user_id=null;
  $sql$,'55000','WEEKLY_SOURCE_ROOT_AUTHORISATION_WITHDRAWAL_IMMUTABLE',
  'clearing a withdrawal to bring a generation back to life');
  perform pg_temp.expect_failure($sql$
    update public.weekly_source_root_authorisations
       set withdrawn_by_user_id='b0000000-0000-4000-8000-000000000001',
           withdrawn_at_utc=pg_catalog.clock_timestamp();
  $sql$,'55000','WEEKLY_SOURCE_ROOT_AUTHORISATION_WITHDRAWAL_IMMUTABLE',
  'rewriting an existing withdrawal');
  perform pg_temp.expect_failure(pg_catalog.format($sql$
    update public.weekly_source_root_authorisations
       set current_entitlement_head_id=%L;
  $sql$,v_head),'55000','WEEKLY_SOURCE_ROOT_AUTHORISATION_WITHDRAWN',
  'giving a withdrawn generation an entitlement head again');

  -- Re-authorisation appends generation 2; generation 1 stays as history with
  -- an identical family, physical id and version.
  insert into public.weekly_source_root_authorisations(
    root_timesheet_id,family_booking_id,timesheet_version,authorisation_generation,
    authorised_row_signature,authorised_by_user_id
  ) values (
    'b0000000-0000-4000-8000-000000000006','WSENT-0001',1,2,
    'signature-generation-2','b0000000-0000-4000-8000-000000000001'
  );
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_root_authorisations
      where root_timesheet_id='b0000000-0000-4000-8000-000000000006')=2,
    'generation 1 must remain as history after re-authorisation');
  perform pg_temp.assert_true(
    (select pg_catalog.count(distinct (root_timesheet_id,timesheet_version,family_booking_id))
       from public.weekly_source_root_authorisations)=1,
    'family, physical id and version must be identical across the two generations');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_root_authorisations
      where withdrawn_at_utc is null)=1,
    'exactly one live generation must remain after re-authorisation');
end
$verify_entitlement_lineage$;

-- ---------------------------------------------------------------------------
-- 8. The symmetric one-penny source-charge predicate
--
-- WP-04 handoff N1. 25 section 6: "The tolerance is symmetric: exact signed
-- source pence must have the same non-zero sign as the CloudTMS charge and may
-- differ by no more than one penny in either direction." 03 section 2:
-- abs(source_charge_difference_pence)=1 with matching non-zero signs, and "no
-- directional constraint is permitted" (24 section 13; NHSP-BR-013).
-- ---------------------------------------------------------------------------
do $verify_symmetric_penny$
declare
  v_upload_row uuid:='b0000000-0000-4000-8000-0000000000e6';
  v_resolution uuid:='b0000000-0000-4000-8000-0000000000e8';
begin
  insert into public.weekly_source_charge_checks(
    upload_row_id,row_resolution_id,generation,row_sign_kind,
    source_commission_pence,source_total_cost_pence,source_shift_charge_pence,
    calculated_segment_charge_pence,source_charge_difference_pence,
    comparison_profile_version,comparison_result,comparison_reason_code,phase_severity,
    charge_calculation_fingerprint
  )
  select v_upload_row,v_resolution,probe.generation,probe.row_sign_kind,
         probe.commission,probe.total_cost,probe.shift_charge,probe.calculated,
         probe.shift_charge-probe.calculated,
         'NHSP_TWO_COMPONENT_PENCE_V1','SOURCE_ROUNDING_EQUIVALENT',
         'NHSP_SOURCE_ROUNDING','NONE',
         pg_catalog.decode(pg_catalog.repeat('c1',32),'hex')
  from (values
    (11,'POSITIVE',1::bigint,15000::bigint,15001::bigint,15000::bigint),
    (12,'POSITIVE',0::bigint,14999::bigint,14999::bigint,15000::bigint),
    (13,'FULL_NEGATIVE',0::bigint,-14999::bigint,-14999::bigint,-15000::bigint),
    (14,'FULL_NEGATIVE',-1::bigint,-15000::bigint,-15001::bigint,-15000::bigint)
  ) as probe(generation,row_sign_kind,commission,total_cost,shift_charge,calculated);

  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_charge_checks
      where comparison_result='SOURCE_ROUNDING_EQUIVALENT'
        and source_charge_difference_pence=-1)=2,
    'the symmetric rule must admit a source one penny BELOW the calculation, in both signs');
  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_source_charge_checks
      where comparison_result='SOURCE_ROUNDING_EQUIVALENT'
        and source_charge_difference_pence=1)=2,
    'the symmetric rule must admit a source one penny ABOVE the calculation, in both signs');

  perform pg_temp.expect_failure($sql$
    insert into public.weekly_source_charge_checks(
      upload_row_id,row_resolution_id,generation,row_sign_kind,
      source_commission_pence,source_total_cost_pence,source_shift_charge_pence,
      calculated_segment_charge_pence,source_charge_difference_pence,
      comparison_profile_version,comparison_result,comparison_reason_code,phase_severity,
      charge_calculation_fingerprint
    ) values (
      'b0000000-0000-4000-8000-0000000000e6','b0000000-0000-4000-8000-0000000000e8',21,
      'POSITIVE',2,15000,15002,15000,2,
      'NHSP_TWO_COMPONENT_PENCE_V1','SOURCE_ROUNDING_EQUIVALENT','NHSP_SOURCE_ROUNDING','NONE',
      pg_catalog.decode(pg_catalog.repeat('c2',32),'hex')
    );
  $sql$,'23514','weekly_source_charge_checks_check4','a two-penny difference above the calculation');
  perform pg_temp.expect_failure($sql$
    insert into public.weekly_source_charge_checks(
      upload_row_id,row_resolution_id,generation,row_sign_kind,
      source_commission_pence,source_total_cost_pence,source_shift_charge_pence,
      calculated_segment_charge_pence,source_charge_difference_pence,
      comparison_profile_version,comparison_result,comparison_reason_code,phase_severity,
      charge_calculation_fingerprint
    ) values (
      'b0000000-0000-4000-8000-0000000000e6','b0000000-0000-4000-8000-0000000000e8',22,
      'POSITIVE',0,14998,14998,15000,-2,
      'NHSP_TWO_COMPONENT_PENCE_V1','SOURCE_ROUNDING_EQUIVALENT','NHSP_SOURCE_ROUNDING','NONE',
      pg_catalog.decode(pg_catalog.repeat('c3',32),'hex')
    );
  $sql$,'23514','weekly_source_charge_checks_check4','a two-penny difference below the calculation');

  perform pg_temp.expect_failure($sql$
    insert into public.weekly_source_charge_checks(
      upload_row_id,row_resolution_id,generation,row_sign_kind,
      source_commission_pence,source_total_cost_pence,source_shift_charge_pence,
      calculated_segment_charge_pence,source_charge_difference_pence,
      comparison_profile_version,comparison_result,comparison_reason_code,phase_severity,
      charge_calculation_fingerprint
    ) values (
      'b0000000-0000-4000-8000-0000000000e6','b0000000-0000-4000-8000-0000000000e8',23,
      'POSITIVE',0,1,1,-1,2,
      'NHSP_TWO_COMPONENT_PENCE_V1','SOURCE_ROUNDING_EQUIVALENT','NHSP_SOURCE_ROUNDING','NONE',
      pg_catalog.decode(pg_catalog.repeat('c4',32),'hex')
    );
  $sql$,'23514','','a wrong-sign row admitted as rounding equivalent');

  if exists(
    select 1 from pg_catalog.pg_constraint c
    where c.conrelid='public.weekly_source_charge_checks'::pg_catalog.regclass
      and c.contype='c'
      and pg_catalog.pg_get_constraintdef(c.oid) like '%SOURCE_ROUNDING_EQUIVALENT%'
      and pg_catalog.pg_get_constraintdef(c.oid) like '%row_sign_kind%'
  ) then
    raise exception 'the one-penny predicate still binds the arithmetic direction to row_sign_kind';
  end if;
end
$verify_symmetric_penny$;

-- ---------------------------------------------------------------------------
-- 9. The comparison classes the established engine emits (WP-04 handoff N8)
-- ---------------------------------------------------------------------------
do $verify_comparison_classes$
declare
  v_cycle uuid:='b0000000-0000-4000-8000-0000000000f7';
  v_upload uuid:='b0000000-0000-4000-8000-0000000000e5';
  v_upload_row uuid:='b0000000-0000-4000-8000-0000000000e6';
  v_event uuid:='b0000000-0000-4000-8000-0000000000e7';
  v_publication uuid:='b0000000-0000-4000-8000-0000000000d5';
begin
  insert into public.weekly_source_projection_publications(
    id,source_cycle_id,authority_scope_kind,upload_id,authority_scope_version,
    comparison_manifest_hash,issue_set_hash
  ) values (
    v_publication,v_cycle,'CYCLE',v_upload,1,
    pg_catalog.decode(pg_catalog.repeat('d1',32),'hex'),
    pg_catalog.decode(pg_catalog.repeat('d2',32),'hex')
  );

  -- A paired class carries both sides.
  insert into public.weekly_timesheet_source_comparisons(
    source_cycle_id,upload_id,projection_publication_id,upload_row_id,timesheet_id,
    timesheet_revision,work_event_id,contract_id,work_date,comparison_state,
    candidate_start_at_local,candidate_end_at_local,candidate_break_minutes,
    total_break_minutes_match,source_reference_number,comparison_fingerprint
  ) values (
    v_cycle,v_upload,v_publication,v_upload_row,'b0000000-0000-4000-8000-000000000006',
    1,v_event,'b0000000-0000-4000-8000-000000000004','2026-03-02','EXACT_MATCH',
    '2026-03-02 08:00','2026-03-02 16:00',30,true,'REF-1',
    pg_catalog.decode(pg_catalog.repeat('d3',32),'hex')
  );

  -- UNMATCHED / AMBIGUOUS: a signed Timesheet day that resolves to no single
  -- source work event.
  insert into public.weekly_timesheet_source_comparisons(
    source_cycle_id,upload_id,projection_publication_id,timesheet_id,
    timesheet_revision,contract_id,work_date,comparison_state,
    candidate_start_at_local,candidate_end_at_local,candidate_break_minutes,
    total_break_minutes_match,comparison_fingerprint
  ) values (
    v_cycle,v_upload,v_publication,'b0000000-0000-4000-8000-000000000006',
    1,'b0000000-0000-4000-8000-000000000004','2026-03-03','SOURCE_SHIFT_MISSING',
    '2026-03-03 08:00','2026-03-03 16:00',30,false,
    pg_catalog.decode(pg_catalog.repeat('d4',32),'hex')
  ),(
    v_cycle,v_upload,v_publication,'b0000000-0000-4000-8000-000000000006',
    2,'b0000000-0000-4000-8000-000000000004','2026-03-04','AMBIGUOUS_SOURCE_ROW',
    '2026-03-04 08:00','2026-03-04 16:00',30,false,
    pg_catalog.decode(pg_catalog.repeat('d5',32),'hex')
  );

  -- HR_ONLY: a source row with no signed Timesheet day, so no Candidate times.
  insert into public.weekly_timesheet_source_comparisons(
    source_cycle_id,upload_id,projection_publication_id,upload_row_id,timesheet_id,
    timesheet_revision,work_event_id,contract_id,work_date,comparison_state,
    candidate_break_minutes,total_break_minutes_match,comparison_fingerprint
  ) values (
    v_cycle,v_upload,v_publication,v_upload_row,'b0000000-0000-4000-8000-000000000006',
    3,v_event,'b0000000-0000-4000-8000-000000000004','2026-03-05','REFERENCE_MISSING',
    0,false,pg_catalog.decode(pg_catalog.repeat('d6',32),'hex')
  );

  perform pg_temp.assert_true(
    (select pg_catalog.count(*) from public.weekly_timesheet_source_comparisons)=4,
    'all four representable comparison shapes must store');

  -- A paired class still requires both sides.
  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into public.weekly_timesheet_source_comparisons(
      source_cycle_id,upload_id,projection_publication_id,timesheet_id,
      timesheet_revision,contract_id,work_date,comparison_state,
      candidate_start_at_local,candidate_end_at_local,candidate_break_minutes,
      total_break_minutes_match,comparison_fingerprint
    ) values (
      %L,%L,%L,'b0000000-0000-4000-8000-000000000006',
      4,'b0000000-0000-4000-8000-000000000004','2026-03-06','HOURS_MISMATCH',
      '2026-03-06 08:00','2026-03-06 16:00',30,false,
      pg_catalog.decode(pg_catalog.repeat('d7',32),'hex')
    );
  $sql$,v_cycle,v_upload,v_publication),
  '23514','','a paired comparison class with no work event');

  -- Candidate start and end move together.
  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into public.weekly_timesheet_source_comparisons(
      source_cycle_id,upload_id,projection_publication_id,timesheet_id,
      timesheet_revision,work_event_id,contract_id,work_date,comparison_state,
      candidate_start_at_local,candidate_break_minutes,
      total_break_minutes_match,comparison_fingerprint
    ) values (
      %L,%L,%L,'b0000000-0000-4000-8000-000000000006',
      5,%L,'b0000000-0000-4000-8000-000000000004','2026-03-07','REFERENCE_MISSING',
      '2026-03-07 08:00',30,false,
      pg_catalog.decode(pg_catalog.repeat('d8',32),'hex')
    );
  $sql$,v_cycle,v_upload,v_publication,v_event),
  '23514','','a Candidate start with no Candidate end');

  -- A row with neither side is not a comparison at all.
  perform pg_temp.expect_failure(pg_catalog.format($sql$
    insert into public.weekly_timesheet_source_comparisons(
      source_cycle_id,upload_id,projection_publication_id,timesheet_id,
      timesheet_revision,contract_id,work_date,comparison_state,
      candidate_break_minutes,total_break_minutes_match,comparison_fingerprint
    ) values (
      %L,%L,%L,'b0000000-0000-4000-8000-000000000006',
      6,'b0000000-0000-4000-8000-000000000004','2026-03-08','REFERENCE_MISSING',
      0,false,pg_catalog.decode(pg_catalog.repeat('d9',32),'hex')
    );
  $sql$,v_cycle,v_upload,v_publication),
  '23514','','a comparison row carrying neither side');
end
$verify_comparison_classes$;

-- Force this file's three deferred assertions to run against the complete final
-- state, which a rollback-only proof would otherwise never reach.  Named, never
-- ALL: `ALL` would also fire the Banking Pay deferred finalisation trigger that
-- the seeded Timesheets leave pending.  See the prohibition on
-- pg_temp.expect_deferred_failure at the top of this file.
set constraints
  public.weekly_source_entitlement_head_inventory_assert,
  public.weekly_source_entitlement_head_component_inventory_assert,
  public.weekly_source_entitlement_head_receipt_assert immediate;

select pg_catalog.jsonb_build_object(
  'ok',true,
  'verification','weekly_source_entitlement_schema_v1',
  'committed_current_heads',(
    select pg_catalog.count(*) from public.weekly_source_entitlement_heads
     where state='COMMITTED_CURRENT'),
  'heads',(select pg_catalog.count(*) from public.weekly_source_entitlement_heads),
  'head_components',(
    select pg_catalog.count(*) from public.weekly_source_entitlement_head_components),
  'decision_bundles',(
    select pg_catalog.count(*) from public.weekly_source_entitlement_decision_bundles),
  'pending_bundles',(
    select pg_catalog.count(*) from public.weekly_source_pending_entitlement_bundles),
  'publication_receipts',(
    select pg_catalog.count(*) from private.weekly_source_entitlement_publication_receipts),
  'source_row_bindings',(
    select pg_catalog.count(*) from public.weekly_source_row_timesheet_lineages),
  'root_authorisation_generations',(
    select pg_catalog.count(*) from public.weekly_source_root_authorisations),
  'symmetric_penny_rows_admitted',(
    select pg_catalog.count(*) from public.weekly_source_charge_checks
     where comparison_result='SOURCE_ROUNDING_EQUIVALENT'),
  'comparison_rows',(
    select pg_catalog.count(*) from public.weekly_timesheet_source_comparisons)
);

\if :{?weekly_source_verification_outer_transaction}
\else
rollback;
\endif
