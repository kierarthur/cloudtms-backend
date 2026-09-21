-- Weekly Source Plan 6.2, package WP-08c.
--
-- One additive relation: the bounded, pageable child records that HANDOVER 2
-- round-5 ruling B4.4 requires for a pending-release manual review.
--
-- Why this is a separate migration and not an edit of the Gate 1 schema
-- migration: `15092026_1534_weekly_source_plan6_schema.sql` is already recorded
-- in `supabase/release/migration-lock.json`, and the release runner's
-- `lock:update` refuses to re-lock a changed migration ("Refusing to relock
-- changed migration"), exactly as `cloudtms-backend/AGENTS.md` requires.  The
-- Gate 1 file is therefore closed to edits and this is the additive route.
--
-- Why it cannot live in the repeatable owners: it is a RELATION.  Repeatables
-- carry replacement function/view definitions only, and the release runner
-- installs them by re-running the whole closure, so a `create table` there
-- would either fail on the second run or need an `if not exists` that the
-- contract export could not police.
--
-- Why `private` and not `public`: the Weekly Source ACL contract sweeps every
-- `public.weekly_*` relation and raises `WEEKLY_SOURCE_ACL_TABLE_UNCLASSIFIED`
-- for any that is absent from its closed 98-entry set
-- (`supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql`), and
-- that file belongs to another package.  It is also the right home on its
-- merits: these rows are diagnostic evidence about Banking Pay items that no
-- browser role may ever read, and the publication receipts already live in
-- `private` for the same reason.  The relation carries its own owner policy,
-- revocations and immutability guards explicitly, exactly as
-- `private.weekly_source_entitlement_publication_receipts` does.

\set ON_ERROR_STOP on

begin;

-- HANDOVER 2 round-5 ruling B4.4: "The Office reason's 1,000-character summary
-- may remain bounded, but exact census item identifiers must be retained
-- losslessly in bounded, pageable child records and shown through the Office
-- detail view.  Never concatenate an unbounded list into one field and never
-- silently drop the fifth or later identifier."
--
-- WP-08b's review finding F3 measured the defect: the reason reached 1,055
-- characters at five census error items, so from the fifth item onward the
-- identifiers were cut.  WP-08b raised the cap to 8,000 characters and made the
-- reason a structured object, which is better but is still ONE field; a census
-- with enough items still drops some, and an Office screen still has to scrape
-- a string.  This relation is that child record: one row per census item, in
-- the census's own order, with the identifiers as their own typed columns.  The
-- reason keeps only a bounded summary and a pointer to here.
--
-- Why `private` and not `public`:
--   * the ACL contract's `WEEKLY_SOURCE_ACL_TABLE_UNCLASSIFIED` sweep is a
--     closed set of exactly 98 `public.weekly_*` relations, owned by a file this
--     package does not own; a public relation could not be added without
--     changing it, and
--   * the reason detail is diagnostic evidence about Banking Pay items that no
--     browser role may ever read.  `private` is where the publication receipts
--     already live, so this follows the established Weekly Source pattern
--     (the receipts relation below) and gives the rows no browser surface at
--     all.  The Office reaches them only through the service-only pageable
--     reader in the release repeatable.
--
-- Append-only: a manual-review reason is evidence, so the rows are written once
-- and never updated or deleted.  A later attempt on the same bundle writes a NEW
-- pending_revision generation rather than rewriting the old one, so the Office
-- can see what each attempt found.
create table private.weekly_source_pending_release_review_items (
  id uuid primary key default pg_catalog.gen_random_uuid(),
  pending_bundle_id uuid not null
    references public.weekly_source_pending_entitlement_bundles(id) on delete restrict,
  -- The generation this evidence belongs to.  (bundle, revision, ordinal) is
  -- unique, so a replayed writer cannot duplicate a generation's items and a
  -- new attempt cannot overwrite an old one's.
  pending_revision bigint not null check (pending_revision>=1),
  item_ordinal integer not null check (item_ordinal>=1),
  -- The refusal that produced this evidence, and the census verdict it came
  -- from, so one bundle's successive manual reviews stay distinguishable.
  refusal_code text not null check (pg_catalog.btrim(refusal_code)<>''
                                    and pg_catalog.length(refusal_code)<=200),
  census_result text check (census_result is null
                            or pg_catalog.btrim(census_result)<>''),
  -- The exact identifiers.  Typed columns, never a concatenated list.  A census
  -- item is allowed to lack one of them (an error item raised against a family
  -- rather than against one pay batch item), which is why they are nullable;
  -- what is NOT allowed is for a present identifier to be dropped.
  item_class text not null check (pg_catalog.btrim(item_class)<>''),
  item_reason text,
  pay_batch_item_id uuid,
  timesheet_id uuid,
  -- The complete original item, so a future census member is not lost either.
  item_json jsonb not null check (pg_catalog.jsonb_typeof(item_json)='object'),
  recorded_at_utc timestamptz not null default pg_catalog.transaction_timestamp(),
  unique (pending_bundle_id,pending_revision,item_ordinal)
);
alter table private.weekly_source_pending_release_review_items owner to postgres;
create index weekly_source_pending_release_review_items_page_idx
  on private.weekly_source_pending_release_review_items(
       pending_bundle_id,pending_revision desc,item_ordinal);

create function private.weekly_source_pending_release_review_item_immutable_v1()
returns trigger
language plpgsql
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  raise exception 'WEEKLY_SOURCE_PENDING_RELEASE_REVIEW_ITEM_IMMUTABLE'
    using errcode='55000',
          detail=pg_catalog.jsonb_build_object(
            'reason_code','WEEKLY_SOURCE_PENDING_RELEASE_REVIEW_ITEM_IMMUTABLE',
            'table_name',tg_table_name,
            'operation',tg_op
          )::text;
end;
$function$;
alter function private.weekly_source_pending_release_review_item_immutable_v1() owner to postgres;
revoke all on function private.weekly_source_pending_release_review_item_immutable_v1()
  from public,anon,authenticated,service_role;

create trigger weekly_source_pending_release_review_item_immutable
  before update or delete on private.weekly_source_pending_release_review_items
  for each row execute function private.weekly_source_pending_release_review_item_immutable_v1();
create trigger weekly_source_pending_release_review_item_truncate_guard
  before truncate on private.weekly_source_pending_release_review_items
  for each statement execute function private.weekly_source_pending_release_review_item_immutable_v1();

alter table private.weekly_source_pending_release_review_items enable row level security;
alter table private.weekly_source_pending_release_review_items force row level security;
revoke all on table private.weekly_source_pending_release_review_items
  from public,anon,authenticated,service_role;
do $weekly_source_pending_release_review_item_policy$
begin
  execute pg_catalog.format(
    'create policy cloudtms_miget_service_owner_all on private.weekly_source_pending_release_review_items for all to %I using (true) with check (true)',
    current_user
  );
end;
$weekly_source_pending_release_review_item_policy$;

commit;
