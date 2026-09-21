-- Weekly Source Plan 6.2 — Banking Pay evidence fixtures (WP-16a), core library.
--
-- Scope and authority
--   Contract decision D2: Banking Pay's unfinished new logic is out of scope.
--   Every state below is built as a FIXTURE in the EXISTING Banking Pay evidence
--   tables, either by driving a real installed owner or by a named, cited seed.
--   Nothing here changes, wraps or re-creates any Banking Pay function or table.
--
--   All objects live in schema `ws_banking_fixture`, which exists only inside a
--   disposable local clone.  No object is installed into `public` or `private`,
--   nothing is added to `supabase/`, and no release file references this schema.
--
-- Local only
--   `ws_banking_fixture.assert_local_only()` refuses to run anywhere that is not
--   a disposable local proof database.  It is called by every public entry point.
--
-- Constraint proof
--   Every seeded row is re-validated against every installed CHECK constraint,
--   NOT NULL, unique index and foreign key of its table by
--   `ws_banking_fixture.assert_rows_satisfy_constraints(...)`, which reads
--   `pg_constraint` / `pg_index` / `pg_attribute` live rather than trusting a
--   hand-written list.

\set ON_ERROR_STOP on

create schema if not exists ws_banking_fixture;

-- ---------------------------------------------------------------------------
-- Local-only guard
-- ---------------------------------------------------------------------------
create or replace function ws_banking_fixture.assert_local_only()
returns void
language plpgsql
as $$
declare
  v_database text := current_database();
  v_environment text;
begin
  -- Disposable local proof databases only:
  --   `banking_modal_v2_test`            the harness NEW-mode database
  --   `banking_modal_v2_release<N>_<8>`  a release-runner rebuild
  --   `ws62_%`                           a `ws62_template` fast clone
  if not (
       v_database = 'banking_modal_v2_test'
       or v_database ~ '^banking_modal_v2_(contract|release[0-9]*)_[0-9]{8}$'
       or v_database ~ '^ws62_[a-z0-9_]+$'
     ) then
    raise exception 'WS_BANKING_FIXTURE_LOCAL_ONLY'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'code', 'WS_BANKING_FIXTURE_LOCAL_ONLY',
              'current_database', v_database
            )::text;
  end if;

  if to_regclass('private.cloudtms_database_identity') is not null then
    select identity_row.environment
    into v_environment
    from private.cloudtms_database_identity as identity_row
    limit 1;

    if upper(coalesce(v_environment, '')) = 'LIVE' then
      raise exception 'WS_BANKING_FIXTURE_LOCAL_ONLY'
        using errcode = 'P0001',
              detail = jsonb_build_object(
                'code', 'WS_BANKING_FIXTURE_LOCAL_ONLY',
                'reason', 'DATABASE_IDENTITY_IS_LIVE'
              )::text;
    end if;
  end if;
end;
$$;

-- ---------------------------------------------------------------------------
-- Deterministic fixture identities
-- ---------------------------------------------------------------------------
-- Every fixture row id is derived from a stable text key, so a state can be
-- rebuilt, re-read and asserted without carrying ids through the caller.
create or replace function ws_banking_fixture.fid(p_key text)
returns uuid
language sql
immutable
as $$
  select md5('ws_banking_fixture:' || coalesce(p_key, ''))::uuid;
$$;

-- A 64-hex token derived from a stable key, for columns whose CHECK requires
-- `^[0-9a-f]{64}$` (correction scope hashes, reauth proofs).
create or replace function ws_banking_fixture.fhash(p_key text)
returns text
language sql
immutable
as $$
  select encode(sha256(convert_to('ws_banking_fixture:' || coalesce(p_key, ''), 'UTF8')), 'hex');
$$;

-- ---------------------------------------------------------------------------
-- Constraint verification for seeded rows
-- ---------------------------------------------------------------------------
-- `assert_rows_satisfy_constraints('public.pay_batch_items', 'id = any($ids)')`
-- re-evaluates, for the named rows:
--   * every CHECK constraint on the table (pg_get_constraintdef, evaluated);
--   * every NOT NULL column;
--   * every UNIQUE / PRIMARY KEY index (including partial ones);
--   * every FOREIGN KEY (by proving the referenced row exists).
-- It raises with the exact failing constraint name.  This is deliberately a
-- re-derivation from the live catalogue, not a copy of a constraint list: a
-- schema change in WP-01a cannot silently weaken it.
create or replace function ws_banking_fixture.assert_rows_satisfy_constraints(
  p_relation regclass,
  p_row_predicate text,
  p_context text default null
)
returns jsonb
language plpgsql
as $$
declare
  v_constraint record;
  v_column record;
  v_index record;
  v_failing bigint;
  v_checked_checks integer := 0;
  v_checked_not_null integer := 0;
  v_checked_unique integer := 0;
  v_checked_fk integer := 0;
  v_row_count bigint;
  v_sql text;
  v_index_columns text;
  v_index_predicate text;
begin
  perform ws_banking_fixture.assert_local_only();

  execute format('select count(*) from %s as fixture_row where %s', p_relation, p_row_predicate)
  into v_row_count;

  if coalesce(v_row_count, 0) = 0 then
    raise exception 'WS_BANKING_FIXTURE_CONSTRAINT_CHECK_MATCHED_NO_ROWS'
      using errcode = 'P0001',
            detail = jsonb_build_object(
              'relation', p_relation::text,
              'row_predicate', p_row_predicate,
              'context', p_context
            )::text;
  end if;

  -- CHECK constraints
  for v_constraint in
    select constraint_row.conname,
           pg_get_constraintdef(constraint_row.oid) as definition
    from pg_constraint as constraint_row
    where constraint_row.conrelid = p_relation
      and constraint_row.contype = 'c'
    order by constraint_row.conname
  loop
    v_sql := format(
      'select count(*) from %s as fixture_row where (%s) and not (%s)',
      p_relation,
      p_row_predicate,
      regexp_replace(v_constraint.definition, '^CHECK\s*\((.*)\)$', '\1')
    );
    execute v_sql into v_failing;
    if coalesce(v_failing, 0) > 0 then
      raise exception 'WS_BANKING_FIXTURE_CHECK_CONSTRAINT_VIOLATED'
        using errcode = 'P0001',
              detail = jsonb_build_object(
                'relation', p_relation::text,
                'constraint', v_constraint.conname,
                'failing_rows', v_failing,
                'context', p_context
              )::text;
    end if;
    v_checked_checks := v_checked_checks + 1;
  end loop;

  -- NOT NULL columns
  for v_column in
    select attribute_row.attname
    from pg_attribute as attribute_row
    where attribute_row.attrelid = p_relation
      and attribute_row.attnum > 0
      and not attribute_row.attisdropped
      and attribute_row.attnotnull
    order by attribute_row.attnum
  loop
    execute format(
      'select count(*) from %s as fixture_row where (%s) and fixture_row.%I is null',
      p_relation, p_row_predicate, v_column.attname
    ) into v_failing;
    if coalesce(v_failing, 0) > 0 then
      raise exception 'WS_BANKING_FIXTURE_NOT_NULL_VIOLATED'
        using errcode = 'P0001',
              detail = jsonb_build_object(
                'relation', p_relation::text,
                'column', v_column.attname,
                'failing_rows', v_failing,
                'context', p_context
              )::text;
    end if;
    v_checked_not_null := v_checked_not_null + 1;
  end loop;

  -- UNIQUE / PRIMARY KEY indexes, including partial indexes
  for v_index in
    select index_class.relname as index_name,
           pg_get_indexdef(index_row.indexrelid) as definition
    from pg_index as index_row
    join pg_class as index_class on index_class.oid = index_row.indexrelid
    where index_row.indrelid = p_relation
      and (index_row.indisunique or index_row.indisprimary)
    order by index_class.relname
  loop
    v_index_columns := regexp_replace(v_index.definition, '^.*USING [a-z]+ \((.*?)\)( WHERE .*)?$', '\1');
    v_index_predicate := case
      when v_index.definition ~ ' WHERE ' then regexp_replace(v_index.definition, '^.* WHERE (.*)$', '\1')
      else 'true'
    end;

    v_sql := format(
      'select count(*) from ('
      || ' select 1 from %s as fixture_row where (%s)'
      || ' group by %s having count(*) > 1) as duplicate_groups',
      p_relation,
      format('(%s) and (%s)', v_index_predicate, 'true'),
      v_index_columns
    );
    execute v_sql into v_failing;
    if coalesce(v_failing, 0) > 0 then
      raise exception 'WS_BANKING_FIXTURE_UNIQUE_INDEX_VIOLATED'
        using errcode = 'P0001',
              detail = jsonb_build_object(
                'relation', p_relation::text,
                'index', v_index.index_name,
                'duplicate_groups', v_failing,
                'context', p_context
              )::text;
    end if;
    v_checked_unique := v_checked_unique + 1;
  end loop;

  -- FOREIGN KEY constraints: prove the referenced row exists for the fixture rows
  for v_constraint in
    select constraint_row.conname,
           constraint_row.confrelid,
           constraint_row.conkey,
           constraint_row.confkey
    from pg_constraint as constraint_row
    where constraint_row.conrelid = p_relation
      and constraint_row.contype = 'f'
    order by constraint_row.conname
  loop
    select string_agg(
             format(
               'fixture_row.%I is not distinct from referenced_row.%I',
               (select attname from pg_attribute where attrelid = p_relation and attnum = local_key.attnum),
               (select attname from pg_attribute where attrelid = v_constraint.confrelid and attnum = foreign_key.attnum)
             ), ' and ')
    into v_sql
    from unnest(v_constraint.conkey) with ordinality as local_key(attnum, ord)
    join unnest(v_constraint.confkey) with ordinality as foreign_key(attnum, ord)
      on foreign_key.ord = local_key.ord;

    execute format(
      'select count(*) from %s as fixture_row where (%s) and %s and not exists ('
      || ' select 1 from %s as referenced_row where %s)',
      p_relation,
      p_row_predicate,
      (
        select string_agg(format('fixture_row.%I is not null',
                 (select attname from pg_attribute where attrelid = p_relation and attnum = local_key.attnum)), ' and ')
        from unnest(v_constraint.conkey) as local_key(attnum)
      ),
      v_constraint.confrelid::regclass,
      v_sql
    ) into v_failing;

    if coalesce(v_failing, 0) > 0 then
      raise exception 'WS_BANKING_FIXTURE_FOREIGN_KEY_VIOLATED'
        using errcode = 'P0001',
              detail = jsonb_build_object(
                'relation', p_relation::text,
                'constraint', v_constraint.conname,
                'failing_rows', v_failing,
                'context', p_context
              )::text;
    end if;
    v_checked_fk := v_checked_fk + 1;
  end loop;

  return jsonb_build_object(
    'relation', p_relation::text,
    'rows', v_row_count,
    'check_constraints', v_checked_checks,
    'not_null_columns', v_checked_not_null,
    'unique_indexes', v_checked_unique,
    'foreign_keys', v_checked_fk,
    'context', p_context
  );
end;
$$;

-- ---------------------------------------------------------------------------
-- Seed register
-- ---------------------------------------------------------------------------
-- Every direct seed records the installed writer whose output it reproduces and
-- the exact code lines cited.  `ws_banking_fixture.seed_register` is asserted by
-- `selfcheck.sql`: a seed with no citation is a test failure, not a warning.
create table if not exists ws_banking_fixture.seed_register (
  seed_name text primary key,
  state_key text not null,
  relation text not null,
  installed_writer text not null,
  cited_lines text not null,
  note text,
  recorded_at_utc timestamptz not null default now()
);

create or replace function ws_banking_fixture.register_seed(
  p_seed_name text,
  p_state_key text,
  p_relation text,
  p_installed_writer text,
  p_cited_lines text,
  p_note text default null
)
returns void
language plpgsql
as $$
begin
  perform ws_banking_fixture.assert_local_only();

  if nullif(btrim(coalesce(p_installed_writer, '')), '') is null
     or nullif(btrim(coalesce(p_cited_lines, '')), '') is null then
    raise exception 'WS_BANKING_FIXTURE_SEED_CITATION_REQUIRED'
      using errcode = 'P0001',
            detail = jsonb_build_object('seed_name', p_seed_name)::text;
  end if;

  insert into ws_banking_fixture.seed_register(
    seed_name, state_key, relation, installed_writer, cited_lines, note
  ) values (
    p_seed_name, p_state_key, p_relation, p_installed_writer, p_cited_lines, p_note
  )
  on conflict (seed_name) do update
  set state_key = excluded.state_key,
      relation = excluded.relation,
      installed_writer = excluded.installed_writer,
      cited_lines = excluded.cited_lines,
      note = excluded.note,
      recorded_at_utc = now();
end;
$$;

-- ---------------------------------------------------------------------------
-- State register
-- ---------------------------------------------------------------------------
create table if not exists ws_banking_fixture.state_register (
  state_key text primary key,
  build_method text not null check (build_method in ('REAL_OWNER', 'NAMED_SEED', 'MIXED')),
  proofs text not null,
  result_json jsonb not null,
  built_at_utc timestamptz not null default now()
);

create or replace function ws_banking_fixture.register_state(
  p_state_key text,
  p_build_method text,
  p_proofs text,
  p_result_json jsonb
)
returns jsonb
language plpgsql
as $$
begin
  insert into ws_banking_fixture.state_register(state_key, build_method, proofs, result_json)
  values (p_state_key, p_build_method, p_proofs, p_result_json)
  on conflict (state_key) do update
  set build_method = excluded.build_method,
      proofs = excluded.proofs,
      result_json = excluded.result_json,
      built_at_utc = now();

  return p_result_json || jsonb_build_object(
    'state_key', p_state_key,
    'build_method', p_build_method,
    'proofs', p_proofs
  );
end;
$$;
