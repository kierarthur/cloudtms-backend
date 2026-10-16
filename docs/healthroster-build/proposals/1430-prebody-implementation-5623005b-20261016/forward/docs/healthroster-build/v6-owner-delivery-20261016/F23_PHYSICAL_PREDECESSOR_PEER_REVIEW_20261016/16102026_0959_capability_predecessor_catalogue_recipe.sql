-- SOURCE-ONLY RECIPE; NOT EXECUTED. One SELECT, no DDL/DML/locks/external effects.
-- Root supplies the exact canonical managed pending prefix in its existing
-- shared-lock rollback session. Run AFTER 13102026_1133 has created the real
-- relation, and BEFORE 14102026_0400 changes its CHECKs.
-- Caller must already pin search_path to pg_catalog, public; this query reports
-- refusal when that precondition, database or PostgreSQL major version differs.
-- A structural census is never expression approval. Review complete native
-- definitions against the pinned constructor before generating any companion.
with target as (
  select relation.oid,relation.relkind,relation.relispartition
  from pg_catalog.pg_class relation
  join pg_catalog.pg_namespace namespace on namespace.oid=relation.relnamespace
  where namespace.nspname='public'
    and relation.relname='weekly_source_group_rule_assignments'
), checks as (
  select constraint_row.oid,constraint_row.conname::text constraint_name,
    constraint_row.convalidated is_validated,
    constraint_row.condeferrable is_deferrable,
    constraint_row.condeferred is_deferred,
    constraint_row.connoinherit is_no_inherit,
    constraint_row.conislocal is_local,
    constraint_row.coninhcount inherited_count,
    constraint_row.conparentid=0 has_no_parent_constraint,
    pg_catalog.pg_get_constraintdef(constraint_row.oid) selector_definition_default_false,
    pg_catalog.pg_get_constraintdef(constraint_row.oid,true) evidence_definition_pretty_true,
    pg_catalog.pg_get_expr(constraint_row.conbin,constraint_row.conrelid,true) evidence_expression_pretty_true,
    array(select attribute.attname::text
      from pg_catalog.unnest(constraint_row.conkey) with ordinality key_column(attnum,ordinal)
      join pg_catalog.pg_attribute attribute
        on attribute.attrelid=constraint_row.conrelid and attribute.attnum=key_column.attnum
        and not attribute.attisdropped
      order by key_column.ordinal) key_columns_in_native_order,
    array(select attribute.attname::text
      from pg_catalog.unnest(constraint_row.conkey) key_column(attnum)
      join pg_catalog.pg_attribute attribute
        on attribute.attrelid=constraint_row.conrelid and attribute.attnum=key_column.attnum
        and not attribute.attisdropped
      order by attribute.attname::text collate "C") key_columns_sorted
  from target
  join pg_catalog.pg_constraint constraint_row on constraint_row.conrelid=target.oid
  where constraint_row.contype='c'
), selected as (
  select checks.*,
    case
      when position('PROTECTED_SOURCE' in selector_definition_default_false)>0 then 'ALREADY_EXTENDED'
      when position('rule_set_version_id' in selector_definition_default_false)>0 then 'VERSION'
      when position('RULE_SET' in selector_definition_default_false)>0 then 'PURPOSE'
      else 'UNEXPECTED'
    end selector_role
  from checks
  where position('feature_purpose' in selector_definition_default_false)>0
    and position('SIGNED_CHECK_ONLY' in selector_definition_default_false)>0
), summary as (
  select count(*) selected_count,
    count(*) filter(where selector_role='PURPOSE') purpose_count,
    count(*) filter(where selector_role='VERSION') version_count,
    count(*) filter(where selector_role in ('ALREADY_EXTENDED','UNEXPECTED')) refused_role_count,
    count(*) filter(where not is_validated or is_deferrable or is_deferred
      or is_no_inherit or not is_local or inherited_count<>0 or not has_no_parent_constraint) refused_metadata_count,
    count(*) filter(where
      (selector_role='PURPOSE' and key_columns_sorted is distinct from array['feature_purpose']::text[])
      or (selector_role='VERSION' and key_columns_sorted is distinct from array['feature_purpose','rule_set_version_id']::text[])) refused_column_count
  from selected
), source_contract(role_name,source_expression,expected_columns) as (
  values
    ('PURPOSE','feature_purpose in (''RULE_SET'',''SIGNED_CHECK_ONLY'')',array['feature_purpose']::text[]),
    ('VERSION','feature_purpose is null or ((rule_set_version_id is null)=(feature_purpose=''SIGNED_CHECK_ONLY''))',array['feature_purpose','rule_set_version_id']::text[])
)
select pg_catalog.jsonb_build_object(
  'status','CENSUS_CAPTURE_ONLY_EXPRESSION_REVIEW_REQUIRED',
  'database',pg_catalog.current_database(),
  'postgresql_version_num',pg_catalog.current_setting('server_version_num')::integer,
  'search_path',pg_catalog.current_setting('search_path'),
  'backend_pid',pg_catalog.pg_backend_pid(),
  'transaction_started_at',pg_catalog.transaction_timestamp(),
  'relation','public.weekly_source_group_rule_assignments',
  'relation_present',exists(select 1 from target),
  'expected_selected_count',2,
  'selected_count',summary.selected_count,
  'purpose_count',summary.purpose_count,
  'version_count',summary.version_count,
  'all_check_count',(select count(*) from checks),
  'retained_check_count',(select count(*) from checks)-summary.selected_count,
  'capture_complete',summary.selected_count<=3,
  'structural_preconditions_match',
    pg_catalog.current_database()='cloudtms_test_clone'
    and pg_catalog.current_setting('server_version_num')::integer between 170000 and 179999
    and pg_catalog.current_setting('search_path')='pg_catalog, public'
    and (select count(*) from target)=1
    and exists(select 1 from target where relkind='r' and not relispartition)
    and summary.selected_count=2 and summary.purpose_count=1 and summary.version_count=1
    and summary.refused_role_count=0 and summary.refused_metadata_count=0 and summary.refused_column_count=0,
  'constructor_path','supabase/migrations/13102026_1133_weekly_source_rule_sets_schema.sql',
  'constructor_sha256','8fc11ed0be84c7c53f4bf41a9d92775e2b4f071c49c3cb69c07458cded30e7bf',
  'consumer_path','supabase/migrations/14102026_0400_healthroster_protected_source_capability.sql',
  'consumer_sha256','5f0a2fd5535897286601dc4d2c84d07f0a14cda9c0222158ac89168758a70a16',
  'source_expectations',(select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'role',role_name,'source_expression',source_expression,'expected_columns',expected_columns)
    order by role_name collate "C") from source_contract),
  'selected_checks',coalesce((select pg_catalog.jsonb_agg(pg_catalog.to_jsonb(captured)
    order by captured.constraint_name collate "C") from (
      select selected.constraint_name,selected.selector_role,
        selected.selector_definition_default_false,selected.evidence_definition_pretty_true,
        selected.evidence_expression_pretty_true,selected.key_columns_in_native_order,
        selected.key_columns_sorted,selected.is_validated,selected.is_deferrable,
        selected.is_deferred,selected.is_no_inherit,selected.is_local,
        selected.inherited_count,selected.has_no_parent_constraint
      from selected order by selected.constraint_name collate "C" limit 3
    ) captured),'[]'::jsonb),
  'expression_approved',false,
  'native_companion_approved',false
) capability_predecessor_catalogue
from summary;
