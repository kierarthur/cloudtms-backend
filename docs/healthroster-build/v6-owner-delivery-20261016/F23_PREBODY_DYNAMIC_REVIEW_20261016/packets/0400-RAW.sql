begin;
do $managed_writer$ begin
  if not exists(select 1 from pg_catalog.pg_locks where locktype='advisory'
    and pid=pg_catalog.pg_backend_pid() and granted and mode='ExclusiveLock' and objsubid=1
    and classid=((pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0)>>32)&4294967295)::oid
    and objid=(pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0)&4294967295)::oid)
  then raise exception 'CLOUDTMS_MANAGED_WRITER_GUARD_LOST'; end if;
end $managed_writer$;
-- Exact0400 predecessor companion; no transaction wrapper or migration edit.
-- Capture32ee8711 / actual canonical pending prefixbcfd75a0 / PG17.11.
-- Same existing atomic transaction: LOCK + assertion BEFORE immutable0400 DO.
lock table public.weekly_source_group_rule_assignments in access exclusive mode;

do $health_capability_prebody$
declare v_actual jsonb; v_all_checks integer;
begin
  if not exists(select 1 from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where c.oid='public.weekly_source_group_rule_assignments'::regclass
      and n.nspname='public' and c.relname='weekly_source_group_rule_assignments'
      and c.relkind='r' and not c.relispartition
      and c.relowner=(select oid from pg_catalog.pg_roles where rolname=current_user)
      and not exists(select 1 from pg_catalog.pg_inherits i where i.inhrelid=c.oid or i.inhparent=c.oid)) then
    raise exception 'HEALTHROSTER_CAPABILITY_PREDECESSOR_RELATION_CHANGED' using errcode='55000';
  end if;
  select count(*)::integer into v_all_checks from pg_catalog.pg_constraint c
    where c.conrelid='public.weekly_source_group_rule_assignments'::regclass and c.contype='c';
  select coalesce(jsonb_agg(jsonb_build_object(
    'constraint_name',c.conname::text,
    'selector_definition_default_false',d.definition,
    'evidence_definition_pretty_true',pg_catalog.pg_get_constraintdef(c.oid,true),
    'evidence_expression_pretty_true',pg_catalog.pg_get_expr(c.conbin,c.conrelid,true),
    'selector_role',case when position('PROTECTED_SOURCE' in d.definition)>0 then 'ALREADY_EXTENDED'
      when position('rule_set_version_id' in d.definition)>0 then 'VERSION'
      when position('RULE_SET' in d.definition)>0 then 'PURPOSE' else 'UNEXPECTED' end,
    'key_columns_in_native_order',array(select a.attname::text
      from unnest(c.conkey) with ordinality k(attnum,ord)
      join pg_catalog.pg_attribute a on a.attrelid=c.conrelid and a.attnum=k.attnum
      order by k.ord),
    'key_columns_sorted',array(select a.attname::text
      from unnest(c.conkey) k(attnum)
      join pg_catalog.pg_attribute a on a.attrelid=c.conrelid and a.attnum=k.attnum
      order by a.attname),
    'is_local',c.conislocal,'is_validated',c.convalidated,
    'is_deferred',c.condeferred,'is_deferrable',c.condeferrable,
    'is_no_inherit',c.connoinherit,'inherited_count',c.coninhcount,
    'has_no_parent_constraint',c.conparentid=0
  ) order by c.conname),'[]'::jsonb) into v_actual
  from pg_catalog.pg_constraint c
  cross join lateral(select pg_catalog.pg_get_constraintdef(c.oid) definition)d
  where c.conrelid='public.weekly_source_group_rule_assignments'::regclass and c.contype='c'
    and position('feature_purpose' in d.definition)>0
    and position('SIGNED_CHECK_ONLY' in d.definition)>0;
  if v_all_checks<>10 or jsonb_array_length(v_actual)<>2
     or v_actual is distinct from $expected0400$[{"is_local":true,"is_deferred":false,"is_validated":true,"is_deferrable":false,"is_no_inherit":false,"selector_role":"VERSION","constraint_name":"weekly_source_group_rule_assignments_check2","inherited_count":0,"key_columns_sorted":["feature_purpose","rule_set_version_id"],"has_no_parent_constraint":true,"key_columns_in_native_order":["feature_purpose","rule_set_version_id"],"evidence_definition_pretty_true":"CHECK (feature_purpose IS NULL OR (rule_set_version_id IS NULL) = (feature_purpose = 'SIGNED_CHECK_ONLY'::text))","evidence_expression_pretty_true":"feature_purpose IS NULL OR (rule_set_version_id IS NULL) = (feature_purpose = 'SIGNED_CHECK_ONLY'::text)","selector_definition_default_false":"CHECK (((feature_purpose IS NULL) OR ((rule_set_version_id IS NULL) = (feature_purpose = 'SIGNED_CHECK_ONLY'::text))))"},{"is_local":true,"is_deferred":false,"is_validated":true,"is_deferrable":false,"is_no_inherit":false,"selector_role":"PURPOSE","constraint_name":"weekly_source_group_rule_assignments_feature_purpose_check","inherited_count":0,"key_columns_sorted":["feature_purpose"],"has_no_parent_constraint":true,"key_columns_in_native_order":["feature_purpose"],"evidence_definition_pretty_true":"CHECK (feature_purpose = ANY (ARRAY['RULE_SET'::text, 'SIGNED_CHECK_ONLY'::text]))","evidence_expression_pretty_true":"feature_purpose = ANY (ARRAY['RULE_SET'::text, 'SIGNED_CHECK_ONLY'::text])","selector_definition_default_false":"CHECK ((feature_purpose = ANY (ARRAY['RULE_SET'::text, 'SIGNED_CHECK_ONLY'::text])))"}]$expected0400$::jsonb then
    raise exception 'HEALTHROSTER_CAPABILITY_PREDECESSOR_CHECKS_CHANGED' using errcode='55000';
  end if;
end;
$health_capability_prebody$;

-- One-time reviewed capability CHECK extension; source only until protected release.

-- Proposal: extend only two existing assignment CHECK constraints. No table,
-- column, pricing setting, capability store, policy overlay or exposed route.
do $extend_existing_purpose$
declare row_data record; purpose_checks integer:=0; version_checks integer:=0;
begin
  for row_data in select conname,pg_get_constraintdef(oid) definition from pg_catalog.pg_constraint
    where conrelid='public.weekly_source_group_rule_assignments'::regclass and contype='c'
  loop
    if position('feature_purpose' in row_data.definition)>0 and position('SIGNED_CHECK_ONLY' in row_data.definition)>0 then
      if position('PROTECTED_SOURCE' in row_data.definition)>0 then
        raise exception 'WEEKLY_SOURCE_CAPABILITY_SCHEMA_ALREADY_EXTENDED' using errcode='55000'; end if;
      if position('rule_set_version_id' in row_data.definition)>0 then version_checks:=version_checks+1;
      elsif position('RULE_SET' in row_data.definition)>0 then purpose_checks:=purpose_checks+1;
      else raise exception 'WEEKLY_SOURCE_CAPABILITY_SCHEMA_UNEXPECTED_CHECK' using errcode='55000'; end if;
      execute format('alter table public.weekly_source_group_rule_assignments drop constraint %I',row_data.conname);
    end if;
  end loop;
  if purpose_checks<>1 or version_checks<>1 then
    raise exception 'WEEKLY_SOURCE_CAPABILITY_SCHEMA_CHECK_CENSUS_CHANGED' using errcode='55000'; end if;
  alter table public.weekly_source_group_rule_assignments add constraint weekly_source_assignment_feature_purpose_v2
    check (feature_purpose in ('RULE_SET','SIGNED_CHECK_ONLY','PROTECTED_SOURCE'));
  alter table public.weekly_source_group_rule_assignments add constraint weekly_source_assignment_feature_version_v2
    check (feature_purpose is null or ((rule_set_version_id is null)=(feature_purpose in ('SIGNED_CHECK_ONLY','PROTECTED_SOURCE'))));
end;
$extend_existing_purpose$;



do $managed_writer$ begin
  if not exists(select 1 from pg_catalog.pg_locks where locktype='advisory'
    and pid=pg_catalog.pg_backend_pid() and granted and mode='ExclusiveLock' and objsubid=1
    and classid=((pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0)>>32)&4294967295)::oid
    and objid=(pg_catalog.hashtextextended('cloudtms_database_release_admission_v1',0)&4294967295)::oid)
  then raise exception 'CLOUDTMS_MANAGED_WRITER_GUARD_LOST'; end if;
end $managed_writer$;
insert into private.cloudtms_migration_ledger(path,content_sha256,first_release_id)
    values('supabase/migrations/14102026_0400_healthroster_protected_source_capability.sql','5f0a2fd5535897286601dc4d2c84d07f0a14cda9c0222158ac89168758a70a16','HEALTHROSTER_F23_PREBODY_SOURCE_ONLY_20261016');
commit;