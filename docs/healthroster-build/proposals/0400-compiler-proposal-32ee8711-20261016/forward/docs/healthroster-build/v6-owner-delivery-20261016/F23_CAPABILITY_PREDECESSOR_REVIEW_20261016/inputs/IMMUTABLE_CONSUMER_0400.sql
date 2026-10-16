-- One-time reviewed capability CHECK extension; source only until protected release.
begin;
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

commit;
