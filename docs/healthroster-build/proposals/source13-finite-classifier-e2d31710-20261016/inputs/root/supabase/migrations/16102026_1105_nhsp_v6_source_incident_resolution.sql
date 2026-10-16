-- One-time CloudTMS schema/data migration: nhsp_v6_source_incident_resolution
-- Source-only Health assembly; actual cross-owner dependencies remain mandatory.

\set ON_ERROR_STOP on

begin;

-- Exact d863577918ce7d439206296e91ad78de298e98b2:docs/healthroster-build/proposals/source-pay-decision-v5-foundation-20261015/one-time/06_native_source_contact_resolution.sqlinc
-- Finite Health predecessor guard before the exact d863 one-time/06 body.
-- The transaction lock prevents another DDL writer replacing the checked object.
lock table public.weekly_discrepancy_incidents in access exclusive mode;
do $source_incident_resolution_predecessor$
declare actual_definition text;
begin
 select pg_catalog.pg_get_constraintdef(c.oid,true)
 into strict actual_definition
 from pg_catalog.pg_constraint c
 join pg_catalog.pg_class t on t.oid=c.conrelid
 join pg_catalog.pg_namespace n on n.oid=t.relnamespace
 join pg_catalog.pg_attribute a on a.attrelid=t.oid and a.attname='resolution_kind'
 where n.nspname='public' and t.relname='weekly_discrepancy_incidents'
  and t.relkind='r' and not a.attisdropped and a.atttypid='pg_catalog.text'::regtype
  and not a.attnotnull and a.attgenerated=''
  and c.conname='weekly_discrepancy_incidents_resolution_kind_check'
  and c.contype='c' and c.convalidated and not c.condeferrable and not c.condeferred
  and c.conislocal and c.coninhcount=0 and not c.connoinherit
  and c.conkey=array[a.attnum]::smallint[];
 if actual_definition is distinct from
 $source_expected_predecessor$CHECK (resolution_kind IS NULL OR (resolution_kind = ANY (ARRAY['SOURCE_MATCHED'::text, 'OFFICE_ACCEPTED_SYSTEM_HOURS'::text, 'CANDIDATE_CORRECTED'::text, 'MANAGER_CONFIRMED_SYSTEM_HOURS'::text, 'RECORDED_NOT_WORKED'::text, 'CORRECT_FINAL_SOURCE'::text])))$source_expected_predecessor$ then
  raise exception 'SOURCE_INCIDENT_RESOLUTION_PREDECESSOR_MISMATCH' using errcode='55000';
 end if;
exception when no_data_found or too_many_rows then
 raise exception 'SOURCE_INCIDENT_RESOLUTION_PREDECESSOR_MISMATCH' using errcode='55000';
end;
$source_incident_resolution_predecessor$;

-- Narrow existing native classification successor, after immutable root01.
-- Input: a934ee1d460b444939234e812babccb90dc3ddff:
-- supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql,
-- public.weekly_discrepancy_incidents.resolution_kind inline CHECK.
-- Root mounts in a later Health creation migration after its actual Bank02.
alter table public.weekly_discrepancy_incidents drop constraint weekly_discrepancy_incidents_resolution_kind_check;
alter table public.weekly_discrepancy_incidents add constraint weekly_discrepancy_incidents_resolution_kind_check
 check(resolution_kind is null or resolution_kind in('SOURCE_MATCHED','OFFICE_ACCEPTED_SYSTEM_HOURS','CANDIDATE_CORRECTED',
  'MANAGER_CONFIRMED_SYSTEM_HOURS','RECORDED_NOT_WORKED','CORRECT_FINAL_SOURCE','SOURCE_PAY_DECISION'));

commit;
