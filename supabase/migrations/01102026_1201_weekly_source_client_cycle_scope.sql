-- One-time CloudTMS schema/data migration: weekly_source_client_cycle_scope
-- Import-authoritative ROSTER clients own independent cycles. Existing null
-- scopes remain legacy/group cycles; NHSP report scopes are unchanged.

\set ON_ERROR_STOP on

begin;

alter table public.weekly_source_cycles
  add column scope_client_id uuid references public.clients(id) on delete restrict;

alter table public.weekly_source_cycles
  drop constraint weekly_source_cycles_source_group_id_finalisation_week_endi_key;

alter table public.weekly_source_cycles
  add constraint weekly_source_cycles_group_week_client_uq
  unique nulls not distinct (source_group_id,finalisation_week_ending,scope_client_id);

-- Retain IDs and all historical facts. Attribute an existing ROSTER cycle only
-- when its current source proves one SOURCE_AUTHORITY client. Empty cycles and
-- TIMESHEET_AUTHORITY cycles retain their previous semantics.
update public.weekly_source_cycles cycle
set scope_client_id=upload.safe_client_id
from (select id,case
    when pg_catalog.pg_input_is_valid(file_metadata_json->>'client_id','uuid')
    then (file_metadata_json->>'client_id')::uuid
    else null end as safe_client_id
  from public.weekly_source_uploads) upload,public.weekly_source_groups source_group
where upload.id=cycle.current_complete_upload_id
  and source_group.id=cycle.source_group_id and source_group.source_family='ROSTER'
  and upload.safe_client_id is not null
  and exists(select 1 from public.weekly_source_client_policies policy
    where policy.source_group_id=cycle.source_group_id
      and policy.client_id=upload.safe_client_id
      and cycle.finalisation_week_ending between policy.effective_from
        and coalesce(policy.effective_to,'infinity'::date)
      and policy.authority_mode='SOURCE_AUTHORITY')
  and not exists(select 1 from public.weekly_source_client_cycle_completions completion
    where completion.source_cycle_id=cycle.id and completion.state='CURRENT'
      and completion.client_id<>upload.safe_client_id);

comment on column public.weekly_source_cycles.scope_client_id is
  'Independent import-authoritative ROSTER Client scope; null preserves legacy group and NHSP cycle authority.';

commit;
