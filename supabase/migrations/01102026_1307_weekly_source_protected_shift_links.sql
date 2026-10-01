-- One-time CloudTMS schema/data migration: weekly_source_protected_shift_links
-- Append an explicit Office shift choice to the existing immutable per-row
-- choices. No historical choice, source row, entitlement or payment is changed.

\set ON_ERROR_STOP on

begin;

alter table private.weekly_source_office_row_choices
  add column work_event_id uuid references public.weekly_work_events(id) on delete restrict,
  add column match_reason text,
  add column separate_shift boolean not null default false,
  add constraint weekly_source_office_shift_match_reason_check
    check ((work_event_id is null and not separate_shift and match_reason is null)
      or (((work_event_id is not null and not separate_shift) or (work_event_id is null and separate_shift))
        and match_reason is not null and char_length(btrim(match_reason)) between 1 and 1000));

comment on column private.weekly_source_office_row_choices.work_event_id is
  'Office-confirmed existing protected shift identity for this exact immutable source row; revalidated during publication.';

commit;
