-- Candidate App evidence lineage and one-active-TIMESHEET integrity.
--
-- Installation precondition: no timesheet may currently have more than one
-- non-SUPERSEDED TIMESHEET evidence row.  The guarded DO block fails closed if
-- readiness has changed since the audit; it does not repair or delete evidence.

alter table public.timesheet_evidence
  add column if not exists document_role text,
  add column if not exists candidate_component_id uuid;

-- Existing evidence is classified before the column is constrained.  This is
-- intentionally narrow: an existing TIMESHEET item is a signed-timesheet
-- document; every other pre-Candidate-App item remains source evidence.
update public.timesheet_evidence
set document_role=case
  when upper(btrim(kind))='TIMESHEET' then 'SIGNED_TIMESHEET'
  else 'SOURCE_EVIDENCE'
end
where document_role is null;

alter table public.timesheet_evidence
  alter column document_role set default 'SOURCE_EVIDENCE',
  alter column document_role set not null;

do $migration$
begin
  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.timesheet_evidence'::regclass
      and conname = 'timesheet_evidence_document_role_ck'
  ) then
    alter table public.timesheet_evidence
      add constraint timesheet_evidence_document_role_ck
      check (document_role in (
        'SOURCE_EVIDENCE','MILEAGE_CLAIM_FORM','EXPENSE_MILEAGE_APPROVAL_SUMMARY',
        'SIGNED_TIMESHEET','MANAGER_SIGNATURE','CANDIDATE_SIGNATURE'
      ));
  end if;

  if not exists (
    select 1 from pg_constraint
    where conrelid = 'public.timesheet_evidence'::regclass
      and conname = 'timesheet_evidence_candidate_component_fk'
  ) then
    alter table public.timesheet_evidence
      add constraint timesheet_evidence_candidate_component_fk
      foreign key (candidate_component_id)
      references public.candidate_submission_components(id)
      on delete restrict;
  end if;

  if exists (
    select 1
    from public.timesheet_evidence evidence
    where upper(btrim(evidence.kind)) = 'TIMESHEET'
      and evidence.processing_state <> 'SUPERSEDED'
    group by evidence.timesheet_id
    having count(*) > 1
  ) then
    raise exception 'CANDIDATE_APP_TIMESHEET_EVIDENCE_READINESS_FAILED'
      using errcode = '23505',
            detail = jsonb_build_object(
              'code','CANDIDATE_APP_TIMESHEET_EVIDENCE_READINESS_FAILED',
              'required_action','Resolve duplicate active TIMESHEET evidence before installing the unique index'
            )::text;
  end if;
end;
$migration$;

create unique index if not exists timesheet_evidence_candidate_component_uq
  on public.timesheet_evidence(candidate_component_id)
  where candidate_component_id is not null;

create unique index if not exists timesheet_evidence_one_active_timesheet_uq
  on public.timesheet_evidence(timesheet_id)
  where upper(btrim(kind)) = 'TIMESHEET'
    and processing_state <> 'SUPERSEDED';

comment on column public.timesheet_evidence.document_role is
  'Semantic evidence/document role. Expense and Mileage Approval Summary is OTHER kind with EXPENSE_MILEAGE_APPROVAL_SUMMARY role.';
comment on column public.timesheet_evidence.candidate_component_id is
  'Immutable Candidate App component lineage. The component owns the exact-byte SHA-256 digest; it is not duplicated here.';
comment on index public.timesheet_evidence_one_active_timesheet_uq is
  'At most one non-SUPERSEDED TIMESHEET evidence row per CloudTMS timesheet.';
