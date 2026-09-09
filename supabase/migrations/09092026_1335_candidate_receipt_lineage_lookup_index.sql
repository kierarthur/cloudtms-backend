-- Support the atomic Candidate receipt-lineage admission check.  Immutable
-- source rows are found by their primary key; this index finds every derived
-- use without scanning the complete Candidate evidence history.

\set ON_ERROR_STOP on

begin;

create index if not exists candidate_submission_components_source_lineage_idx
  on public.candidate_submission_components(
    source_component_id,
    workflow_id,
    workflow_generation,
    expense_category,
    state
  )
  where source_component_id is not null;

commit;
