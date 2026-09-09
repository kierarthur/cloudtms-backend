-- A recovered Candidate receipt is now uploaded to a fresh R2 key before it
-- becomes immutable.  Root rows and recovery rows with an expected digest are
-- the physical owners of their keys; generated lineage copies remain logical
-- references and may intentionally share their owner's key.

do $migration$
declare
  v_duplicate_storage_key text;
  v_duplicate_expected_digest bytea;
  v_unbound_pending_receipt uuid;
begin
  -- Do not install the new admission contract while a direct receipt from the
  -- former protocol still has an unbound PENDING upload ticket.  Its bytes
  -- cannot be compared safely with a new reservation, so release coordination
  -- must let the old ticket expire and reconcile the row explicitly.
  select component.id
  into v_unbound_pending_receipt
  from public.candidate_submission_components component
  where component.source_component_id is null
    and component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
    and component.state='PENDING'
    and component.expected_source_content_sha256 is null
  order by component.created_at_utc,component.id
  limit 1;

  if v_unbound_pending_receipt is not null then
    raise exception 'CANDIDATE_COMPONENT_UNBOUND_PENDING_RECEIPT'
      using errcode='55000';
  end if;

  select component.storage_key
  into v_duplicate_storage_key
  from public.candidate_submission_components component
  where component.storage_key is not null
    and (
      component.source_component_id is null
      or (
        component.expected_source_content_sha256 is not null
        and component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
      )
    )
  group by component.storage_key
  having count(*)>1
  order by component.storage_key
  limit 1;

  if v_duplicate_storage_key is not null then
    raise exception 'CANDIDATE_COMPONENT_PHYSICAL_STORAGE_KEY_DUPLICATE'
      using errcode='23505';
  end if;

  -- PREPARE writes the claimed receipt hash to the expected-digest column
  -- before any bytes are uploaded.  A live direct/root upload must therefore
  -- be the only row reserving those bytes, including while it is PENDING.
  select component.expected_source_content_sha256
  into v_duplicate_expected_digest
  from public.candidate_submission_components component
  where component.expected_source_content_sha256 is not null
    and component.source_component_id is null
    and component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
    and component.state in ('PENDING','IMMUTABLE')
  group by component.expected_source_content_sha256
  having count(*)>1
  order by component.expected_source_content_sha256
  limit 1;

  if v_duplicate_expected_digest is not null then
    raise exception 'CANDIDATE_COMPONENT_LIVE_RECEIPT_DIGEST_DUPLICATE'
      using errcode='23505';
  end if;
end;
$migration$;

create unique index if not exists candidate_submission_components_physical_storage_key_uq
  on public.candidate_submission_components(storage_key)
  where storage_key is not null
    and (
      source_component_id is null
      or (
        expected_source_content_sha256 is not null
        and component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
      )
    );

comment on index public.candidate_submission_components_physical_storage_key_uq is
  'One physical owner per Candidate component storage key. Root uploads and exact recovery re-uploads own bytes; generated lineage copies do not.';

create unique index if not exists candidate_submission_components_live_receipt_expected_sha256_uq
  on public.candidate_submission_components(expected_source_content_sha256)
  where expected_source_content_sha256 is not null
    and source_component_id is null
    and component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
    and state in ('PENDING','IMMUTABLE');

comment on index public.candidate_submission_components_live_receipt_expected_sha256_uq is
  'Reserves exact receipt bytes at Candidate PREPARE so concurrent first-use uploads cannot both receive admission. Ended roots and source-linked recovery rows are excluded.';
