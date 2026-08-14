\set ON_ERROR_STOP on

do $$
declare
  v_workbench text;
  v_authorise text;
  v_bulk_authorise text;
  v_dataset text;
  v_finalise text;
begin
  v_workbench := pg_get_functiondef(
    'public.bulk_timesheet_workbench_row_source_v1(jsonb)'::regprocedure
  );
  v_authorise := pg_get_functiondef(
    'public.timesheet_authorise_generic_atomic(uuid,uuid,uuid,timestamp with time zone,text)'::regprocedure
  );
  v_bulk_authorise := pg_get_functiondef(
    'public.timesheet_authorise_bulk_atomic(jsonb,uuid,timestamp with time zone)'::regprocedure
  );
  v_dataset := pg_get_functiondef(
    'public.bulk_authorise_dataset_v1(jsonb)'::regprocedure
  );
  v_finalise := pg_get_functiondef(
    'public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamp with time zone,jsonb)'::regprocedure
  );

  if v_workbench not like '%''Unprocessed''%'
     or v_workbench not like '%''Processed''%'
     or v_workbench not like '%''Authorised for Invoicing''%'
     or v_workbench not like '%''Partially Invoiced''%'
     or v_workbench not like '%''Invoiced''%'
     or v_workbench not like '%''Archived''%'
     or v_workbench not like '%''Processing Delayed''%' then
    raise exception 'Candidate Office processing-status catalogue is incomplete';
  end if;

  if v_workbench like '%Awaiting signed QR timesheet%'
     or v_workbench like '%Awaiting Authorisation%'
     or v_workbench like '%QR_NOT_ISSUED%'
     or v_workbench like '%QR_ISSUED_AWAITING_SIGNATURE%' then
    raise exception 'Legacy QR/authorisation projection remains in Processing Status';
  end if;

  if v_authorise like '%AWAITING_SIGNED_QR%'
     or v_bulk_authorise like '%AWAITING_SIGNED_QR%'
     or v_authorise like '%qr_unsigned%'
     or v_bulk_authorise like '%qr_unsigned%' then
    raise exception 'Legacy QR fields still gate Office Authorise';
  end if;

  if v_authorise not like '%v_prev_status NOT IN (%PENDING_AUTH%READY_FOR_HR%'
     or v_bulk_authorise not like '%tsfin_processing_status NOT IN (%PENDING_AUTH%READY_FOR_HR%' then
    raise exception 'Canonical processing-state Authorise gate is missing';
  end if;

  if v_dataset like '%qr_unsigned_blocked_calc = FALSE%'
     or v_dataset like '%qr_signed_returned_calc = TRUE%' then
    raise exception 'Legacy QR dataset fields still gate Bulk Authorise';
  end if;

  if v_finalise not like '%v_workflow.state<>''RECEIVED''%'
     or v_finalise not like '%CANDIDATE_PAPER_RETURN_INCOMPLETE%'
     or v_finalise not like '%component_kind=''SIGNED_RETURN''%'
     or v_finalise not like '%paper_return_page_key=expected_page->>''page_key''%'
     or v_finalise not like '%source_content_sha256 is not null%' then
    raise exception 'Complete signed QR return authority is not intact';
  end if;
end
$$;

select 'candidate_office_legacy_route_retirement_verification_pass' as result;
