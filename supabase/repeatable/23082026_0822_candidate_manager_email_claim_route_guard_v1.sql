create or replace function private._candidate_manager_email_claim_route_current_v1(
  p_route_receipt_id uuid,
  p_manager_route_ticket_id uuid,
  p_route_revision bigint,
  p_registration_receipt_sha256_hex text,
  p_workflow_id uuid,
  p_approval_request_id uuid,
  p_request_generation integer
)
returns boolean
language sql
stable
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
  select exists (
    select 1
    from public.candidate_manager_email_route_receipts route_receipt
    where route_receipt.route_receipt_id = p_route_receipt_id
      and route_receipt.manager_route_ticket_id = p_manager_route_ticket_id
      and route_receipt.route_revision = p_route_revision
      and pg_catalog.encode(route_receipt.registration_receipt_sha256, 'hex') =
            pg_catalog.lower(pg_catalog.btrim(coalesce(p_registration_receipt_sha256_hex, '')))
      and route_receipt.workflow_id = p_workflow_id
      and route_receipt.approval_request_id = p_approval_request_id
      and route_receipt.request_generation = p_request_generation
      and route_receipt.state = 'CURRENT'
  );
$function$;

alter function private._candidate_manager_email_claim_route_current_v1(
  uuid,uuid,bigint,text,uuid,uuid,integer
) owner to postgres;

revoke all on function private._candidate_manager_email_claim_route_current_v1(
  uuid,uuid,bigint,text,uuid,uuid,integer
) from public,anon,authenticated,service_role;

grant execute on function private._candidate_manager_email_claim_route_current_v1(
  uuid,uuid,bigint,text,uuid,uuid,integer
) to service_role;
