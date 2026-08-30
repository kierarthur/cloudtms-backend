-- Completes one returned PAPER page only after the actual uploaded JPEG has
-- independently passed the exact current TSQ2 page proof. The established
-- component lifecycle remains the mutation owner; this adapter adds the
-- durable proof receipt in the same database transaction.

\set ON_ERROR_STOP on

begin;

create or replace function public.candidate_paper_return_component_complete_v2(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_component_id uuid,
  p_source_content_sha256_hex text,
  p_verified_byte_size bigint,
  p_verified_media_type text,
  p_verified_image_width integer,
  p_verified_image_height integer,
  p_manifest_sha256_hex text,
  p_page_key text,
  p_qr_payload jsonb,
  p_proof_receipt_sha256_hex text,
  p_qr_payload_sha256_hex text,
  p_idempotency_key text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
volatile
security definer
set search_path = pg_catalog, public, private, extensions, pg_temp
as $function$
declare
  v_proof jsonb;
  v_result jsonb;
  v_component public.candidate_submission_components%rowtype;
begin
  if p_component_id is null
     or lower(coalesce(p_source_content_sha256_hex,'')) !~ '^[0-9a-f]{64}$'
     or p_verified_byte_size not between 1 and 15728640
     or lower(coalesce(p_verified_media_type,''))<>'image/jpeg'
     or p_verified_image_width not between 1 and 10000
     or p_verified_image_height not between 1 and 10000
     or lower(coalesce(p_proof_receipt_sha256_hex,'')) !~ '^[0-9a-f]{64}$'
     or lower(coalesce(p_qr_payload_sha256_hex,'')) !~ '^[0-9a-f]{64}$' then
    raise exception 'CANDIDATE_PAPER_QR_PROOF_INVALID' using errcode='22023';
  end if;

  v_proof:=public.candidate_paper_return_proof_validate_v2(
    p_session_id,
    p_environment,
    p_workflow_id,
    p_expected_generation,
    p_manifest_sha256_hex,
    p_page_key,
    p_qr_payload,
    p_now_utc
  );
  if v_proof->>'proof_contract_version'<>'CANDIDATE_PAPER_RETURN_PROOF_V2'
     or lower(v_proof->>'proof_receipt_sha256')
       <>lower(p_proof_receipt_sha256_hex)
     or lower(v_proof->>'qr_payload_sha256')
       <>lower(p_qr_payload_sha256_hex)
     or v_proof->>'paper_return_page_key'<>p_page_key then
    raise exception 'CANDIDATE_PAPER_QR_PROOF_STALE' using errcode='40001';
  end if;

  v_result:=public.candidate_workflow_transition_atomic_v1(
    p_session_id,
    p_environment,
    p_workflow_id,
    'COMPONENT_COMPLETE',
    p_expected_generation,
    jsonb_build_object(
      'component_id',p_component_id,
      'source_content_sha256_hex',lower(p_source_content_sha256_hex),
      'verified_byte_size',p_verified_byte_size,
      'verified_media_type','image/jpeg',
      'verified_image_width',p_verified_image_width,
      'verified_image_height',p_verified_image_height
    ),
    p_idempotency_key,
    p_now_utc
  );

  select * into v_component
  from public.candidate_submission_components
  where id=p_component_id
    and workflow_id=p_workflow_id
    and workflow_generation=p_expected_generation
  for update;
  if not found
     or v_component.component_kind<>'SIGNED_RETURN'
     or v_component.paper_return_page_key<>p_page_key
     or v_component.state<>'IMMUTABLE'
     or encode(v_component.source_content_sha256,'hex')
       <>lower(p_source_content_sha256_hex)
     or lower(v_component.media_type)<>'image/jpeg'
     or v_component.byte_size<>p_verified_byte_size
     or (v_component.paper_return_proof_receipt_sha256 is not null
       and encode(v_component.paper_return_proof_receipt_sha256,'hex')
         <>lower(p_proof_receipt_sha256_hex))
     or (v_component.paper_return_qr_payload_sha256 is not null
       and encode(v_component.paper_return_qr_payload_sha256,'hex')
         <>lower(p_qr_payload_sha256_hex)) then
    raise exception 'CANDIDATE_PAPER_QR_PROOF_STALE' using errcode='40001';
  end if;

  update public.candidate_submission_components set
    paper_return_proof_receipt_sha256=decode(lower(p_proof_receipt_sha256_hex),'hex'),
    paper_return_qr_payload_sha256=decode(lower(p_qr_payload_sha256_hex),'hex'),
    paper_return_verified_at_utc=coalesce(paper_return_verified_at_utc,p_now_utc)
  where id=v_component.id;

  return v_result||jsonb_build_object(
    'paper_return_page_verified',true,
    'paper_return_page_key',p_page_key,
    'proof_contract_version','CANDIDATE_PAPER_RETURN_PROOF_V2'
  );
end
$function$;

alter function public.candidate_paper_return_component_complete_v2(
  uuid,text,uuid,integer,uuid,text,bigint,text,integer,integer,text,text,
  jsonb,text,text,text,timestamptz
) owner to postgres;

revoke all on function public.candidate_paper_return_component_complete_v2(
  uuid,text,uuid,integer,uuid,text,bigint,text,integer,integer,text,text,
  jsonb,text,text,text,timestamptz
) from public,anon,authenticated;
grant execute on function public.candidate_paper_return_component_complete_v2(
  uuid,text,uuid,integer,uuid,text,bigint,text,integer,integer,text,text,
  jsonb,text,text,text,timestamptz
) to service_role;

comment on function public.candidate_paper_return_component_complete_v2(
  uuid,text,uuid,integer,uuid,text,bigint,text,integer,integer,text,text,
  jsonb,text,text,text,timestamptz
) is 'Atomically completes one returned PAPER JPEG and stores its exact current TSQ2 server-verification receipt.';

notify pgrst, 'reload schema';

commit;
