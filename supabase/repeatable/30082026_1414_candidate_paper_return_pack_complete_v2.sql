-- Accepts an entire returned QR pack in one database transaction. Every
-- staged JPEG has already been decoded from its real bytes by the private
-- Worker. This routine revalidates the exact manifest/page proofs, completes
-- every page, and performs PAPER_RETURN only when the full set succeeds.

\set ON_ERROR_STOP on

begin;

create or replace function public.candidate_paper_return_pack_complete_v2(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_verified_pages jsonb,
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
  v_session jsonb;
  v_candidate_id uuid;
  v_workflow public.candidate_submission_workflows%rowtype;
  v_page jsonb;
  v_page_count integer;
  v_expected_count integer;
  v_key_hex text;
  v_component_key text;
  v_result jsonb;
begin
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_writes');
  if p_expected_generation is null or p_expected_generation<1
     or jsonb_typeof(p_verified_pages)<>'array'
     or jsonb_array_length(p_verified_pages) not between 1 and 100
     or nullif(btrim(coalesce(p_idempotency_key,'')),'') is null
     or btrim(p_idempotency_key) !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$' then
    raise exception 'CANDIDATE_PAPER_RETURN_PACK_INVALID' using errcode='22023';
  end if;
  v_session:=private._candidate_session_context_v1(
    p_session_id,p_environment,null,p_now_utc,true
  );
  v_candidate_id:=nullif(v_session->>'selected_candidate_id','')::uuid;
  select * into v_workflow
  from public.candidate_submission_workflows
  where id=p_workflow_id and candidate_id=v_candidate_id
  for update;
  if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
  if v_workflow.generation<>p_expected_generation
     or v_workflow.route<>'PAPER'
     or v_workflow.state not in ('AWAITING_PAPER_RETURN','RECEIVED','FINALISED')
     or coalesce((v_workflow.paper_return_manifest_json->>'manifest_version')::integer,0)<>2
     or v_workflow.paper_return_manifest_json->>'qr_contract_version'
       <>'CANDIDATE_PAPER_PAGE_QR_V2'
     or v_workflow.paper_return_manifest_sha256 is null
     or private._candidate_sha256_jsonb_v1(v_workflow.paper_return_manifest_json)
       is distinct from v_workflow.paper_return_manifest_sha256 then
    raise exception 'CANDIDATE_PAPER_RETURN_PACK_STALE' using errcode='40001';
  end if;

  v_page_count:=jsonb_array_length(p_verified_pages);
  v_expected_count:=jsonb_array_length(v_workflow.paper_return_manifest_json->'pages');
  if v_page_count<>v_expected_count
     or exists(
       select 1
       from jsonb_array_elements(p_verified_pages) supplied(page)
       where jsonb_typeof(supplied.page)<>'object'
          or nullif(supplied.page->>'page_key','') is null
          or nullif(supplied.page->>'component_id','') is null
          or lower(coalesce(supplied.page->>'source_content_sha256','')) !~ '^[0-9a-f]{64}$'
          or lower(coalesce(supplied.page->>'proof_receipt_sha256','')) !~ '^[0-9a-f]{64}$'
          or lower(coalesce(supplied.page->>'qr_payload_sha256','')) !~ '^[0-9a-f]{64}$'
          or lower(coalesce(supplied.page->>'media_type',''))<>'image/jpeg'
     )
     or (select count(distinct supplied.page->>'page_key')
         from jsonb_array_elements(p_verified_pages) supplied(page))<>v_expected_count
     or (select count(distinct supplied.page->>'component_id')
         from jsonb_array_elements(p_verified_pages) supplied(page))<>v_expected_count
     or exists(
       select 1
       from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') expected(page)
       where not exists(
         select 1 from jsonb_array_elements(p_verified_pages) supplied(page)
         where supplied.page->>'page_key'=expected.page->>'page_key'
       )
     ) then
    raise exception 'CANDIDATE_PAPER_RETURN_PACK_INCOMPLETE' using errcode='22023';
  end if;

  if v_workflow.state<>'AWAITING_PAPER_RETURN' then
    v_result:=public.candidate_workflow_transition_atomic_v1(
      p_session_id,p_environment,p_workflow_id,'PAPER_RETURN',p_expected_generation,
      jsonb_build_object('paper_return_pack_v2',true,'page_count',v_page_count),
      p_idempotency_key,p_now_utc
    );
    return v_result||jsonb_build_object(
      'paper_return_pack_verified',true,
      'paper_return_page_count',v_page_count,
      'proof_contract_version','CANDIDATE_PAPER_RETURN_PROOF_V2'
    );
  end if;

  for v_page in
    select supplied.page
    from jsonb_array_elements(p_verified_pages) supplied(page)
    order by (supplied.page->>'ordinal')::integer
  loop
    v_key_hex:=encode(extensions.digest(
      p_idempotency_key||'|paper-page|'||(v_page->>'page_key'),'sha256'
    ),'hex');
    v_component_key:=substring(v_key_hex from 1 for 8)||'-'
      ||substring(v_key_hex from 9 for 4)||'-4'
      ||substring(v_key_hex from 14 for 3)||'-8'
      ||substring(v_key_hex from 18 for 3)||'-'
      ||substring(v_key_hex from 21 for 12);
    perform public.candidate_paper_return_component_complete_v2(
      p_session_id,p_environment,p_workflow_id,p_expected_generation,
      (v_page->>'component_id')::uuid,
      v_page->>'source_content_sha256',(v_page->>'byte_size')::bigint,
      v_page->>'media_type',(v_page->>'image_width')::integer,
      (v_page->>'image_height')::integer,v_page->>'manifest_sha256',
      v_page->>'page_key',v_page->'qr_payload',v_page->>'proof_receipt_sha256',
      v_page->>'qr_payload_sha256',v_component_key,p_now_utc
    );
  end loop;

  v_result:=public.candidate_workflow_transition_atomic_v1(
    p_session_id,p_environment,p_workflow_id,'PAPER_RETURN',p_expected_generation,
    jsonb_build_object('paper_return_pack_v2',true,'page_count',v_page_count),
    p_idempotency_key,p_now_utc
  );
  return v_result||jsonb_build_object(
    'paper_return_pack_verified',true,
    'paper_return_page_count',v_page_count,
    'proof_contract_version','CANDIDATE_PAPER_RETURN_PROOF_V2'
  );
end
$function$;

alter function public.candidate_paper_return_pack_complete_v2(
  uuid,text,uuid,integer,jsonb,text,timestamptz
) owner to postgres;

revoke all on function public.candidate_paper_return_pack_complete_v2(
  uuid,text,uuid,integer,jsonb,text,timestamptz
) from public,anon,authenticated;
grant execute on function public.candidate_paper_return_pack_complete_v2(
  uuid,text,uuid,integer,jsonb,text,timestamptz
) to service_role;

comment on function public.candidate_paper_return_pack_complete_v2(
  uuid,text,uuid,integer,jsonb,text,timestamptz
) is 'Atomically accepts a complete current Candidate PAPER QR pack; missing, duplicate or mismatched staged pages roll back the entire receipt.';

notify pgrst, 'reload schema';

commit;
