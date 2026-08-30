-- Final upgrade-order authority for the Candidate returned-paper V1 proof.
-- The routine locks the Candidate session through _candidate_session_context_v1,
-- so PostgreSQL requires VOLATILE. This newest repeatable makes a blank NEW
-- installation and an incremental UPGRADE finish with the same safe definition.

\set ON_ERROR_STOP on

begin;

create or replace function public.candidate_paper_return_proof_validate_v1(
  p_session_id uuid,
  p_environment text,
  p_workflow_id uuid,
  p_expected_generation integer,
  p_manifest_sha256_hex text,
  p_page_key text,
  p_qr_token text default null,
  p_qr_token_sha256_hex text default null,
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
  v_timesheet public.timesheets%rowtype;
  v_page jsonb;
  v_manifest_hex text;
  v_qr_required boolean;
  v_qr_hash text;
  v_proof_identity text;
begin
  perform private._candidate_require_feature_v1(p_environment,'candidate_app_writes');
  if p_expected_generation is null or p_expected_generation<1
     or lower(coalesce(p_manifest_sha256_hex,'')) !~ '^[0-9a-f]{64}$'
     or nullif(btrim(coalesce(p_page_key,'')),'') is null then
    raise exception 'CANDIDATE_PAPER_QR_PROOF_INVALID' using errcode='22023';
  end if;
  v_session:=private._candidate_session_context_v1(
    p_session_id,p_environment,null,p_now_utc,true
  );
  v_candidate_id:=nullif(v_session->>'selected_candidate_id','')::uuid;
  select * into v_workflow from public.candidate_submission_workflows
  where id=p_workflow_id and candidate_id=v_candidate_id;
  if not found then raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002'; end if;
  if v_workflow.generation<>p_expected_generation
     or v_workflow.route<>'PAPER'
     or v_workflow.state<>'AWAITING_PAPER_RETURN'
     or v_workflow.paper_return_manifest_sha256 is null then
    raise exception 'CANDIDATE_PAPER_QR_PROOF_STALE' using errcode='40001';
  end if;
  v_manifest_hex:=encode(v_workflow.paper_return_manifest_sha256,'hex');
  if v_manifest_hex<>lower(p_manifest_sha256_hex)
     or private._candidate_sha256_jsonb_v1(v_workflow.paper_return_manifest_json)
       is distinct from v_workflow.paper_return_manifest_sha256 then
    raise exception 'CANDIDATE_PAPER_RETURN_MANIFEST_STALE' using errcode='40001';
  end if;
  select page into v_page
  from jsonb_array_elements(v_workflow.paper_return_manifest_json->'pages') page
  where page->>'page_key'=p_page_key;
  if v_page is null then
    raise exception 'CANDIDATE_PAPER_RETURN_PAGE_NOT_EXPECTED' using errcode='22023';
  end if;
  v_qr_required:=coalesce(v_page->>'component_kind','')='HOURS_TIMESHEET';
  select * into v_timesheet from public.timesheets
  where timesheet_id=coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id)
    and is_current=true and archived_at_utc is null;
  if not found then raise exception 'CANDIDATE_TIMESHEET_NOT_FOUND' using errcode='P0002'; end if;

  v_qr_hash:=encode(extensions.digest(coalesce(v_timesheet.qr_token,''),'sha256'),'hex');
  if v_qr_required then
    if nullif(btrim(coalesce(v_timesheet.qr_token,'')),'') is null
       or upper(coalesce(v_timesheet.qr_status::text,''))<>'PENDING'
       or not (
         (p_qr_token is not null and p_qr_token=v_timesheet.qr_token)
         or (lower(coalesce(p_qr_token_sha256_hex,'')) ~ '^[0-9a-f]{64}$'
           and lower(p_qr_token_sha256_hex)=v_qr_hash)
       ) then
      raise exception 'CANDIDATE_PAPER_QR_PROOF_MISMATCH' using errcode='28000';
    end if;
  elsif p_qr_token is not null or p_qr_token_sha256_hex is not null then
    raise exception 'CANDIDATE_PAPER_QR_PROOF_FORBIDDEN' using errcode='22023';
  end if;

  v_proof_identity:=concat_ws('|',
    'CANDIDATE_PAPER_RETURN_PROOF_V1',v_workflow.id,v_workflow.generation,
    v_manifest_hex,p_page_key,coalesce(v_page->>'component_kind',''),
    case when v_qr_required then v_qr_hash else '' end
  );
  return jsonb_build_object(
    'ok',true,
    'proof_contract_version','CANDIDATE_PAPER_RETURN_PROOF_V1',
    'workflow_id',v_workflow.id,
    'workflow_generation',v_workflow.generation,
    'timesheet_id',v_timesheet.timesheet_id,
    'paper_return_manifest_sha256',v_manifest_hex,
    'paper_return_page_key',p_page_key,
    'page_component_kind',v_page->>'component_kind',
    'qr_required',v_qr_required,
    'qr_token_sha256',case when v_qr_required then v_qr_hash else null end,
    'proof_receipt_sha256',encode(extensions.digest(v_proof_identity,'sha256'),'hex')
  );
end
$function$;

alter function public.candidate_paper_return_proof_validate_v1(
  uuid,text,uuid,integer,text,text,text,text,timestamptz
) owner to postgres;

revoke all on function public.candidate_paper_return_proof_validate_v1(
  uuid,text,uuid,integer,text,text,text,text,timestamptz
) from public,anon,authenticated;

grant execute on function public.candidate_paper_return_proof_validate_v1(
  uuid,text,uuid,integer,text,text,text,text,timestamptz
) to service_role;

comment on function public.candidate_paper_return_proof_validate_v1(
  uuid,text,uuid,integer,text,text,text,text,timestamptz
) is 'Validates returned-paper manifest/page authority and the main-page TSQ1 token without retaining the raw token.';

notify pgrst, 'reload schema';

commit;
