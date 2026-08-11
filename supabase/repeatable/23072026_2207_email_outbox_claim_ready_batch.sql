create or replace function public.email_outbox_claim_ready_batch(
  p_limit integer,
  p_attempt_lease_token text,
  p_lease_minutes integer default 5
) returns setof public.mail_outbox
language plpgsql
security invoker
set search_path to 'public','pg_temp'
as $function$
declare
  v_now timestamptz:=now();
  v_limit integer:=greatest(0,least(coalesce(p_limit,0),500));
  v_lease integer:=greatest(1,least(coalesce(p_lease_minutes,5),30));
begin
  if coalesce(btrim(p_attempt_lease_token),'')='' then raise exception 'attempt_lease_token is required'; end if;
  if v_limit=0 then return; end if;

  return query
  with picked as materialized (
    select mo.id
    from public.mail_outbox mo
    where mo.status='QUEUED' and mo.sent_at is null and mo.delivered_at is null and mo.read_at is null
      and coalesce(mo.next_attempt_at_utc,mo.scheduled_for_utc,mo.created_at_utc)<=v_now
      and (mo.attempt_lease_token is null or mo.attempt_lease_expires_at_utc is null
        or mo.attempt_lease_expires_at_utc<=v_now)
      and (
        not (
          nullif(btrim(coalesce(mo.payment_scope_json->>'candidate_workflow_id','')),'') is not null
          or mo.payment_scope_json ? 'candidate_workflow_generation'
          or mo.payment_scope_json ? 'paper_return_manifest_sha256'
          or mo.payment_scope_json ? 'candidate_paper_pack_ready'
          or mo.payment_scope_json ? 'candidate_complete_pack_storage_key'
          or upper(coalesce(mo.payment_scope_json->>'mail_hold_reason',''))
            ='CANDIDATE_PAPER_PACK_PENDING'
          or upper(coalesce(mo.payment_scope_json->>'candidate_mail_authority',''))
            ='MANAGER_APPROVAL_V1'
        )
        or (
          coalesce(mo.payment_scope_json->>'candidate_workflow_id','') ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
          and coalesce(mo.payment_scope_json->>'candidate_workflow_generation','')
            ~ '^[1-9][0-9]{0,8}$'
          and lower(coalesce(mo.payment_scope_json->>'paper_return_manifest_sha256',''))
            ~ '^[0-9a-f]{64}$'
          and lower(coalesce(mo.payment_scope_json->>'candidate_paper_pack_ready','false'))
            in('true','t','1','yes')
          and lower(coalesce(mo.payment_scope_json->>'candidate_paper_generation_retired','false'))
            in('false','f','0','no')
          and lower(coalesce(mo.payment_scope_json->>'mail_held_until_pdf_rendered','true'))
            in('false','f','0','no')
          and nullif(btrim(coalesce(mo.payment_scope_json->>'mail_hold_reason','')),'') is null
          and jsonb_typeof(mo.attachments)='array'
          and jsonb_array_length(mo.attachments)=1
          and nullif(btrim(coalesce(mo.attachments->0->>'r2_key','')),'') is not null
          and lower(coalesce(mo.attachments->0->>'sha256','')) ~ '^[0-9a-f]{64}$'
          and coalesce(mo.attachments->0->>'size_bytes','') ~ '^[1-9][0-9]{0,18}$'
          and coalesce(mo.attachments->0->>'page_count','') ~ '^[1-9][0-9]{0,8}$'
          and lower(coalesce(mo.attachments->0->>'content_type',''))='application/pdf'
          and mo.attachments->0->>'r2_key'
            =mo.payment_scope_json->>'candidate_complete_pack_storage_key'
          and lower(mo.attachments->0->>'sha256')
            =lower(mo.payment_scope_json->>'candidate_complete_pack_sha256')
          and mo.attachments->0->>'size_bytes'
            =mo.payment_scope_json->>'candidate_complete_pack_size_bytes'
          and mo.attachments->0->>'page_count'
            =mo.payment_scope_json->>'candidate_complete_pack_page_count'
          and lower(coalesce(mo.payment_scope_json->>'candidate_complete_pack_media_type',''))
            ='application/pdf'
          and mo.attachments->0->>'candidate_workflow_id'
            =mo.payment_scope_json->>'candidate_workflow_id'
          and mo.attachments->0->>'candidate_workflow_generation'
            =mo.payment_scope_json->>'candidate_workflow_generation'
          and lower(mo.attachments->0->>'paper_return_manifest_sha256')
            =lower(mo.payment_scope_json->>'paper_return_manifest_sha256')
          and exists(
            select 1
            from public.candidate_submission_workflows workflow
            where workflow.id=case
                when coalesce(mo.payment_scope_json->>'candidate_workflow_id','') ~*
                  '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
                then (mo.payment_scope_json->>'candidate_workflow_id')::uuid
                else null::uuid end
              and workflow.generation=case
                when coalesce(mo.payment_scope_json->>'candidate_workflow_generation','')
                  ~ '^[1-9][0-9]{0,8}$'
                then (mo.payment_scope_json->>'candidate_workflow_generation')::integer
                else null::integer end
              and workflow.route='PAPER'
              and workflow.state='AWAITING_PAPER_RETURN'
              and encode(workflow.paper_return_manifest_sha256,'hex')
                =lower(mo.payment_scope_json->>'paper_return_manifest_sha256')
              and coalesce(workflow.target_timesheet_id,workflow.anchor_timesheet_id)=mo.context_id
          )
        )
        or (
          upper(coalesce(mo.payment_scope_json->>'candidate_mail_authority',''))
            ='MANAGER_APPROVAL_V1'
          and upper(coalesce(mo.payment_scope_json->>'candidate_manager_mail_kind',''))
            in ('INITIAL','REMINDER','RENEWAL','WITHDRAWAL')
          and lower(coalesce(mo.payment_scope_json->>'candidate_manager_mail_retired','false'))
            in ('false','f','0','no')
          and coalesce(mo.payment_scope_json->>'candidate_manager_workflow_id','') ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          and coalesce(mo.payment_scope_json->>'candidate_manager_workflow_generation','')
            ~ '^[1-9][0-9]{0,8}$'
          and coalesce(mo.payment_scope_json->>'candidate_approval_request_id','') ~*
            '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
          and coalesce(mo.payment_scope_json->>'candidate_approval_request_generation','')
            ~ '^[1-9][0-9]{0,8}$'
          and mo.context_kind='CANDIDATE_WORKFLOW'
          and mo.context_id=(mo.payment_scope_json->>'candidate_manager_workflow_id')::uuid
          and exists (
            select 1
            from public.candidate_approval_requests request_row
            join public.candidate_submission_workflows workflow
              on workflow.id=request_row.workflow_id
            where request_row.id=(mo.payment_scope_json->>'candidate_approval_request_id')::uuid
              and request_row.workflow_id=mo.context_id
              and request_row.workflow_generation=
                    (mo.payment_scope_json->>'candidate_manager_workflow_generation')::integer
              and request_row.request_generation=
                    (mo.payment_scope_json->>'candidate_approval_request_generation')::integer
              and request_row.method='EMAIL'
              and request_row.manager_email_normalized=mo."to"
              and (
                (
                  upper(mo.payment_scope_json->>'candidate_manager_mail_kind')
                    in ('INITIAL','REMINDER','RENEWAL')
                  and request_row.state='PENDING'
                  and request_row.expires_at_utc>v_now
                  and workflow.generation=request_row.workflow_generation
                  and workflow.route='EMAIL'
                  and workflow.state='AWAITING_MANAGER_APPROVAL'
                  and workflow.review_manifest_sha256=request_row.review_manifest_sha256
                )
                or (
                  upper(mo.payment_scope_json->>'candidate_manager_mail_kind')='WITHDRAWAL'
                  and request_row.state in ('CANCELLED','SUPERSEDED','EXPIRED','REFUSED')
                )
              )
          )
        )
      )
      and (
        upper(coalesce(mo.type,''))<>'INVOICE'
        or (
          mo.attachments_ready=true and mo.waiting_invoice_operation_id is null
          and jsonb_typeof(mo.attachments)='array' and jsonb_array_length(mo.attachments)>0
          and not exists (
            select 1 from jsonb_array_elements(mo.attachments) a
            left join lateral (
              select
                case when coalesce(a->>'document_version_id','') ~*
                  '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
                  then(a->>'document_version_id')::uuid end document_version_id,
                case when coalesce(a->>'invoice_id','') ~*
                  '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
                  then(a->>'invoice_id')::uuid end invoice_id,
                case when coalesce(a->>'size_bytes','') ~ '^[0-9]{1,18}$'
                  then(a->>'size_bytes')::bigint end size_bytes,
                case when coalesce(a->>'page_count','') ~ '^[0-9]{1,9}$'
                  then(a->>'page_count')::integer end page_count,
                upper(coalesce(nullif(a->>'delivery_mode',''),
                  'ATTACHMENT')) delivery_mode,
                lower(coalesce(a->>'secure_link_required','false'))
                  in('true','t','1','yes') secure_link_required
            ) parsed on true
            left join public.invoice_document_versions v
              on v.id=parsed.document_version_id
            left join public.invoices i on i.id=parsed.invoice_id
            where parsed.document_version_id is null or parsed.invoice_id is null
              or parsed.delivery_mode not in('ATTACHMENT','SECURE_LINK')
              or nullif(a->>'sha256','') is null
              or nullif(a->>'filename','') is null
              or coalesce(parsed.size_bytes,0)<=0 or coalesce(parsed.page_count,0)<=0
              or v.id is null or v.status<>'READY' or v.superseded_at_utc is not null
              or v.purpose<>'FINAL_ISSUE' or v.entity_type<>'INVOICE'
              or v.entity_id is distinct from parsed.invoice_id
              or i.id is null or i.status<>'ISSUED'
              or i.issued_document_version_id is distinct from v.id
              or v.sha256 is distinct from a->>'sha256'
              or v.size_bytes is distinct from parsed.size_bytes
              or v.page_count is distinct from parsed.page_count
              or(parsed.delivery_mode='ATTACHMENT' and(
                nullif(a->>'r2_key','') is null
                or nullif(a->>'mime_type','') is null
                or v.r2_key is distinct from a->>'r2_key'))
              or(parsed.delivery_mode='SECURE_LINK' and(
                parsed.secure_link_required is not true
                or nullif(a->>'r2_key','') is not null))
          )
        )
      )
    order by coalesce(mo.next_attempt_at_utc,mo.scheduled_for_utc,mo.created_at_utc),
      mo.created_at_utc,mo.id
    for update skip locked limit v_limit
  ),
  updated as (
    update public.mail_outbox mo set attempt_lease_token=p_attempt_lease_token,
      attempt_leased_at_utc=v_now,attempt_lease_expires_at_utc=v_now+make_interval(mins=>v_lease)
    from picked p where mo.id=p.id returning mo.*
  )
  select u.* from updated u
  order by coalesce(u.next_attempt_at_utc,u.scheduled_for_utc,u.created_at_utc),u.created_at_utc,u.id;
end;
$function$;

revoke all on function public.email_outbox_claim_ready_batch(integer,text,integer)
  from public,anon,authenticated;
grant execute on function public.email_outbox_claim_ready_batch(integer,text,integer)
  to service_role;
