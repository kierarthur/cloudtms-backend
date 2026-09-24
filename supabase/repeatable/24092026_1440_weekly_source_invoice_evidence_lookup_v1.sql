-- Weekly Source invoice evidence: service-only, exact CURRENT invoice bindings.
-- Does not grant table reads, alter invoices or touch Banking Pay.

\set ON_ERROR_STOP on

begin;

create or replace function public.weekly_source_invoice_evidence_v1(p_request jsonb)
returns jsonb
language plpgsql
stable
security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
declare
  v_actor uuid;
  v_invoice_id uuid;
  v_invoice public.invoices%rowtype;
  v_manifest public.weekly_source_client_manifests%rowtype;
  v_binding_count bigint;
  v_joined_count bigint;
  v_rows jsonb;
begin
  perform private.weekly_source_query_require_service_v1();
  if p_request is null or pg_catalog.jsonb_typeof(p_request)<>'object'
     or exists (select 1 from pg_catalog.jsonb_object_keys(p_request) key
                where key not in ('actor_user_id','invoice_id'))
     or not pg_catalog.pg_input_is_valid(coalesce(p_request->>'actor_user_id',''),'uuid')
     or not pg_catalog.pg_input_is_valid(coalesce(p_request->>'invoice_id',''),'uuid') then
    raise exception 'WEEKLY_SOURCE_INVOICE_EVIDENCE_INVALID' using errcode='22023';
  end if;
  v_actor := (p_request->>'actor_user_id')::uuid;
  v_invoice_id := (p_request->>'invoice_id')::uuid;

  select * into v_invoice from public.invoices where id=v_invoice_id;
  if not found or v_invoice.header_snapshot_json->>'schema_version'<>'WEEKLY_SOURCE_SELF_BILL_INVOICE_V1' then
    return pg_catalog.jsonb_build_object('is_weekly_source_invoice',false,'evidence','[]'::jsonb);
  end if;
  if not pg_catalog.pg_input_is_valid(
      coalesce(v_invoice.header_snapshot_json#>>'{meta,client_manifest_id}',''),'uuid') then
    raise exception 'WEEKLY_SOURCE_INVOICE_EVIDENCE_LINEAGE_INCOMPLETE' using errcode='P0001';
  end if;
  select * into strict v_manifest from public.weekly_source_client_manifests
    where id=(v_invoice.header_snapshot_json#>>'{meta,client_manifest_id}')::uuid;
  perform private.weekly_source_office_authority_v1(
    v_actor,'VIEW_SOURCE_PROGRESS',v_manifest.source_group_id,
    v_manifest.client_id,v_manifest.finalisation_week_ending
  );

  select pg_catalog.count(*) into v_binding_count
    from public.weekly_source_invoice_line_bindings binding
    where binding.invoice_id=v_invoice_id and binding.state='CURRENT';
  if v_binding_count>10000 then
    raise exception 'WEEKLY_SOURCE_INVOICE_EVIDENCE_CENSUS_LIMIT' using errcode='54000';
  end if;
  select pg_catalog.count(*) into v_joined_count
    from public.weekly_source_invoice_line_bindings binding
    join public.weekly_source_invoice_presentation_lines presentation
      on presentation.id=binding.presentation_line_id
    join public.weekly_source_final_revisions revision
      on revision.id=presentation.final_revision_id
    join public.weekly_source_uploads upload on upload.id=revision.upload_id
    where binding.invoice_id=v_invoice_id and binding.state='CURRENT';
  if v_binding_count<>v_joined_count then
    raise exception 'WEEKLY_SOURCE_INVOICE_EVIDENCE_LINEAGE_INCOMPLETE' using errcode='P0001';
  end if;

  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
    'upload_id',source.id,
    'filename',source.original_filename,
    'uploaded_at_utc',source.uploaded_at_utc,
    'byte_count',source.byte_count,
    'content_sha256',pg_catalog.encode(source.content_sha256,'hex'),
    'source_file_r2_key',source.file_metadata_json->>'source_file_r2_key'
  ) order by source.uploaded_at_utc,source.id),'[]'::jsonb) into v_rows
  from (
    select distinct upload.id,upload.original_filename,upload.uploaded_at_utc,
      upload.byte_count,upload.content_sha256,upload.file_metadata_json
    from public.weekly_source_invoice_line_bindings binding
    join public.weekly_source_invoice_presentation_lines presentation
      on presentation.id=binding.presentation_line_id
    join public.weekly_source_final_revisions revision
      on revision.id=presentation.final_revision_id
    join public.weekly_source_uploads upload on upload.id=revision.upload_id
    where binding.invoice_id=v_invoice_id and binding.state='CURRENT'
  ) source;
  return pg_catalog.jsonb_build_object('is_weekly_source_invoice',true,'evidence',v_rows);
end;
$function$;

alter function public.weekly_source_invoice_evidence_v1(jsonb) owner to postgres;
revoke all on function public.weekly_source_invoice_evidence_v1(jsonb) from public,anon,authenticated;
grant execute on function public.weekly_source_invoice_evidence_v1(jsonb) to service_role;
notify pgrst, 'reload schema';

commit;
