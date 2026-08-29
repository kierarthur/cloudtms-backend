create or replace function public.trg_invoice_document_immutability_guard()
returns trigger
language plpgsql
set search_path to 'pg_catalog'
as $function$
begin
  if tg_table_name='invoice_document_versions' then
    if tg_op='DELETE' then
      if old.status='READY' then
        raise exception using errcode='55000',
          message='IMMUTABLE_READY_DOCUMENT';
      end if;
      return old;
    end if;

    if old.status='READY'
       and(
         new.operation_id is distinct from old.operation_id
         or new.entity_type is distinct from old.entity_type
         or new.entity_id is distinct from old.entity_id
         or new.purpose is distinct from old.purpose
         or new.source_revision is distinct from old.source_revision
         or new.template_version is distinct from old.template_version
         or new.snapshot_json is distinct from old.snapshot_json
         or new.snapshot_hash is distinct from old.snapshot_hash
         or new.manifest_json is distinct from old.manifest_json
         or new.manifest_hash is distinct from old.manifest_hash
         or new.r2_key is distinct from old.r2_key
         or new.sha256 is distinct from old.sha256
         or new.size_bytes is distinct from old.size_bytes
         or new.expected_page_count is distinct from old.expected_page_count
         or new.page_count is distinct from old.page_count
         or new.core_page_count is distinct from old.core_page_count
         or new.supporting_page_count is distinct from old.supporting_page_count
         or new.ready_at_utc is distinct from old.ready_at_utc
         or new.verified_at_utc is distinct from old.verified_at_utc
      ) then
      raise exception using errcode='55000',
        message='IMMUTABLE_READY_DOCUMENT';
    end if;
    if old.status='READY' and old.purpose='FINAL_ISSUE'
       and new.status is distinct from old.status then
      raise exception using errcode='55000',
        message='IMMUTABLE_READY_FINAL_ISSUE_STATUS';
    end if;
    if old.status='READY' and old.purpose<>'FINAL_ISSUE'
       and new.status not in('READY','SUPERSEDED') then
      raise exception using errcode='55000',
        message='READY_DOCUMENT_MAY_ONLY_BE_SUPERSEDED';
    end if;
    return new;
  end if;

  if tg_table_name='invoice_document_assets' then
    if tg_op='DELETE' then
      if old.status='READY' then
        raise exception using errcode='55000',
          message='IMMUTABLE_READY_DOCUMENT_ASSET';
      end if;
      return old;
    end if;
    if new.source_kind is distinct from old.source_kind
       or new.source_id is distinct from old.source_id
       or new.source_revision is distinct from old.source_revision
       or new.original_r2_key is distinct from old.original_r2_key
       or(old.original_sha256 is not null
          and new.original_sha256 is distinct from old.original_sha256) then
      raise exception using errcode='55000',
        message='IMMUTABLE_DOCUMENT_ASSET_SOURCE_IDENTITY';
    end if;
    if old.status='READY' and(
       new.status is distinct from old.status
       or new.normalised_manifest_json is distinct from old.normalised_manifest_json
       or new.normalised_r2_key is distinct from old.normalised_r2_key
       or new.normalised_sha256 is distinct from old.normalised_sha256
       or new.normalised_manifest_hash is distinct from old.normalised_manifest_hash
       or new.normalised_size_bytes is distinct from old.normalised_size_bytes
       or new.normalised_page_count is distinct from old.normalised_page_count
       or new.ready_at_utc is distinct from old.ready_at_utc) then
      raise exception using errcode='55000',
        message='IMMUTABLE_READY_DOCUMENT_ASSET_OUTPUT';
    end if;
    return new;
  end if;

  raise exception using errcode='55000',
    message='IMMUTABILITY_GUARD_ATTACHED_TO_UNSUPPORTED_TABLE';
end;
$function$;

drop trigger if exists trg_invoice_document_versions_immutability
  on public.invoice_document_versions;
create trigger trg_invoice_document_versions_immutability
before update or delete on public.invoice_document_versions
for each row execute function public.trg_invoice_document_immutability_guard();

drop trigger if exists trg_invoice_document_assets_immutability
  on public.invoice_document_assets;
create trigger trg_invoice_document_assets_immutability
before update or delete on public.invoice_document_assets
for each row execute function public.trg_invoice_document_immutability_guard();

revoke all on function public.trg_invoice_document_immutability_guard()
  from public,anon,authenticated;
grant execute on function public.trg_invoice_document_immutability_guard()
  to postgres,service_role;
