-- Repeatable CloudTMS function/view authority: weekly_source_local_publication_origin_guard_v2
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- This guard seals the typed fields and saved evidence. It does not publish,
-- authorise, price, resolve a query, change a Draft or create a financial hold.
create or replace function private.weekly_source_local_publication_origin_guard_v2()
returns trigger language plpgsql security definer
set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  if tg_op in ('DELETE','TRUNCATE') then
    raise exception 'WEEKLY_PROTECTED_LOCAL_RECEIPT_IMMUTABLE' using errcode='55000';
  end if;
  if tg_op='INSERT' then
    if new.state is distinct from 'PREPARING'
       or new.preparing_transaction_id is distinct from pg_current_xact_id()
       or new.completed_at_utc is not null then
      raise exception 'WEEKLY_PROTECTED_LOCAL_PREPARATION_REQUIRED' using errcode='55000';
    end if;
    return new;
  end if;
  if (to_jsonb(new)-array['approved_snapshot_json','state','result_json','completed_at_utc',
       'common_decision_bundle_id','common_bundle_revision','publication_origin_kind',
       'publication_origin_digest','source_qualification_digest']) is distinct from
     (to_jsonb(old)-array['approved_snapshot_json','state','result_json','completed_at_utc',
       'common_decision_bundle_id','common_bundle_revision','publication_origin_kind',
       'publication_origin_digest','source_qualification_digest']) then
    raise exception 'WEEKLY_PROTECTED_LOCAL_RECEIPT_IMMUTABLE' using errcode='55000';
  end if;
  if old.state='PREPARING' then
    if old.preparing_transaction_id is distinct from pg_current_xact_id() then
      raise exception 'WEEKLY_PROTECTED_LOCAL_PREPARATION_REQUIRED' using errcode='55000';
    end if;
  else
    if row(new.approved_snapshot_json,new.common_decision_bundle_id,new.common_bundle_revision,
         new.publication_origin_kind,new.publication_origin_digest,new.source_qualification_digest)
       is distinct from
       row(old.approved_snapshot_json,old.common_decision_bundle_id,old.common_bundle_revision,
         old.publication_origin_kind,old.publication_origin_digest,old.source_qualification_digest)
       or not (new.state=old.state or (old.state='PENDING_FREEZE' and new.state='COMPLETE'))
       or new.completed_at_utc is distinct from old.completed_at_utc
       or (new.state=old.state and new.result_json is distinct from old.result_json) then
      raise exception 'WEEKLY_PROTECTED_LOCAL_RECEIPT_IMMUTABLE' using errcode='55000';
    end if;
  end if;
  return new;
end;
$function$;
alter function private.weekly_source_local_publication_origin_guard_v2() owner to postgres;
revoke all on function private.weekly_source_local_publication_origin_guard_v2()
  from public,anon,authenticated,service_role;

drop trigger if exists weekly_source_local_publication_origin_guard_v2
  on private.weekly_source_local_protected_decision_receipts;
create trigger weekly_source_local_publication_origin_guard_v2
  before insert or update or delete on private.weekly_source_local_protected_decision_receipts
  for each row execute function private.weekly_source_local_publication_origin_guard_v2();
drop trigger if exists weekly_source_local_publication_origin_truncate_guard_v2
  on private.weekly_source_local_protected_decision_receipts;
create trigger weekly_source_local_publication_origin_truncate_guard_v2
  before truncate on private.weekly_source_local_protected_decision_receipts
  for each statement execute function private.weekly_source_local_publication_origin_guard_v2();

commit;
