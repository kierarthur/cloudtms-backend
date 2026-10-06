-- The Source publication receipt is inserted only after its complete one- or
-- two-root head publication and its existing Workbench invalidation checks.
-- It is the common final write for immediate and deferred publication.  This
-- exact-receipt hook does no Candidate-history search and cannot publish an
-- intermediate first-authorisation made inside a two-root Source operation.
\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_source_receipt_publish_v1()
returns trigger
language plpgsql security definer
set search_path = pg_catalog, private, public
as $function$
declare
  v_n integer;
  v_member record;
  v_stage record;
  v_work_ids uuid[]:=array[]::uuid[];
  v_revision_ids uuid[]:=array[]::uuid[];
  v_event_ids uuid[]:=array[]::uuid[];
begin
  if (select active_owner from private.bpay_next_module_control
      where id=1 for share)<>'NEXT' then
    return new;
  end if;
  v_n:=pg_catalog.cardinality(new.member_root_ids);
  if v_n not between 1 and 2
     or v_n<>pg_catalog.cardinality(new.head_ids)
     or v_n<>pg_catalog.cardinality(new.member_family_booking_ids) then
    raise exception using errcode='23514',
      message='BPAY_NEXT_SOURCE_RECEIPT_SCOPE_INVALID';
  end if;
  -- Every Source owner reaches this insertion after its source locks.  Sort
  -- the exact receipt members before touching any Banking Pay work row so
  -- opposite request-array order cannot reverse the two-root lock order.
  for v_member in
    select new.member_root_ids[i] as root_id,
           new.head_ids[i] as head_id,
           new.member_family_booking_ids[i] as booking_id
      from pg_catalog.generate_subscripts(new.member_root_ids,1) as i
      order by new.member_family_booking_ids[i],new.member_root_ids[i]
  loop
    select * into strict v_stage
      from private.bpay_next_stage_source_current_v1(
        v_member.root_id,v_member.head_id,v_member.head_id);
    if (select w.candidate_id from private.bpay_next_work w
        where w.id=v_stage.work_id) is distinct from new.candidate_id then
      raise exception using errcode='23514',
        message='BPAY_NEXT_SOURCE_RECEIPT_CANDIDATE_MISMATCH';
    end if;
    v_work_ids:=pg_catalog.array_append(v_work_ids,v_stage.work_id);
    v_revision_ids:=pg_catalog.array_append(v_revision_ids,v_stage.revision_id);
    v_event_ids:=pg_catalog.array_append(v_event_ids,v_member.head_id);
  end loop;
  perform * from private.bpay_next_publish_source_staged_pair_v1(
    v_work_ids,v_revision_ids,v_event_ids);
  return new;
end
$function$;

alter function private.bpay_next_source_receipt_publish_v1() owner to postgres;
revoke all on function private.bpay_next_source_receipt_publish_v1()
  from public,anon,authenticated,service_role;

drop trigger if exists bpay_next_source_receipt_publish_v1
  on private.weekly_source_entitlement_publication_receipts;
create trigger bpay_next_source_receipt_publish_v1
after insert on private.weekly_source_entitlement_publication_receipts
for each row execute function private.bpay_next_source_receipt_publish_v1();

commit;
