-- Reassert the saved Source public owner with one NEW-only handoff at its
-- successful outer tail. The private first-authorise core is also invoked
-- inside two-root publication and must NOT publish an intermediate B row.
\set ON_ERROR_STOP on

begin;

create or replace function public.weekly_source_first_authorise_v1(
  p_timesheet_id uuid,p_expected_timesheet_id uuid,
  p_expected_row_signature text,p_actor_user_id uuid
) returns jsonb
language plpgsql security definer
set search_path = public, private, extensions, pg_catalog, pg_temp
as $function$
declare
  v_context jsonb;
  v_lock jsonb;
  v_core jsonb;
  v_canonical uuid;
  v_work uuid;
  v_revision uuid;
  v_event uuid;
begin
  if coalesce(
       pg_catalog.current_setting('request.jwt.claim.role',true),
       nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role',
       ''
     )<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;

  if p_timesheet_id is null or p_actor_user_id is null then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_FIRST_AUTHORISE_REQUEST_INVALID',
      'retryable',false,'reason','TIMESHEET_AND_ACTOR_REQUIRED');
  end if;

  v_context:=private.bpay_source_authorise_identity_v1(p_timesheet_id);
  if coalesce((v_context->>'ok')::boolean,false) is not true then
    return v_context;
  end if;

  v_lock:=private.bpay_source_authorise_lock_v1(
    (v_context->>'candidate_id')::uuid,p_timesheet_id,pg_catalog.gen_random_uuid());
  if coalesce((v_lock->>'ok')::boolean,false) is not true then
    return v_lock;
  end if;

  select (family_element.value->>'canonical_timesheet_id')::uuid
    into v_canonical
  from pg_catalog.jsonb_array_elements(coalesce(v_lock->'families','[]'::jsonb))
    as family_element(value)
  where (family_element.value->>'requested_timesheet_id')::uuid=p_timesheet_id;

  if p_expected_timesheet_id is not null
     and p_expected_timesheet_id is distinct from v_canonical then
    return pg_catalog.jsonb_build_object(
      'ok',false,'code','WEEKLY_SOURCE_TIMESHEET_ROTATED_BEFORE_AUTHORISATION',
      'retryable',false,'timesheet_id',p_timesheet_id,
      'canonical_timesheet_id',v_canonical);
  end if;

  v_core:=private.weekly_source_first_authorise_core_v1(
    p_timesheet_id,p_expected_row_signature,p_actor_user_id,v_lock);
  if coalesce((v_core->>'ok')::boolean,false) is true
     and (select active_owner from private.bpay_next_module_control
          where id=1)='NEXT' then
    v_event:=(v_core->>'root_authorisation_id')::uuid;
    select s.work_id,s.revision_id into strict v_work,v_revision
      from private.bpay_next_stage_source_current_v1(
        p_timesheet_id,v_event,null) s;
    perform * from private.bpay_next_publish_source_staged_pair_v1(
      array[v_work],array[v_revision],array[v_event]);
  end if;
  return v_core||pg_catalog.jsonb_build_object('gate',v_lock->>'gate');
end
$function$;

alter function public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)
  owner to postgres;
-- The original public Source RPC's existing grants are deliberately retained
-- by CREATE OR REPLACE; do not broaden them with a blanket GRANT here.

commit;
