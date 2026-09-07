-- Reassert the Office delete context after the historical Candidate workflow
-- installer. The older installer predates reject-before-delete and must not
-- remove the narrow delete_timesheet/CANCEL authority on a clean or upgraded
-- database.

\set ON_ERROR_STOP on

begin;

create or replace function private._candidate_office_service_context_open_v1(
  p_environment text,
  p_actor_user_id uuid,
  p_permission text,
  p_action text,
  p_now_utc timestamptz default now()
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_environment text:=private._candidate_assert_environment(p_environment);
  v_permission text:=lower(btrim(coalesce(p_permission,'')));
  v_action text:=upper(btrim(coalesce(p_action,'')));
  v_context jsonb;
begin
  -- delete_timesheet and CANCEL are one inseparable Office authority pair.
  if p_actor_user_id is null
     or v_permission not in (
       'change_route','reject_submission','send_manager_reminder',
       'send_manager_reminder_batch','renew_manager_request',
       'cancel_manager_request','manage_phone_approval','manage_paper',
       'retry_finalisation','delete_timesheet'
     )
     or v_action not in (
       'ROUTE_CONFIRM','REJECT_CONFIRM','REMIND','RENEW',
       'MANAGER_REQUEST_CANCEL','CANCEL_MANAGER_HANDOFF',
       'BEGIN_MANAGER_REVIEW','RECORD_REVIEW_PROGRESS','PHONE_APPROVE',
       'MANAGER_REFUSE','REGISTER_REVIEW_COMPONENT',
       'REGISTER_FINAL_SIGNED_DOCUMENT','BEGIN_CANONICAL_DAILY_SAVE',
       'PAPER_PACK_RELEASE','PAPER_PACK_ATTEMPT_CLAIM','PAPER_PACK_MARK_FAILURE',
       'RETRY_FINALISATION','CANCEL','REJECT_EXPENSE_CATEGORY'
     )
     or (v_permission='delete_timesheet' and v_action<>'CANCEL')
     or (v_permission<>'delete_timesheet' and v_action='CANCEL')
     or (v_action='REJECT_EXPENSE_CATEGORY' and v_permission<>'reject_submission') then
    raise exception 'CANDIDATE_OFFICE_SERVICE_CONTEXT_INVALID' using errcode='28000';
  end if;
  v_context:=jsonb_build_object(
    'contract_version','CANDIDATE_OFFICE_SERVICE_CONTEXT_V1',
    'environment',v_environment,
    'actor_user_id',p_actor_user_id,
    'permission',v_permission,
    'action',v_action,
    'opened_at_utc',coalesce(p_now_utc,now())
  );
  perform set_config('cloudtms.office_candidate_context',v_context::text,true);
  return v_context;
end;
$function$;

alter function private._candidate_office_service_context_open_v1(
  text,uuid,text,text,timestamptz
) owner to postgres;

revoke all on function private._candidate_office_service_context_open_v1(
  text,uuid,text,text,timestamptz
) from public,anon,authenticated,service_role;

comment on function private._candidate_office_service_context_open_v1(
  text,uuid,text,text,timestamptz
) is 'Opens the transaction-local Office Candidate action context, including the narrow reject-before-delete cancellation authority.';

commit;
