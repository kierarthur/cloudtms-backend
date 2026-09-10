-- Allow the deliberately inactive, non-login MyTMS system actor to perform
-- only the canonical archive step used when an empty Candidate Expense
-- carrier must leave current views while its financial history is retained.
--
-- The existing archive function continues to require an active Office user
-- for every other action and removal kind. The system actor exception is
-- pinned to the configured identity and its non-login safety properties.

\set ON_ERROR_STOP on

begin;

do $migration$
declare
  v_definition text;
  v_patched text;
begin
  select pg_get_functiondef(
    'public.timesheet_archive_transition_v1(uuid,text,text,uuid,uuid,text,timestamptz)'::regprocedure
  ) into v_definition;

  v_patched:=regexp_replace(
    v_definition,
    'WHERE actor\.id = p_actor_user_id[[:space:]]+AND actor\.is_active = true;',
    $replacement$WHERE actor.id = p_actor_user_id
    AND (
      actor.is_active = true
      OR (
        v_action = 'ARCHIVE'
        AND v_kind = 'WEEKLY_MANUAL_ADJUSTMENT_DELETE'
        AND actor.id = (
          SELECT settings.candidate_app_system_actor_user_id
          FROM public.settings_defaults AS settings
          WHERE settings.id = 1
        )
        AND actor.is_active = false
        AND actor.email = 'candidate-app-system@cloudtms.invalid'
        AND actor.role = 'user'
        AND actor.password_hash = '!cloudtms-system-actor-no-login-v1!'
        AND COALESCE(actor.payment_authoriser, false) = false
        AND COALESCE(actor.payment_golden_key, false) = false
      )
    );$replacement$
  );

  if v_patched=v_definition then
    raise exception 'CANDIDATE_SYSTEM_ACTOR_ARCHIVE_PATCH_TARGET_NOT_FOUND';
  end if;

  execute v_patched;
end;
$migration$;

commit;
