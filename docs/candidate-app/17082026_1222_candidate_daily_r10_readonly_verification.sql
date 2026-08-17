-- Candidate Daily R10 read-only TEST verification.
-- Run only against the confirmed test-cloudtms project.
-- This file contains SELECT/WITH statements only and invokes no business RPC.

select current_database() as database_name, current_setting('server_version') as postgres_version,
  now() as observed_at_utc;

select encode(extensions.digest(convert_to(pg_get_functiondef(
  'public.candidate_daily_authority_transition_atomic_v1(jsonb,uuid,text,jsonb,uuid,text,text,text)'::regprocedure
), 'UTF8'), 'sha256'), 'hex') as canonical_function_sha256,
  pg_get_functiondef('public.candidate_daily_authority_transition_atomic_v1(jsonb,uuid,text,jsonb,uuid,text,text,text)'::regprocedure)
    ~ 'v_prior_mode\s*<>\s*v_new_mode\s+and\s+v_actual_disposition\s*=\s*''NONE'''
    as r10_guard_present;

select p.oid::regprocedure::text as signature,
  p.prosecdef as security_definer,
  p.proconfig as function_config,
  has_function_privilege('service_role',p.oid,'EXECUTE') as service_role_execute,
  has_function_privilege('anon',p.oid,'EXECUTE') as anon_execute,
  has_function_privilege('authenticated',p.oid,'EXECUTE') as authenticated_execute
from pg_proc p
join pg_namespace n on n.oid=p.pronamespace
where n.nspname='public' and p.proname='candidate_daily_authority_transition_atomic_v1';

select coalesce((candidate_app_feature_flags_json->>'candidate_daily_enabled')::boolean,false)
  as candidate_daily_enabled
from public.settings_defaults where id=1;

select count(*) filter(where value='true') as enabled_candidate_flag_count
from public.settings_defaults s,
lateral jsonb_each_text(s.candidate_app_feature_flags_json) f(key,value)
where s.id=1;

with counts(label,row_count) as (
  values
    ('candidate_app_accounts',(select count(*) from public.candidate_app_accounts)),
    ('candidate_app_sessions',(select count(*) from public.candidate_app_sessions)),
    ('candidate_auth_challenges',(select count(*) from public.candidate_auth_challenges)),
    ('candidate_submission_workflows',(select count(*) from public.candidate_submission_workflows)),
    ('candidate_submission_components',(select count(*) from public.candidate_submission_components)),
    ('candidate_approval_requests',(select count(*) from public.candidate_approval_requests)),
    ('candidate_notifications',(select count(*) from public.candidate_notifications)),
    ('candidate_daily_authority_scopes',(select count(*) from private.candidate_daily_authority_scopes)),
    ('candidate_daily_entitlements',(select count(*) from private.candidate_daily_entitlements)),
    ('candidate_daily_source_links',(select count(*) from private.candidate_daily_source_links)),
    ('candidate_daily_command_receipts',(select count(*) from public.candidate_daily_command_receipts)),
    ('candidate_daily_batch_receipts',(select count(*) from private.candidate_daily_batch_receipts)),
    ('candidate_daily_rota_generations',(select count(*) from public.candidate_daily_rota_generations)),
    ('candidate_daily_rota_days',(select count(*) from public.candidate_daily_rota_days)),
    ('candidate_daily_availability_days',(select count(*) from public.candidate_daily_availability_days)),
    ('candidate_daily_sheet_projection_outbox',(select count(*) from public.candidate_daily_sheet_projection_outbox)),
    ('candidate_daily_sync_state',(select count(*) from private.candidate_daily_sync_state)),
    ('candidate_daily_authority_transitions',(select count(*) from private.candidate_daily_authority_transitions)),
    ('candidate_daily_external_effect_receipts',(select count(*) from private.candidate_daily_external_effect_receipts))
)
select * from counts order by label;

select count(*) as candidate_bound_mail_rows
from public.mail_outbox
where context_kind='CANDIDATE_WORKFLOW'
   or deterministic_outbox_key like 'CANDIDATE\_%' escape '\';
