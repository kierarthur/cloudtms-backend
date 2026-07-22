-- Hard-cutover retirements for the import-review contract.
-- Run after the replacement functions in this package.  The exact drops are
-- rerunnable and intentionally do not preserve browser-authoritative legacy
-- mutation paths.

drop function if exists public.hr_weekly_validation_apply_send_emails(uuid,jsonb,uuid);

drop function if exists public.hr_issue_emails_touch(
  text,uuid,uuid,uuid,text,text,uuid,uuid,text,text,date
);

drop function if exists public.import_apply_operation_claim_v1(
  uuid,public.hr_source_enum,text,text,uuid,jsonb,timestamptz
);

drop function if exists public.import_review_follow_up_update_v1(
  uuid,uuid,text,text,text,text,uuid
);

-- Replacement review creation is now atomic in import_review_replace_v1.
-- The former two-step supersede surface must not be callable or recreated.
drop function if exists public.import_review_supersede_v1(uuid,uuid,bigint,uuid);

-- Remove superseded draft overloads so PostgREST has one unambiguous contract.
drop function if exists public.import_review_get_v1(uuid,text,integer,bigint,integer);
drop function if exists public.import_review_apply_guard_v1(
  uuid,bigint,text,text,uuid,text,jsonb,boolean,uuid
);
