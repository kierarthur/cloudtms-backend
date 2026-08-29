-- Compact task presentation over the already-certified complete issue owners.
-- No payment arithmetic, selection change or new action permission.

\set ON_ERROR_STOP on

begin;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_task_title_v2(p_message_id text)
RETURNS text LANGUAGE sql IMMUTABLE SECURITY INVOKER SET search_path TO ''
AS $function$
 SELECT CASE p_message_id
  WHEN 'MSG-044' THEN 'Payment method changed'
  WHEN 'MSG-045' THEN 'Rate decision required'
  WHEN 'MSG-046' THEN 'Amount decision required'
  WHEN 'MSG-060' THEN 'Bank account setup in progress'
  WHEN 'MSG-062' THEN 'Bank account setup failed'
  WHEN 'MSG-064' THEN 'Umbrella company inactive'
  WHEN 'MSG-066' THEN 'Candidate bank details are missing.'
  WHEN 'MSG-067' THEN 'The umbrella company''s bank details are missing.'
  WHEN 'MSG-068' THEN 'Account name check required.'
  WHEN 'MSG-071' THEN 'The account name is a close match. Check the name and bank details before accepting them.'
  WHEN 'MSG-072' THEN 'The bank could not check the account name. Check the bank details before deciding whether to accept them.'
  WHEN 'MSG-073' THEN 'The account name does not match the bank details. Check both before accepting them.'
  WHEN 'MSG-077' THEN 'Bank account needs setting up.'
 END;
$function$;
ALTER FUNCTION private.pay_workbench_modal_task_title_v2(text) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_task_title_v2(text) FROM PUBLIC, anon, authenticated, service_role;

CREATE OR REPLACE FUNCTION private.pay_workbench_modal_task_summaries_v2(
 p_session public.banking_pay_workbench_sessions,p_channel text,p_provider text,p_environment text,
 p_progress jsonb,p_can_open_owner boolean,p_can_refresh boolean
) RETURNS TABLE(identity text,issue_state text,task_family text,title_message_id text,title text,
 affected_candidate_count bigint,affected_payment_count bigint,affected_payment_count_complete boolean,search_text text)
LANGUAGE sql STABLE SECURITY INVOKER SET search_path TO ''
AS $function$
 WITH refs AS MATERIALIZED (
  SELECT i.* FROM private.pay_workbench_modal_issue_index_v2(p_session,p_channel,p_provider,p_environment,
   p_progress,p_can_open_owner,p_can_refresh) i WHERE i.issue_state IN ('ACTION_REQUIRED','UPDATING')
 ), members AS MATERIALIZED (
  SELECT f.task_key,f.candidate_id,f.preview_row_id,
   f.task_json->'context_only' IS DISTINCT FROM 'true'::jsonb AS affected,true AS membership_known,
   f.title_message_id,NULL::text AS current_title,'ACTION_REQUIRED'::text AS state
  FROM private.pay_workbench_modal_finance_task_members_v2(p_session,p_channel) f
  UNION ALL
  SELECT b.task_key,b.candidate_id,b.preview_row_id,b.affected_by_task,
   COALESCE(b.task_json->'payment_membership_known'='true'::jsonb,true),
   b.task_json->>'title_message_id',NULL::text,b.task_json->>'state'
  FROM private.pay_workbench_modal_bank_task_members_v2(p_session,p_channel,p_provider,p_environment,p_can_open_owner) b
  UNION ALL
  SELECT s.task_key,s.candidate_id,s.preview_row_id,false,false,NULL::text,s.task_json->>'title',s.task_json->>'state'
  FROM private.pay_workbench_modal_source_issue_members_v2(p_session,p_channel,p_progress,p_can_refresh) s
 ), counts AS MATERIALIZED (
  SELECT m.task_key,count(DISTINCT m.candidate_id) AS candidates,
   count(DISTINCT m.preview_row_id) FILTER(WHERE m.affected) AS payments,
   bool_and(m.membership_known) AS complete,
   string_agg(DISTINCT lower(COALESCE(c.display_name,'')||' '||COALESCE(c.tms_ref,'')),chr(10)) AS candidate_search
  FROM members m JOIN refs r ON r.task_key=m.task_key
  JOIN public.candidates c ON c.id=m.candidate_id
  GROUP BY m.task_key
 ), labels AS MATERIALIZED (
  -- A stale related member cannot replace the current task's explanation.
  -- Every member and its original action remains in the full detail reader.
  SELECT DISTINCT ON(r.identity) r.identity,m.title_message_id,m.current_title
  FROM refs r JOIN members m ON m.task_key=r.task_key AND m.state=r.issue_state
  WHERE NULLIF(m.title_message_id,'') IS NOT NULL OR NULLIF(m.current_title,'') IS NOT NULL
  ORDER BY r.identity,m.title_message_id COLLATE "C",m.current_title COLLATE "C"
 ), labelled AS (
  SELECT r.identity,r.issue_state,r.task_family,l.title_message_id,
   COALESCE(private.pay_workbench_modal_task_title_v2(l.title_message_id),l.current_title) AS title,
   c.candidates,c.payments,c.complete,c.candidate_search
  FROM refs r LEFT JOIN counts c ON c.task_key=r.task_key LEFT JOIN labels l ON l.identity=r.identity
 )
 SELECT l.identity,l.issue_state,l.task_family,l.title_message_id,l.title,l.candidates,
  CASE WHEN l.complete THEN l.payments END,l.complete,
  lower(COALESCE(l.title,''))||chr(10)||COALESCE(l.candidate_search,'')
 FROM labelled l;
$function$;
ALTER FUNCTION private.pay_workbench_modal_task_summaries_v2(public.banking_pay_workbench_sessions,text,text,text,jsonb,boolean,boolean) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_workbench_modal_task_summaries_v2(public.banking_pay_workbench_sessions,text,text,text,jsonb,boolean,boolean) FROM PUBLIC, anon, authenticated, service_role;

commit;
