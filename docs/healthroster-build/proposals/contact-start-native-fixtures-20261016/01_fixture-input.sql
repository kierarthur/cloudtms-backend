-- TEMP-only fixture hand-off; never an API, deployed routine or persistence protocol.
-- Root's genuine native prefix runs after this file and inserts exactly two DUE intents.
-- Prefix ownership and required real constructors are in RUNBOOK.md. No fallback seed.
CREATE TEMP TABLE contact_start_fixture_input(
 kind text PRIMARY KEY CHECK(kind IN('PUSH','EMAIL')),
 message_intent_id uuid NOT NULL UNIQUE,
 actor_user_id uuid NOT NULL,
 source_prefix_receipt jsonb NOT NULL CHECK(jsonb_typeof(source_prefix_receipt)='object')
) ON COMMIT DROP;
CREATE TEMP TABLE contact_start_fixture_targets(
 kind text PRIMARY KEY CHECK(kind IN('PUSH','EMAIL')),
 dispatch_target_id uuid NOT NULL UNIQUE,
 dispatch_command_id uuid NOT NULL UNIQUE,
 lease_token uuid NOT NULL,
 worker_id text NOT NULL,
 agency_id uuid NOT NULL,
 environment text NOT NULL CHECK(environment='TEST')
) ON COMMIT DROP;
CREATE TEMP TABLE contact_start_fixture_absence_ids(
 relation_name text NOT NULL, column_name text NOT NULL, object_id uuid NOT NULL,
 PRIMARY KEY(relation_name,column_name,object_id)
) ON COMMIT DROP;
CREATE TEMP TABLE contact_start_fixture_results(case_name text PRIMARY KEY, assertions text NOT NULL CHECK(assertions='PASS')) ON COMMIT DROP;

CREATE OR REPLACE FUNCTION pg_temp.contact_assert_v1(p_ok boolean,p_message text)
RETURNS void LANGUAGE plpgsql AS $function$
BEGIN IF p_ok IS NOT TRUE THEN RAISE EXCEPTION 'CONTACT_NATIVE_ASSERT: %',p_message; END IF; END;
$function$;
CREATE OR REPLACE FUNCTION pg_temp.contact_start_request_v1(p_kind text)
RETURNS jsonb LANGUAGE sql AS $function$
 SELECT jsonb_build_object('dispatch_target_id',dispatch_target_id,'lease_token',lease_token,
  'worker_id',worker_id,'agency_id',agency_id,'environment',environment)
 FROM pg_temp.contact_start_fixture_targets WHERE kind=p_kind;
$function$;
CREATE OR REPLACE FUNCTION pg_temp.contact_consume_request_v1(p_start jsonb)
RETURNS jsonb LANGUAGE sql AS $function$
 SELECT jsonb_build_object('permit_id',p_start->'permit_id','provider_attempt_id',p_start->'provider_attempt_id',
  'dispatch_target_id',t.id,'dispatch_command_id',c.id,'target_snapshot_hash',encode(t.target_snapshot_hash,'hex'),
  'provider',t.provider,'content_hash',encode(t.rendered_content_hash,'hex'),'agency_id',c.agency_id,'environment',c.environment)
 FROM public.weekly_message_dispatch_targets t JOIN public.weekly_message_dispatch_commands c ON c.id=t.dispatch_command_id
 JOIN pg_temp.contact_start_fixture_targets f ON f.dispatch_target_id=t.id WHERE f.kind='PUSH';
$function$;
