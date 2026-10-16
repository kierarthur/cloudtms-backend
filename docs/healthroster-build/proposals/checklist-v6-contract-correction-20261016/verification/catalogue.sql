-- Bounded SELECT-only catalogue qualification. No business rows, SQL execution or native acceptance is implied.
with expected(signature) as (values
 ('private.timesheet_contract_move_row_hash_v1(jsonb)'),
 ('private.timesheet_contract_move_changed_columns_v1(jsonb,jsonb)'),
 ('private.timesheet_contract_move_uuid_array_v1(uuid[])'),
 ('private.timesheet_contract_move_accepting_v1(uuid)'),
 ('private.timesheet_contract_current_placement_v1(uuid)'),
 ('private.timesheet_contract_move_family_lock_v1(uuid)'),
 ('private.timesheet_contract_move_immutable_v1()'),
 ('private.timesheet_contract_move_operation_guard_v1()'),
 ('private.timesheet_contract_move_request_guard_v1()'),
 ('private.timesheet_contract_move_member_guard_v1()'),
 ('private.timesheet_contract_move_placement_guard_v1()'),
 ('private.timesheet_contract_move_commit_guard_v1()'),
 ('private.timesheet_contract_move_context_actor_allowed_v1(uuid,text,uuid)'),
 ('private.timesheet_contract_move_financial_economics_v1(jsonb)'),
 ('private.timesheet_contract_move_context_v1()'),
 ('private.timesheet_contract_move_context_guard_v1()'),
 ('private.timesheet_contract_move_context_commit_guard_v1()'),
 ('private.timesheet_contract_move_event_register_v1(text,text,oid,text,jsonb,jsonb,uuid,uuid)'),
 ('private.timesheet_contract_move_event_assert_v1(uuid,jsonb)'),
 ('private.timesheet_contract_move_capability_issue_v1(uuid,text,text,text)'),
 ('private.timesheet_contract_move_capability_assert_v1(uuid,text,oid,text,jsonb,jsonb)'),
 ('private.timesheet_contract_move_capability_resolve_v1(text,oid,text,jsonb,jsonb)'),
 ('private.timesheet_contract_move_capability_consume_v1(uuid,text,oid,text,jsonb,jsonb)'),
 ('private.timesheet_contract_move_capability_cleanup_v1()'),
 ('private.timesheet_contract_move_evidence_v1(uuid)'),
 ('private.timesheet_contract_move_scope_v1(uuid,boolean)'),
 ('private.timesheet_contract_move_review_members_v1(uuid,jsonb)'),
 ('private.timesheet_contract_move_booking_part_v1(text)'),
 ('private.timesheet_contract_move_booking_v1(uuid,uuid,date,integer)'),
 ('private.timesheet_contract_move_inherited_evidence_v1(jsonb)'),
 ('private.timesheet_contract_move_evidence_source_v1(uuid)'),
 ('private.timesheet_contract_move_stage_v1(uuid,uuid,bytea)'),
 ('public.timesheet_archive_row_guard_v1()'),
 ('public.timesheet_archived_evidence_guard_v1()'),
 ('private.timesheet_contract_move_financial_row_guard_v1()'),
 ('private.timesheet_contract_move_original_finance_assert_v1(uuid,text,bytea,bytea)'),
 ('private.timesheet_contract_move_assert_v1(uuid,uuid)'),
 ('private.timesheet_contract_move_reason_v1(text,text)'),
 ('private.timesheet_contract_move_request_assert_v1(jsonb,text)'),
 ('private.timesheet_contract_move_request_hash_v1(jsonb,text)'),
 ('public.timesheet_contract_move_accept_v1(jsonb)'),
 ('private.daily_rate_authority_lock_v1()'),
 ('private.daily_rate_correction_authority_v1(jsonb,boolean)'),
 ('private.daily_rate_correction_context_v1(uuid,uuid,boolean)'),
 ('public.timesheet_daily_rate_correction_context_v1(jsonb)'),
 ('private.daily_rate_correction_scope_v1(uuid,boolean)'),
 ('private.daily_rate_calculator_round_assert_v1(double precision,numeric)'),
 ('private.daily_rate_correction_snapshot_assert_v1(jsonb,jsonb,jsonb,uuid)'),
 ('private.daily_rate_correction_normalized_snapshot_v1(jsonb,jsonb,jsonb,uuid)'),
 ('private.daily_rate_correction_financial_id_v1(uuid,text)'),
 ('private.daily_rate_correction_revision_admit_v1(uuid,text)'),
 ('private.daily_rate_correction_accepting_v1(uuid)'),
 ('private.daily_rate_correction_assert_v1(uuid,uuid)'),
 ('private.daily_rate_correction_request_assert_v1(jsonb,text)'),
 ('private.daily_rate_correction_request_hash_v1(jsonb,text)'),
 ('private.daily_rate_correction_preview_replay_v1(jsonb)')
)
select e.signature,p.oid is not null as installed,n.nspname,p.prosecdef,p.proconfig,
 case when p.oid is null then null else has_function_privilege('anon',p.oid,'EXECUTE') end as anon_execute,
 case when p.oid is null then null else has_function_privilege('authenticated',p.oid,'EXECUTE') end as browser_execute,
 case when p.oid is null then null else has_function_privilege('service_role',p.oid,'EXECUTE') end as service_execute,
 exists(select 1 from aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a where a.grantee=0 and a.privilege_type='EXECUTE') as public_execute
from expected e left join pg_proc p on p.oid=to_regprocedure(e.signature) left join pg_namespace n on n.oid=p.pronamespace order by e.signature limit 56;

select c.relname,c.relrowsecurity,c.relforcerowsecurity,
 has_table_privilege('service_role',c.oid,'SELECT,INSERT,UPDATE,DELETE') as service_table_access,
 has_table_privilege('anon',c.oid,'SELECT,INSERT,UPDATE,DELETE') as anon_table_access,
 has_table_privilege('authenticated',c.oid,'SELECT,INSERT,UPDATE,DELETE') as browser_table_access
from pg_class c join pg_namespace n on n.oid=c.relnamespace where n.nspname='private'
 and c.relname in('timesheet_contract_move_operations','timesheet_contract_move_requests','timesheet_contract_move_reviews',
 'timesheet_contract_move_members','timesheet_contract_current_placements','timesheet_contract_family_aliases',
 'timesheet_contract_move_execution_contexts','timesheet_contract_move_capabilities','timesheet_contract_move_capability_consumptions')
order by c.relname limit 9;

select c.conname,pg_get_constraintdef(c.oid) as definition from pg_constraint c
where c.conrelid in(to_regclass('private.timesheet_contract_move_capabilities'),to_regclass('private.timesheet_contract_move_capability_consumptions'))
 and c.contype='u' order by c.conrelid,c.conname limit 4;

with expected(name,relation,routine) as (values
 ('timesheet_contract_move_operation_guard','private.timesheet_contract_move_operations','private.timesheet_contract_move_operation_guard_v1()'),
 ('timesheet_contract_move_request_guard','private.timesheet_contract_move_requests','private.timesheet_contract_move_request_guard_v1()'),
 ('timesheet_contract_move_member_guard','private.timesheet_contract_move_members','private.timesheet_contract_move_member_guard_v1()'),
 ('timesheet_contract_move_placement_guard','private.timesheet_contract_current_placements','private.timesheet_contract_move_placement_guard_v1()'),
 ('timesheet_contract_move_review_immutable','private.timesheet_contract_move_reviews','private.timesheet_contract_move_immutable_v1()'),
 ('timesheet_contract_move_alias_immutable','private.timesheet_contract_family_aliases','private.timesheet_contract_move_immutable_v1()'),
 ('timesheet_contract_move_consumption_immutable','private.timesheet_contract_move_capability_consumptions','private.timesheet_contract_move_immutable_v1()'),
 ('timesheet_contract_move_commit_guard','private.timesheet_contract_move_operations','private.timesheet_contract_move_commit_guard_v1()'),
 ('timesheet_contract_move_member_commit_guard','private.timesheet_contract_move_members','private.timesheet_contract_move_commit_guard_v1()'),
 ('timesheet_contract_move_context_guard','private.timesheet_contract_move_execution_contexts','private.timesheet_contract_move_context_guard_v1()'),
 ('timesheet_contract_move_context_commit_guard','private.timesheet_contract_move_execution_contexts','private.timesheet_contract_move_context_commit_guard_v1()'),
 ('zz_timesheet_contract_move_financial_row_guard_v1','public.timesheets_financials','private.timesheet_contract_move_financial_row_guard_v1()'),
 ('daily_rate_authority_client_lock_v1','public.rates_client_defaults','private.daily_rate_authority_lock_v1()'),
 ('daily_rate_authority_candidate_lock_v1','public.rates_candidate_overrides','private.daily_rate_authority_lock_v1()'),
 ('trg_timesheet_archive_row_guard_v1','public.timesheets','public.timesheet_archive_row_guard_v1()'),
 ('trg_timesheet_archived_evidence_guard_v1','public.timesheet_evidence','public.timesheet_archived_evidence_guard_v1()')
)
select e.*,t.oid is not null as installed,t.tgenabled,t.tgtype,pg_get_triggerdef(t.oid,true) as definition,
 t.tgfoid=to_regprocedure(e.routine) as exact_function
from expected e left join pg_trigger t on t.tgrelid=to_regclass(e.relation) and t.tgname=e.name
 and not t.tgisinternal order by e.relation,e.name limit 16;
