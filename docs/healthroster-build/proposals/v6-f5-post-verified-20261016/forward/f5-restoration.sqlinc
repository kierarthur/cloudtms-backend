-- Existing native release helper requires real VERIFIED/completed/current
-- catalogue/managed TEST evidence. This SELECT manufactures no release state.
with release as (
 select r.* from private.cloudtms_database_releases r order by started_at_utc desc,release_id desc limit 1
), auth as (
 select a.value from release r cross join lateral jsonb_array_elements(r.evidence_json->'weekly_source_protected_test_setup_authorizations') a
 where a.value->>'schema_version'='WEEKLY_SOURCE_OFF_RULE_TEST_AUTHORIZATION_V1'
 and a.value->>'purpose'='SOURCE_FIXTURE_SETUP'
 and a.value->'allowed_operations' @> '["weekly_source_upload_stage_begin_atomic_v1","weekly_source_upload_stage_rows_atomic_v1","weekly_source_upload_seal_atomic_v1","weekly_source_invoice_hold_release_atomic_v1"]'::jsonb
), registrations as (
 select a.after_json-'artifacts'-'receipt_tree_proof' receipt,a.after_json->'artifacts' artifacts
 from public.audit_events a cross join release r
 where a.object_type='weekly_source_post_verified_evidence' and a.object_id_text=r.release_id
 and a.action='WEEKLY_SOURCE_POST_VERIFIED_EVIDENCE_RECORDED' and a.event_sequence_is_authoritative
 and a.after_json->>'artifact_slot'='weekly_source_protected_test_setup_authorizations'
 and exists(select 1 from auth where a.after_json->'artifacts' @> jsonb_build_array(auth.value))
), cycles as (select (value->>'source_cycle_id')::uuid id from auth), contracts as (
 select distinct member.value::uuid id from auth a cross join lateral jsonb_array_elements_text(a.value#>'{fixture_manifest,contract_ids}') member(value)
), timesheets as (select t.* from public.timesheets t where t.contract_id in(select id from contracts)),
footprint as (
 select jsonb_build_object(
  'release',(select jsonb_build_object('release_id',r.release_id,'git_commit',r.git_commit,'status',r.status,
    'installed_contract_sha256',r.installed_contract_sha256,'completed_at_utc',r.completed_at_utc,'evidence_json',r.evidence_json) from release r),
  'cycles',(select jsonb_agg(to_jsonb(c) order by c.id) from public.weekly_source_cycles c where c.id in(select id from cycles)),
  'assignments',(select jsonb_agg(to_jsonb(a) order by a.id) from public.weekly_source_group_rule_assignments a where a.id in(select (value->>'assignment_id')::uuid from auth)),
  'uploads',(select jsonb_agg(to_jsonb(u) order by u.id) from public.weekly_source_uploads u where u.source_cycle_id in(select id from cycles)),
  'hold_releases',(select jsonb_agg(to_jsonb(h) order by h.id) from public.weekly_source_invoice_hold_releases h join public.weekly_source_upload_rows r on r.id=h.upload_row_id
    join public.weekly_source_uploads u on u.id=r.upload_id where u.source_cycle_id in(select id from cycles)),
  'scope_audits',(select jsonb_agg(to_jsonb(a) order by a.id) from public.audit_events a where a.object_type in('weekly_source_off_rule_test_journey','weekly_source_off_rule_command')
    and (a.object_id_text in(select value->>'scope_id' from auth) or a.after_json->>'source_cycle_id' in(select id::text from cycles))),
  'timesheets',(select jsonb_agg(to_jsonb(t) order by t.timesheet_id) from timesheets t),
  'workflows',(select jsonb_agg(to_jsonb(w) order by w.id) from public.candidate_submission_workflows w where w.id in(select candidate_workflow_id from timesheets)),
  'approval_requests',(select jsonb_agg(to_jsonb(a) order by a.id) from public.candidate_approval_requests a where a.workflow_id in(select candidate_workflow_id from timesheets)),
  'components',(select jsonb_agg(to_jsonb(c) order by c.id) from public.candidate_submission_components c where c.workflow_id in(select candidate_workflow_id from timesheets)),
  'financials',(select jsonb_agg(to_jsonb(f) order by f.id) from public.timesheets_financials f where f.timesheet_id in(select timesheet_id from timesheets)),
  'invoice_lines',(select jsonb_agg(to_jsonb(l) order by l.id) from public.invoice_lines l where l.invoice_id in(select invoice_id from public.invoice_lines where timesheet_id in(select timesheet_id from timesheets))),
  'invoices',(select jsonb_agg(to_jsonb(i) order by i.id) from public.invoices i where i.id in(select invoice_id from public.invoice_lines where timesheet_id in(select timesheet_id from timesheets))),
  'evidence',(select jsonb_agg(to_jsonb(e) order by e.id) from public.timesheet_evidence e where e.timesheet_id in(select timesheet_id from timesheets)),
  'document_assets',(select jsonb_agg(to_jsonb(a) order by a.id) from public.invoice_document_assets a where
   a.id in(select manual_document_asset_id from timesheets) or a.id in(select document_asset_id from public.timesheet_evidence where timesheet_id in(select timesheet_id from timesheets))),
  'document_versions',(select jsonb_agg(to_jsonb(d) order by d.id) from public.invoice_document_versions d where
    (d.entity_type='TIMESHEET' and d.entity_id in(select timesheet_id from timesheets)) or (d.entity_type='INVOICE' and d.entity_id in(select invoice_id from public.invoice_lines where timesheet_id in(select timesheet_id from timesheets))))) value
)
select jsonb_build_object('database',current_database(),'execution',private.weekly_source_protected_qualification_release_v1(),
 'f5_fixture_context',(select jsonb_build_object('authorization',auth.value,'observed_at_utc',clock_timestamp(),
  'cycle',jsonb_build_object('id',c.id,'source_group_id',c.source_group_id,'scope_client_id',c.scope_client_id,
   'finalisation_week_ending',c.finalisation_week_ending,'cutoff_at_utc',c.cutoff_at_utc,
   'previous_cutoff_at_utc',coalesce((select max(prior.cutoff_at_utc) from public.weekly_source_cycles prior
    where prior.source_group_id=c.source_group_id and prior.cutoff_at_utc<c.cutoff_at_utc),
    ((c.cutoff_at_utc at time zone 'Europe/London')-interval '7 days') at time zone 'Europe/London'),
   'state',c.state,'current_complete_upload_id',c.current_complete_upload_id,'current_projection_publication_id',c.current_projection_publication_id,
   'current_final_revision_id',c.current_final_revision_id)) from auth join public.weekly_source_cycles c on c.id=(auth.value->>'source_cycle_id')::uuid),
 'authority_registrations',coalesce((select jsonb_agg(jsonb_build_object('receipt',receipt,'artifacts',artifacts) order by receipt->>'receipt_sha256') from registrations),'[]'::jsonb),
 'fixture_vacant',not exists(select 1 from public.tms_users where id::text like 'f5510000-%' or email like 'f5-%@example.invalid')
  and not exists(select 1 from public.clients where id::text like 'f5510000-%' or name='F5 fixture self bill')
  and not exists(select 1 from public.weekly_source_groups where id::text like 'f5510000-%' or code='F5_VERIFY_MONEY')
  and not exists(select 1 from public.client_settings where id::text like 'f5510000-%')
  and not exists(select 1 from public.weekly_source_group_clients where id::text like 'f5510000-%')
  and not exists(select 1 from public.weekly_source_client_policies where id::text like 'f5510000-%')
  and not exists(select 1 from public.weekly_source_cycles where id::text like 'f5510000-%')
  and not exists(select 1 from public.weekly_source_rule_sets where code='F5_VERIFY_RULES' or created_by_user_id::text like 'f5510000-%')
  and not exists(select 1 from public.weekly_source_rule_set_versions where created_by_user_id::text like 'f5510000-%')
  and not exists(select 1 from public.weekly_source_group_rule_assignments where created_by_user_id::text like 'f5510000-%')
  and not exists(select 1 from public.weekly_source_invoice_hold_releases where actor_user_id::text like 'f5510000-%')
  and not exists(select 1 from public.weekly_source_uploads where original_filename='f5-verify-money.xlsx')
  and to_regprocedure('pg_temp.f5_native(jsonb,text,jsonb)') is null,
 'restoration_sha256',encode(private.weekly_source_sha256_jsonb_v1('F5_ROLLBACK_RESTORATION_V1',value),'hex'))::text from footprint;
