-- Run fresh and read-only before and after root's rollback trial; compare the entire JSON value.
with wanted(signature) as(values
('private._weekly_source_immutable_record_guard_v1()'),
('private.weekly_source_final_rule_version_guard_v1()'),
('private.weekly_source_self_bill_binding_guard_v1()'),
('private._weekly_source_rule_values_v1(jsonb)'),
('private._weekly_source_rule_group_context_v1(uuid,date,date,jsonb,boolean)'),
('private.weekly_source_is_import_authoritative_v1(uuid,uuid,date)'),
('private.weekly_source_effective_rule_set_v1(uuid,uuid)'),
('private.weekly_source_has_self_bill_rules_v1(uuid,uuid)'),
('private.weekly_source_agency_invoice_check_only_v1(uuid)'),
('private.weekly_source_signed_check_only_v1(uuid)'),
('private.weekly_source_configured_agency_invoice_v1(uuid,uuid,uuid,text,date)'),
('private.bpay_next_initial_tsfin_bind_v1(uuid)'),
('public.weekly_source_invoice_hold_release_atomic_v1(jsonb)'),
('public.candidate_submission_finalize_atomic_v1(uuid,text,uuid,integer,text,text,timestamp with time zone,jsonb)'),
('public.candidate_workflow_transition_atomic_v1(uuid,text,uuid,text,integer,jsonb,text,timestamp with time zone)'),
('private.candidate_notification_visible_v1(public.candidate_notifications,timestamp with time zone,text)'),
('private.weekly_source_checklist_legacy_hold_review_v1(uuid,jsonb,uuid)'),
('private.bpay_next_initial_tsfin_certificate_v1(uuid)'),
('private.bpay_next_initial_tsfin_bound_assert_v1(uuid)')
), routines as(
 select w.signature,p.oid::text oid,p.proowner::pg_catalog.regrole::text owner,p.provolatile::text volatility,
  p.prosecdef security_definer,p.proacl::text acl,p.proconfig::text configuration,
  case when p.oid is null then null else pg_catalog.md5(pg_catalog.pg_get_functiondef(p.oid)) end definition_md5
 from wanted w left join pg_catalog.pg_proc p on p.oid=pg_catalog.to_regprocedure(w.signature)
), triggers as(
 select n.nspname schema_name,c.relname table_name,t.tgname trigger_name,
  pg_catalog.md5(pg_catalog.pg_get_triggerdef(t.oid,true)) definition_md5,t.tgenabled enabled
 from pg_catalog.pg_trigger t join pg_catalog.pg_class c on c.oid=t.tgrelid
 join pg_catalog.pg_namespace n on n.oid=c.relnamespace
 where not t.tgisinternal and n.nspname in('private','public')
)
, wanted_tables(name) as(values
('private.cloudtms_database_releases'),
('private.weekly_source_agency_timesheet_change_requests'),
('public.candidates'),
('public.clients'),
('public.contract_weeks'),
('public.contracts'),
('public.invoices'),
('public.timesheet_validations'),
('public.timesheets'),
('public.timesheets_financials'),
('public.tms_users'),
('public.weekly_final_source_correction_sessions'),
('public.weekly_source_billing_movements'),
('public.weekly_source_charge_checks'),
('public.weekly_source_client_manifests'),
('public.weekly_source_cycles'),
('public.weekly_source_final_revisions'),
('public.weekly_source_format_profiles'),
('public.weekly_source_group_rule_assignments'),
('public.weekly_source_groups'),
('public.weekly_source_invoice_hold_releases'),
('public.weekly_source_invoice_placements'),
('public.weekly_source_invoice_presentation_lines'),
('public.weekly_source_money_cell_evidence'),
('public.weekly_source_projection_publications'),
('public.weekly_source_report_scopes'),
('public.weekly_source_row_resolutions'),
('public.weekly_source_rule_set_versions'),
('public.weekly_source_rule_sets'),
('public.weekly_source_self_bill_report_line_bindings'),
('public.weekly_source_self_bill_reports'),
('public.weekly_source_upload_rows'),
('public.weekly_source_upload_supersessions'),
('public.weekly_source_uploads'),
('public.weekly_work_events')
), table_frames as(
 select w.name,c.oid::text oid,c.relkind::text kind,c.relowner::pg_catalog.regrole::text owner,c.relrowsecurity rls,c.relforcerowsecurity force_rls,c.relacl::text acl,
 (select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('number',a.attnum,'name',a.attname,'type',pg_catalog.format_type(a.atttypid,a.atttypmod),'not_null',a.attnotnull,'default',pg_catalog.pg_get_expr(d.adbin,d.adrelid)) order by a.attnum) from pg_catalog.pg_attribute a left join pg_catalog.pg_attrdef d on d.adrelid=a.attrelid and d.adnum=a.attnum where a.attrelid=c.oid and a.attnum>0 and not a.attisdropped) columns,
 (select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('name',q.conname,'type',q.contype,'definition',pg_catalog.pg_get_constraintdef(q.oid,true),'validated',q.convalidated) order by q.conname) from pg_catalog.pg_constraint q where q.conrelid=c.oid) constraints,
 (select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('name',q.polname,'command',q.polcmd,'roles',q.polroles,'using',pg_catalog.pg_get_expr(q.polqual,q.polrelid),'check',pg_catalog.pg_get_expr(q.polwithcheck,q.polrelid)) order by q.polname) from pg_catalog.pg_policy q where q.polrelid=c.oid) policies,
 (select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('definition',pg_catalog.pg_get_indexdef(i.indexrelid),'valid',i.indisvalid,'ready',i.indisready) order by pg_catalog.pg_get_indexdef(i.indexrelid)) from pg_catalog.pg_index i where i.indrelid=c.oid) indexes
 from wanted_tables w left join pg_catalog.pg_class c on c.oid=pg_catalog.to_regclass(w.name)
)
select pg_catalog.jsonb_build_object('contract','CHECKLIST_CONSTRUCTOR_SCHEMA_ROUTINE_TRIGGER_FRAME_V2',
 'tables',(select pg_catalog.jsonb_agg(pg_catalog.to_jsonb(t) order by name) from table_frames t),
 'routines',(select pg_catalog.jsonb_agg(pg_catalog.to_jsonb(r) order by signature) from routines r),
 'triggers',(select pg_catalog.jsonb_agg(pg_catalog.to_jsonb(t) order by schema_name,table_name,trigger_name) from triggers t));
