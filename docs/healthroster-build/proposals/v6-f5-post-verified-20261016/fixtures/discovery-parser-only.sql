-- OFFLINE GRAMMAR ONLY; synthetic IDs, NOT a native observation. DO NOT EXECUTE.
with native as (
 select g.*,c.id cycle_id,c.scope_client_id,c.finalisation_week_ending,cl.cli_ref
 from public.weekly_source_groups g join public.weekly_source_cycles c on c.source_group_id=g.id
 join public.clients cl on cl.id=c.scope_client_id
 where g.id='f5520000-0000-4000-8000-000000000002'::uuid and c.id='f5520000-0000-4000-8000-000000000003'::uuid
 and g.environment='TEST' and g.active and g.source_family='ROSTER' and g.code ~ '^WSQ_TEST_[A-Z0-9_]+$'
 and c.finalisation_week_ending<(clock_timestamp() at time zone 'Europe/London')::date
 and c.cutoff_at_utc<clock_timestamp() and c.current_complete_upload_id is null and c.current_projection_publication_id is null
 and c.current_final_revision_id is null and c.state not in('FINALISING','FINALISED','CORRECTION_IN_PROGRESS')
 and cl.cli_ref ~ '^CLI-913013[0-9]{2}$'
 and not exists(select 1 from public.weekly_source_uploads u where u.source_cycle_id=c.id)
 and (select count(*) from public.weekly_source_group_clients m where m.source_group_id=g.id
  and c.finalisation_week_ending between m.valid_from and coalesce(m.valid_to,'infinity'::date))=1
 and exists(select 1 from public.weekly_source_group_clients m where m.source_group_id=g.id and m.client_id=cl.id
  and c.finalisation_week_ending between m.valid_from and coalesce(m.valid_to,'infinity'::date))
 and private.weekly_source_pay_actor_allowed_v1('f5520000-0000-4000-8000-000000000001'::uuid)
), contract_scope as (
 select k.id,k.candidate_id,p.tms_ref from public.contracts k join native n on n.scope_client_id=k.client_id
 join public.candidates p on p.id=k.candidate_id
 where daterange(k.start_date,k.end_date,'[]') && daterange(n.finalisation_week_ending-6,n.finalisation_week_ending,'[]')
), assignment as (
 select a.*,to_jsonb(a) native_row,v.values_sha256,v.values_json from public.weekly_source_group_rule_assignments a
 join native n on n.id=a.source_group_id and n.finalisation_week_ending between a.effective_from_week_ending and coalesce(a.effective_to_week_ending,'infinity'::date)
 join public.weekly_source_rule_set_versions v on v.id=a.rule_set_version_id
), future_assignment as (
 select a.*,to_jsonb(a) native_row,v.values_sha256,v.values_json from public.weekly_source_group_rule_assignments a
 join native n on n.id=a.source_group_id join public.weekly_source_rule_set_versions v on v.id=a.rule_set_version_id
 where a.effective_from_week_ending>=(clock_timestamp() at time zone 'Europe/London')::date
), release as (select r.* from private.cloudtms_database_releases r order by started_at_utc desc,release_id desc limit 1)
select jsonb_build_object('execution',private.weekly_source_protected_qualification_release_v1(),
 'controls',(select evidence_json->'weekly_source_off_rule_test_controls' from release),
 'expires_at_utc',to_char((clock_timestamp()+interval '1 hour') at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
 'agency_id',n.agency_id,'group_version',n.version::text,'client_id',n.scope_client_id,'group_code',n.code,'week_ending',n.finalisation_week_ending,
 'future_week_ending',((clock_timestamp() at time zone 'Europe/London')::date+14+(7-extract(dow from (clock_timestamp() at time zone 'Europe/London')::date+14)::integer)%7),
 'future_assignment_count',(select count(*) from future_assignment),'future_assignment',(select to_jsonb(a) from future_assignment a),
 'future_assignment_sha256',(select encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_OFF_RULE_ASSIGNMENT_V1',a.native_row),'hex') from future_assignment a),
 'assignment_count',(select count(*) from assignment),'assignment',(select to_jsonb(a) from assignment a),
 'assignment_version',(select a.version::text from assignment a),
 'assignment_sha256',(select encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_OFF_RULE_ASSIGNMENT_V1',
   a.native_row),'hex') from assignment a),
 'fixture_manifest',jsonb_build_object('group_code',n.code,'client_ids',jsonb_build_array(n.scope_client_id),
  'contract_ids',(select jsonb_agg(id order by id) from contract_scope),
  'candidate_ids',(select jsonb_agg(candidate_id order by candidate_id) from (select distinct candidate_id from contract_scope) p),
  'client_refs',jsonb_build_array(jsonb_build_object('client_id',n.scope_client_id,'cli_ref',n.cli_ref)),
  'candidate_refs',(select jsonb_agg(jsonb_build_object('candidate_id',candidate_id,'tms_ref',tms_ref) order by candidate_id)
   from(select distinct candidate_id,tms_ref from contract_scope) p)))::text from native n;
