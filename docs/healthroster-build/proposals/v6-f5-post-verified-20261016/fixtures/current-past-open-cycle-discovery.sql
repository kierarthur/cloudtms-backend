-- Bounded read-only native metadata discovery. No fixture eligibility or
-- protected authority is manufactured. Root selects an actual identity and
-- still runs the existing complete native setup/registration validators.
with scopes as (
 select g.id source_group_id,g.code,g.agency_id,c.id cycle_id,c.scope_client_id,
  c.finalisation_week_ending,c.cutoff_at_utc,c.state,c.current_complete_upload_id,
  c.current_projection_publication_id,c.current_final_revision_id,cl.cli_ref,
  coalesce((select max(prior.cutoff_at_utc) from public.weekly_source_cycles prior
   where prior.source_group_id=g.id and prior.cutoff_at_utc<c.cutoff_at_utc),
   ((c.cutoff_at_utc at time zone 'Europe/London')-interval '7 days') at time zone 'Europe/London') previous_cutoff_at_utc
 from public.weekly_source_groups g join public.weekly_source_cycles c on c.source_group_id=g.id
 join public.clients cl on cl.id=c.scope_client_id
 where g.environment='TEST' and g.active and g.source_family='ROSTER' and g.code ~ '^WSQ_TEST_[A-Z0-9_]+$'
 and cl.cli_ref ~ '^CLI-913013[0-9]{2}$' and c.cutoff_at_utc<clock_timestamp()
 and c.finalisation_week_ending<(clock_timestamp() at time zone 'Europe/London')::date
 and c.current_complete_upload_id is null and c.current_projection_publication_id is null and c.current_final_revision_id is null
 and c.state not in('FINALISING','FINALISED','CORRECTION_IN_PROGRESS')
 and not exists(select 1 from public.weekly_source_uploads u join public.weekly_source_cycles uc on uc.id=u.source_cycle_id
  where uc.source_group_id=g.id or (uc.finalisation_week_ending=c.finalisation_week_ending and
   (uc.scope_client_id=cl.id or u.file_metadata_json->>'client_id'=cl.id::text)))
 order by g.id,c.id limit 25
)
select jsonb_build_object('database',current_database(),'observed_at_utc',clock_timestamp(),
 'candidate_scope_count',count(*),'candidate_scopes',coalesce(jsonb_agg(to_jsonb(scopes) order by source_group_id,cycle_id),'[]'::jsonb),
 'qualification','READ_ONLY_CANDIDATE_INPUTS_ONLY','native_setup_and_registration_required',true)::text from scopes;
