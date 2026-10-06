-- Source-owned actual public import/Final owner regression. Always inside the
-- runner's outer rollback on the named empty local PG17 clone. Parser/economic
-- INPUTS come from the existing factual fixture, not a seeded Final or receipt.
-- Four already-past cutoffs process old work dates. No clock/cutoff is bypassed.
do $final_report_boundary$
declare
  v_actor constant uuid:='b8550000-0000-4000-8000-000000000001';
  v_review uuid; v_work uuid; v_row uuid; v_publication uuid;
  v_request jsonb; v_result jsonb; v_later jsonb; v_opened jsonb;
begin
  if current_database()<>'source_local_joined_20261005' then
    raise exception 'QUERY_FINAL_BOUNDARY_REQUIRES_OWN_LOCAL_CLONE'; end if;
  select r.id,r.work_event_id,r.upload_row_id into strict v_review,v_work,v_row
    from private.weekly_source_manual_reviews r where r.state='OPEN';
  select p.id into strict v_publication from public.weekly_source_projection_publications p
    join public.weekly_source_upload_rows u on u.upload_id=p.upload_id
    where u.id=v_row and p.state='CURRENT';
  select jsonb_build_object('actor_user_id',v_actor,'source_cycle_id',p.source_cycle_id,
    'authority_scope_kind',p.authority_scope_kind,'report_scope_id',p.report_scope_id,
    'upload_id',p.upload_id,'projection_publication_id',p.id,
    'expected_authority_scope_version',p.authority_scope_version,
    'expected_row_manifest_hash',encode(u.row_manifest_hash,'hex'),
    'expected_comparison_manifest_hash',encode(p.comparison_manifest_hash,'hex'),
    'expected_issue_set_hash',encode(p.issue_set_hash,'hex')) into strict v_request
    from public.weekly_source_projection_publications p
    join public.weekly_source_uploads u on u.id=p.upload_id where p.id=v_publication;
  v_result:=public.weekly_source_finalise_atomic_v1(v_request);
  perform pg_temp.bpsc_assert(v_result->>'final_revision_id' is not null,'actual opening report Final succeeds');
  perform pg_temp.bpsc_assert((select state='OPEN' from private.weekly_source_manual_reviews where id=v_review),
    'finalising the already-open report must NOT clear its manual query');
  perform pg_temp.bpsc_assert((public.weekly_source_finalise_atomic_v1(v_request)->>'idempotent')::boolean,
    'actual opening report Final replay');
  perform pg_temp.bpsc_assert((select state='OPEN' from private.weekly_source_manual_reviews where id=v_review),
    'opening report replay cannot clear query');

  v_later:=pg_temp.bpsc_import(true,'2026-09-13',jsonb_build_array(jsonb_build_object(
    'key','manual-query','date','2026-08-31','end','17:00','minutes',480,'break',0,'expense',0,
    'prior_event',v_work)),'SOURCE_QUERY_LATER_UNCHANGED',null,false);
  perform pg_temp.bpsc_assert((select state='OPEN' from private.weekly_source_manual_reviews where id=v_review),
    'a genuinely later published import is not yet a finalised remedy');
  select r.id into strict v_row from public.weekly_source_upload_rows r
    where r.upload_id=(v_later->>'upload_id')::uuid;
  perform pg_temp.bpsc_assert((select work_event_id=v_work from public.weekly_source_row_resolutions
    where upload_row_id=v_row order by generation desc limit 1),'actual later import retains durable shift');
  select jsonb_build_object('actor_user_id',v_actor,'source_cycle_id',p.source_cycle_id,
    'authority_scope_kind',p.authority_scope_kind,'report_scope_id',p.report_scope_id,
    'upload_id',p.upload_id,'projection_publication_id',p.id,
    'expected_authority_scope_version',p.authority_scope_version,
    'expected_row_manifest_hash',encode(u.row_manifest_hash,'hex'),
    'expected_comparison_manifest_hash',encode(p.comparison_manifest_hash,'hex'),
    'expected_issue_set_hash',encode(p.issue_set_hash,'hex')) into strict v_request
    from public.weekly_source_projection_publications p
    join public.weekly_source_uploads u on u.id=p.upload_id where p.id=(v_later->>'publication_id')::uuid;
  v_result:=public.weekly_source_finalise_atomic_v1(v_request);
  perform pg_temp.bpsc_assert(v_result->>'final_revision_id' is not null,'actual later unchanged report Final succeeds');
  perform pg_temp.bpsc_assert((select state='RESOLVED' from private.weekly_source_manual_reviews where id=v_review),
    'later actually finalised report clears contained shift even with unchanged hours');

  v_opened:=public.weekly_source_manual_review_open_v1(jsonb_build_object('actor_user_id',v_actor,
    'source_row_id',v_row,'reason','New question after the later report was finalised',
    'command_id','b8550000-0000-4000-8000-000000000904'));
  v_review:=(v_opened->>'review_id')::uuid;
  perform pg_temp.bpsc_assert(v_opened->>'ok'='true' and v_opened->>'already_open'='false','new actual Final-anchored query');
  perform pg_temp.bpsc_assert((public.weekly_source_finalise_atomic_v1(v_request)->>'idempotent')::boolean,
    'actual completed report replay after requery');
  perform pg_temp.bpsc_assert((select state='OPEN' from private.weekly_source_manual_reviews where id=v_review),
    'replay of opening Final does not clear newly opened query');

  v_later:=pg_temp.bpsc_import(true,'2026-09-20','[{"key":"other-shift","date":"2026-09-01","end":"17:00","minutes":480,"break":0,"expense":0}]'::jsonb,
    'SOURCE_QUERY_LATER_REPORT_OMITS_TARGET',null,true);
  perform pg_temp.bpsc_assert(v_later->>'final_revision_id' is not null,'actual omitted-target report Final succeeds');
  perform pg_temp.bpsc_assert((select state='OPEN' from private.weekly_source_manual_reviews where id=v_review),
    'finalised report containing only another shift cannot clear this query');
  v_later:=pg_temp.bpsc_import(true,'2026-09-27',jsonb_build_array(jsonb_build_object(
    'key','manual-query','date','2026-08-31','end','17:00','minutes',480,'break',0,'expense',0,
    'prior_event',v_work)),'SOURCE_QUERY_LATER_FINAL_CONTAINS_TARGET',null,true);
  perform pg_temp.bpsc_assert(v_later->>'final_revision_id' is not null,'actual subsequent contained-target Final succeeds');
  perform pg_temp.bpsc_assert((select state='RESOLVED' from private.weekly_source_manual_reviews where id=v_review),
    'later contained-target Final resolves Final-anchored query');
  raise notice 'SOURCE_MANUAL_QUERY_FINAL_REPORT_BOUNDARY_PASS';
end $final_report_boundary$;
select 'SOURCE_MANUAL_QUERY_FINAL_REPORT_BOUNDARY_PASS';
