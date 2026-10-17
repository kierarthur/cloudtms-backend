'backgroundObservedAt',clock_timestamp(),'backgroundSnapshotStartedAt',transaction_timestamp(),
  'backgroundSnapshotXmax',pg_snapshot_xmax((select snapshot_value from background_statement_snapshot))::text,
  'backgroundSnapshotEpochZero',pg_snapshot_xmax((select snapshot_value from background_statement_snapshot))::text::numeric<4294967296,
  'backgroundSnapshotXip',(select coalesce(jsonb_agg(x::text),'[]'::jsonb) from pg_snapshot_xip((select snapshot_value from background_statement_snapshot)) x),
  'backgroundRows',jsonb_build_object('private.candidate_daily_batch_receipts',(select coalesce(jsonb_agg(jsonb_build_object('keyHash',encode(pg_catalog.sha256(pg_catalog.convert_to((r.batch_receipt_id)::text,'UTF8')),'hex'),
   'rowHash',encode(pg_catalog.sha256(pg_catalog.convert_to((to_jsonb(r))::text,'UTF8')),'hex'))
   order by r.batch_receipt_id),'[]'::jsonb) from private.candidate_daily_batch_receipts r),'public.ts_financials_outbox',(select coalesce(jsonb_agg(jsonb_build_object('keyHash',encode(pg_catalog.sha256(pg_catalog.convert_to((r.id)::text,'UTF8')),'hex'),'rowHash',encode(pg_catalog.sha256(pg_catalog.convert_to((to_jsonb(r))::text,'UTF8')),'hex'),
   'stableHash',encode(pg_catalog.sha256(pg_catalog.convert_to((to_jsonb(r)-array['attempt_count','next_attempt_at'])::text,'UTF8')),'hex'),'xmin',r.xmin::text,
   'xactStatus',case when pg_snapshot_xmax((select snapshot_value from background_statement_snapshot))::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,
   'attemptCount',r.attempt_count,'nextAttemptAt',r.next_attempt_at,'createdAt',r.created_at)
   order by r.id),'[]'::jsonb) from public.ts_financials_outbox r)),
  'backgroundWriterRoutines',(select coalesce(jsonb_agg(r order by r->>'schema',r->>'identity'),'[]'::jsonb)
    from catalogue c cross join lateral jsonb_array_elements(c.jsonb_build_object::jsonb->'routines') r
    where r->>'identity' ~ '^(candidate_daily_projection_claim_v1|_candidate_daily_json_sha256_v1|tsfin_dequeue_batch_ids|tsfin_work_fail)\(')