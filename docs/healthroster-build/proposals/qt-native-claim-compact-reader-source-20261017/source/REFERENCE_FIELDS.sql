'backgroundObservedAt',clock_timestamp(),'backgroundSnapshotStartedAt',transaction_timestamp(),
  'backgroundSnapshotXmax',pg_snapshot_xmax((select snapshot_value from background_statement_snapshot))::text,
  'backgroundSnapshotEpochZero',pg_snapshot_xmax((select snapshot_value from background_statement_snapshot))::text::numeric<4294967296,
  'backgroundSnapshotXip',(select coalesce(jsonb_agg(x::text),'[]'::jsonb) from pg_snapshot_xip((select snapshot_value from background_statement_snapshot)) x),
  'backgroundRows',jsonb_build_object('private.candidate_daily_batch_receipts',(select coalesce(jsonb_agg(jsonb_build_object('keyHash',encode(pg_catalog.sha256(pg_catalog.convert_to((r.batch_receipt_id)::text,'UTF8')),'hex'),
   'rowHash',encode(pg_catalog.sha256(pg_catalog.convert_to((to_jsonb(r))::text,'UTF8')),'hex'),'xmin',r.xmin::text,'xactStatus',case when pg_snapshot_xmax((select snapshot_value from background_statement_snapshot))::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,
   'nativeClaimProved',coalesce((r.environment='TEST' and r.actor_class='SIGNED_SYSTEM' and r.operation_class='PROJECTION_CLAIM'
  and r.claim_target='MASTER_AVAILABILITY_SHEET' and r.claim_limit between 1 and 100
  and r.claim_request_id is not null and r.item_count=1
  and r.item_keys_json=jsonb_build_array('claim:'||r.claim_request_id::text)
  and r.state='COMPLETED' and r.terminal_http_status=200
  and r.terminal_response_body->>'batch_receipt_id'=r.batch_receipt_id::text
  and r.terminal_response_body->>'claim_request_id'=r.claim_request_id::text
  and r.terminal_response_body->'items'=r.claimed_items_json
  and (case when pg_input_is_valid(r.terminal_response_body->>'lease_set_expires_at','timestamptz')
    then (r.terminal_response_body->>'lease_set_expires_at')::timestamptz end)=r.lease_expires_at_utc
  and extract(epoch from r.lease_expires_at_utc-r.created_at_utc) between 30 and 600
  and extract(epoch from r.lease_expires_at_utc-r.created_at_utc)=trunc(extract(epoch from r.lease_expires_at_utc-r.created_at_utc))
  and r.request_hash=private._candidate_daily_json_sha256_v1(jsonb_build_object('operation','PROJECTION_CLAIM',
    'claim_request_id',r.claim_request_id,'target',r.claim_target,'claimant',r.lease_owner,
    'max_items',r.claim_limit,'lease_seconds',extract(epoch from r.lease_expires_at_utc-r.created_at_utc)::integer))
  and r.terminal_response_sha256=private._candidate_daily_json_sha256_v1(r.terminal_response_body)),false),'createdAt',r.created_at_utc,'updatedAt',r.updated_at_utc,'completedAt',r.completed_at_utc)
   order by r.batch_receipt_id),'[]'::jsonb) from private.candidate_daily_batch_receipts r),'public.ts_financials_outbox',(select coalesce(jsonb_agg(jsonb_build_object('keyHash',encode(pg_catalog.sha256(pg_catalog.convert_to((r.id)::text,'UTF8')),'hex'),'rowHash',encode(pg_catalog.sha256(pg_catalog.convert_to((to_jsonb(r))::text,'UTF8')),'hex'),
   'stableHash',encode(pg_catalog.sha256(pg_catalog.convert_to((to_jsonb(r)-array['attempt_count','next_attempt_at'])::text,'UTF8')),'hex'),'xmin',r.xmin::text,
   'xactStatus',case when pg_snapshot_xmax((select snapshot_value from background_statement_snapshot))::text::numeric<4294967296 then pg_xact_status(r.xmin::text::xid8) end,
   'attemptCount',r.attempt_count,'nextAttemptAt',r.next_attempt_at,'createdAt',r.created_at)
   order by r.id),'[]'::jsonb) from public.ts_financials_outbox r)),
  'backgroundWriterRoutines',(select coalesce(jsonb_agg(r order by r->>'schema',r->>'identity'),'[]'::jsonb)
    from catalogue c cross join lateral jsonb_array_elements(c.jsonb_build_object::jsonb->'routines') r
    where r->>'identity' ~ '^(candidate_daily_projection_claim_v1|_candidate_daily_json_sha256_v1|tsfin_dequeue_batch_ids|tsfin_work_fail)\(')