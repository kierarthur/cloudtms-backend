-- Prefix IDs must already have been recorded here by the real native prefix.
-- Add every original row allocated by this suffix (not only the root group).
INSERT INTO pg_temp.contact_start_fixture_absence_ids
 SELECT 'public.weekly_message_dispatch_targets','id',dispatch_target_id FROM pg_temp.contact_start_fixture_targets
 UNION ALL SELECT 'public.weekly_message_dispatch_commands','id',dispatch_command_id FROM pg_temp.contact_start_fixture_targets
 UNION ALL SELECT 'public.weekly_message_renders','id',c.message_render_id FROM public.weekly_message_dispatch_commands c
 JOIN pg_temp.contact_start_fixture_targets f ON f.dispatch_command_id=c.id
 UNION ALL SELECT 'public.weekly_manager_review_batches','id',b.id FROM public.weekly_manager_review_batches b
 JOIN public.weekly_message_dispatch_commands c ON c.message_render_id=b.message_render_id
 JOIN pg_temp.contact_start_fixture_targets f ON f.dispatch_command_id=c.id
 UNION ALL SELECT 'public.weekly_manager_review_items','id',i.id FROM public.weekly_manager_review_items i
 JOIN public.weekly_manager_review_batches b ON b.id=i.review_batch_id
 JOIN public.weekly_message_dispatch_commands c ON c.message_render_id=b.message_render_id
 JOIN pg_temp.contact_start_fixture_targets f ON f.dispatch_command_id=c.id
 UNION ALL SELECT 'public.weekly_manager_route_receipts','id',r.id FROM public.weekly_manager_route_receipts r
 JOIN public.weekly_manager_review_batches b ON b.id=r.review_batch_id
 JOIN public.weekly_message_dispatch_commands c ON c.message_render_id=b.message_render_id
 JOIN pg_temp.contact_start_fixture_targets f ON f.dispatch_command_id=c.id
 UNION ALL SELECT 'public.weekly_manager_route_preparations','id',r.id FROM public.weekly_manager_route_preparations r
 JOIN pg_temp.contact_start_fixture_input f ON f.message_intent_id=r.message_intent_id
 ON CONFLICT DO NOTHING;
-- No payload, recipient, credential, token or full body is returned.
SELECT jsonb_build_object('version','CONTACT_FIXTURE_ABSENCE_SET_V1','database',current_database(),
 'restoration',(SELECT source_prefix_receipt->'restoration' FROM pg_temp.contact_start_fixture_input WHERE kind='PUSH'),
 'source_prefix_receipts',(SELECT jsonb_object_agg(kind,source_prefix_receipt) FROM pg_temp.contact_start_fixture_input),
 'objects',(SELECT jsonb_agg(to_jsonb(x) ORDER BY relation_name,column_name,object_id)
  FROM pg_temp.contact_start_fixture_absence_ids x)) AS fixture_absence_set;
