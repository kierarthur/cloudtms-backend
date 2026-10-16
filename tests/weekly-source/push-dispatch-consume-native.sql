-- Constructor/use fixture for saved-target advisory and PUSH consume/result.
-- It explicitly constructs an already-admitted native attempt and permit.
-- It does NOT qualify the unfinished original START or producer admission.
-- All timestamps below are genuine server time for disposable fixture rows.
do $fixture$
declare v_base constant text:='f7160000-0000-4000-8000-';v_now timestamptz:=clock_timestamp();
 v_read jsonb;v_consume jsonb;v_result jsonb;v_replay jsonb;v_bad jsonb;v_key text;
 v_role text;v_constraint text;v_hash bytea:=decode(repeat('41',32),'hex');
 v_snapshot bytea:=decode(repeat('42',32),'hex');v_recipient bytea:=decode(repeat('43',32),'hex');
 v_safe jsonb;v_policy public.candidate_push_timing_policy%rowtype;
begin
 if current_database()<>'cloudtms_test_clone' or exists(select 1 from public.weekly_source_groups
  where id=(v_base||'000000000300')::uuid or code='HR_QT_DISPATCH_ROLLBACK') then
  raise exception 'DISPATCH_FIXTURE_IDENTITY_OR_VACANCY_REFUSED';end if;
 v_role:=current_setting('request.jwt.claim.role',true);
 perform set_config('request.jwt.claim.role','service_role',true);
 select * into strict v_policy from public.candidate_push_timing_policy
  where agency_id=(v_base||'000000000019')::uuid and environment='TEST';
 insert into public.weekly_source_groups(id,environment,agency_id,code,display_name,source_family,cutoff_weekday,cutoff_local_time)
 values((v_base||'000000000300')::uuid,'TEST',v_policy.agency_id,'HR_QT_DISPATCH_ROLLBACK',
  'Disposable dispatch constructor','ROSTER',3,'15:00');
 insert into public.weekly_source_cycles(id,source_group_id,finalisation_week_ending,cutoff_at_utc)
 values((v_base||'000000000301')::uuid,(v_base||'000000000300')::uuid,'2026-09-06',v_now+interval '1 day');
 insert into public.weekly_route_activations(id,source_cycle_id,candidate_id,client_id,audience_route,route_mode,activated_at_utc)
 values((v_base||'000000000302')::uuid,(v_base||'000000000301')::uuid,(v_base||'000000000010')::uuid,
  (v_base||'000000000011')::uuid,'CANDIDATE','CANDIDATE_FIRST',v_now);
 insert into public.weekly_candidate_cohorts(id,source_cycle_id,candidate_id,client_id,manager_recipient_route_key)
 values((v_base||'000000000303')::uuid,(v_base||'000000000301')::uuid,(v_base||'000000000010')::uuid,
  (v_base||'000000000011')::uuid,v_recipient);
 insert into public.weekly_candidate_outreach_generations(id,source_cycle_id,candidate_cohort_id,candidate_id,client_id,
  generation_number,activation_id,trigger_kind,request_kind,route_mode,started_at_utc,reminder_due_at_utc,deadline_at_utc,state,membership_hash)
 values((v_base||'000000000304')::uuid,(v_base||'000000000301')::uuid,(v_base||'000000000303')::uuid,
  (v_base||'000000000010')::uuid,(v_base||'000000000011')::uuid,1,(v_base||'000000000302')::uuid,
  'OFFICE_ASK','CHECK_HOURS','CANDIDATE_FIRST',v_now,v_now+interval '6 hours',v_now+interval '12 hours','ACTIVE',v_hash);
 update public.weekly_candidate_cohorts set current_generation_id=(v_base||'000000000304')::uuid
 where id=(v_base||'000000000303')::uuid;
 insert into public.candidate_app_accounts(id,environment,email_normalized,status,notification_preferences_json)
 values((v_base||'000000000305')::uuid,'TEST','healthroster-qt-dispatch@example.invalid','ACTIVE',
  '{"push":true,"timesheet_expense_attention":true}');
 insert into public.candidate_app_global_membership_links(membership_id,global_account_identity_hmac,account_id,candidate_id,membership_generation,state)
 values((v_base||'000000000306')::uuid,v_recipient,(v_base||'000000000305')::uuid,(v_base||'000000000010')::uuid,1,'ACTIVE');
 insert into public.weekly_message_intents(id,environment,agency_id,source_cycle_id,audience_kind,candidate_cohort_id,
  candidate_generation_id,tranche_kind,tranche_sequence,logical_key,state,due_at_utc)
 values((v_base||'000000000307')::uuid,'TEST',v_policy.agency_id,(v_base||'000000000301')::uuid,'CANDIDATE',
  (v_base||'000000000303')::uuid,(v_base||'000000000304')::uuid,'CANDIDATE_INITIAL',1,v_hash,'RENDERED',v_now);
 insert into public.weekly_message_renders(id,message_intent_id,render_sequence,membership_hash,policy_version,
  renderer_version,plain_body,rendered_content_hash,provider_idempotency_key,state)
 values((v_base||'000000000308')::uuid,(v_base||'000000000307')::uuid,1,v_hash,'fixture','fixture',
  'Disposable constructor; never submitted',v_hash,'HR_QT_DISPATCH_RENDER_ROLLBACK','CURRENT');
 insert into public.weekly_message_dispatch_commands(id,message_intent_id,message_render_id,environment,agency_id,source_cycle_id,
  candidate_generation_id,tranche_kind,tranche_sequence,sorted_trigger_event_hash,membership_hash,policy_version,logical_key,state,
  rendered_content_hash,control_plane_snapshot_id)
 values((v_base||'000000000309')::uuid,(v_base||'000000000307')::uuid,(v_base||'000000000308')::uuid,
  'TEST',v_policy.agency_id,(v_base||'000000000301')::uuid,(v_base||'000000000304')::uuid,
  'CANDIDATE_INITIAL',1,v_hash,v_hash,'fixture',v_hash,'READY',v_hash,(v_base||'000000000314')::uuid);
 v_safe:=jsonb_build_object('control_plane_snapshot_id',v_base||'000000000314','snapshot_device_id',v_base||'000000000313',
  'provider','APNS','target_revision_hash',encode(v_snapshot,'hex'));
 insert into public.weekly_message_dispatch_targets(id,dispatch_command_id,target_ordinal,channel,target_kind,provider,external_target_id,
  keyed_target_fingerprint,target_snapshot_hash,target_version,safe_target_snapshot_json,rendered_content_hash,provider_idempotency_key,
  state,lease_owner,lease_token,lease_expires_at_utc,attempt_count)
 values((v_base||'000000000310')::uuid,(v_base||'000000000309')::uuid,1,'PUSH','CANDIDATE_DEVICE','APNS',
  (v_base||'000000000313')::uuid,v_recipient,v_snapshot,1,v_safe,v_hash,'HR_QT_DISPATCH_TARGET_ROLLBACK',
  'SUBMISSION_STARTED','qt-native-fixture',(v_base||'000000000315')::uuid,v_now+interval '2 minutes',1);
 insert into public.weekly_message_target_attempts(id,dispatch_target_id,attempt_number,provider_idempotency_key,
  lease_owner,lease_token,submission_started_at_utc)
 values((v_base||'000000000311')::uuid,(v_base||'000000000310')::uuid,1,'HR_QT_DISPATCH_TARGET_ROLLBACK/attempt-1',
  'qt-native-fixture',(v_base||'000000000315')::uuid,v_now);
 insert into public.weekly_contact_submission_permits(id,owner_kind,logical_delivery_key,environment,agency_id,
  dispatch_target_id,dispatch_command_id,provider_attempt_id,channel,provider,content_hash,recipient_hash,scope_revisions_json,
  target_snapshot_hash,safe_target_snapshot_json,policy_revision,policy_sha256,calendar_digest,lease_owner,lease_token,
  lease_expires_at_utc,status,admitted_at_utc,consume_before_utc,provider_idempotency_identity)
 values((v_base||'000000000312')::uuid,'WEEKLY_SOURCE_TARGET',v_hash,'TEST',v_policy.agency_id,
  (v_base||'000000000310')::uuid,(v_base||'000000000309')::uuid,(v_base||'000000000311')::uuid,'PUSH','APNS',
  v_hash,v_recipient,'[]',v_snapshot,v_safe,v_policy.revision,v_policy.policy_sha256,v_hash,'qt-native-fixture',
  v_base||'000000000315',v_now+interval '2 minutes','STARTED',v_now,v_now+interval '30 seconds',
  'HR_QT_DISPATCH_TARGET_ROLLBACK/attempt-1');
 v_read:=jsonb_build_object('version','PUSH_TIMING_DISPATCH_READ_V1','dispatch_target_id',v_base||'000000000310',
  'dispatch_command_id',v_base||'000000000309','provider','APNS','target_snapshot_hash',encode(v_snapshot,'hex'),
  'agency_id',v_policy.agency_id,'environment','TEST');
 -- An already-started fixture with no actionable membership is obsolete;
 -- neither this advisory read nor a NULL currentness can authorise a send.
 v_result:=public.candidate_push_timing_for_dispatch_v1(v_read);
 if v_result->'allowed_now' is distinct from 'false'::jsonb or v_result->>'reason' is distinct from 'OBSOLETE'
  or v_result->>'policy_revision' is distinct from v_policy.revision::text then raise exception 'ADVISORY_OBSOLETE_FAILED';end if;
 v_consume:=jsonb_build_object('permit_id',v_base||'000000000312','provider_attempt_id',v_base||'000000000311',
  'dispatch_target_id',v_base||'000000000310','dispatch_command_id',v_base||'000000000309','provider','APNS',
  'target_snapshot_hash',encode(v_snapshot,'hex'),'content_hash',encode(v_hash,'hex'),
  'agency_id',v_policy.agency_id,'environment','TEST');
 foreach v_key in array array['permit_id','provider_attempt_id','dispatch_target_id','dispatch_command_id','agency_id'] loop
  v_bad:=jsonb_set(v_consume,array[v_key],to_jsonb(v_base||'000000000399'));
  begin perform public.weekly_contact_submission_consume_v1(v_bad);raise exception 'FOREIGN_CONSUME_ALLOWED';
  exception when sqlstate '42501' or sqlstate 'P0001' then
   if SQLERRM not in('CONTACT_PERMIT_INVALID','PUSH_TIMING_DISPATCH_SCOPE_INVALID') then raise;end if;end;
 end loop;
 foreach v_key in array array['target_snapshot_hash','content_hash'] loop
  v_bad:=jsonb_set(v_consume,array[v_key],to_jsonb(repeat('f',64)));
  begin perform public.weekly_contact_submission_consume_v1(v_bad);raise exception 'ALTERED_HASH_ALLOWED';
  exception when sqlstate '42501' then if SQLERRM<>'PUSH_TIMING_DISPATCH_SCOPE_INVALID' then raise;end if;end;
 end loop;
 begin perform public.weekly_contact_submission_consume_v1(v_consume||'{"allow":true}');raise exception 'EXTRA_CONSUME_ALLOWED';
 exception when sqlstate '22023' then if SQLERRM<>'PUSH_TIMING_REQUEST_INVALID' then raise;end if;end;
 begin
  insert into public.weekly_contact_submission_permits
  select (jsonb_populate_record(null::public.weekly_contact_submission_permits,
   to_jsonb(p)||jsonb_build_object('id',v_base||'000000000316','logical_delivery_key',decode(repeat('44',32),'hex')))).*
  from public.weekly_contact_submission_permits p where id=(v_base||'000000000312')::uuid;
  raise exception 'DUPLICATE_NATIVE_ATTEMPT_PERMIT_ALLOWED';
 exception when unique_violation then
  get stacked diagnostics v_constraint=CONSTRAINT_NAME;
  if v_constraint<>'weekly_contact_submission_permits_native_attempt_uq' then raise;end if;
 end;
 begin
  perform private.weekly_contact_source_result_v1((v_base||'000000000311')::uuid,'ACCEPTED',null,'{}','{}');
  raise exception 'UNCONSUMED_PUSH_ACCEPTED';
 exception when sqlstate 'P0001' then if SQLERRM<>'CONTACT_PERMIT_INVALID' then raise;end if;end;
 -- A discarded subtransaction supplies an expired constructor time; no saved
 -- business timestamp is changed and no receipt claims real elapsed waiting.
 begin
  update public.weekly_contact_submission_permits set admitted_at_utc=v_now-interval '31 seconds',
   consume_before_utc=v_now-interval '1 second' where id=(v_base||'000000000312')::uuid;
  perform public.weekly_contact_submission_consume_v1(v_consume);
  raise exception 'EXPIRED_UNCONSUMED_PERMIT_ALLOWED';
 exception when sqlstate 'P0001' then if SQLERRM<>'CONTACT_PERMIT_EXPIRED' then raise;end if;end;
 if not exists(select 1 from public.weekly_contact_submission_permits where id=(v_base||'000000000312')::uuid
  and consumed_at_utc is null and admitted_at_utc=v_now and status='STARTED') then
  raise exception 'NEGATIVE_CONSUME_CHANGED_ORIGINAL_PERMIT';end if;
 -- Exact first use, repeated consumption and post-result replay all share one
 -- actual native attempt; no provider/network routine is invoked by fixture.
 v_result:=public.weekly_contact_submission_consume_v1(v_consume);
 if v_result->'call_once' is distinct from 'true'::jsonb or v_result->>'version'<>'CONTACT_CONSUMED_V1'
  or v_result->>'provider_attempt_id' is distinct from v_base||'000000000311' then raise exception 'FIRST_CONSUME_FAILED';end if;
 v_replay:=public.weekly_contact_submission_consume_v1(v_consume);
 if v_replay is distinct from jsonb_build_object('ok',false,'reason','ALREADY_CONSUMED',
  'provider_attempt_id',v_base||'000000000311','call_once',false) then raise exception 'CONSUME_REPLAY_FAILED';end if;
 if (select count(*) from public.weekly_contact_submission_permits where provider_attempt_id=(v_base||'000000000311')::uuid)<>1
  then raise exception 'ATTEMPT_PERMIT_NOT_ONCE';end if;
 -- Exercise the permit metadata bridge directly inside the same constructor;
 -- full native result/accepted logical budget integration is a later union.
 perform private.weekly_contact_source_result_v1((v_base||'000000000311')::uuid,'AMBIGUOUS',null,
  '{}'::jsonb,'{"error_code":"TEST_OUTCOME_UNKNOWN"}'::jsonb);
 if not exists(select 1 from public.weekly_contact_submission_permits where id=(v_base||'000000000312')::uuid
  and status='AMBIGUOUS' and consumed_at_utc is not null and not proved_no_network) then
  raise exception 'ORIGINAL_UNCERTAINTY_NOT_RETAINED';end if;
 v_replay:=public.weekly_contact_submission_consume_v1(v_consume);
 if v_replay->'call_once' is distinct from 'false'::jsonb then raise exception 'AMBIGUOUS_REPLAY_ALLOWED';end if;
 perform set_config('request.jwt.claim.role','authenticated',true);
 begin perform public.weekly_contact_submission_consume_v1(v_consume);raise exception 'BROWSER_CONSUME_ALLOWED';
 exception when sqlstate '42501' then null;end;
 perform set_config('request.jwt.claim.role',coalesce(v_role,''),true);
 raise notice 'PUSH_DISPATCH_CONSUME_CONSTRUCTOR_PASS; ORIGINAL_START_AND_NATIVE_RESULT_UNION_NOT_RUN';
end;
$fixture$;
