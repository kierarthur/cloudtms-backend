-- Synthetic first-use proof. All rows, jobs, links and receipts roll back.
-- Ordinary invoice/financial/Banking triggers deliberately remain enabled.
\set ON_ERROR_STOP on
begin;
set local statement_timeout='40s';
create function pg_temp.rota_removal_preserved_rows(p_candidate uuid,p_contract uuid,p_timesheet uuid,p_account uuid)
returns jsonb language sql as $$
  select jsonb_build_object(
    'candidate',(select to_jsonb(c)-array['key_norm','updated_at','rev'] from public.candidates c where id=p_candidate),
    'account',(select to_jsonb(a) from public.candidate_app_accounts a where id=p_account),
    'membership',(select jsonb_agg(to_jsonb(m) order by membership_id) from public.candidate_app_global_membership_links m where candidate_id=p_candidate),
    'contract',(select to_jsonb(c) from public.contracts c where id=p_contract),
    'timesheet',(select to_jsonb(t) from public.timesheets t where timesheet_id=p_timesheet),
    'financials',(select jsonb_agg(to_jsonb(f) order by timesheet_version) from public.timesheets_financials f where timesheet_id=p_timesheet),
    'evidence',(select jsonb_agg(to_jsonb(e) order by id) from public.timesheet_evidence e where timesheet_id=p_timesheet));
$$;
do $proof$
declare
  v_candidate uuid:=gen_random_uuid(); v_other uuid:=gen_random_uuid();
  v_account uuid:=gen_random_uuid(); v_client uuid:=gen_random_uuid();
  v_contract uuid:=gen_random_uuid(); v_timesheet uuid:=gen_random_uuid();
  v_code text:='CID1-'||upper(replace(gen_random_uuid()::text,'-',''));
  v_other_code text:='CID1-'||upper(replace(gen_random_uuid()::text,'-',''));
  v_source text:=encode(extensions.digest(gen_random_uuid()::text,'sha256'),'hex');
  v_other_source text:=encode(extensions.digest(gen_random_uuid()::text,'sha256'),'hex');
  v_operation uuid:=gen_random_uuid(); v_second_operation uuid:=gen_random_uuid();
  v_integration uuid:=gen_random_uuid(); v_batch uuid:=gen_random_uuid();
  v_today date:=(clock_timestamp() at time zone 'Europe/London')::date;
  v_corr text:='01K2ABCDEFGHJKMNPQRSTVWXYZ';
  v_system jsonb:=jsonb_build_object('policy','SIGNED_SYSTEM_SYNC','environment','TEST',
    'system_auth_verified',true,'nonce_consumed',true,'environment_trusted',true,
    'stable_operation_identity',true,'approved_source_mapping',true,'source_scope_ready',true,
    'authority_mode_compatible',true,'transition_ready',true);
  v_context jsonb; v_request jsonb; v_attach jsonb; v_item jsonb; v_other_item jsonb;
  v_days jsonb; v_result jsonb; v_old_rows jsonb; v_after_rows jsonb;
  v_generation uuid; v_link uuid; v_rev_generate bigint; v_rev_issue bigint; v_bank_seq bigint;
  v_removed timestamptz; v_created timestamptz; v_error text;
  v_command uuid:=gen_random_uuid(); v_effect uuid:=gen_random_uuid();
  v_outbox uuid:=gen_random_uuid(); v_state text; v_bad jsonb;
  v_unpublished uuid:=gen_random_uuid(); v_unpublished_source text:=encode(extensions.digest(gen_random_uuid()::text,'sha256'),'hex');
  v_unpublished_code text:='CID1-'||upper(replace(gen_random_uuid()::text,'-',''));
  v_unpublished_request jsonb; v_unpublished_context jsonb;
begin
  insert into public.candidates(id,email,display_name,first_name,last_name,phone,active,key_norm,pay_method)
    values(v_candidate,'rota-removal@example.invalid','Rota Removal Proof','Rota','Removal Proof',
      '07700900161',true,lower(v_code),'PAYE'),
    (v_other,'rota-other@example.invalid','Rota Other Proof','Rota','Other Proof','07700900162',true,v_other_code,'PAYE');
  insert into public.clients(id,name) values(v_client,'Rota removal proof client');
  insert into public.client_settings(id,client_id,effective_from,default_submission_mode)
    values(gen_random_uuid(),v_client,current_date-1,'ELECTRONIC');
  insert into public.contracts(id,candidate_id,client_id,start_date,end_date,week_ending_weekday_snapshot,default_submission_mode,pay_method_snapshot)
    values(v_contract,v_candidate,v_client,current_date-30,current_date+30,extract(dow from current_date)::integer,'ELECTRONIC','PAYE');
  insert into public.timesheets(timesheet_id,contract_id,booking_id,week_ending_date,line_type,sheet_scope,submission_mode,
    occupant_key_norm,hospital_norm,ward_norm,job_title_norm,r2_nurse_key,r2_auth_key)
    values(v_timesheet,v_contract,'ROTA-REMOVE-PROOF-'||v_timesheet::text,current_date,'HOURS','WEEKLY','ELECTRONIC',
      lower(v_code),'removal proof hospital','removal proof ward','rn','test/rota-removal/candidate','test/rota-removal/manager');
  insert into public.timesheets_financials(timesheet_id,timesheet_version,candidate_id,client_id,is_current,processing_status,total_hours)
    values(v_timesheet,1,v_candidate,v_client,true,'UNPROCESSED',8);
  insert into public.timesheet_evidence(timesheet_id,kind,display_name,storage_key)
    values(v_timesheet,'TIMESHEET','Retained proof evidence','test/rota-removal/'||v_timesheet::text);
  insert into public.candidate_app_accounts(id,environment,email_normalized,status)
    values(v_account,'TEST','rota-removal-'||v_account::text||'@example.invalid','ACTIVE');
  insert into public.candidate_app_global_membership_links(membership_id,global_account_identity_hmac,
    account_id,candidate_id,candidate_code,membership_generation,state)
    values(gen_random_uuid(),extensions.digest(v_account::text,'sha256'),v_account,v_candidate,v_code,1,'ACTIVE');

  select jsonb_agg(jsonb_build_object('date',(v_today+n)::text,'booked',false,
    'system_blocked',false,'source_row_hash',lpad(to_hex(n+1),64,'0')) order by n)
    into v_days from generate_series(0,13)n;
  v_item:=jsonb_build_object('candidate_global_key',v_code,'candidate_source_hmac',v_source,
    'source_hmac_key_version',1,'source_event_id','removal-first-'||v_batch::text,'source_revision','removal-proof-1',
    'source_hash',repeat('a',64),'window_start',v_today::text,'days',v_days,
    'source_event_time',clock_timestamp()::text,'item_key','removal-first-'||v_candidate::text);
  v_result:=public.candidate_daily_rota_generation_publish_atomic_v1(v_system,v_batch,
    'removal-first-'||v_batch::text,jsonb_build_array(v_item),v_corr);
  if v_result#>>'{outcomes,0,status}' is distinct from 'COMMITTED' then
    raise exception 'ROTA_REMOVE_PROOF: initial generation failed: %',v_result#>>'{outcomes,0,error_code}';
  end if;
  v_generation:=(v_result#>>'{outcomes,0,generation_id}')::uuid;
  select link_id into strict v_link from private.candidate_daily_source_links
    where environment='TEST' and candidate_id=v_candidate and identifier_hmac=v_source;
  insert into private.candidate_daily_entitlements(environment,candidate_id,enabled,reason,evidence_sha256)
    values('TEST',v_candidate,true,'Synthetic removal proof',repeat('a',64))
    on conflict(environment,candidate_id) do update set enabled=true;
  v_context:=jsonb_build_object('environment','TEST','route_context_verified',true,
    'audience','GOOGLE_ROTA_REMOVE','project_role','MASTER','integration_id',v_integration,'operation_id',v_operation);
  v_request:=jsonb_build_object('operation_id',v_operation,'candidate_code',lower(v_code),
    'candidate_source_hmac',v_source,'source_hmac_key_version',1,'surname','Removal Proof',
    'email','rota-removal@example.invalid','mobile','07700900161','row_fingerprint',repeat('d',64));
  v_old_rows:=pg_temp.rota_removal_preserved_rows(v_candidate,v_contract,v_timesheet,v_account);
  -- A lower-case CID is valid, but neither a wrong role nor a mismatched operation is.
  begin
    perform public.candidate_google_rota_remove_v1(v_context||'{"project_role":"AVAILABILITY"}',v_request,v_corr,clock_timestamp());
    raise exception 'ROTA_REMOVE_PROOF: wrong role accepted';
  exception when sqlstate '22023' then
    if sqlerrm<>'GOOGLE_ROTA_REMOVAL_INVALID' then raise; end if;
  end;
  begin
    perform public.candidate_google_rota_remove_v1(v_context,v_request||jsonb_build_object('operation_id',gen_random_uuid()),v_corr,clock_timestamp());
    raise exception 'ROTA_REMOVE_PROOF: wrong operation accepted';
  exception when sqlstate '22023' then
    if sqlerrm<>'GOOGLE_ROTA_REMOVAL_INVALID' then raise; end if;
  end;
  update private.candidate_daily_authority_scopes set transition_in_progress=true
    where environment='TEST' and candidate_id=v_candidate;
  v_result:=public.candidate_google_rota_remove_v1(v_context,v_request,v_corr,clock_timestamp());
  if v_result->>'state' is distinct from 'BUSY' then raise exception 'ROTA_REMOVE_PROOF: transition not guarded'; end if;
  update private.candidate_daily_authority_scopes set transition_in_progress=false
    where environment='TEST' and candidate_id=v_candidate;
  -- Every undelivered projection, including an existing lease, must settle
  -- through the normal drain before removal. These are local synthetic rows.
  insert into public.candidate_daily_command_receipts(command_id,environment,candidate_id,actor_class,
    command_class,idempotency_key,request_sha256,correlation_id)
    values(v_command,'TEST',v_candidate,'CANDIDATE','AVAILABILITY_SET',v_command::text,repeat('c',64),v_corr);
  insert into public.candidate_daily_sheet_projection_outbox(outbox_id,environment,candidate_id,
    availability_date,availability_version,preference,command_id,correlation_id)
    values(v_outbox,'TEST',v_candidate,v_today,1,'PENDING',v_command,v_corr);
  foreach v_state in array array['PENDING','RETRY','CLAIMED','DEFERRED_OVERLAY'] loop
    update public.candidate_daily_sheet_projection_outbox set state=v_state,
      lease_owner='synthetic-owner',lease_token='synthetic-lease',lease_expires_at_utc=clock_timestamp()+interval '1 minute',
      overlay_generation_id=v_generation,overlay_generation_version=1,overlay_source_row_hash=repeat('a',64)
      where outbox_id=v_outbox;
    v_result:=public.candidate_google_rota_remove_v1(v_context,v_request,v_corr,clock_timestamp());
    if v_result->>'state' is distinct from 'BUSY' then raise exception 'ROTA_REMOVE_PROOF: pending projection % not guarded',v_state; end if;
  end loop;
  update public.candidate_daily_sheet_projection_outbox set state='DELIVERED',completed_at_utc=clock_timestamp(),
    lease_owner=null,lease_token=null,lease_expires_at_utc=null where outbox_id=v_outbox;
  insert into private.candidate_daily_external_effect_receipts(effect_receipt_id,environment,candidate_id,
    effect_key,operation,request_hash,idempotency_key,first_claimed_at_utc,stable_provider_request_id,
    correlation_id,retain_until_utc)
    values(v_effect,'TEST',v_candidate,v_effect::text,'MESSAGE_SEEN',repeat('c',64),v_effect::text,
      clock_timestamp(),v_effect::text,v_corr,clock_timestamp()+interval '1 day');
  foreach v_state in array array['IN_PROGRESS','UNKNOWN'] loop
    if v_state='UNKNOWN' then
      update private.candidate_daily_external_effect_receipts set state='UNKNOWN',terminal_result_json='{}',
        terminal_body_sha256=repeat('d',64),completed_at_utc=clock_timestamp() where effect_receipt_id=v_effect;
    end if;
    v_result:=public.candidate_google_rota_remove_v1(v_context,v_request,v_corr,clock_timestamp());
    if v_result->>'state' is distinct from 'BUSY' then raise exception 'ROTA_REMOVE_PROOF: uncertain effect % not guarded',v_state; end if;
  end loop;
  update private.candidate_daily_external_effect_receipts set state='COMPLETED' where effect_receipt_id=v_effect;
  foreach v_bad in array array[v_request-'email',v_request||'{"candidate_code":null}'::jsonb,
    v_request||'{"candidate_source_hmac":null}'::jsonb,v_request||'{"source_hmac_key_version":null}'::jsonb,
    v_request||'{"row_fingerprint":null}'::jsonb,v_request||'{"unexpected":true}'::jsonb] loop
    begin
      perform public.candidate_google_rota_remove_v1(v_context,v_bad,v_corr,clock_timestamp());
      raise exception 'ROTA_REMOVE_PROOF: missing/null/unknown request value accepted';
    exception when sqlstate '22023' then
      if sqlerrm<>'GOOGLE_ROTA_REMOVAL_INVALID' then raise; end if;
    end;
  end loop;
  -- A current booked shift is never silently orphaned.
  update public.candidate_daily_rota_days set booked=true,booking_id='ROTA-REMOVAL-BOOKED',
    shift_starts_at=clock_timestamp(),shift_ends_at=clock_timestamp()+interval '1 hour'
    where generation_id=v_generation and rota_date=v_today;
  begin
    perform public.candidate_google_rota_remove_v1(v_context,v_request,v_corr,clock_timestamp());
    raise exception 'ROTA_REMOVE_PROOF: future booking accepted';
  exception when sqlstate '55000' then
    if sqlerrm<>'GOOGLE_ROTA_BOOKINGS_EXIST' then raise; end if;
  end;
  update public.candidate_daily_rota_days set booked=false,booking_id=null,shift_starts_at=null,shift_ends_at=null
    where generation_id=v_generation and rota_date=v_today;
  if exists(select 1 from private.candidate_google_rota_removal_receipts where candidate_id=v_candidate) then
    raise exception 'ROTA_REMOVE_PROOF: blocked request left a receipt';
  end if;

  insert into public.ts_financials_outbox(timesheet_id,reason,next_attempt_at)
    values(v_timesheet,'CONTEXT_CHANGED',clock_timestamp()+interval '1 day')
    on conflict(timesheet_id,reason) do update set next_attempt_at=excluded.next_attempt_at;
  select pg_sequence_last_value('private.invoice_generate_candidate_change_seq'::regclass),
    pg_sequence_last_value('private.invoice_issue_candidate_change_seq'::regclass) into v_rev_generate,v_rev_issue;
  select seq into v_bank_seq from public.app_change_counters where entity_key='pay_candidate:'||v_candidate::text;
  v_result:=public.candidate_google_rota_remove_v1(v_context,v_request,v_corr,clock_timestamp());
  if v_result->>'state' is distinct from 'REMOVED' or v_result->>'idempotent_replay'<>'false' then
    raise exception 'ROTA_REMOVE_PROOF: linked removal failed: %',v_result->>'state';
  end if;
  if exists(select 1 from public.candidates where id=v_candidate and key_norm is not null)
    or exists(select 1 from private.candidate_daily_entitlements where candidate_id=v_candidate and enabled)
    or (select state from private.candidate_daily_source_links where link_id=v_link)<>'RETIRED'
    or (select state from public.candidate_daily_rota_generations where generation_id=v_generation)<>'SUPERSEDED'
    or (select count(*) from public.candidate_daily_rota_days where generation_id=v_generation)<>14
    or exists(select 1 from private.candidate_daily_authority_scopes where candidate_id=v_candidate and active_generation_id is not null) then
    raise exception 'ROTA_REMOVE_PROOF: access was retained or history was erased';
  end if;
  if v_old_rows is distinct from pg_temp.rota_removal_preserved_rows(v_candidate,v_contract,v_timesheet,v_account) then
    raise exception 'ROTA_REMOVE_PROOF: non-Rota source rows changed';
  end if;
  if not exists(select 1 from public.ts_financials_outbox where timesheet_id=v_timesheet and reason='CONTEXT_CHANGED' and next_attempt_at is null)
    or pg_sequence_last_value('private.invoice_generate_candidate_change_seq'::regclass)<=v_rev_generate
    or pg_sequence_last_value('private.invoice_issue_candidate_change_seq'::regclass)<=v_rev_issue
    or (select seq from public.app_change_counters where entity_key='pay_candidate:'||v_candidate::text)<=v_bank_seq
    or not exists(select 1 from public.banking_pay_workbench_jobs where candidate_id=v_candidate
      and job_type='WORKBENCH_CANDIDATE_DIRTY_APPLY' and status='QUEUED') then
    raise exception 'ROTA_REMOVE_PROOF: ordinary derived rechecks suppressed';
  end if;
  select removed_at_utc into strict v_removed from private.candidate_google_rota_removal_receipts
    where environment='TEST' and integration_id=v_integration and operation_id=v_operation;
  v_result:=public.candidate_google_rota_remove_v1(v_context,v_request,v_corr,clock_timestamp());
  if v_result->>'idempotent_replay'<>'true' then raise exception 'ROTA_REMOVE_PROOF: exact retry not idempotent'; end if;
  begin
    perform public.candidate_google_rota_remove_v1(v_context,v_request||jsonb_build_object('row_fingerprint',repeat('e',64)),v_corr,clock_timestamp());
    raise exception 'ROTA_REMOVE_PROOF: changed payload accepted';
  exception when unique_violation then
    if sqlerrm<>'IDEMPOTENCY_KEY_REUSED' then raise; end if;
  end;

  -- Other Candidates must continue through a batch containing a removed Candidate.
  v_other_item:=v_item||jsonb_build_object('candidate_global_key',v_other_code,'candidate_source_hmac',v_other_source,
    'source_event_id','removal-other-'||v_other::text,'item_key','removal-other-'||v_other::text);
  v_result:=public.candidate_daily_rota_generation_publish_atomic_v1(v_system,gen_random_uuid(),
    'removal-mixed-'||v_operation::text,jsonb_build_array(v_item,v_other_item),v_corr);
  if v_result#>>'{outcomes,0,error_code}' is distinct from 'SOURCE_EVENT_CONFLICT'
    or v_result#>>'{outcomes,1,status}' is distinct from 'COMMITTED' then
    raise exception 'ROTA_REMOVE_PROOF: stale publication or mixed-batch failure: %',v_result;
  end if;
  v_result:=public.candidate_daily_rota_generation_publish_atomic_v1(v_system,v_batch,
    'removal-first-'||v_batch::text,jsonb_build_array(v_item),v_corr);
  if v_result->>'_idempotent_replay'<>'true'
    or exists(select 1 from private.candidate_daily_authority_scopes where candidate_id=v_candidate and active_generation_id is not null) then
    raise exception 'ROTA_REMOVE_PROOF: historical replay republished';
  end if;

  v_attach:=jsonb_build_object('environment','TEST','route_context_verified',true,'audience','GOOGLE_PROVISIONING_ATTACH',
    'project_role','MASTER','operation_id',gen_random_uuid(),'operation_created_at_utc',v_removed-interval '1 second');
  begin
    perform public.candidate_google_provisioning_attach_v1(v_attach,v_candidate,v_code,'Removal Proof',
      'rota-removal@example.invalid','07700900161',v_source,1,v_corr,clock_timestamp());
    raise exception 'ROTA_REMOVE_PROOF: old Create authority re-enrolled';
  exception when serialization_failure then
    if sqlerrm<>'GOOGLE_PROVISIONING_REENROLMENT_REQUIRED' then raise; end if;
  end;
  -- A second explicit remove while already unlinked is also retained; only the
  -- latest removal boundary controls the next fresh enrolment.
  v_result:=public.candidate_google_rota_remove_v1(v_context||jsonb_build_object('operation_id',v_second_operation),
    v_request||jsonb_build_object('operation_id',v_second_operation),v_corr,clock_timestamp());
  if v_result->>'state' is distinct from 'UNLINKED' then raise exception 'ROTA_REMOVE_PROOF: already unlinked result'; end if;
  v_created:=clock_timestamp();
  v_attach:=v_attach||jsonb_build_object('operation_id',gen_random_uuid(),'operation_created_at_utc',v_created);
  v_result:=public.candidate_google_provisioning_attach_v1(v_attach,v_candidate,lower(v_code),'Removal Proof',
    'rota-removal@example.invalid','07700900161',v_source,1,v_corr,clock_timestamp());
  if v_result->>'state' is distinct from 'ATTACHED'
    or (select state from private.candidate_daily_source_links where link_id=v_link)<>'PRIMARY'
    or exists(select 1 from private.candidate_daily_entitlements where candidate_id=v_candidate and enabled) then
    raise exception 'ROTA_REMOVE_PROOF: exact fresh re-enrolment failed or enabled without a complete window';
  end if;
  v_result:=public.candidate_google_rota_remove_v1(v_context,v_request,v_corr,clock_timestamp());
  if v_result->>'idempotent_replay'<>'true'
    or (select key_norm from public.candidates where id=v_candidate) is distinct from v_code then
    raise exception 'ROTA_REMOVE_PROOF: old removal retry removed re-enrolled Candidate';
  end if;
  v_result:=public.candidate_daily_rota_generation_publish_atomic_v1(v_system,gen_random_uuid(),
    'removal-stale-after-'||v_operation::text,jsonb_build_array(v_item),v_corr);
  if v_result#>>'{outcomes,0,error_code}' is distinct from 'SOURCE_EVENT_CONFLICT' then
    raise exception 'ROTA_REMOVE_PROOF: pre-removal snapshot accepted after re-enrolment';
  end if;
  v_item:=v_item||jsonb_build_object('source_event_id','removal-fresh-'||v_operation::text,
    'item_key','removal-fresh-'||v_operation::text,'source_hash',repeat('b',64),'source_event_time',clock_timestamp()::text);
  v_result:=public.candidate_daily_rota_generation_publish_atomic_v1(v_system,gen_random_uuid(),
    'removal-fresh-'||v_operation::text,jsonb_build_array(v_item),v_corr);
  if v_result#>>'{outcomes,0,status}' is distinct from 'COMMITTED' then
    raise exception 'ROTA_REMOVE_PROOF: fresh complete publication rejected: %',v_result;
  end if;
  if v_old_rows is distinct from pg_temp.rota_removal_preserved_rows(v_candidate,v_contract,v_timesheet,v_account)
    or (select count(*) from public.candidate_daily_rota_generations where candidate_id=v_candidate)<>2
    or (select count(*) from private.candidate_daily_source_links where candidate_id=v_candidate)<>1 then
    raise exception 'ROTA_REMOVE_PROOF: re-enrolment damaged retained history or non-Rota rows';
  end if;
  -- Before first publication, a matching CID without the exact three identity
  -- fields is not sufficient authority. Exact un-published removal is supported.
  insert into public.candidates(id,email,display_name,first_name,last_name,phone,active,key_norm,pay_method)
    values(v_unpublished,'unpublished@example.invalid','Unpublished Proof','Unpublished','Proof','07700900163',true,v_unpublished_code,'PAYE');
  v_unpublished_request:=v_request||jsonb_build_object('operation_id',gen_random_uuid(),
    'candidate_code',v_unpublished_code,'candidate_source_hmac',v_unpublished_source,
    'surname','Proof','email','unpublished@example.invalid','mobile','07700900163');
  v_unpublished_context:=v_context||jsonb_build_object('operation_id',v_unpublished_request->>'operation_id');
  foreach v_bad in array array[v_unpublished_request||'{"email":"wrong@example.invalid"}'::jsonb,
    v_unpublished_request||'{"surname":null}'::jsonb,v_unpublished_request||'{"mobile":null}'::jsonb] loop
    begin
      perform public.candidate_google_rota_remove_v1(v_unpublished_context,v_bad,v_corr,clock_timestamp());
      raise exception 'ROTA_REMOVE_PROOF: un-published identity mismatch accepted';
    exception when serialization_failure then
      if sqlerrm<>'GOOGLE_ROTA_IDENTITY_CHANGED' then raise; end if;
    end;
  end loop;
  v_result:=public.candidate_google_rota_remove_v1(v_unpublished_context,v_unpublished_request,v_corr,clock_timestamp());
  if v_result->>'state' is distinct from 'REMOVED'
    or not exists(select 1 from public.candidates where id=v_unpublished and key_norm is null) then
    raise exception 'ROTA_REMOVE_PROOF: exact un-published identity removal failed';
  end if;
  if has_function_privilege('anon','public.candidate_google_rota_remove_v1(jsonb,jsonb,text,timestamptz)','EXECUTE')
    or has_function_privilege('authenticated','public.candidate_google_rota_remove_v1(jsonb,jsonb,text,timestamptz)','EXECUTE')
    or not has_function_privilege('service_role','public.candidate_google_rota_remove_v1(jsonb,jsonb,text,timestamptz)','EXECUTE')
    or has_table_privilege('service_role','private.candidate_google_rota_removal_receipts','SELECT,INSERT,UPDATE,DELETE') then
    raise exception 'ROTA_REMOVE_PROOF: service-only RPC or private receipt boundary';
  end if;
  raise notice 'ROTA_REMOVE_PROOF: PASS removal, preserved account/membership/contracts/hours/expenses/evidence/history, ordinary derived rechecks, role/operation/booking/transition/pending projection/uncertain effect guards, missing/null input, first-publication identity, replay, changed payload, mixed batch, fresh exact re-enrolment and stale snapshot rejection';
end;
$proof$;
rollback;
