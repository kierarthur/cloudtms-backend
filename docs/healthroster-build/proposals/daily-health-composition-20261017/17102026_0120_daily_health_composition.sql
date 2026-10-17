-- Health-only internal composition proposal. No public success composer or
-- Banking return ABI is declared here. Root owns native mounts and contracts.
create or replace function private.daily_rate_correction_health_reason_note_v1(p_stored_reason text)
returns text language plpgsql immutable security definer
set search_path=pg_catalog,private,pg_temp
as $function$
declare prefix text:='Applying updated rates'; note text;
begin
 if p_stored_reason=prefix then note:='';
 elsif left(p_stored_reason,char_length(prefix)+3)=prefix||' — ' then
  note:=substr(p_stored_reason,char_length(prefix)+4);
 else raise exception 'DAILY_RATE_STORED_REASON_INVALID' using errcode='23514';end if;
 -- The stored reason is canonical, not a raw Details note. Recover exactly
 -- one prefix then prove the actual native Unicode normalizer round-trips it.
 -- This admits the full1969-character note even when the stored reason is longer.
 if private.timesheet_contract_move_reason_v1(note,'DAILY_RATE_CORRECTION') is distinct from p_stored_reason then
  raise exception 'DAILY_RATE_STORED_REASON_INVALID' using errcode='23514';end if;
 return note;
exception when sqlstate '22023' then
 raise exception 'DAILY_RATE_STORED_REASON_INVALID' using errcode='23514';
end;
$function$;

create or replace function private.daily_rate_correction_health_actor_require_v1(p_actor_user_id uuid,p_lock boolean)
returns void language plpgsql volatile security definer
set search_path=pg_catalog,public,private,pg_temp
as $function$
declare actor public.tms_users%rowtype;
begin
 if p_actor_user_id is null or p_lock is null then raise exception 'DAILY_RATE_ACTOR_NOT_ALLOWED' using errcode='42501';end if;
 if p_lock then select u.* into actor from public.tms_users u where u.id=p_actor_user_id for share;
 else select u.* into actor from public.tms_users u where u.id=p_actor_user_id;end if;
 if actor.id is null or actor.is_active is distinct from true or lower(btrim(actor.role)) is distinct from 'admin'
  or private.weekly_source_pay_actor_allowed_v1(p_actor_user_id) is distinct from true then
  raise exception 'DAILY_RATE_ACTOR_NOT_ALLOWED' using errcode='42501';end if;
end;
$function$;

create or replace function private.daily_rate_correction_health_basis_v1(p_args jsonb,p_lock boolean)
returns jsonb language plpgsql volatile security definer
set search_path=pg_catalog,public,private,pg_temp
as $function$
declare context jsonb; snapshot jsonb; normalized jsonb; actor uuid;
begin
 perform private.daily_rate_correction_request_assert_v1(p_args,'PREVIEW');
 if p_lock is null then raise exception 'DAILY_RATE_LOCK_MODE_REQUIRED' using errcode='22023';end if;
 -- The native rate writer mutex serialises writers but cannot refresh an
 -- already-established RR/serializable snapshot. No generic rehearsal waiver.
 if p_lock and current_setting('transaction_isolation') is distinct from 'read committed' then
  raise exception 'DAILY_RATE_LOCKED_CONTEXT_ISOLATION_REQUIRED' using errcode='55000';end if;
 actor:=(p_args->>'actor_user_id')::uuid;
 perform private.daily_rate_correction_health_actor_require_v1(actor,false);
 context:=private.daily_rate_correction_context_v1((p_args->>'timesheet_id')::uuid,actor,p_lock);
 -- Recheck and lock the real actor AFTER all context/rate waits. A prior
 -- session or pre-wait boolean is not approval after revocation.
 perform private.daily_rate_correction_health_actor_require_v1(actor,p_lock);
 if context->>'context_digest' is distinct from p_args->>'context_digest' then
  raise exception 'REVIEW_STALE' using errcode='40001';end if;
 snapshot:=private.daily_rate_correction_snapshot_assert_v1(context,p_args->'calculation_snapshot',p_args->'rate_provenance',actor);
 normalized:=private.daily_rate_correction_normalized_snapshot_v1(context,snapshot,p_args->'rate_provenance',actor);
 -- This is exactly the existing0955 prospective Daily input, not a Bank DTO.
 return jsonb_build_object('context',context,'calculation_snapshot',snapshot,
  'normalized_snapshot',normalized,'rate_provenance',p_args->'rate_provenance');
end;
$function$;

create or replace function private.daily_rate_correction_health_review_prepare_v1(p_args jsonb)
returns jsonb language plpgsql volatile security definer
set search_path=pg_catalog,public,private,pg_temp
as $function$
declare daily jsonb; physical jsonb; invoice jsonb; member jsonb; operation uuid; actor uuid; ts uuid; reason text;
begin
 daily:=private.daily_rate_correction_health_basis_v1(p_args,false);
 actor:=(p_args->>'actor_user_id')::uuid;ts:=(p_args->>'timesheet_id')::uuid;
 reason:=private.timesheet_contract_move_reason_v1(p_args->>'reason','DAILY_RATE_CORRECTION');
 operation:=substr(encode(private.timesheet_contract_move_row_hash_v1(jsonb_build_array(
  'DAILY_RATE_HEALTH_REVIEW_OPERATION_V1',p_args->>'request_id',actor,ts)),'hex'),1,32)::uuid;
 -- The final public composer must call native preview_replay first. This
 -- initializer neither overwrites an operation nor returns a partial replay.
 if exists(select 1 from private.timesheet_contract_move_requests q where q.request_id=(p_args->>'request_id')::uuid)
  or exists(select 1 from private.timesheet_contract_move_operations o where o.id=operation) then
  raise exception 'DAILY_RATE_REQUEST_CONFLICT' using errcode='22023';end if;
 insert into private.timesheet_contract_move_operations(id,purpose,state,state_revision,family_root_id,from_timesheet_id,
  from_contract_id,to_contract_id,candidate_id,actual_client_id,invoice_authority,invoice_move_policy,
  financial_state,reason,created_by_user_id,created_at_utc,unchanged_work_sha256)
 values(operation,'DAILY_RATE_CORRECTION','REVIEWED',1,ts,ts,null,null,
  (daily#>>'{context,identity,candidate_id}')::uuid,(daily#>>'{context,identity,client_id}')::uuid,
  'UNRESOLVED','REVIEW_REQUIRED','REVIEW_REQUIRED',reason,actor,now(),decode(daily#>>'{context,unchanged_work_sha256}','hex'));
 physical:=private.daily_rate_correction_scope_v1(operation,false);
 if jsonb_array_length(physical->'members')<>1 then raise exception 'DAILY_RATE_NATIVE_IDENTITY_UNBOUND' using errcode='23514';end if;
 member:=physical#>'{members,0}';
 insert into private.timesheet_contract_move_members(operation_id,family_root_id,member_key,role,from_timesheet_id,
  from_version,from_financial_id,from_contract_id,to_contract_id,original_booking_id,expense_component_ids,workflow_ids,
  source_lineage_ids,immutable_evidence_hash,approval_evidence_hash,member_hash)
 values(operation,ts,member->>'member_key','WORK',ts,(member->>'from_version')::integer,
  (member->>'from_financial_id')::uuid,null,null,member->>'original_booking_id',
  array(select x::uuid from jsonb_array_elements_text(member->'expense_component_ids') x),
  array(select x::uuid from jsonb_array_elements_text(member->'workflow_ids') x),
  array(select x::uuid from jsonb_array_elements_text(member->'source_lineage_ids') x),
  decode(member->>'immutable_evidence_hash','hex'),decode(member->>'approval_evidence_hash','hex'),
  private.timesheet_contract_move_row_hash_v1(member));
 invoice:=private.invoice_contract_move_inspect_v1(physical,daily,actor,false);
 perform private.daily_rate_correction_health_actor_require_v1(actor,false);
 -- Existing deferred timesheet_contract_move_commit_guard forbids committing
 -- these new metadata rows without the genuine immutable review and all three
 -- scope/pay-cut/invoice hashes. Root must compose the real Bank join in this
 -- SAME transaction; it may not persist or publish this partial basis.
 return jsonb_build_object('version','DAILY_RATE_HEALTH_REVIEW_BASIS_V1','operation_id',operation,
  'daily',daily,'physical',physical,'invoice',invoice);
end;
$function$;

create or replace function private.daily_rate_correction_health_accept_basis_v1(p_args jsonb)
returns jsonb language plpgsql volatile security definer
set search_path=pg_catalog,public,private,pg_temp
as $function$
declare o private.timesheet_contract_move_operations%rowtype; locked_o private.timesheet_contract_move_operations%rowtype;
 review private.timesheet_contract_move_reviews%rowtype; locked_review private.timesheet_contract_move_reviews%rowtype;
 daily jsonb; physical jsonb; invoice_preflight jsonb; actor uuid; preview_args jsonb;
begin
 perform private.daily_rate_correction_request_assert_v1(p_args,'ACCEPT');actor:=(p_args->>'actor_user_id')::uuid;
 if current_setting('transaction_isolation') is distinct from 'read committed' then
  raise exception 'DAILY_RATE_LOCKED_CONTEXT_ISOLATION_REQUIRED' using errcode='55000';end if;
 perform private.daily_rate_correction_health_actor_require_v1(actor,false);
 select x.* into o from private.timesheet_contract_move_operations x where x.id=(p_args->>'operation_id')::uuid;
 select r.* into review from private.timesheet_contract_move_reviews r where r.id=(p_args->>'review_id')::uuid and r.operation_id=o.id;
 if o.id is null or o.purpose is distinct from 'DAILY_RATE_CORRECTION' or o.state is distinct from 'REVIEWED'
  or o.created_by_user_id is distinct from actor or o.from_contract_id is not null or o.to_contract_id is not null
  or review.id is null or encode(review.review_digest,'hex') is distinct from p_args->>'review_digest'
  or o.state_revision::text is distinct from p_args->>'expected_state_revision'
  or o.expected_scope_hash is distinct from review.scope_hash or o.before_pay_cut_hash is distinct from review.pay_cut_hash
  or o.before_invoice_hash is distinct from review.invoice_hash then
  raise exception 'REVIEW_STALE' using errcode='40001';end if;
 preview_args:=jsonb_build_object('version','DAILY_RATE_CORRECTION_V1','request_id',review.preview_request_id,
  'actor_user_id',actor,'timesheet_id',o.from_timesheet_id,
  'reason',private.daily_rate_correction_health_reason_note_v1(o.reason),
  'context_digest',review.manifest_json#>>'{daily,context,context_digest}',
  'calculation_snapshot',review.manifest_json#>'{daily,calculation_snapshot}',
  'rate_provenance',review.manifest_json#>'{daily,rate_provenance}');
 if private.daily_rate_correction_request_hash_v1(preview_args,'PREVIEW') is distinct from review.preview_request_hash then
  raise exception 'REVIEW_STALE' using errcode='40001';end if;
 daily:=private.daily_rate_correction_health_basis_v1(preview_args,true);
 if daily is distinct from review.manifest_json->'daily' then raise exception 'REVIEW_STALE' using errcode='40001';end if;
 physical:=private.daily_rate_correction_scope_v1(o.id,false);
 if physical is distinct from review.manifest_json->'physical'
  or physical->>'scope_sha256' is distinct from encode(review.scope_hash,'hex') then
  raise exception 'REVIEW_STALE' using errcode='40001';end if;
 invoice_preflight:=private.invoice_contract_move_preflight_v1(review.id,actor,true);
 if invoice_preflight->'ready_to_confirm' is distinct from 'true'::jsonb then
  raise exception 'DAILY_RATE_INVOICE_PREFLIGHT_NOT_READY' using errcode='55000';end if;
 -- Existing shared metadata locks come LAST, after genuine native locks.
 select x.* into strict locked_o from private.timesheet_contract_move_operations x where x.id=o.id for update;
 select r.* into strict locked_review from private.timesheet_contract_move_reviews r where r.id=review.id and r.operation_id=o.id for share;
 if to_jsonb(locked_o) is distinct from to_jsonb(o) or to_jsonb(locked_review) is distinct from to_jsonb(review) then
  raise exception 'REVIEW_STALE' using errcode='40001';end if;
 return jsonb_build_object('version','DAILY_RATE_HEALTH_ACCEPT_BASIS_V1','operation_id',o.id,'review_id',review.id,
  'state_revision',o.state_revision::text,'review_digest',encode(review.review_digest,'hex'),
  'daily',daily,'physical',physical,'invoice_preflight',invoice_preflight);
 -- This returns locked Health facts only. Root's full accept must independently
 -- revalidate actual Bank cut/currentness, register exact capabilities, perform
 -- native revision/publication+invoice registration, then seal COMPLETE once.
end;
$function$;

create or replace function private.daily_rate_correction_health_result_basis_v1(p_operation_id uuid,p_actor_user_id uuid)
returns jsonb language plpgsql volatile security definer
set search_path=pg_catalog,public,private,pg_temp
as $function$
declare o private.timesheet_contract_move_operations%rowtype; member private.timesheet_contract_move_members%rowtype; billing jsonb;
begin
 -- Daily GET/public1342 admits the current active Office reader independently
 -- of the creator. Admin/pay qualification belongs to mutation and projection,
 -- not a newly invented blanket read gate. Native0955 owns billing redaction.
 if not exists(select 1 from public.tms_users u where u.id=p_actor_user_id and u.is_active) then
  raise exception 'CORRECTION_RESULT_NOT_VISIBLE' using errcode='42501';end if;
 select x.* into o from private.timesheet_contract_move_operations x where x.id=p_operation_id;
 if o.id is null or o.purpose is distinct from 'DAILY_RATE_CORRECTION' or o.from_contract_id is not null or o.to_contract_id is not null
  or not exists(select 1 from public.timesheets t where t.timesheet_id=o.from_timesheet_id and t.sheet_scope::text='DAILY' and t.contract_id is null)
  or (select count(*) from private.timesheet_contract_move_members m where m.operation_id=o.id)<>1 then
  raise exception 'CORRECTION_RESULT_NOT_VISIBLE' using errcode='42501';end if;
 select m.* into strict member from private.timesheet_contract_move_members m where m.operation_id=o.id;
 if member.from_timesheet_id is distinct from o.from_timesheet_id or member.from_contract_id is not null or member.to_contract_id is not null
  or (o.state in('COMPLETE','SUPERSEDED') and (o.to_timesheet_id is distinct from o.from_timesheet_id
   or member.to_timesheet_id is distinct from o.from_timesheet_id or member.to_version is distinct from member.from_version
   or o.placement_receipt_json->>'version' is distinct from 'DAILY_RATE_REVISION_RECEIPT_V1'
   or o.placement_receipt_json->>'operation_id' is distinct from o.id::text
   or o.placement_receipt_json->>'review_id' is distinct from o.accepted_review_id::text
   or o.placement_receipt_json->>'current_timesheet_id' is distinct from o.from_timesheet_id::text
   or o.placement_receipt_json->>'previous_financial_id' is distinct from member.from_financial_id::text
   or o.placement_receipt_json->>'current_financial_id' is distinct from member.to_financial_id::text
   or o.placement_receipt_json->>'unchanged_work_sha256' is distinct from encode(o.unchanged_work_sha256,'hex')
   or not exists(select 1 from private.timesheet_contract_move_reviews r where r.operation_id=o.id and r.id=o.accepted_review_id
     and r.review_digest=o.accepted_review_digest)
   or not exists(select 1 from public.timesheets_financials f where f.id=member.to_financial_id
     and f.timesheet_id=o.from_timesheet_id and f.timesheet_version=member.to_version
     and f.authorised_at_utc is not null and f.authorised_by_user_id=o.created_by_user_id))) then
  raise exception 'CORRECTION_RESULT_NOT_VISIBLE' using errcode='42501';end if;
 billing:=private.invoice_contract_move_billing_read_v1(o.id,p_actor_user_id);
 if not exists(select 1 from public.tms_users u where u.id=p_actor_user_id and u.is_active) then
  raise exception 'CORRECTION_RESULT_NOT_VISIBLE' using errcode='42501';end if;
 return jsonb_build_object('version','DAILY_RATE_HEALTH_RESULT_BASIS_V1','operation_id',o.id,'state',o.state,
  'state_revision',o.state_revision::text,'physical_timesheet_id',o.from_timesheet_id,
  'placement_receipt_id',case when o.placement_receipt_json is not null then o.placement_receipt_json->'receipt_id' else 'null'::jsonb end,
  'accepted_review_id',o.accepted_review_id,'financial_command_id',null,'current_financial_cut_id',null,'billing',billing);
 -- No raw native receipt or financial IDs cross this reader. Genuine Bank
 -- current-reader visibility/semantic join remains separately required even
 -- for an Admin; this Health basis grants no guessed financial projection.
end;
$function$;

revoke all on function private.daily_rate_correction_health_reason_note_v1(text) from public,anon,authenticated,service_role;
revoke all on function private.daily_rate_correction_health_actor_require_v1(uuid,boolean) from public,anon,authenticated,service_role;
revoke all on function private.daily_rate_correction_health_basis_v1(jsonb,boolean) from public,anon,authenticated,service_role;
revoke all on function private.daily_rate_correction_health_review_prepare_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function private.daily_rate_correction_health_accept_basis_v1(jsonb) from public,anon,authenticated,service_role;
revoke all on function private.daily_rate_correction_health_result_basis_v1(uuid,uuid) from public,anon,authenticated,service_role;
