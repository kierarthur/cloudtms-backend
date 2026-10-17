-- REQUIRED_NOT_RUN. Root's native PG17 rollback transaction only. psql supplies
-- reviewed actual Worker-produced PREVIEW service JSON; no browser quote.
\if :{?daily_health_test_args}
\else
\echo 'daily_health_test_args required; fixture selection/setup is not implicit'
\quit 3
\endif
create temporary table daily_health_input(args jsonb not null) on commit drop;
insert into daily_health_input values(:'daily_health_test_args'::jsonb);
do $native$
declare args jsonb; health jsonb; basis jsonb; operation uuid; rejected boolean; signature text;
begin
 if current_setting('server_version_num')::integer/10000<>17 then raise exception 'PG17_REQUIRED';end if;
 if current_setting('transaction_isolation')<>'read committed' then raise exception 'RC_NATIVE_PROGRAM_REQUIRED';end if;
 select i.args into strict args from daily_health_input i;
 basis:=private.daily_rate_correction_health_basis_v1(args,true);
 if basis is distinct from jsonb_build_object('context',basis->'context','calculation_snapshot',args->'calculation_snapshot',
  'normalized_snapshot',private.daily_rate_correction_normalized_snapshot_v1(basis->'context',args->'calculation_snapshot',args->'rate_provenance',(args->>'actor_user_id')::uuid),
  'rate_provenance',args->'rate_provenance') then raise exception 'EXACT_NATIVE_DAILY_BASIS_MISMATCH';end if;
 operation:=substr(encode(private.timesheet_contract_move_row_hash_v1(jsonb_build_array(
  'DAILY_RATE_HEALTH_REVIEW_OPERATION_V1',args->>'request_id',(args->>'actor_user_id')::uuid,(args->>'timesheet_id')::uuid)),'hex'),1,32)::uuid;
 if exists(select 1 from private.timesheet_contract_move_operations o where o.id=operation) then raise exception 'VACANT_REVIEW_FIXTURE_REQUIRED';end if;
 rejected:=false;
 begin
  health:=private.daily_rate_correction_health_review_prepare_v1(args);
  if health->>'operation_id' is distinct from operation::text or health->'daily' is distinct from basis
   or health#>>'{physical,operation_id}' is distinct from operation::text
   or health#>>'{physical,current_placement,timesheet_id}' is distinct from args->>'timesheet_id'
   or not exists(select 1 from private.timesheet_contract_move_operations o where o.id=operation and o.state='REVIEWED'
    and o.expected_scope_hash is null and o.before_pay_cut_hash is null and o.before_invoice_hash is null
    and o.accept_request_id is null and o.receipt_json is null)
   or (select count(*) from private.timesheet_contract_move_members m where m.operation_id=operation)<>1
   or exists(select 1 from private.timesheet_contract_move_reviews r where r.operation_id=operation)
   or exists(select 1 from private.timesheet_contract_move_requests q where q.operation_id=operation) then
   raise exception 'HEALTH_METADATA_INITIALIZER_MISMATCH';end if;
  -- Existing native deferred guard, never a newly fabricated partial state.
  set constraints timesheet_contract_move_commit_guard,timesheet_contract_move_member_commit_guard immediate;
 exception when check_violation then
  if sqlerrm<>'CONTRACT_MOVE_INCOMPLETE_TRANSACTION' then raise;end if;
  rejected:=true;
 end;
 if not rejected or exists(select 1 from private.timesheet_contract_move_operations o where o.id=operation)
  or exists(select 1 from private.timesheet_contract_move_members m where m.operation_id=operation) then
  raise exception 'PARTIAL_REVIEW_COMMIT_OR_SUBTRANSACTION_ESCAPE';end if;
 foreach signature in array array['private.daily_rate_correction_health_reason_note_v1(text)','private.daily_rate_correction_health_actor_require_v1(uuid,boolean)',
  'private.daily_rate_correction_health_basis_v1(jsonb,boolean)','private.daily_rate_correction_health_review_prepare_v1(jsonb)',
  'private.daily_rate_correction_health_accept_basis_v1(jsonb)','private.daily_rate_correction_health_result_basis_v1(uuid,uuid)'] loop
  if has_function_privilege('anon',signature,'EXECUTE') or has_function_privilege('authenticated',signature,'EXECUTE')
   or has_function_privilege('service_role',signature,'EXECUTE') then raise exception 'HEALTH_PRIVATE_BOUNDARY_EXPOSED';end if;
 end loop;
end;
$native$;
select 'DAILY_HEALTH_RC_BASIS_AND_INCOMPLETE_REVIEW_ROLLBACK_ASSERTED' as result;
