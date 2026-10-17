-- REQUIRED_NOT_RUN. Run once with root transaction isolation REPEATABLE READ,
-- and once SERIALIZABLE. Native server owns/rolls back each outer transaction.
-- No SET TRANSACTION, savepoint escape or rehearsal-specific exception.
do $isolation$
declare rejected boolean:=false;
begin
 if current_setting('transaction_isolation') not in('repeatable read','serializable') then
  raise exception 'RR_OR_SERIALIZABLE_NATIVE_CASE_REQUIRED';end if;
 begin
  -- Valid closed shape, syntactically canonical IDs; the explicit isolation
  -- refusal occurs before any subject lookup or native mutation/lock.
  perform private.daily_rate_correction_health_accept_basis_v1(jsonb_build_object(
   'version','DAILY_RATE_CORRECTION_V1','request_id','00000000-0000-0000-0000-000000000001',
   'actor_user_id','00000000-0000-0000-0000-000000000002','operation_id','00000000-0000-0000-0000-000000000003',
   'review_id','00000000-0000-0000-0000-000000000004','review_digest',repeat('a',64),'expected_state_revision','1'));
 exception when object_not_in_prerequisite_state then
  if sqlerrm<>'DAILY_RATE_LOCKED_CONTEXT_ISOLATION_REQUIRED' then raise;end if;rejected:=true;
 end;
 if not rejected then raise exception 'OLD_SNAPSHOT_LOCKED_ACCEPT_WAS_ADMITTED';end if;
end;
$isolation$;
select current_setting('transaction_isolation') as proved_refused_isolation;
