-- ONE_TIME_ADMISSION only. Never included in the rerunnable repeatable.
-- No business data or lock change; refuse any previously owned namespace.
do $admission$
declare signature text;
begin
 foreach signature in array array[
  'private.daily_rate_correction_health_reason_note_v1(text)',
  'private.daily_rate_correction_health_actor_require_v1(uuid,boolean)',
  'private.daily_rate_correction_health_basis_v1(jsonb,boolean)',
  'private.daily_rate_correction_health_review_prepare_v1(jsonb)',
  'private.daily_rate_correction_health_accept_basis_v1(jsonb)',
  'private.daily_rate_correction_health_result_basis_v1(uuid,uuid)'
 ] loop
  if pg_catalog.to_regprocedure(signature) is not null then
   raise exception 'DAILY_HEALTH_SUPPORT_NAMESPACE_COLLISION: %',signature using errcode='55000';
  end if;
 end loop;
end;
$admission$;
