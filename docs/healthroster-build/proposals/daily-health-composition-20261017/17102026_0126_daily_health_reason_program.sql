-- REQUIRED_NOT_RUN. PG17 root-owned rollback transaction after exact helpers.
-- Exercises actual SQL Unicode normalizer; no fixture or business mutation.
do $reason$
declare details text; stored text; recovered text; bad text; rejected boolean; args jsonb;
begin
 foreach details in array array['',repeat('x',1969),repeat(chr(128512),1969),
  repeat('e'||chr(769),984)||'z','Applying updated rates',
  chr(65279)||chr(160)||' details '||chr(12288)] loop
  stored:=private.timesheet_contract_move_reason_v1(details,'DAILY_RATE_CORRECTION');
  recovered:=private.daily_rate_correction_health_reason_note_v1(stored);
  if private.timesheet_contract_move_reason_v1(recovered,'DAILY_RATE_CORRECTION') is distinct from stored then
   raise exception 'DAILY_REASON_NATIVE_ROUNDTRIP_FAILED';end if;
  if char_length(details)=1969 and (char_length(recovered)<>1969 or char_length(stored)<=1969) then
   raise exception 'FULL1969_NATIVE_DETAILS_LOST';end if;
  args:=jsonb_build_object('version','DAILY_RATE_CORRECTION_V1','request_id','00000000-0000-0000-0000-000000000001',
   'actor_user_id','00000000-0000-0000-0000-000000000002','timesheet_id','00000000-0000-0000-0000-000000000003',
   'context_digest',null,'calculation_snapshot',null,'rate_provenance',null,'reason',details);
  if private.daily_rate_correction_request_hash_v1(args,'PREVIEW') is distinct from
   private.daily_rate_correction_request_hash_v1(jsonb_set(args,'{reason}',to_jsonb(recovered)),'PREVIEW') then
   raise exception 'RECONSTRUCTED_PREVIEW_REQUEST_HASH_CHANGED';end if;
 end loop;
 foreach bad in array array[null::text,'','Applying updated rates — ','Applying updated rates - details',
  ' Applying updated rates','Applying updated rates — details ',
  'Applying updated rates — '||chr(160)||'details',
  'Applying updated rates — '||repeat(chr(128512),1970)] loop
  rejected:=false;
  begin perform private.daily_rate_correction_health_reason_note_v1(bad);
  exception when check_violation then
   if sqlerrm<>'DAILY_RATE_STORED_REASON_INVALID' then raise;end if;rejected:=true;
  end;
  if not rejected then raise exception 'MALFORMED_CANONICAL_DAILY_REASON_ACCEPTED';end if;
 end loop;
end;
$reason$;
select 'DAILY_HEALTH_NATIVE_UNICODE_REASON_ROUNDTRIP_ASSERTED' as result;
