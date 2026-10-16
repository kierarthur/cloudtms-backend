-- Executable pure-helper tests for root's rollback rehearsal. No business rows,
-- fixture DDL, native producer doubles, payment/network effects or durable writes.
-- NOT_RUN by physical source lane; offline grammar is not a native pass.
do $tests$
declare old_row jsonb; next_row jsonb; request jsonb; base_hash bytea; rejected boolean; key text;
begin
 if private.timesheet_contract_move_reason_v1(null)<>'Correcting the Contract'
  or private.timesheet_contract_move_reason_v1(U&'\00a0\2003Details\3000')<>'Correcting the Contract — Details'
  or private.timesheet_contract_move_reason_v1('','DAILY_RATE_CORRECTION')<>'Applying updated rates' then
  raise exception 'reason defaults/Unicode trim failed';end if;
 rejected:=false;begin perform private.timesheet_contract_move_reason_v1('',null);
 exception when invalid_parameter_value then rejected:=true;end;
 if not rejected then raise exception 'null purpose admitted';end if;
 rejected:=false;begin perform private.timesheet_contract_move_reason_v1(repeat('😀',1970));
 exception when invalid_parameter_value then rejected:=true;end;
 if not rejected then raise exception 'overlong Unicode details admitted';end if;
 request:='{"version":"TIMESHEET_CONTRACT_CORRECTION_V3","request_id":"00000000-0000-0000-0000-000000000001","timesheet_id":"00000000-0000-0000-0000-000000000002","to_contract_id":"00000000-0000-0000-0000-000000000003","actor_user_id":"00000000-0000-0000-0000-000000000004"}';
 base_hash:=private.timesheet_contract_move_request_hash_v1(request,'PREVIEW');
 if base_hash is distinct from private.timesheet_contract_move_request_hash_v1(request||'{"reason":"  "}','PREVIEW') then
  raise exception 'blank reason retry hash changed';end if;
 if base_hash=private.timesheet_contract_move_request_hash_v1(request||'{"actor_user_id":"00000000-0000-0000-0000-000000000005"}','PREVIEW') then
  raise exception 'actor was omitted from immutable request hash';end if;
 foreach key in array array['money','booking_id','component_resolutions','action'] loop
  rejected:=false;begin perform private.timesheet_contract_move_request_assert_v1(request||jsonb_build_object(key,'forbidden'),'PREVIEW');
  exception when invalid_parameter_value then rejected:=true;end;
  if not rejected then raise exception 'unknown request key admitted: %',key;end if;
 end loop;
 rejected:=false;begin perform private.timesheet_contract_move_request_assert_v1(request,null);
 exception when invalid_parameter_value then rejected:=true;end;
 if not rejected then raise exception 'null request kind admitted';end if;
 request:='{"version":"TIMESHEET_CONTRACT_CORRECTION_V3","request_id":"00000000-0000-0000-0000-000000000001","operation_id":"00000000-0000-0000-0000-000000000002","review_id":"00000000-0000-0000-0000-000000000003","actor_user_id":"00000000-0000-0000-0000-000000000004","review_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","expected_state_revision":"9223372036854775807"}';
 perform private.timesheet_contract_move_request_assert_v1(request,'ACCEPT');
 foreach key in array array['0','01','9223372036854775808'] loop
  rejected:=false;begin perform private.timesheet_contract_move_request_assert_v1(request||jsonb_build_object('expected_state_revision',key),'ACCEPT');
  exception when invalid_parameter_value then rejected:=true;end;
  if not rejected then raise exception 'noncanonical/out-of-range revision admitted: %',key;end if;
 end loop;
 old_row:='{"id":"actual-finance-fixture","rate_pay_day":18.5,"total_pay":111.01,"paid_at_utc":"2026-10-01T12:00:00Z","updated_at":"old","locked_by_invoice_id":"original-invoice","locked_at_utc":"old-lock","unlocked_by_credit_note_id":null,"invoice_breakdown_json":{"vat":22.20,"segments":[{"segment_id":"day1","hours":6,"net":111.01,"invoice_locked_invoice_id":"original-invoice"},{"segment_id":"day2","hours":2,"net":30,"invoice_locked_invoice_id":"other-invoice"}]}}';
 next_row:=jsonb_set(old_row||'{"updated_at":"new","locked_at_utc":null,"locked_by_invoice_id":null}',
  '{invoice_breakdown_json,segments,0,invoice_locked_invoice_id}','null');
 if private.timesheet_contract_move_financial_economics_v1(old_row)
  is distinct from private.timesheet_contract_move_financial_economics_v1(next_row) then
  raise exception 'exact native invoice lock edit changed sealed economics';end if;
 foreach key in array array['rate_pay_day','total_pay','paid_at_utc','unlocked_by_credit_note_id'] loop
  if private.timesheet_contract_move_financial_economics_v1(old_row)
   is not distinct from private.timesheet_contract_move_financial_economics_v1(jsonb_set(next_row,array[key],'"changed"')) then
   raise exception 'original finance field escaped seal: %',key;end if;
 end loop;
 if private.timesheet_contract_move_financial_economics_v1(old_row)
   is not distinct from private.timesheet_contract_move_financial_economics_v1(jsonb_set(next_row,'{invoice_breakdown_json,segments,0,net}','222.02'))
  or private.timesheet_contract_move_financial_economics_v1(old_row)
   is not distinct from private.timesheet_contract_move_financial_economics_v1(jsonb_set(next_row,'{invoice_breakdown_json,segments,0,segment_id}','"new-id"'))
  or private.timesheet_contract_move_financial_economics_v1(old_row)
   is not distinct from private.timesheet_contract_move_financial_economics_v1(jsonb_set(next_row,'{invoice_breakdown_json,vat}','99.99')) then
  raise exception 'segment identity/economics or VAT escaped seal';end if;
end;
$tests$;
