-- Root rollback rehearsal only. These invoke real pure owner helpers; no
-- substitute definitions, business fixture, financial publisher or SQL execution.
do $test$
declare refused boolean;
begin
 perform private.daily_rate_calculator_round_assert_v1(0::double precision,0::numeric);
 perform private.daily_rate_calculator_round_assert_v1(1.005::double precision,1::numeric);
 perform private.daily_rate_calculator_round_assert_v1(2.675::double precision,2.68::numeric);
 perform private.daily_rate_calculator_round_assert_v1(-1.005::double precision,-1::numeric);
 refused:=false;
 begin
  perform private.daily_rate_calculator_round_assert_v1(1.005::double precision,1.01::numeric);
 exception when check_violation then refused:=true;end;
 if not refused then raise exception 'DAILY_ROUNDING_BINARY_BOUNDARY_NOT_ENFORCED';end if;
 refused:=false;
 begin
  perform private.daily_rate_calculator_round_assert_v1(0::double precision,null::numeric);
 exception when check_violation then refused:=true;end;
 if not refused then raise exception 'DAILY_MISSING_MONEY_ACCEPTED_AS_ZERO';end if;
 refused:=false;
 begin
  perform private.daily_rate_calculator_round_assert_v1('NaN'::double precision,0::numeric);
 exception when check_violation then refused:=true;end;
 if not refused then raise exception 'DAILY_NONFINITE_MONEY_ACCEPTED_AS_ZERO';end if;
end;
$test$;

-- Immutable request identity excludes trusted freshly derived pricing, while
-- retaining every actual browser/actor/reason input. Fresh review still must
-- validate the complete canonical snapshot before allocating any metadata.
do $test$
declare call jsonb; changed jsonb; refused boolean;
begin
 call:=jsonb_build_object('version','DAILY_RATE_CORRECTION_V1','request_id','00000000-0000-0000-0000-000000000001',
  'timesheet_id','00000000-0000-0000-0000-000000000002','actor_user_id','00000000-0000-0000-0000-000000000003',
  'context_digest',repeat('a',64),'calculation_snapshot','{}'::jsonb,'rate_provenance',null,'reason',' Updated rates ');
 changed:=call||jsonb_build_object('context_digest',null,'calculation_snapshot',null,'rate_provenance',null,'reason','Updated rates');
 if private.daily_rate_correction_request_hash_v1(call,'PREVIEW') is distinct from private.daily_rate_correction_request_hash_v1(changed,'PREVIEW') then
  raise exception 'DAILY_REPLAY_PRICING_OR_REASON_NORMALIZATION_DRIFT';end if;
 if private.daily_rate_correction_request_hash_v1(call,'PREVIEW') is not distinct from
  private.daily_rate_correction_request_hash_v1(call||jsonb_build_object('reason','Different request'),'PREVIEW') then
  raise exception 'DAILY_REPLAY_BROWSER_INPUT_NOT_BOUND';end if;
 refused:=false;
 begin
  perform private.daily_rate_correction_request_assert_v1(call||jsonb_build_object('to_contract_id','00000000-0000-0000-0000-000000000004'),'PREVIEW');
 exception when invalid_parameter_value then refused:=true;end;
 if not refused then raise exception 'DAILY_REPLAY_ACCEPTED_CONTRACT_KEY';end if;
end;
$test$;
