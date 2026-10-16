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
