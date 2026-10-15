create function private.hr_nhsp_case_f109f63548eb97c2() returns boolean language plpgsql as $exact_case$ declare v_match jsonb:='{}';v_item jsonb:='{}';begin if (case when v_match ? 'nhsp_negative_population' then
      v_match#>>'{current_native_position,schema_version}' is distinct from 'WEEKLY_SOURCE_NHSP_CURRENT_OVERLAP_POSITION_INPUT_V1'
      or v_match#>>'{current_native_position,state}' is distinct from 'CURRENT_NATIVE_POSITION'
      or v_match#>'{current_native_position,work_event_id}' is distinct from v_match->'work_event_id'
      or v_match#>'{current_native_position,contract_id}' is distinct from v_match->'contract_id'
      or v_match#>'{current_native_position,native_current_basis_sha256}' is distinct from v_match#>'{nhsp_negative_population,native_current_basis,inputs_sha256}'
      or v_match#>'{nhsp_negative_population,negative_population_complete}' is distinct from 'true'::jsonb
      or v_match#>'{nhsp_negative_population,current_position_available}' is distinct from 'true'::jsonb
      or v_match#>'{nhsp_negative_population,no_reversal_proved}' is distinct from 'true'::jsonb
      or v_match#>'{nhsp_negative_population,paid_writer_permission}' is distinct from 'null'::jsonb
      else v_match#>>'{current_pay_snapshot,state}' is distinct from 'CURRENT'
    or v_match#>>'{current_pay_snapshot,snapshot_line_id}' is distinct from v_match->>'snapshot_line_id'
    or v_match#>>'{current_pay_snapshot,movement_id}' is distinct from v_match->>'current_pay_head_movement_id' end) then return true;end if;return false;end $exact_case$;
create function private.hr_nhsp_case_6865613e2d014c27() returns boolean language plpgsql as $exact_case$ declare v_match jsonb:='{}';v_item jsonb:='{}';begin if (case when v_match ? 'nhsp_negative_population' then
      v_match#>>'{current_native_position,schema_version}' is distinct from 'WEEKLY_SOURCE_NHSP_CURRENT_OVERLAP_POSITION_INPUT_V1'
      or v_match#>>'{current_native_position,state}' is distinct from 'CURRENT_NATIVE_POSITION'
      or v_match#>'{current_native_position,work_event_id}' is distinct from v_match->'work_event_id'
      or v_match#>'{current_native_position,contract_id}' is distinct from v_match->'contract_id'
      or v_match#>'{current_native_position,native_current_basis_sha256}' is distinct from v_match#>'{nhsp_negative_population,native_current_basis,inputs_sha256}'
      or v_match#>'{nhsp_negative_population,negative_population_complete}' is distinct from 'true'::jsonb
      or v_match#>'{nhsp_negative_population,current_position_available}' is distinct from 'true'::jsonb
      or v_match#>'{nhsp_negative_population,no_reversal_proved}' is distinct from 'true'::jsonb
      or v_match#>'{nhsp_negative_population,paid_writer_permission}' is distinct from 'null'::jsonb
      else v_match#>>'{current_pay_snapshot,state}' is distinct from 'CURRENT'
    or v_match#>>'{current_pay_snapshot,snapshot_line_id}' is distinct from v_match->>'snapshot_line_id'
    or v_match#>>'{current_pay_snapshot,movement_id}' is distinct from v_match->>'current_pay_head_movement_id' end) then return true;end if;return false;end $exact_case$;
create function private.hr_nhsp_case_4d34d2097f4f1694() returns boolean language plpgsql as $exact_case$ declare v_match jsonb:='{}';v_item jsonb:='{}';begin if (case when v_item ? 'nhsp_negative_population' then
      v_item#>>'{current_native_position,schema_version}' is distinct from 'WEEKLY_SOURCE_NHSP_CURRENT_OVERLAP_POSITION_INPUT_V1'
      or v_item#>>'{current_native_position,state}' is distinct from 'CURRENT_NATIVE_POSITION'
      or v_item#>'{current_native_position,work_event_id}' is distinct from v_item->'work_event_id'
      or v_item#>'{current_native_position,contract_id}' is distinct from v_item->'contract_id'
      or v_item#>'{current_native_position,native_current_basis_sha256}' is distinct from v_item#>'{nhsp_negative_population,native_current_basis,inputs_sha256}'
      or v_item#>'{nhsp_negative_population,negative_population_complete}' is distinct from 'true'::jsonb
      or v_item#>'{nhsp_negative_population,current_position_available}' is distinct from 'true'::jsonb
      or v_item#>'{nhsp_negative_population,no_reversal_proved}' is distinct from 'true'::jsonb
      or v_item#>'{nhsp_negative_population,paid_writer_permission}' is distinct from 'null'::jsonb
      else v_item#>>'{current_pay_snapshot,state}' is distinct from 'CURRENT'
    or v_item#>>'{current_pay_snapshot,snapshot_line_id}' is distinct from v_item->>'snapshot_line_id'
    or v_item#>>'{current_pay_snapshot,movement_id}' is distinct from v_item->>'current_pay_head_movement_id' end) then return true;end if;return false;end $exact_case$;
select private.hr_nhsp_case_f109f63548eb97c2() as default_unavailable_branch_is_true;
select private.hr_nhsp_case_6865613e2d014c27() as default_unavailable_branch_is_true;
select private.hr_nhsp_case_4d34d2097f4f1694() as default_unavailable_branch_is_true;