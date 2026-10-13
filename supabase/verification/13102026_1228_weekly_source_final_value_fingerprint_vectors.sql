-- Data-free first-use vectors for WP-F2. Integrator-owned execution after the
-- exact complete successor is installed; no business fixtures or ledger writes.
-- These vectors qualify helpers only, not U-1/U-12/HC-2/SM joined acceptance.
\set ON_ERROR_STOP on
begin;

do $proof$
declare
  v_vector jsonb:='{"row_sign":1,"paid_minutes":600,"break_minutes":60,"hours":{"day":10,"night":0,"sat":0,"sun":0,"bh":0},"rates":{"day":10,"night":11,"sat":12,"sun":13,"bh":14},"total_pence":10000,"calculation_fingerprint":"before"}'::jsonb;
  v_authority jsonb:='{"authority_mode":"SOURCE_AUTHORITY","self_bill_enabled":true,"source_group_id":"00000000-0000-4000-8000-000000000001","self_bill_correction_presentation":"FULL_REVERSAL_REPLACEMENT"}'::jsonb;
  v_state jsonb;
  v_noise jsonb;
  v_patch jsonb;
  v_path text[];
  v_hash bytea;
  v_invoice bytea;
  v_money jsonb:='{"source_shift_charge_pence":10000,"source_gross_pence":12000,"source_vat_pence":2000}'::jsonb;
begin
  v_state:=pg_catalog.jsonb_build_object(
    'work_event_id','synthetic-event','candidate_id','synthetic-candidate',
    'client_id','synthetic-client','contract_id','synthetic-contract',
    'work_date','2026-09-06','start_at_local','2026-09-06T08:00:00',
    'end_at_local','2026-09-06T19:00:00','break_minutes',60,'actual_net_minutes',600,
    'pay_vector',v_vector,'charge_vector',v_vector,
    'invoice_vat_rate_pct',20,'invoice_vat_chargeable',true,'authority_subset',v_authority
  );
  v_hash:=private.weekly_source_final_state_fingerprint_v1(v_state);
  v_noise:=v_state||'{"contract_updated_at":"2026-10-13T12:00:00Z","client_settings_updated_at":"later","global_settings_updated_at":"later","bh_feed_list":"later","mapping_fingerprint":"after","rate_fingerprint":"after","policy_fingerprint":"after","source_fingerprint":"after"}'::jsonb;
  v_noise:=pg_catalog.jsonb_set(v_noise,'{pay_vector,calculation_fingerprint}','"after"'::jsonb);
  v_noise:=pg_catalog.jsonb_set(v_noise,'{charge_vector,calculation_fingerprint}','"after"'::jsonb);
  if private.weekly_source_final_state_fingerprint_v1(v_noise) is distinct from v_hash then
    raise exception 'F2_TIMESTAMP_OR_NESTED_HASH_NOISE';
  end if;
  for v_patch in select value from pg_catalog.jsonb_array_elements(
    '[{"work_event_id":"different"},{"candidate_id":"different"},{"client_id":"different"},{"contract_id":"different"},{"work_date":"2026-09-07"},{"start_at_local":"2026-09-06T08:30:00"},{"end_at_local":"2026-09-06T18:30:00"},{"break_minutes":30},{"actual_net_minutes":570},{"invoice_vat_rate_pct":0},{"invoice_vat_chargeable":false}]'::jsonb
  ) loop
    if private.weekly_source_final_state_fingerprint_v1(v_state||v_patch)=v_hash then
      raise exception 'F2_REAL_VALUE_CHANGE_IGNORED: %',v_patch;
    end if;
  end loop;
  for v_path in select route from (values
    (array['pay_vector','total_pence']::text[]),
    (array['pay_vector','hours','day']::text[]),
    (array['pay_vector','rates','day']::text[]),
    (array['charge_vector','total_pence']::text[]),
    (array['charge_vector','rates','bh']::text[])
  ) paths(route) loop
    if private.weekly_source_final_state_fingerprint_v1(pg_catalog.jsonb_set(v_state,v_path,'99'::jsonb))=v_hash then
      raise exception 'F2_VECTOR_VALUE_CHANGE_IGNORED: %',v_path;
    end if;
  end loop;
  for v_patch in select value from pg_catalog.jsonb_array_elements(
    '[{"authority_mode":"TIMESHEET_AUTHORITY"},{"self_bill_enabled":false},{"source_group_id":"00000000-0000-4000-8000-000000000002"},{"self_bill_correction_presentation":"NET_DIFFERENCE_PRESENTATION"}]'::jsonb
  ) loop
    if private.weekly_source_final_state_fingerprint_v1(
      pg_catalog.jsonb_set(v_state,'{authority_subset}',v_authority||v_patch)
    )=v_hash then raise exception 'F2_AUTHORITY_CHANGE_IGNORED: %',v_patch; end if;
  end loop;
  if private.weekly_source_authority_subset_v1(v_authority||'{"settings_updated_at":"later","unrelated_setting":true}'::jsonb)
     is distinct from v_authority then raise exception 'F2_AUTHORITY_PROJECTION_NOISE'; end if;
  v_invoice:=private.weekly_source_invoice_economics_fingerprint_v1(v_money,'LINE_GROSS_MINUS_NET_FROM_SOURCE',20);
  for v_patch in select value from pg_catalog.jsonb_array_elements(
    '[{"source_shift_charge_pence":10001},{"source_gross_pence":12001},{"source_vat_pence":2001}]'::jsonb
  ) loop
    if private.weekly_source_invoice_economics_fingerprint_v1(v_money||v_patch,'LINE_GROSS_MINUS_NET_FROM_SOURCE',20)=v_invoice then
      raise exception 'F2_SOURCE_MONEY_CHANGE_IGNORED: %',v_patch;
    end if;
    if private.weekly_source_final_state_fingerprint_v1(v_state||v_patch) is distinct from v_hash then
      raise exception 'F2_INVOICE_ONLY_MONEY_CONTAMINATED_PAY';
    end if;
  end loop;
  if private.weekly_source_invoice_economics_fingerprint_v1(v_money,'ROUND_EACH_LINE_FROM_NET',20)=v_invoice
     or private.weekly_source_invoice_economics_fingerprint_v1(v_money,'LINE_GROSS_MINUS_NET_FROM_SOURCE',0)=v_invoice then
    raise exception 'F2_INVOICE_VAT_METHOD_OR_RATE_IGNORED';
  end if;
  begin
    perform private.weekly_source_invoice_economics_fingerprint_v1(v_money-'source_gross_pence','LINE_GROSS_MINUS_NET_FROM_SOURCE',20);
    raise exception 'F2_MISSING_SOURCE_MONEY_ADMITTED';
  exception when sqlstate '55000' then
    if sqlerrm<>'WEEKLY_SOURCE_INVOICE_ECONOMICS_NOT_READY' then raise; end if;
  end;
end;
$proof$;

rollback;
