-- Native data-free S09/pence/zero-Pay and exact window-helper security gates.
-- Full FIN/admission/issue/model/physical-window/concurrency proof is separate.
\set ON_ERROR_STOP on
do $verify$
declare vector record; actual bigint; pay jsonb; helper record;
begin
  for vector in select * from (values
    ('V1',10000::numeric,2100::numeric,2000::numeric,'ONCE_ON_TOTAL_HALF_UP',2000::bigint,0::bigint),
    ('V2',200,40,30,'ONCE_ON_TOTAL_HALF_UP',40,10),
    ('V3',200,40,50,'ONCE_ON_TOTAL_HALF_UP',40,-10),
    ('V4',200,40,40,'ONCE_ON_TOTAL_HALF_UP',40,0),
    ('V5',200,40,40,'ONCE_ON_TOTAL_HALF_UP',40,0),
    ('V6',-3,-1,-1,'ONCE_ON_TOTAL_HALF_UP',-1,0),
    ('V7',-3,-1,-1,'ONCE_ON_TOTAL_TRUNCATE',0,1)
  ) expected(id,net,raw_vat,admitted_vat,method,target,admitted_delta)
  loop
    actual:=private.weekly_source_invoice_vat_target_pence_v1(vector.net,vector.admitted_vat,vector.method,20);
    if actual is distinct from vector.target or actual-vector.admitted_vat is distinct from vector.admitted_delta then
      raise exception 'WEEKLY_SOURCE_S09_NATIVE_PENCE_VECTOR_INVALID:%',vector.id; end if;
  end loop;
  if private.weekly_source_invoice_vat_target_pence_v1(-3,-1,'SUM_OF_LINES',20) is distinct from -1 then
    raise exception 'WEEKLY_SOURCE_S09_NATIVE_SUM_INVALID'; end if;
  begin perform private.weekly_source_invoice_vat_target_pence_v1(0.1,0,'ONCE_ON_TOTAL_HALF_UP',20);
    raise exception 'WEEKLY_SOURCE_S09_FRACTIONAL_PENCE_ADMITTED';
  exception when invalid_parameter_value then
    if sqlerrm<>'WEEKLY_SOURCE_VAT_ROUNDING_MONEY_INVALID' then raise; end if;
  end;
  pay:=private.weekly_source_invoice_only_vector_v1('PAY',0);
  if pay->>'kind' is distinct from 'PAY' or (pay->>'total_pence')::numeric<>0
    or (pay->>'paid_minutes')::numeric<>0 or (pay->>'break_minutes')::numeric<>0
    or exists(select 1 from jsonb_each_text(pay->'hours') h where h.value::numeric<>0)
    or exists(select 1 from jsonb_each_text(pay->'rates') r where r.value::numeric<>0) then
    raise exception 'WEEKLY_SOURCE_INVOICE_ONLY_NATIVE_PAY_VECTOR_INVALID'; end if;
  begin perform private.weekly_source_invoice_only_vector_v1('PAY',1);
    raise exception 'WEEKLY_SOURCE_INVOICE_ONLY_NONZERO_PAY_ADMITTED';
  exception when invalid_parameter_value then
    if sqlerrm<>'WEEKLY_SOURCE_INVOICE_VECTOR_INVALID' then raise; end if;
  end;
  for helper in select expected.signature,expected.definer,proc.* from (values
    ('private.weekly_source_invoice_vat_target_pence_v1(numeric,numeric,text,numeric)',false),
    ('private.weekly_source_invoice_window_scope_v1(uuid,uuid,uuid,uuid)',true),
    ('private.weekly_source_invoice_rounding_receipt_set_v1(uuid,jsonb)',true),
    ('private.weekly_source_invoice_rounding_suppression_witness_v1(uuid)',true),
    ('private.weekly_source_invoice_window_lock_inventory_v1(uuid,bigint,uuid,uuid)',true),
    ('private.weekly_source_invoice_window_cycle_v1(uuid,uuid)',true),
    ('private.weekly_source_invoice_admission_receipt_map_v1(uuid)',true),
    ('private.weekly_source_invoice_manifest_admission_money_v1(uuid)',true),
    ('private.weekly_source_invoice_rounding_authority_rebuild_v1(uuid)',true),
    ('private.weekly_source_invoice_presentation_expected_source_hash_v1(uuid)',true),
    ('private.weekly_source_invoice_admitted_member_money_v1(uuid,uuid)',true),
    ('private.weekly_source_invoice_receipt_projection_assert_v1(uuid)',true),
    ('private.weekly_source_invoice_receipt_support_v1(uuid)',true),
    ('private.weekly_source_invoice_legacy_render_support_v1(uuid)',true),
    ('private.weekly_source_invoice_movement_window_id_v1(uuid)',true),
    ('private.weekly_source_vat_rounding_movement_insert_v1(uuid,uuid,uuid,uuid)',true)
  ) expected(signature,definer) left join pg_proc proc on proc.oid=to_regprocedure(expected.signature)
  loop
    if helper.oid is null or helper.prosecdef is distinct from helper.definer
      or helper.proowner<>(current_user::regrole)::oid
      or not exists(select 1 from unnest(helper.proconfig) c where c='search_path=pg_catalog, pg_temp')
      or has_function_privilege('anon',helper.oid,'EXECUTE')
      or has_function_privilege('authenticated',helper.oid,'EXECUTE')
      or has_function_privilege('service_role',helper.oid,'EXECUTE') then
      raise exception 'WEEKLY_SOURCE_WINDOW_HELPER_SECURITY_INVALID:%',helper.signature; end if;
  end loop;
end;
$verify$;
