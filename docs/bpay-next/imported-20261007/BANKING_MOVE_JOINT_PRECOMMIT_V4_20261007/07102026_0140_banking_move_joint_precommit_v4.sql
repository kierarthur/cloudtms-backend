-- E-only joint Source/Banking precommit helper candidate. No installation.
-- Source owner has already rederived the full planned request with its real quote.
-- Uses 1157 selected quote only; no alternate financial calculator or history sum.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_source_comparison_case_quote_snapshot_v2(
  p_context jsonb,p_selection jsonb,p_paid numeric,p_entitlement numeric)
returns jsonb language plpgsql immutable security invoker set search_path=pg_catalog,private,public
as $function$
declare cb jsonb:=p_context->'case_basis';ch jsonb;choice jsonb;cc jsonb:=p_selection->'case_choice';
  maps jsonb:='[]';q jsonb;wq jsonb;oq jsonb;s numeric;w numeric;j numeric:=0;o numeric;p numeric;counted integer;
  mode text;native_j numeric:=0;
begin
  if cb is null or cb='null'::jsonb then
    if cc is distinct from 'null'::jsonb or p_selection->'case_hold_choices' is distinct from '[]'::jsonb then
      raise exception 'BPAY_NEXT_COMPARISON_CASE_SELECTION_SCOPE_INVALID' using errcode='22023';end if;
    return jsonb_build_object('case_resolution',null,'case_hold_mappings',maps);
  end if;
  if jsonb_typeof(cc) is distinct from 'object' or jsonb_typeof(p_selection->'case_hold_choices') is distinct from 'array' then
    raise exception 'BPAY_NEXT_COMPARISON_CASE_SELECTION_REVIEW_REQUIRED' using errcode='55000';end if;
  perform private.weekly_source_publication_require_keys_v1(cc,array[
    'selected_mode','source_outstanding_ex_vat','manual_amount_ex_vat','manual_written_off_amount_ex_vat'],'financial.case_choice');
  mode:=cc->>'selected_mode';s:=(cc->>'source_outstanding_ex_vat')::numeric;
  if mode not in ('SUGGESTED_EQUIVALENT_BASIS','MANUAL_AMOUNT') or mode is null or s is null
    or s<0 or s<>round(s,2) or s>=10000000000000000
    or (mode='SUGGESTED_EQUIVALENT_BASIS' and
      (cc->'manual_amount_ex_vat' is distinct from 'null'::jsonb or cc->'manual_written_off_amount_ex_vat' is distinct from 'null'::jsonb))
    or (mode='MANUAL_AMOUNT' and ((cc->>'manual_amount_ex_vat')::numeric is null
      or (cc->>'manual_written_off_amount_ex_vat')::numeric is null
      or (cc->>'manual_amount_ex_vat')::numeric<0 or (cc->>'manual_written_off_amount_ex_vat')::numeric<0
      or (cc->>'manual_amount_ex_vat')::numeric<>round((cc->>'manual_amount_ex_vat')::numeric,2)
      or (cc->>'manual_written_off_amount_ex_vat')::numeric<>round((cc->>'manual_written_off_amount_ex_vat')::numeric,2)
      or ((cb->>'W')::numeric=0 and (cc->>'manual_written_off_amount_ex_vat')::numeric<>0))) then
    raise exception 'BPAY_NEXT_COMPARISON_CASE_ACCEPTED_CHOICE_REQUIRED' using errcode='55000';end if;
  wq:=private.pay_finance_selected_quote_v1(mode::public.pay_finance_component_resolution_mode_enum,
    (cb->>'W')::numeric,cb->>'case_source_channel',p_context#>>'{policy,target_pay_channel}',null,null,
    (cc->>'manual_written_off_amount_ex_vat')::numeric,(p_context#>>'{policy,erni_pct}')::numeric,
    (p_context#>>'{policy,vat_rate_pct}')::numeric,(p_context#>>'{policy,umbrella_vat_chargeable}')::boolean);
  w:=(wq->>'ex')::numeric;
  for ch in select value from jsonb_array_elements(p_context->'active_case_holds') order by value->>'case_hold_id' loop
    select count(*)::integer,jsonb_agg(value)->0 into counted,choice from jsonb_array_elements(p_selection->'case_hold_choices')
      where value->>'case_hold_id'=ch->>'case_hold_id';
    if counted<>1 then raise exception 'BPAY_NEXT_COMPARISON_CASE_HOLD_MAPPING_REVIEW_REQUIRED' using errcode='55000';end if;
    perform private.weekly_source_publication_require_keys_v1(choice,
      array['case_hold_id','selected_mode','manual_amount_ex_vat'],'financial.case_hold_choice');
    if choice->>'selected_mode' not in ('SUGGESTED_EQUIVALENT_BASIS','MANUAL_AMOUNT') or choice->>'selected_mode' is null
      or (choice->>'selected_mode'='SUGGESTED_EQUIVALENT_BASIS' and choice->'manual_amount_ex_vat' is distinct from 'null'::jsonb)
      or (choice->>'selected_mode'='MANUAL_AMOUNT' and ((choice->>'manual_amount_ex_vat')::numeric is null
        or (choice->>'manual_amount_ex_vat')::numeric<0
        or (choice->>'manual_amount_ex_vat')::numeric<>round((choice->>'manual_amount_ex_vat')::numeric,2))) then
      raise exception 'BPAY_NEXT_COMPARISON_CASE_HOLD_MAPPING_REVIEW_REQUIRED' using errcode='55000';end if;
    q:=private.pay_finance_selected_quote_v1((choice->>'selected_mode')::public.pay_finance_component_resolution_mode_enum,
      (ch->>'native_target_ex_vat')::numeric,ch->>'native_target_channel',p_context#>>'{policy,target_pay_channel}',null,null,
      (choice->>'manual_amount_ex_vat')::numeric,(p_context#>>'{policy,erni_pct}')::numeric,
      (p_context#>>'{policy,vat_rate_pct}')::numeric,(p_context#>>'{policy,umbrella_vat_chargeable}')::boolean);
    j:=j+(q->>'ex')::numeric;native_j:=native_j+(ch->>'source_reserved_ex_vat')::numeric;
    maps:=maps||jsonb_build_array(jsonb_build_object('case_hold_id',ch->>'case_hold_id','comparable_recovery_ex_vat',(q->>'ex')::numeric));
  end loop;
  if jsonb_array_length(p_selection->'case_hold_choices')<>jsonb_array_length(maps) or native_j<>(cb->>'J')::numeric then
    raise exception 'BPAY_NEXT_COMPARISON_CASE_HOLD_MAPPING_REVIEW_REQUIRED' using errcode='55000';end if;
  o:=greatest(p_paid-p_entitlement-w,j,0);
  oq:=private.pay_finance_selected_quote_v1(mode::public.pay_finance_component_resolution_mode_enum,s,
    cb->>'case_source_channel',p_context#>>'{policy,target_pay_channel}',null,null,(cc->>'manual_amount_ex_vat')::numeric,
    (p_context#>>'{policy,erni_pct}')::numeric,(p_context#>>'{policy,vat_rate_pct}')::numeric,
    (p_context#>>'{policy,umbrella_vat_chargeable}')::boolean);
  if (oq->>'ex')::numeric is distinct from o or w is null or w<0 or j<0 or o is null
    or abs(o)>=10000000000000000 or abs(w)>=10000000000000000 or abs(j)>=10000000000000000 then
    raise exception 'BPAY_NEXT_COMPARISON_CASE_ROUNDED_QUOTE_REVIEW_REQUIRED' using errcode='55000';end if;
  p:=(cb->>'K')::numeric+(cb->>'W')::numeric+greatest(s,(cb->>'J')::numeric);
  return jsonb_build_object('case_hold_mappings',maps,'case_resolution',jsonb_build_object(
    'case_source_channel',cb->>'case_source_channel','source_outstanding_ex_vat',s,'source_principal_ex_vat',p,
    'target_outstanding_ex_vat',o,'target_written_off_ex_vat',w,'target_active_capacity_ex_vat',j,'selected_mode',mode,
    'target_pay_channel',p_context#>>'{policy,target_pay_channel}','comparison_policy_id',p_context#>>'{policy,comparison_policy_id}',
    'policy_window_id',p_context#>>'{policy,policy_window_id}','effective_pay_date',p_context#>>'{policy,effective_pay_date}',
    'umbrella_vat_chargeable',p_context#>'{policy,umbrella_vat_chargeable}','selected_quote',oq,'written_off_quote',wq,
    'choice',cc,'rounding_proof',jsonb_build_object('source',s,'quoted_target',oq->'ex','required_target',o,'exact',true)));
end $function$;

create or replace function private.bpay_next_source_comparison_quote_snapshot_v2(p_context jsonb,p_selection jsonb)
returns jsonb language plpgsql immutable security invoker
set search_path=pg_catalog,private,public
as $function$
declare
  v_mode text:=p_selection->>'selected_mode'; v_q jsonb; v_qs jsonb:='[]'; v_h jsonb; v_choice jsonb;
  v_paid numeric:=0; v_held numeric:=0; v_amount numeric; v_channel text; v_units numeric;
  v_current jsonb:=p_context->'current_comparison'; v_reuse boolean:=false; v_count integer;
begin
  perform private.weekly_source_publication_require_keys_v1(p_selection,array[
    'component_id','selected_mode','effective_pay_date','umbrella_vat_chargeable',
    'manual_paid_amount_ex_vat','manual_replacement_rate','paid_partition','hold_choices','case_choice','case_hold_choices','reason'],'financial.selection');
  if p_selection->>'component_id' is distinct from p_context->>'component_id'
    or p_selection->>'effective_pay_date' is distinct from p_context#>>'{policy,effective_pay_date}'
    or p_selection->'umbrella_vat_chargeable' is distinct from p_context#>'{policy,umbrella_vat_chargeable}'
    or char_length(btrim(coalesce(p_selection->>'reason',''))) not between 1 and 1024 then
    raise exception 'BPAY_NEXT_COMPARISON_SELECTION_SCOPE_INVALID' using errcode='22023';
  end if;
  if (v_mode<>'MANUAL_AMOUNT' and p_selection->'manual_paid_amount_ex_vat' is distinct from 'null'::jsonb)
    or (v_mode<>'MANUAL_REPLACEMENT_RATE' and
      (p_selection->'manual_replacement_rate' is distinct from 'null'::jsonb or p_selection->'paid_partition' is distinct from 'null'::jsonb))
    or (v_mode in ('REUSE_CURRENT','NATIVE_IDENTITY','SUGGESTED_EQUIVALENT_BASIS') and p_selection->'hold_choices' is distinct from '[]'::jsonb)
    or (v_mode='MANUAL_REPLACEMENT_RATE' and
      ((p_selection->>'manual_replacement_rate')::numeric is null or (p_selection->>'manual_replacement_rate')::numeric<=0
       or (p_selection->>'manual_replacement_rate')::numeric>=10000000000000000))
    or (v_mode='MANUAL_AMOUNT' and
      ((p_selection->>'manual_paid_amount_ex_vat')::numeric is null
       or (p_selection->>'manual_paid_amount_ex_vat')::numeric<>round((p_selection->>'manual_paid_amount_ex_vat')::numeric,2))) then
    raise exception 'BPAY_NEXT_COMPARISON_SELECTION_INPUT_INVALID' using errcode='22023';
  end if;
  if v_mode='REUSE_CURRENT' then
    if v_current is null or v_current='null'::jsonb
      or (v_current->>'qualification_state' is distinct from 'READY' and (
        v_current->>'qualification_state' is distinct from 'REVIEW'
        or v_current->>'review_issue_code' is distinct from 'CASE_WRITEOFF_COMPARISON_REVIEW_REQUIRED'
        or v_current->>'credit_qualification_state' is distinct from 'READY'
        or (v_current->>'comparable_paid_ex_vat')::numeric is null
        or (v_current->>'comparable_held_ex_vat')::numeric is null
        or (v_current->>'comparable_held_ex_vat')::numeric<0))
      or v_current->'basis' is distinct from p_context->'policy' then
      raise exception 'BPAY_NEXT_COMPARISON_CURRENT_BASIS_REVIEW_REQUIRED' using errcode='55000';
    end if;
    v_reuse:=true; v_mode:=v_current->>'selected_mode';
    if v_mode='MANUAL_REPLACEMENT_RATE' and v_current->>'quantity_partition_qualified' is distinct from 'true' then
      raise exception 'BPAY_NEXT_COMPARISON_PARTITION_REVIEW_REQUIRED' using errcode='55000';
    end if;
    v_paid:=(v_current->>'comparable_paid_ex_vat')::numeric;
    v_held:=(v_current->>'comparable_held_ex_vat')::numeric;
    for v_h in select value from jsonb_array_elements(p_context->'active_holds') order by value->>'hold_id' loop
      if v_h->'existing_comparable_reserved_ex_vat' is null or v_h->'existing_comparable_reserved_ex_vat'='null'::jsonb then
        raise exception 'BPAY_NEXT_COMPARISON_HELD_MAPPING_REVIEW_REQUIRED' using errcode='55000';
      end if;
      v_qs:=v_qs||jsonb_build_array(jsonb_build_object('hold_id',v_h->>'hold_id',
        'comparable_reserved_ex_vat',(v_h->>'existing_comparable_reserved_ex_vat')::numeric));
    end loop;
    if coalesce((select sum((value->>'comparable_reserved_ex_vat')::numeric) from jsonb_array_elements(v_qs)),0)<>v_held then
      raise exception 'BPAY_NEXT_COMPARISON_HELD_MAPPING_REVIEW_REQUIRED' using errcode='55000';
    end if;
  else
    if v_mode not in ('NATIVE_IDENTITY','SUGGESTED_EQUIVALENT_BASIS','MANUAL_REPLACEMENT_RATE','MANUAL_AMOUNT') or v_mode is null then
      raise exception 'BPAY_NEXT_COMPARISON_ACCEPTED_CHOICE_REQUIRED' using errcode='55000';
    end if;
    if v_mode='MANUAL_AMOUNT' and p_context->>'component_semantics' is distinct from 'AMOUNT_LED' then
      raise exception 'BPAY_NEXT_COMPARISON_MANUAL_AMOUNT_RATE_BEARING_FORBIDDEN' using errcode='22023';
    end if;
    if v_mode='NATIVE_IDENTITY' and
      (case when p_context#>>'{policy,target_pay_channel}'='PAYE'
        then (p_context->>'native_realised_umbrella')::numeric<>0 or (p_context->>'native_held_umbrella')::numeric<>0
        else (p_context->>'native_realised_paye')::numeric<>0 or (p_context->>'native_held_paye')::numeric<>0 end) then
      raise exception 'BPAY_NEXT_COMPARISON_IDENTITY_CHANNEL_INVALID' using errcode='23514';
    end if;
    if v_mode='MANUAL_REPLACEMENT_RATE' and (p_context->>'component_semantics' is distinct from 'RATE_BEARING'
      or p_selection->'paid_partition' is distinct from p_context->'paid_partition'
      or (p_context->'paid_partition'='null'::jsonb and
        ((p_context->>'native_realised_paye')::numeric<>0 or (p_context->>'native_realised_umbrella')::numeric<>0))) then
      raise exception 'BPAY_NEXT_COMPARISON_PARTITION_REVIEW_REQUIRED' using errcode='55000';
    end if;
    if v_mode='MANUAL_AMOUNT' then
      v_q:=private.pay_finance_selected_quote_v1('MANUAL_AMOUNT',(p_context->>'native_realised_paye')::numeric+
        (p_context->>'native_realised_umbrella')::numeric,p_context->>'seed_source_channel',
        p_context#>>'{policy,target_pay_channel}',null,null,(p_selection->>'manual_paid_amount_ex_vat')::numeric,
        (p_context#>>'{policy,erni_pct}')::numeric,(p_context#>>'{policy,vat_rate_pct}')::numeric,
        (p_context#>>'{policy,umbrella_vat_chargeable}')::boolean);
      v_paid:=(v_q->>'ex')::numeric;
    elsif v_mode='MANUAL_REPLACEMENT_RATE' then
      v_units:=coalesce((p_context#>>'{paid_partition,units}')::numeric,0);
      v_q:=private.pay_finance_selected_quote_v1('MANUAL_REPLACEMENT_RATE',
        (p_context->>'native_realised_paye')::numeric+(p_context->>'native_realised_umbrella')::numeric,
        p_context->>'seed_source_channel',p_context#>>'{policy,target_pay_channel}',v_units,
        (p_selection->>'manual_replacement_rate')::numeric,null,
        (p_context#>>'{policy,erni_pct}')::numeric,(p_context#>>'{policy,vat_rate_pct}')::numeric,
        (p_context#>>'{policy,umbrella_vat_chargeable}')::boolean);
      v_paid:=(v_q->>'ex')::numeric;
    else
      foreach v_channel in array array['PAYE','UMBRELLA'] loop
        v_amount:=(p_context->>case when v_channel='PAYE' then 'native_realised_paye' else 'native_realised_umbrella' end)::numeric;
        v_q:=private.pay_finance_selected_quote_v1('SUGGESTED_EQUIVALENT_BASIS',v_amount,v_channel,
          p_context#>>'{policy,target_pay_channel}',null,null,null,(p_context#>>'{policy,erni_pct}')::numeric,
          (p_context#>>'{policy,vat_rate_pct}')::numeric,(p_context#>>'{policy,umbrella_vat_chargeable}')::boolean);
        v_paid:=v_paid+(v_q->>'ex')::numeric;
      end loop;
    end if;
    if jsonb_typeof(p_selection->'hold_choices') is distinct from 'array' then
      raise exception 'BPAY_NEXT_COMPARISON_HELD_SELECTION_INVALID' using errcode='22023';
    end if;
    for v_h in select value from jsonb_array_elements(p_context->'active_holds') order by value->>'hold_id' loop
      if v_mode in ('MANUAL_AMOUNT','MANUAL_REPLACEMENT_RATE') then
        select count(*)::integer,jsonb_agg(value)->0 into v_count,v_choice
          from jsonb_array_elements(p_selection->'hold_choices') where value->>'hold_id'=v_h->>'hold_id';
        if v_count<>1 then raise exception 'BPAY_NEXT_COMPARISON_HELD_MAPPING_REVIEW_REQUIRED' using errcode='55000'; end if;
        perform private.weekly_source_publication_require_keys_v1(v_choice,
          array['hold_id','manual_amount_ex_vat','partition'],'financial.hold_choice');
        if v_mode='MANUAL_AMOUNT' and ((v_choice->>'manual_amount_ex_vat')::numeric is null
          or (v_choice->>'manual_amount_ex_vat')::numeric<0
          or (v_choice->>'manual_amount_ex_vat')::numeric<>round((v_choice->>'manual_amount_ex_vat')::numeric,2)) then
          raise exception 'BPAY_NEXT_COMPARISON_HELD_SELECTION_INVALID' using errcode='22023';
        end if;
        if v_mode='MANUAL_REPLACEMENT_RATE' and (v_h->'partition'='null'::jsonb or v_choice->'partition' is distinct from v_h->'partition') then
          raise exception 'BPAY_NEXT_COMPARISON_PARTITION_REVIEW_REQUIRED' using errcode='55000';
        end if;
      end if;
      v_q:=private.pay_finance_selected_quote_v1(
        case when v_mode='NATIVE_IDENTITY' then 'SUGGESTED_EQUIVALENT_BASIS' else v_mode end::public.pay_finance_component_resolution_mode_enum,
        (v_h->>'native_reserved_ex_vat')::numeric,v_h->>'native_source_channel',p_context#>>'{policy,target_pay_channel}',
        (v_h#>>'{partition,units}')::numeric,(p_selection->>'manual_replacement_rate')::numeric,
        (v_choice->>'manual_amount_ex_vat')::numeric,(p_context#>>'{policy,erni_pct}')::numeric,
        (p_context#>>'{policy,vat_rate_pct}')::numeric,(p_context#>>'{policy,umbrella_vat_chargeable}')::boolean);
      if (v_q->>'ex')::numeric<0 then raise exception 'BPAY_NEXT_COMPARISON_HELD_SELECTION_INVALID' using errcode='22023'; end if;
      v_held:=v_held+(v_q->>'ex')::numeric;
      v_qs:=v_qs||jsonb_build_array(jsonb_build_object('hold_id',v_h->>'hold_id',
        'comparable_reserved_ex_vat',(v_q->>'ex')::numeric));
    end loop;
    if v_mode in ('MANUAL_AMOUNT','MANUAL_REPLACEMENT_RATE') and
      jsonb_array_length(p_selection->'hold_choices')<>jsonb_array_length(v_qs) then
      raise exception 'BPAY_NEXT_COMPARISON_HELD_SELECTION_INVALID' using errcode='22023';
    end if;
  end if;
  if v_paid is null or v_paid<>round(v_paid,2) or abs(v_paid)>=10000000000000000
    or v_held is null or v_held<>round(v_held,2) or v_held<0 or v_held>=10000000000000000 then
    raise exception 'BPAY_NEXT_COMPARISON_QUOTE_INVALID' using errcode='22023';
  end if;
  return jsonb_build_object('selected_mode',v_mode,'selection_method',case when v_reuse then 'REUSE_CURRENT' else 'ACCEPTED_CHOICE' end,
    'quantity_partition_qualified',v_mode='MANUAL_REPLACEMENT_RATE',
    'comparable_paid_ex_vat',v_paid,'comparable_held_ex_vat',v_held,'hold_mappings',v_qs);
end
$function$;

create or replace function private.bpay_next_source_comparison_validate_plan_snapshot_v2(
  p_planned_request jsonb,p_financial_context jsonb,p_financial_plan jsonb)
returns jsonb language plpgsql immutable security invoker
set search_path=pg_catalog,private,public
as $function$
declare v_c jsonb; v_p jsonb; v_component jsonb; v_quote jsonb;v_case_quote jsonb; v_n numeric; v_count integer; v_hash text;
  v_expected text[];v_actual text[];v_moved text[];v_own text[];v_scope jsonb;v_key text;v_hp numeric;v_hu numeric;
begin
  if p_financial_context->>'schema_version' is distinct from 'BPAY_NEXT_SOURCE_FINANCIAL_CONTEXT_V3'
    or p_financial_plan->>'schema_version' is distinct from 'BPAY_NEXT_SOURCE_FINANCIAL_PLAN_V3'
    or p_financial_context->>'planned_request_sha256' is distinct from
      encode(private.weekly_source_sha256_jsonb_v1('BPAY_NEXT_SOURCE_PLANNED_REQUEST_V2',p_planned_request),'hex')
    or p_financial_plan->>'planned_request_sha256' is distinct from p_financial_context->>'planned_request_sha256'
    or p_financial_plan->>'candidate_id' is distinct from p_planned_request->>'candidate_id'
    or jsonb_typeof(p_financial_context->'components') is distinct from 'array'
    or jsonb_typeof(p_financial_plan->'components') is distinct from 'array'
    or jsonb_array_length(p_financial_context->'components')<>jsonb_array_length(p_financial_plan->'components') then
    raise exception 'BPAY_NEXT_COMPARISON_PLAN_SNAPSHOT_INVALID' using errcode='23514';
  end if;
  -- Moved A identity remains Source's existing meaning. Own B is not a move.
  select array_agg(value#>>'{}' order by value#>>'{}') into v_moved
    from jsonb_array_elements(p_planned_request#>'{control,moved_component_ids}');
  select array_agg(value->>'component_id' order by value->>'component_id') into v_expected
    from jsonb_array_elements(p_planned_request#>'{financial_request,member_entitlements,1,components}');
  select array_agg(value->>'component_id' order by value->>'component_id') into v_actual
    from jsonb_array_elements(p_financial_context->'components');
  select coalesce(array_agg(item order by item),'{}'::text[]) into v_own
    from unnest(v_expected) item where not (item=any(v_moved));
  v_scope:=jsonb_build_object('schema_version','BPAY_NEXT_SOURCE_FINANCIAL_SCOPE_V3',
    'moved_component_ids',to_jsonb(v_moved),'own_b_component_ids',to_jsonb(v_own),
    'comparison_component_ids',to_jsonb(v_expected));
  if v_moved is null or v_expected is null or v_expected is distinct from v_actual
    or cardinality(v_moved)<>(select count(distinct item) from unnest(v_moved) item)
    or cardinality(v_expected)<>(select count(distinct item) from unnest(v_expected) item)
    or not (v_moved<@v_expected)
    or p_financial_context->'financial_scope' is distinct from v_scope then
    raise exception 'BPAY_NEXT_COMPARISON_WHOLE_VECTOR_INVALID' using errcode='23514';
  end if;
  for v_c in select value from jsonb_array_elements(p_financial_context->'components') order by value->>'component_id' loop
    select count(*)::integer,jsonb_agg(value)->0 into v_count,v_p
      from jsonb_array_elements(p_financial_plan->'components') where value->>'source_component_id'=v_c->>'component_id';
    if v_count<>1 then raise exception 'BPAY_NEXT_COMPARISON_PLAN_COMPONENT_INVALID' using errcode='23514'; end if;
    select count(*)::integer,jsonb_agg(value)->0 into v_count,v_component
      from jsonb_array_elements(p_planned_request#>'{financial_request,member_entitlements,1,components}')
      where value->>'component_id'=v_c->>'component_id';
    if v_count<>1 then raise exception 'BPAY_NEXT_COMPARISON_PLANNED_VECTOR_INVALID' using errcode='23514'; end if;
    foreach v_key in array array['native_realised_paye','native_realised_umbrella','native_held_paye','native_held_umbrella'] loop
      if jsonb_typeof(v_c->v_key) is distinct from 'number' or (v_c->>v_key)::numeric<>round((v_c->>v_key)::numeric,2)
        or abs((v_c->>v_key)::numeric)>=10000000000000000
        or (v_key in ('native_held_paye','native_held_umbrella') and (v_c->>v_key)::numeric<0) then
        raise exception 'BPAY_NEXT_COMPARISON_NATIVE_SNAPSHOT_INVALID' using errcode='23514';
      end if;
    end loop;
    select coalesce(sum((value->>'native_reserved_ex_vat')::numeric) filter(where value->>'native_source_channel'='PAYE'),0),
      coalesce(sum((value->>'native_reserved_ex_vat')::numeric) filter(where value->>'native_source_channel'='UMBRELLA'),0)
      into v_hp,v_hu from jsonb_array_elements(v_c->'active_holds');
    if (v_hp,v_hu) is distinct from ((v_c->>'native_held_paye')::numeric,(v_c->>'native_held_umbrella')::numeric) then
      raise exception 'BPAY_NEXT_COMPARISON_HELD_SNAPSHOT_INVALID' using errcode='23514';
    end if;
    v_n:=(v_component->>'pay_ex_vat')::numeric;
    v_quote:=private.bpay_next_source_comparison_quote_snapshot_v2(v_c,v_p#>'{plan,quote_json,selection}');
    v_case_quote:=private.bpay_next_source_comparison_case_quote_snapshot_v2(v_c,v_p#>'{plan,quote_json,selection}',
      (v_quote->>'comparable_paid_ex_vat')::numeric,v_n);
    if v_p->'context' is distinct from v_c or v_p#>'{plan,quote_json,context}' is distinct from v_c
      or v_p#>'{plan,quote_json,quote}' is distinct from v_quote
      or v_p#>>'{plan,target_pay_channel}' is distinct from v_c#>>'{policy,target_pay_channel}'
      or v_p#>'{plan,comparison_policy_id}' is distinct from v_c#>'{policy,comparison_policy_id}'
      or v_p#>'{plan,policy_window_id}' is distinct from v_c#>'{policy,policy_window_id}'
      or v_p#>'{plan,effective_pay_date}' is distinct from v_c#>'{policy,effective_pay_date}'
      or v_p#>'{plan,umbrella_vat_chargeable}' is distinct from v_c#>'{policy,umbrella_vat_chargeable}'
      or v_p#>'{plan,target_umbrella_id}' is distinct from v_c#>'{policy,target_umbrella_id}'
      or v_p#>'{plan,selected_mode}' is distinct from v_quote->'selected_mode'
      or v_p#>'{plan,quantity_partition_qualified}' is distinct from v_quote->'quantity_partition_qualified'
      or v_p#>'{plan,comparable_paid_ex_vat}' is distinct from v_quote->'comparable_paid_ex_vat'
      or v_p#>'{plan,comparable_held_ex_vat}' is distinct from v_quote->'comparable_held_ex_vat'
      or v_p#>'{plan,hold_mappings}' is distinct from v_quote->'hold_mappings'
      or v_p#>'{plan,case_resolution}' is distinct from v_case_quote->'case_resolution'
      or v_p#>'{plan,case_hold_mappings}' is distinct from v_case_quote->'case_hold_mappings'
      or (v_p#>>'{plan,approved_source_ex_vat}')::numeric is distinct from v_n
      or (v_p#>>'{plan,selected_residual_ex_vat}')::numeric is distinct from
        v_n-(v_quote->>'comparable_paid_ex_vat')::numeric-(v_quote->>'comparable_held_ex_vat')::numeric
      or (v_p#>>'{plan,paid_excess_ex_vat}')::numeric is distinct from greatest((v_quote->>'comparable_paid_ex_vat')::numeric-v_n,0)
      or v_p#>>'{plan,basis_sha256}' is distinct from encode(private.weekly_source_sha256_jsonb_v1(
        'BPAY_NEXT_SOURCE_COMPARISON_BASIS_V2',jsonb_build_object('policy',v_c->'policy','selected_mode',v_quote->>'selected_mode',
          'selection',v_p#>'{plan,quote_json,selection}')),'hex') then
      raise exception 'BPAY_NEXT_COMPARISON_PLAN_NOT_REDERIVED' using errcode='23514';
    end if;
  end loop;
  v_hash:=encode(private.weekly_source_sha256_jsonb_v1('BPAY_NEXT_SOURCE_FINANCIAL_PLAN_V3',
    jsonb_build_object('financial_context',p_financial_context,'financial_plan',p_financial_plan)),'hex');
  return jsonb_build_object('ok',true,'financial_plan_digest',v_hash);
end
$function$;

create or replace function private.bpay_next_source_comparison_plan_v2(p_actor uuid,p_planned_request jsonb,p_selections jsonb,p_target_channel text)
returns jsonb language plpgsql stable security definer
set search_path=pg_catalog,private,public
as $function$
declare
  v_request jsonb; v_inventory jsonb;v_inventory_a jsonb;v_inventory_b jsonb;v_scope jsonb;v_entry record;
  v_moved uuid[];v_actual uuid[];v_own uuid[];v_all uuid[];v_destination uuid[];v_component jsonb;v_original jsonb;
  v_context jsonb; v_contexts jsonb:='[]'; v_plan jsonb; v_plans jsonb:='[]'; v_selection jsonb; v_quote jsonb;v_case_quote jsonb;
  v_binding private.bpay_next_source_component_binding%rowtype;
  v_version private.bpay_next_source_component_binding_version%rowtype;
  v_book private.bpay_next_position%rowtype; v_old private.bpay_next_source_comparison_receipt%rowtype;
  v_line private.bpay_next_approved_line%rowtype; v_seed_line private.bpay_next_approved_line%rowtype;
  v_seed_effect private.bpay_next_financial_effect%rowtype; v_rates jsonb; v_partition jsonb;
  v_paid_origin private.bpay_next_work_collection%rowtype;v_paid_effect_id uuid;v_paid_line_id uuid;
  v_paid_channel text;v_partition_kind text;
  v_final public.weekly_source_final_revisions%rowtype; v_cycle public.weekly_source_cycles%rowtype; v_group public.weekly_source_groups%rowtype;
  v_policy uuid; v_window private.bpay_next_valuation_policy_window%rowtype; v_winners bigint;
  v_target text; v_date date; v_vat boolean; v_real_vat boolean;v_recipient uuid; v_h record; v_holds jsonb; v_hpartition jsonb; v_last uuid;
  v_page integer; v_hp numeric; v_hu numeric; v_units numeric; v_rate_count integer; v_count integer;
  v_existing numeric; v_basis uuid; v_old_json jsonb;v_cb jsonb;v_cholds jsonb;v_ch record;v_cj numeric; v_p numeric; v_hamount numeric; v_n numeric;
  v_fc jsonb; v_fp jsonb; v_valid jsonb; v_method text; v_class text; v_hash text;
begin
  if coalesce(current_setting('request.jwt.claim.role',true),nullif(current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception 'WEEKLY_SOURCE_SERVICE_ROLE_REQUIRED' using errcode='42501';
  end if;
  v_request:=private.weekly_source_publication_request_canonical_v1(p_planned_request,'IMMEDIATE',null);
  if p_actor is null or p_planned_request#>>'{control,bundle_kind}' is distinct from 'CROSS_CONTRACT_A_B'
    or jsonb_array_length(v_request->'member_root_ids')<>2
    or jsonb_typeof(p_selections) is distinct from 'array'
    or jsonb_array_length(v_request#>'{financial_request,member_entitlements,0,components}')<>0 then
    raise exception 'BPAY_NEXT_COMPARISON_WHOLE_PAIR_REQUIRED' using errcode='22023';
  end if;
  select f.* into strict v_final from public.weekly_source_final_revisions f
    where f.id=(v_request#>>'{financial_request,source_revision,final_revision_id}')::uuid and f.state='CURRENT';
  select c.* into strict v_cycle from public.weekly_source_cycles c where c.id=v_final.source_cycle_id;
  select g.* into strict v_group from public.weekly_source_groups g where g.id=v_cycle.source_group_id;
  perform private.weekly_source_office_authority_v1(p_actor,'APPROVE_PROTECTED_PAY',v_group.id,
    (select client_id from public.contracts where id=(v_request#>>'{financial_request,contract_choices,0,contract_id}')::uuid),v_cycle.finalisation_week_ending);
  perform private.weekly_source_office_authority_v1(p_actor,'APPROVE_PROTECTED_PAY',v_group.id,
    (select client_id from public.contracts where id=(v_request#>>'{financial_request,contract_choices,1,contract_id}')::uuid),v_cycle.finalisation_week_ending);
  v_inventory_a:=private.weekly_source_effective_inventory_v1((v_request#>>'{member_root_ids,0}')::uuid);
  v_inventory_b:=private.weekly_source_effective_inventory_v1((v_request#>>'{member_root_ids,1}')::uuid);
  if v_inventory_a->>'ok' is distinct from 'true' or v_inventory_b->>'ok' is distinct from 'true'
    or jsonb_typeof(v_inventory_a->'components') is distinct from 'array'
    or jsonb_typeof(v_inventory_b->'components') is distinct from 'array' then
    raise exception 'BPAY_NEXT_COMPARISON_CURRENT_INVENTORY_REVIEW_REQUIRED' using errcode='55000';
  end if;
  select array_agg((value#>>'{}')::uuid order by value#>>'{}') into v_moved
    from jsonb_array_elements(p_planned_request#>'{control,moved_component_ids}');
  select array_agg((value->>'component_id')::uuid order by value->>'component_id') into v_actual
    from jsonb_array_elements(v_inventory_a->'components');
  select coalesce(array_agg((value->>'component_id')::uuid order by value->>'component_id'),'{}'::uuid[]) into v_own
    from jsonb_array_elements(v_inventory_b->'components');
  select array_agg(item order by item) into v_all from unnest(v_actual||v_own) item;
  select array_agg((value->>'component_id')::uuid order by value->>'component_id') into v_destination
    from jsonb_array_elements(v_request#>'{financial_request,member_entitlements,1,components}');
  if v_moved is null or v_moved is distinct from v_actual or (v_actual&&v_own)
    or cardinality(v_all)<>(select count(distinct item) from unnest(v_all) item)
    or v_all is distinct from v_destination
    or cardinality(v_all) is distinct from jsonb_array_length(p_selections)
    or (select count(distinct value->>'component_id') from jsonb_array_elements(p_selections))<>cardinality(v_all) then
    raise exception 'BPAY_NEXT_COMPARISON_WHOLE_VECTOR_INVALID' using errcode='23514';
  end if;
  v_scope:=jsonb_build_object('schema_version','BPAY_NEXT_SOURCE_FINANCIAL_SCOPE_V3',
    'moved_component_ids',to_jsonb(v_moved),'own_b_component_ids',to_jsonb(v_own),
    'comparison_component_ids',to_jsonb(v_all));
  if p_target_channel not in ('PAYE','UMBRELLA') or p_target_channel is null then
    raise exception 'BPAY_NEXT_COMPARISON_POLICY_INPUT_INVALID' using errcode='22023';
  end if;
  if p_target_channel='UMBRELLA' then
    select c.umbrella_id,u.vat_chargeable into strict v_recipient,v_real_vat
      from public.candidates c left join public.umbrellas u on u.id=c.umbrella_id
      where c.id=(v_request->>'candidate_id')::uuid;
    if v_recipient is null or v_real_vat is null then
      raise exception 'BPAY_NEXT_COMPARISON_UMBRELLA_RECIPIENT_REVIEW_REQUIRED' using errcode='55000';
    end if;
  else v_recipient:=null;v_real_vat:=false;end if;
  select current_policy_id into strict v_policy from private.bpay_next_valuation_policy_control where id=1;
  for v_entry in
    select scoped.component,scoped.member_index from (
      select value as component,0 as member_index from jsonb_array_elements(v_inventory_a->'components')
      union all
      select value as component,1 as member_index from jsonb_array_elements(v_inventory_b->'components')
    ) scoped order by scoped.component->>'component_id'
  loop
    v_original:=v_entry.component;
    v_inventory:=case when v_entry.member_index=0 then v_inventory_a else v_inventory_b end;
    select count(*)::integer,jsonb_agg(value)->0 into v_count,v_selection from jsonb_array_elements(p_selections)
      where value->>'component_id'=v_original->>'component_id';
    if v_count<>1 then raise exception 'BPAY_NEXT_COMPARISON_SELECTION_SCOPE_INVALID' using errcode='22023'; end if;
    select count(*)::integer,jsonb_agg(value)->0 into v_count,v_component
      from jsonb_array_elements(v_request#>'{financial_request,member_entitlements,1,components}')
      where value->>'component_id'=v_original->>'component_id';
    if v_count<>1 then raise exception 'BPAY_NEXT_COMPARISON_PLANNED_VECTOR_INVALID' using errcode='23514'; end if;
    select b.* into strict v_binding from private.bpay_next_source_component_binding b
      where b.candidate_id=(v_request->>'candidate_id')::uuid and b.source_component_id=(v_original->>'component_id')::uuid;
    select v.* into strict v_version from private.bpay_next_source_component_binding_version v where v.id=v_binding.current_binding_version_id;
    select p.* into strict v_book from private.bpay_next_position p
      where p.work_id=v_binding.canonical_work_id and p.component_key=v_binding.canonical_component_key;
    if v_book.source_book_state<>'READY' or v_book.source_book_revision<1 or v_version.component_state<>'PRESENT'
      or v_version.tax_treatment is null or v_version.head_id::text is distinct from v_inventory->>'head_id'
      or v_version.approval_component_key is distinct from v_binding.canonical_component_key then
      raise exception 'BPAY_NEXT_COMPARISON_CURRENT_BOOK_REVIEW_REQUIRED' using errcode='55000';
    end if;
    select a.* into strict v_line from private.bpay_next_approved_line a where a.id=v_version.approved_line_id;
    v_class:=case when v_original->>'component_kind'='WORKED_TIME' or
      (v_original->'unit_count' is not null and v_original->'unit_count'<>'null'::jsonb) then 'RATE_BEARING'
      when v_original->>'component_kind' in ('SOURCE_FIXED_EXPENSE','EXPENSE') then 'AMOUNT_LED' else 'UNQUALIFIED' end;
    if v_class='UNQUALIFIED' or v_line.tax_treatment is distinct from v_version.tax_treatment then
      raise exception 'BPAY_NEXT_COMPARISON_CLASSIFICATION_REVIEW_REQUIRED' using errcode='55000';
    end if;
    -- Source wrapper supplies this exact genuine accepted pricing-context channel.
    -- The pure planned request deliberately has no invented channel extension.
    v_target:=p_target_channel;
    v_date:=(v_selection->>'effective_pay_date')::date; v_vat:=v_real_vat;
    if (v_selection->>'umbrella_vat_chargeable')::boolean is distinct from v_real_vat then
      raise exception 'BPAY_NEXT_COMPARISON_VAT_RECIPIENT_CHANGED' using errcode='40001';
    end if;
    if v_version.tax_treatment<>'TAXABLE' and v_selection->>'selected_mode'<>'REUSE_CURRENT' then
      raise exception 'BPAY_NEXT_COMPARISON_NON_TAXABLE_POLICY_REVIEW_REQUIRED' using errcode='55000';
    end if;
    if v_target not in ('PAYE','UMBRELLA') or v_target is null or not isfinite(v_date)
      or v_date is null or v_vat is null or (v_target='PAYE' and v_vat) then
      raise exception 'BPAY_NEXT_COMPARISON_POLICY_INPUT_INVALID' using errcode='22023'; end if;
    select w.* into v_window from private.bpay_next_valuation_policy_window w where w.policy_id=v_policy
      and v_date>=w.date_from and v_date<=coalesce(w.date_to,'infinity'::date)
      order by w.date_from desc,w.source_window_id desc limit 1;
    if not found then raise exception 'BPAY_NEXT_TARGET_POLICY_WINDOW_MISSING' using errcode='23514'; end if;
    select count(*) into v_winners from private.bpay_next_valuation_policy_window w
      where w.policy_id=v_policy and w.date_from=v_window.date_from and v_date<=coalesce(w.date_to,'infinity'::date);
    if v_winners<>1 then raise exception 'BPAY_NEXT_TARGET_POLICY_WINDOW_AMBIGUOUS' using errcode='23514'; end if;
    v_old_json:='null'::jsonb; v_basis:=null;
    if v_book.source_comparison_receipt_id is not null then
      select r.* into strict v_old from private.bpay_next_source_comparison_receipt r where r.id=v_book.source_comparison_receipt_id;
      if v_old.source_book_revision<>v_book.source_book_revision or
        (v_old.native_realised_paye,v_old.native_realised_umbrella,v_old.native_held_paye,v_old.native_held_umbrella)
        is distinct from (v_book.native_realised_paye,v_book.native_realised_umbrella,v_book.native_held_paye,v_book.native_held_umbrella)
        or v_old.comparison_revision<>v_book.source_comparison_revision then
        raise exception 'BPAY_NEXT_COMPARISON_CURRENT_SNAPSHOT_INVALID' using errcode='23514'; end if;
      v_basis:=v_old.basis_review_id;
      v_old_json:=jsonb_build_object('id',v_old.id,'qualification_state',v_old.qualification_state,'selected_mode',v_old.selected_mode,
        'basis',jsonb_build_object('target_pay_channel',v_old.target_pay_channel,'comparison_policy_id',v_old.comparison_policy_id,
          'policy_window_id',v_old.policy_window_id,'effective_pay_date',v_old.effective_pay_date,'umbrella_vat_chargeable',v_old.umbrella_vat_chargeable,
          'target_umbrella_id',v_old.quote_json#>'{context,policy,target_umbrella_id}',
          'erni_pct',v_window.erni_pct,'vat_rate_pct',v_window.vat_rate_pct),
        'quantity_partition_qualified',coalesce((v_old.quote_json#>>'{quote,quantity_partition_qualified}')::boolean,false),
        'comparable_paid_ex_vat',v_old.comparable_paid_ex_vat,'comparable_held_ex_vat',v_old.comparable_held_ex_vat);
      if v_old.qualification_state='REVIEW'
        and v_old.review_issue_code='CASE_WRITEOFF_COMPARISON_REVIEW_REQUIRED' then
        v_old_json:=private.bpay_next_source_comparison_review_credit_snapshot_v2(v_old.id,v_book.source_book_revision);
        if v_old_json is null or v_old_json='null'::jsonb
          or v_old_json->>'id' is distinct from v_old.id::text
          or v_old_json->>'qualification_state' is distinct from 'REVIEW'
          or v_old_json->>'review_issue_code' is distinct from 'CASE_WRITEOFF_COMPARISON_REVIEW_REQUIRED'
          or v_old_json->>'credit_qualification_state' is distinct from 'READY' then
          raise exception 'BPAY_NEXT_COMPARISON_REVIEW_WORK_CREDIT_UNPROVED' using errcode='55000';
        end if;
      end if;
    end if;
    v_partition:='null'::jsonb;
    -- Paid origin is separate from the immutable approval seed. A normal
    -- ZERO-seeded book's first actual payment is the indexed flat collection.
    -- Only the full immutable actual line and exact native conservation qualify units.
    v_paid_effect_id:=v_book.source_book_seed_effect_id;v_paid_line_id:=v_book.source_book_seed_line_id;
    v_paid_channel:=v_book.source_book_seed_channel;v_partition_kind:='EXACT_FULL_PAID_SEED';
    if v_paid_effect_id is null then
      select origin.* into v_paid_origin from private.bpay_next_work_collection origin
        where origin.work_id=v_binding.canonical_work_id and origin.component_key=v_binding.canonical_component_key
          and origin.candidate_id=v_binding.candidate_id;
      if found and v_paid_origin.paid_basis_qualified and
        private.bpay_next_work_collection_source_original_v2(v_paid_origin) then
        v_paid_effect_id:=v_paid_origin.original_effect_id;v_paid_line_id:=v_paid_origin.original_approved_line_id;
        v_paid_channel:=v_paid_origin.original_source_pay_channel;v_partition_kind:='EXACT_FULL_PAID_ORIGIN';
      end if;
    end if;
    if v_paid_effect_id is not null and v_paid_line_id is not null then
      select a.* into strict v_seed_line from private.bpay_next_approved_line a where a.id=v_paid_line_id;
      select f.* into strict v_seed_effect from private.bpay_next_financial_effect f where f.id=v_paid_effect_id;
      select jsonb_agg(jsonb_build_object('bucket',d.bucket,'units',d.approved_hours,'pay_rate',d.source_pay_rate,
        'charge_rate',s.charge_rate) order by d.bucket),sum(d.approved_hours),count(*)::integer
        into v_rates,v_units,v_rate_count from private.bpay_next_rate_detail d
        join private.bpay_next_rate_schedule s on s.revision_id=v_seed_line.revision_id and s.rate_family='STANDARD' and s.rate_code=d.bucket
        where d.approved_line_id=v_seed_line.id and d.source_pay_rate is not null and s.charge_rate is not null
          and d.approved_hours>=0 and d.source_pay_rate>0 and s.charge_rate>0;
      if v_rate_count=v_seed_line.expected_rate_detail_count and v_rate_count>0 and v_units>0
        and v_seed_effect.source_disposition_ex_vat=v_seed_line.source_pay_ex_vat and v_seed_effect.source_disposition_ex_vat>0
        and v_seed_line.source_component_id=v_binding.source_component_id
        and (case when v_paid_channel='PAYE' then
          v_book.native_realised_paye=v_seed_effect.source_disposition_ex_vat and v_book.native_realised_umbrella=0
          else v_book.native_realised_umbrella=v_seed_effect.source_disposition_ex_vat and v_book.native_realised_paye=0 end)
        and v_book.native_held_paye=0 and v_book.native_held_umbrella=0 then
        v_partition:=jsonb_build_object('kind',v_partition_kind,'effect_id',v_seed_effect.id,'approved_line_id',v_seed_line.id,
          'source_pay_channel',v_paid_channel,
          'source_amount_ex_vat',v_seed_effect.source_disposition_ex_vat,'units',v_units,'buckets',v_rates);
      end if;
    end if;
    v_holds:='[]'; v_last:=null; v_hp:=0; v_hu:=0;
    loop
      v_page:=0;
      -- Existing ACTIVE (work,key,id) index and basis/hold mapping PK; no historical financial rows.
      for v_h in
        select h.id,h.run_line_id,h.work_id,h.component_key,h.source_reserved_ex_vat,l.source_pay_channel,l.approved_line_id,
          l.source_consumed_ex_vat from private.bpay_next_hold h join private.bpay_next_run_line l on l.id=h.run_line_id
        where h.status='ACTIVE' and h.component_key=v_binding.canonical_component_key
          and h.work_id in (v_binding.canonical_work_id,v_version.approval_work_id) and (v_last is null or h.id>v_last)
        union
        select h.id,h.run_line_id,h.work_id,h.component_key,h.source_reserved_ex_vat,l.source_pay_channel,l.approved_line_id,
          l.source_consumed_ex_vat from private.bpay_next_source_comparison_hold_mapping m
          join private.bpay_next_hold h on h.id=m.hold_id join private.bpay_next_run_line l on l.id=h.run_line_id
        where m.basis_review_id=v_basis and h.status='ACTIVE' and (v_last is null or h.id>v_last)
        order by id limit 64
      loop
        v_page:=v_page+1; v_last:=v_h.id; v_hpartition:='null'::jsonb;
        if v_h.source_reserved_ex_vat<>v_h.source_consumed_ex_vat or v_h.source_reserved_ex_vat<=0 then
          raise exception 'BPAY_NEXT_COMPARISON_ACTIVE_HOLD_INVALID' using errcode='23514'; end if;
        select a.* into strict v_seed_line from private.bpay_next_approved_line a where a.id=v_h.approved_line_id;
        select jsonb_agg(jsonb_build_object('bucket',d.bucket,'units',d.approved_hours,'pay_rate',d.source_pay_rate,
          'charge_rate',s.charge_rate) order by d.bucket),sum(d.approved_hours),count(*)::integer
          into v_rates,v_units,v_rate_count from private.bpay_next_rate_detail d
          join private.bpay_next_rate_schedule s on s.revision_id=v_seed_line.revision_id and s.rate_family='STANDARD' and s.rate_code=d.bucket
          where d.approved_line_id=v_seed_line.id and d.source_pay_rate is not null and s.charge_rate is not null
            and d.approved_hours>=0 and d.source_pay_rate>0 and s.charge_rate>0;
        if v_rate_count=v_seed_line.expected_rate_detail_count and v_rate_count>0 and v_units>0
          and v_h.source_reserved_ex_vat=v_seed_line.source_pay_ex_vat and v_seed_line.source_component_id=v_binding.source_component_id then
          v_hpartition:=jsonb_build_object('kind','EXACT_FULL_HELD_LINE','hold_id',v_h.id,'approved_line_id',v_seed_line.id,
            'source_amount_ex_vat',v_h.source_reserved_ex_vat,'units',v_units,'buckets',v_rates);
        end if;
        select m.comparable_reserved_ex_vat into v_existing from private.bpay_next_source_comparison_hold_mapping m
          where m.basis_review_id=v_basis and m.hold_id=v_h.id;
        v_holds:=v_holds||jsonb_build_array(jsonb_build_object('hold_id',v_h.id,'run_line_id',v_h.run_line_id,
          'work_id',v_h.work_id,'component_key',v_h.component_key,'native_source_channel',v_h.source_pay_channel,
          'native_reserved_ex_vat',v_h.source_reserved_ex_vat,'partition',v_hpartition,'existing_comparable_reserved_ex_vat',v_existing));
        if octet_length(v_holds::text)>32768 then
          raise exception 'BPAY_NEXT_COMPARISON_HELD_CERTIFICATE_CAPACITY_REVIEW_REQUIRED' using errcode='55000'; end if;
        if v_h.source_pay_channel='PAYE' then v_hp:=v_hp+v_h.source_reserved_ex_vat; else v_hu:=v_hu+v_h.source_reserved_ex_vat; end if;
      end loop;
      exit when v_page=0;
    end loop;
    if (v_hp,v_hu) is distinct from (v_book.native_held_paye,v_book.native_held_umbrella) then
      raise exception 'BPAY_NEXT_COMPARISON_HELD_OWNER_SCOPE_REVIEW_REQUIRED' using errcode='55000'; end if;
    v_cb:=private.bpay_next_source_comparison_case_basis_snapshot_v2(v_binding.canonical_work_id,v_binding.canonical_component_key);
    v_cholds:='[]';v_last:=null;v_cj:=0;
    if v_cb is not null and v_cb<>'null'::jsonb then
      loop
        v_page:=0;
        for v_ch in select h.id,h.instruction_id,h.allocation_result_id,h.source_reserved_ex_vat,h.target_amount_ex_vat,
          h.target_amount_vat,h.target_amount_inc_vat,i.source_pay_channel,i.target_pay_channel
          from private.bpay_next_case_hold h
          join private.bpay_next_run_case_instruction i on i.id=h.instruction_id
          join private.bpay_next_case_allocation_result ar on ar.id=h.allocation_result_id and ar.instruction_id=i.id
          where h.case_id=(v_cb->>'case_id')::uuid and h.case_component_id=(v_cb->>'case_component_id')::uuid
            and h.status='ACTIVE' and h.purpose in ('GROSS_RECOVERY','NET_RECOVERY_CAPACITY')
            and (v_last is null or h.id>v_last)
            and h.candidate_id=(v_request->>'candidate_id')::uuid and i.case_id=h.case_id
            and i.case_component_id=h.case_component_id and i.candidate_id=h.candidate_id
            and ar.allocated_source_ex_vat=h.source_reserved_ex_vat
            and (ar.allocated_target_ex_vat,ar.allocated_target_vat,ar.allocated_target_inc_vat)
              =(h.target_amount_ex_vat,h.target_amount_vat,h.target_amount_inc_vat)
          order by h.id limit 64
        loop
          v_page:=v_page+1;v_last:=v_ch.id;v_cj:=v_cj+v_ch.source_reserved_ex_vat;
          if v_ch.source_pay_channel is distinct from v_cb->>'case_source_channel' then
            raise exception 'BPAY_NEXT_COMPARISON_CASE_HOLD_BASIS_REVIEW_REQUIRED' using errcode='55000';end if;
          v_cholds:=v_cholds||jsonb_build_array(jsonb_build_object('case_hold_id',v_ch.id,'instruction_id',v_ch.instruction_id,
            'allocation_result_id',v_ch.allocation_result_id,'source_reserved_ex_vat',v_ch.source_reserved_ex_vat,
            'native_target_channel',v_ch.target_pay_channel,'native_target_ex_vat',v_ch.target_amount_ex_vat,
            'native_target_vat',v_ch.target_amount_vat,'native_target_inc_vat',v_ch.target_amount_inc_vat));
          if octet_length(v_cholds::text)>32768 then
            raise exception 'BPAY_NEXT_COMPARISON_HELD_CERTIFICATE_CAPACITY_REVIEW_REQUIRED' using errcode='55000';end if;
        end loop;
        exit when v_page=0;
      end loop;
      if v_cj is distinct from (v_cb->>'J')::numeric then
        raise exception 'BPAY_NEXT_COMPARISON_CASE_HOLD_OWNER_SCOPE_REVIEW_REQUIRED' using errcode='55000';end if;
    end if;
    v_context:=jsonb_build_object('case_basis',v_cb,'active_case_holds',v_cholds,'component_id',v_original->>'component_id','candidate_id',v_request->>'candidate_id',
      'canonical_work_id',v_binding.canonical_work_id,'canonical_component_key',v_binding.canonical_component_key,
      'source_binding_version_id',v_version.id,'source_book_revision',v_book.source_book_revision,
      'source_comparison_receipt_id',v_book.source_comparison_receipt_id,'source_comparison_revision',v_book.source_comparison_revision,
      'native_realised_paye',v_book.native_realised_paye,'native_realised_umbrella',v_book.native_realised_umbrella,
      'native_held_paye',v_book.native_held_paye,'native_held_umbrella',v_book.native_held_umbrella,
      'seed_source_channel',v_book.source_book_seed_channel,'tax_treatment',v_version.tax_treatment,'component_semantics',v_class,
      'original_component',v_original,'paid_partition',v_partition,'active_holds',v_holds,'current_comparison',v_old_json,
      'policy',jsonb_build_object('target_pay_channel',v_target,'comparison_policy_id',v_policy,'policy_window_id',v_window.source_window_id,
        'effective_pay_date',v_date,'umbrella_vat_chargeable',v_vat,'target_umbrella_id',v_recipient,
        'erni_pct',v_window.erni_pct,'vat_rate_pct',v_window.vat_rate_pct));
    v_quote:=private.bpay_next_source_comparison_quote_snapshot_v2(v_context,v_selection);
    v_p:=(v_quote->>'comparable_paid_ex_vat')::numeric; v_hamount:=(v_quote->>'comparable_held_ex_vat')::numeric;
    v_n:=(v_component->>'pay_ex_vat')::numeric;
    v_case_quote:=private.bpay_next_source_comparison_case_quote_snapshot_v2(v_context,v_selection,v_p,v_n);
    v_plan:=(v_context->'policy')-'erni_pct'-'vat_rate_pct'||jsonb_build_object(
      'selected_mode',v_quote->>'selected_mode','quantity_partition_qualified',v_quote->'quantity_partition_qualified',
      'comparable_paid_ex_vat',v_p,'comparable_held_ex_vat',v_hamount,
      'approved_source_ex_vat',v_n,'selected_residual_ex_vat',v_n-v_p-v_hamount,'paid_excess_ex_vat',greatest(v_p-v_n,0),
      'hold_mappings',v_quote->'hold_mappings','case_resolution',v_case_quote->'case_resolution',
      'case_hold_mappings',v_case_quote->'case_hold_mappings','reason',v_selection->>'reason',
      'quote_json',jsonb_build_object('schema_version','BPAY_NEXT_SOURCE_SELECTED_QUOTE_V2','context',v_context,'selection',v_selection,'quote',v_quote),
      'basis_sha256',encode(private.weekly_source_sha256_jsonb_v1('BPAY_NEXT_SOURCE_COMPARISON_BASIS_V2',
        jsonb_build_object('policy',v_context->'policy','selected_mode',v_quote->>'selected_mode','selection',v_selection)),'hex'));
    if octet_length((v_plan->'quote_json')::text)>32768 then
      raise exception 'BPAY_NEXT_COMPARISON_HELD_CERTIFICATE_CAPACITY_REVIEW_REQUIRED' using errcode='55000'; end if;
    v_contexts:=v_contexts||jsonb_build_array(v_context);
    v_plans:=v_plans||jsonb_build_array(jsonb_build_object('source_component_id',v_original->>'component_id','context',v_context,'plan',v_plan));
  end loop;
  v_hash:=encode(private.weekly_source_sha256_jsonb_v1('BPAY_NEXT_SOURCE_PLANNED_REQUEST_V2',p_planned_request),'hex');
  v_fc:=jsonb_build_object('schema_version','BPAY_NEXT_SOURCE_FINANCIAL_CONTEXT_V3','candidate_id',v_request->>'candidate_id',
    'actor_user_id',p_actor,'planned_request_sha256',v_hash,'financial_scope',v_scope,'components',v_contexts);
  v_fp:=jsonb_build_object('schema_version','BPAY_NEXT_SOURCE_FINANCIAL_PLAN_V3','candidate_id',v_request->>'candidate_id',
    'planned_request_sha256',v_hash,'components',v_plans);
  v_valid:=private.bpay_next_source_comparison_validate_plan_snapshot_v2(p_planned_request,v_fc,v_fp);
  return jsonb_build_object('financial_context',v_fc,'financial_plan',v_fp,'financial_plan_digest',v_valid->>'financial_plan_digest');
end
$function$;

create or replace function private.bpay_next_source_comparison_register_review_v2(
  p_source_receipt_id uuid,p_source_component_id uuid,p_review_certificate_id uuid)
returns uuid language plpgsql volatile security definer
set search_path=pg_catalog,private,public
as $function$
declare v_w private.weekly_source_destination_reprice_witness_v1%rowtype;
  v_r private.weekly_source_entitlement_publication_receipts%rowtype;
  v_b private.bpay_next_source_component_binding_version%rowtype;
  v_a private.bpay_next_approved_line%rowtype; v_request jsonb; v_item jsonb; v_component jsonb;
  v_context jsonb; v_plan jsonb; v_valid jsonb; v_count integer;
begin
  select w.* into strict v_w from private.weekly_source_destination_reprice_witness_v1 w where w.id=p_review_certificate_id;
  select r.* into strict v_r from private.weekly_source_entitlement_publication_receipts r where r.id=p_source_receipt_id;
  select b.* into strict v_b from private.bpay_next_source_component_binding_version b
    where b.source_receipt_id=p_source_receipt_id and b.source_component_id=p_source_component_id;
  v_request:=v_w.accepted_request_json;
  -- Remove only the certificate pointer and server decision-time acknowledgement:
  -- financial snapshot binds the exact server-priced pre-accept quote.request.
  v_request:=v_request#-'{financial_request,contract_choices,1,destination_reprice}';
  v_request:=jsonb_set(v_request,'{control,whole_root_office_review}',v_w.context_json#>'{base_request,control,whole_root_office_review}');
  v_valid:=private.bpay_next_source_comparison_validate_plan_snapshot_v2(v_request,
    v_w.context_json->'financial_context',v_w.context_json->'financial_plan');
  if v_valid->>'financial_plan_digest' is distinct from v_w.context_json->>'financial_plan_digest'
    or (v_r.decision_bundle_id,v_r.bundle_revision,v_r.candidate_id,v_r.decided_by_user_id)
      is distinct from (v_w.decision_bundle_id,v_w.bundle_revision,(v_w.accepted_request_json->>'candidate_id')::uuid,v_w.actor_user_id)
    or v_b.request_digest is distinct from v_r.request_digest or v_b.decision_bundle_id<>v_r.decision_bundle_id
    or v_b.bundle_revision<>v_r.bundle_revision or v_b.component_state<>'PRESENT'
    or not exists(select 1 from private.bpay_next_source_component_binding b where b.current_binding_version_id=v_b.id
      and b.canonical_work_id=v_b.canonical_work_id and b.canonical_component_key=v_b.canonical_component_key)
    or not exists(select 1 from public.weekly_source_entitlement_heads h where h.id=v_b.head_id
      and h.state='COMMITTED_CURRENT' and h.id=any(v_r.head_ids) and h.decision_bundle_id=v_r.decision_bundle_id
      and h.bundle_revision=v_r.bundle_revision)
    or not exists(select 1 from public.weekly_source_entitlement_decision_bundles b
      where b.decision_bundle_id=v_w.decision_bundle_id and b.bundle_revision=v_w.bundle_revision and b.state in ('PROPOSED','COMMITTED')
        and b.decided_by_user_id=v_w.actor_user_id and b.whole_root_reviewed_by_user_id=v_w.actor_user_id) then
    raise exception 'BPAY_NEXT_COMPARISON_SOURCE_CERTIFICATE_SCOPE_INVALID' using errcode='23514';
  end if;
  select count(*)::integer,jsonb_agg(value)->0 into v_count,v_item
    from jsonb_array_elements(v_w.context_json#>'{financial_plan,components}') where value->>'source_component_id'=p_source_component_id::text;
  if v_count<>1 then raise exception 'BPAY_NEXT_COMPARISON_PLAN_COMPONENT_INVALID' using errcode='23514'; end if;
  v_context:=v_item->'context';v_plan:=v_item->'plan';
  select count(*)::integer,jsonb_agg(value)->0 into v_count,v_component
    from jsonb_array_elements(v_w.accepted_request_json#>'{financial_request,member_entitlements,1,components}')
    where value->>'component_id'=p_source_component_id::text;
  select a.* into strict v_a from private.bpay_next_approved_line a where a.id=v_b.approved_line_id;
  if v_count<>1 or v_b.canonical_work_id::text is distinct from v_context->>'canonical_work_id'
    or v_b.canonical_component_key is distinct from v_context->>'canonical_component_key'
    or v_b.approval_source_channel is distinct from v_plan->>'target_pay_channel'
    or v_a.source_pay_ex_vat is distinct from (v_plan->>'approved_source_ex_vat')::numeric
    or v_a.source_pay_ex_vat is distinct from (v_component->>'pay_ex_vat')::numeric
    or v_a.source_component_id is distinct from p_source_component_id
    or v_a.tax_treatment is distinct from v_context->>'tax_treatment'
    or v_b.component_sha256 is distinct from private.weekly_source_publication_request_digest_v1(
      private.weekly_source_publication_component_content_v1(v_component)) then
    raise exception 'BPAY_NEXT_COMPARISON_ACTUAL_APPROVAL_NOT_PLANNED' using errcode='23514';
  end if;
  -- Core locks current canonical position and proves exact input revisions/native
  -- tuple, or the same accepted PENDING basis carried by actual known operations.
  -- No future APPLIED status is fabricated here; preparation owns that later gate.
  return private.bpay_next_source_comparison_register_core_v2(v_b.id,p_review_certificate_id,v_context,v_plan);
end
$function$;

alter function private.bpay_next_source_comparison_case_quote_snapshot_v2(jsonb,jsonb,numeric,numeric) owner to postgres;
alter function private.bpay_next_source_comparison_quote_snapshot_v2(jsonb,jsonb) owner to postgres;
alter function private.bpay_next_source_comparison_validate_plan_snapshot_v2(jsonb,jsonb,jsonb) owner to postgres;
alter function private.bpay_next_source_comparison_plan_v2(uuid,jsonb,jsonb,text) owner to postgres;
alter function private.bpay_next_source_comparison_register_review_v2(uuid,uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_source_comparison_case_quote_snapshot_v2(jsonb,jsonb,numeric,numeric),
  private.bpay_next_source_comparison_quote_snapshot_v2(jsonb,jsonb),
  private.bpay_next_source_comparison_validate_plan_snapshot_v2(jsonb,jsonb,jsonb),
  private.bpay_next_source_comparison_plan_v2(uuid,jsonb,jsonb,text),
  private.bpay_next_source_comparison_register_review_v2(uuid,uuid,uuid) from public,anon,authenticated,service_role;
commit;
