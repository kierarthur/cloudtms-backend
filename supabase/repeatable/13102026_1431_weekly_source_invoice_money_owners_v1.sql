-- F5/F6 invoice-only owners. Existing Weekly pay/rate/Banking functions remain
-- their authorities. All money is read from immutable staged/revision facts.
\set ON_ERROR_STOP on
begin;

create or replace function private.weekly_source_billing_movement_hash_v1(p_state jsonb)
returns bytea language plpgsql immutable set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  if jsonb_typeof(p_state) is distinct from 'object' or exists(select 1 from jsonb_object_keys(p_state) k
    where k not in ('transition_id','nhsp_upload_row_id','expense_authority_generation_id','self_bill_report_row_id','vat_rounding_revision_id',
      'final_revision_id','finalisation_cycle_id','actual_client_id','candidate_id','contract_id','work_event_id','movement_role',
      'correction_unit_id','prior_movement_id','source_profile_kind','source_line_kind','source_facts','pay_vector','charge_vector',
      'total_pay_pence','total_pay_ex_vat','calculated_comparison_charge_pence','source_validation_charge_pence',
      'invoice_presentation_charge_pence','vat_rate_pct','vat_amount','total_inc_vat','price_check_result','price_check_fingerprint',
      'charge_acceptance_id','mapping_rate_policy_fingerprint','invoice_timesheet_id','vector')) then
    raise exception 'WEEKLY_SOURCE_BILLING_HASH_INPUT_INVALID' using errcode='22023';
  end if;
  -- Keep the existing domain and each old producer's exact JSON field set.
  -- Newly present origins are added by their producer, never by NULL backfill.
  return private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_BILLING_MOVEMENT_V1',p_state);
end;
$function$;

create or replace function private.weekly_source_invoice_movement_money_v1(p_movement_id uuid,p_invoice_hold_release_id uuid default null)
returns jsonb language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare m public.weekly_source_billing_movements%rowtype; s public.weekly_source_upload_rows%rowtype;
  receipt public.weekly_source_invoice_hold_releases%rowtype; release_id uuid:=p_invoice_hold_release_id;
  ids uuid[]; rules jsonb; authority jsonb;
begin
  select * into strict m from public.weekly_source_billing_movements where weekly_source_billing_movements.id=p_movement_id;
  if release_id is null then
    -- Historical money follows its exact admitted presentation. An unadmitted
    -- live receipt or today's settings never reprices a prior movement.
    select array_agg(distinct p.invoice_hold_release_id) into ids
      from public.weekly_source_invoice_line_bindings b join public.weekly_source_invoice_presentation_lines p on p.id=b.presentation_line_id
      where b.billing_movement_id=m.id and b.state='CURRENT' and p.invoice_hold_release_id is not null
        and m.movement_role in ('POSITIVE','REPLACEMENT') and m.source_line_kind in ('SOURCE_ORDINARY','SOURCE_REPLACEMENT','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION');
    if cardinality(ids)>1 then raise exception 'WEEKLY_SOURCE_INVOICE_RELEASE_PRESENTATION_AMBIGUOUS' using errcode='55000'; end if;
    release_id:=ids[1];
    if release_id is null and exists(select 1 from public.weekly_source_invoice_line_bindings old_binding
      join public.weekly_source_invoice_presentation_lines old_presentation on old_presentation.id=old_binding.presentation_line_id
      where old_binding.billing_movement_id=m.id and old_presentation.invoice_hold_release_id is not null) then
      raise exception 'WEEKLY_SOURCE_INVOICE_RELEASE_HISTORICAL_IDENTITY_UNAVAILABLE' using errcode='55000'; end if;
  end if;
  if release_id is null then return jsonb_build_object('net_pence',m.invoice_presentation_charge_pence,
    'vat_pence',(m.vat_amount*100)::bigint,'gross_pence',(m.total_inc_vat*100)::bigint,'release_authority',null); end if;
  select * into strict receipt from public.weekly_source_invoice_hold_releases where weekly_source_invoice_hold_releases.id=release_id;
  select * into strict s from public.weekly_source_upload_rows where weekly_source_upload_rows.id=receipt.upload_row_id;
  rules:=private.weekly_source_frozen_rule_values_v1(m.final_revision_id);
  if receipt.hold_kind<>'VAT_FIT' or m.source_facts_json->>'upload_row_id' is distinct from s.id::text
    or m.source_profile_kind<>'HEALTHROSTER_ACTUAL_ROWS'
    or not ((m.source_line_kind in ('SOURCE_ORDINARY','SOURCE_REPLACEMENT','SOURCE_REBILL_LINE') and m.movement_role in ('POSITIVE','REPLACEMENT'))
      or (m.source_line_kind='SOURCE_INVOICE_CORRECTION' and m.movement_role='REPLACEMENT'))
    or rules is null or rules->>'invoice_sender' is distinct from 'CLIENT_SELF_BILL'
    or rules->>'invoice_amount_basis' is distinct from 'SOURCE_AMOUNT'
    or rules->>'source_amount_basis' is distinct from 'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY'
    or s.source_shift_charge_pence is distinct from receipt.net_pence or m.invoice_presentation_charge_pence is distinct from receipt.net_pence
    or s.source_gross_pence is null or s.source_vat_pence is null
    or abs(5::numeric*s.source_gross_pence-6::numeric*s.source_shift_charge_pence)<=5
    or receipt.vat_pence is distinct from (case receipt.action when 'BILL_AT_NET_PLUS_20' then round(receipt.net_pence::numeric*20/100)::bigint
      when 'BILL_AS_FILE' then s.source_vat_pence else null end)
    or receipt.gross_pence is distinct from receipt.net_pence+receipt.vat_pence
    or receipt.release_hash is distinct from private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_INVOICE_HOLD_RELEASE_V1',jsonb_build_object(
      'upload_row_id',s.id,'source_row_hash',encode(s.normalised_row_hash,'hex'),'hold_kind',receipt.hold_kind,'action',receipt.action,
      'net_pence',receipt.net_pence,'vat_pence',receipt.vat_pence,'gross_pence',receipt.gross_pence,'actor_user_id',receipt.actor_user_id)) then
    raise exception 'WEEKLY_SOURCE_INVOICE_RELEASE_AUTHORITY_INVALID' using errcode='55000'; end if;
  authority:=jsonb_build_object('invoice_hold_release_id',receipt.id,'release_hash',encode(receipt.release_hash,'hex'),
    'upload_row_id',s.id,'source_row_hash',encode(s.normalised_row_hash,'hex'),'original_movement_id',m.id,
    'original_movement_hash',encode(m.movement_economic_hash,'hex'),'net_pence',receipt.net_pence,
    'vat_pence',receipt.vat_pence,'gross_pence',receipt.gross_pence);
  return jsonb_build_object('net_pence',receipt.net_pence,'vat_pence',receipt.vat_pence,'gross_pence',receipt.gross_pence,'release_authority',authority);
end;
$function$;

create or replace function private.weekly_source_invoice_presentation_release_authority_v1(p_movement_ids uuid[],p_invoice_hold_release_id uuid)
returns jsonb language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare ids uuid[];
begin
  if p_invoice_hold_release_id is null then return null; end if;
  select array_agg(m.id) into ids from public.weekly_source_billing_movements m
    join public.weekly_source_invoice_hold_releases receipt on receipt.id=p_invoice_hold_release_id
      and m.source_facts_json->>'upload_row_id'=receipt.upload_row_id::text
    where m.id=any(p_movement_ids) and m.movement_role in ('POSITIVE','REPLACEMENT')
      and m.source_line_kind in ('SOURCE_ORDINARY','SOURCE_REPLACEMENT','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION');
  if cardinality(ids) is distinct from 1 then raise exception 'WEEKLY_SOURCE_INVOICE_RELEASE_MEMBER_INVALID' using errcode='55000'; end if;
  return private.weekly_source_invoice_movement_money_v1(ids[1],p_invoice_hold_release_id)->'release_authority';
end;
$function$;

create or replace function private.weekly_source_invoice_presentation_source_hash_v1(p_movement_ids uuid[],p_invoice_hold_release_id uuid default null)
returns bytea language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare members jsonb; original bytea; authority jsonb; count_rows integer;
begin
  select jsonb_agg(jsonb_build_object('movement_id',m.id,'movement_economic_hash',encode(m.movement_economic_hash,'hex')) order by m.id),count(*)
    into members,count_rows from public.weekly_source_billing_movements m where m.id=any(p_movement_ids);
  if count_rows is distinct from cardinality(p_movement_ids) or count_rows not in (1,2) then
    raise exception 'WEEKLY_SOURCE_INVOICE_SOURCE_MEMBER_INVALID' using errcode='55000'; end if;
  original:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_INVOICE_PRESENTATION_SOURCE_V1',members);
  if p_invoice_hold_release_id is null then return original; end if;
  authority:=private.weekly_source_invoice_presentation_release_authority_v1(p_movement_ids,p_invoice_hold_release_id);
  return private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_INVOICE_PRESENTATION_RELEASE_SOURCE_V1',
    jsonb_build_object('original_presentation_source_hash',encode(original,'hex'),'release_authority',authority));
end;
$function$;

create or replace function private.weekly_source_vat_rounding_movement_insert_v1(p_final_revision_id uuid,p_client_id uuid,p_identity_movement_id uuid)
returns uuid language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare revision public.weekly_source_final_revisions%rowtype; identity_row public.weekly_source_billing_movements%rowtype;
  rules jsonb; net numeric; vat numeric; rate numeric; target numeric; difference bigint; rate_count integer;
  id uuid; facts jsonb; pay jsonb; charge jsonb; mapping bytea; h bytea; profile text;
begin
  select * into strict revision from public.weekly_source_final_revisions where weekly_source_final_revisions.id=p_final_revision_id;
  rules:=private.weekly_source_frozen_rule_values_v1(revision.id);
  if rules is null or rules->>'invoice_sender'<>'CLIENT_SELF_BILL' or rules->>'invoice_vat_method'='SUM_OF_LINES' then return null; end if;
  if revision.state not in ('CURRENT','PREPARED') or p_client_id is null then raise exception 'WEEKLY_SOURCE_VAT_ROUNDING_SCOPE_INVALID' using errcode='55000'; end if;
  select m.id into id from public.weekly_source_billing_movements m where m.vat_rounding_revision_id=revision.id and m.actual_client_id=p_client_id;
  if found then return id; end if;
  select coalesce(sum(m.invoice_presentation_charge_pence),0),coalesce(sum(m.vat_amount*100),0),min(m.vat_rate_pct),count(distinct m.vat_rate_pct),min(m.source_profile_kind)
    into net,vat,rate,rate_count,profile from public.weekly_source_billing_movements m
    where m.final_revision_id=revision.id and m.actual_client_id=p_client_id and m.source_line_kind<>'INVOICE_VAT_ROUNDING';
  if rate_count=0 then return null; end if;
  if rate_count<>1 or profile not in ('SELF_BILL_REPORT','HEALTHROSTER_ACTUAL_ROWS','GENERIC_COMPLETE_SNAPSHOT') then
    raise exception 'WEEKLY_SOURCE_VAT_ROUNDING_RATE_AMBIGUOUS' using errcode='55000'; end if;
  target:=case rules->>'invoice_vat_method' when 'ONCE_ON_TOTAL_HALF_UP' then round(net*rate/100)
    when 'ONCE_ON_TOTAL_TRUNCATE' then trunc(net*rate/100) else null end;
  if target is null then raise exception 'WEEKLY_SOURCE_VAT_ROUNDING_METHOD_INVALID' using errcode='55000'; end if;
  difference:=(target-vat)::bigint; if difference=0 then return null; end if;
  if p_identity_movement_id is not null then
    select * into strict identity_row from public.weekly_source_billing_movements where weekly_source_billing_movements.id=p_identity_movement_id;
    if identity_row.final_revision_id<>revision.id or identity_row.actual_client_id<>p_client_id
      or identity_row.source_line_kind='INVOICE_VAT_ROUNDING' or identity_row.candidate_id is null
      or identity_row.contract_id is null or identity_row.work_event_id is null or identity_row.invoice_timesheet_id is null then
      raise exception 'WEEKLY_SOURCE_VAT_ROUNDING_IDENTITY_INVALID' using errcode='55000'; end if;
  end if;
  -- The calling integration must select A(NULL) or B(real last line) only after
  -- hosted admission/render/pay-isolation proof. There is no UI/default choice.
  pay:=private.weekly_source_invoice_only_vector_v1('PAY',0); charge:=private.weekly_source_invoice_only_vector_v1('CHARGE',0);
  facts:=jsonb_build_object('schema_version','WEEKLY_SOURCE_VAT_ROUNDING_FACTS_V1','rule_set_version_id',revision.rule_set_version_id,
    'invoice_vat_method',rules->>'invoice_vat_method','membership_net_pence',net,'membership_vat_pence',vat,
    'target_vat_pence',target,'identity_movement_id',p_identity_movement_id,'description','VAT rounding (matches the client''s self-bill)');
  mapping:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_VAT_ROUNDING_V1',jsonb_build_object('rule_set_version_id',revision.rule_set_version_id));
  h:=private.weekly_source_billing_movement_hash_v1(jsonb_build_object('vat_rounding_revision_id',revision.id,'final_revision_id',revision.id,
    'finalisation_cycle_id',revision.source_cycle_id,'actual_client_id',p_client_id,'candidate_id',identity_row.candidate_id,'contract_id',identity_row.contract_id,
    'work_event_id',identity_row.work_event_id,'movement_role','POSITIVE','prior_movement_id',p_identity_movement_id,'source_profile_kind',profile,
    'source_line_kind','INVOICE_VAT_ROUNDING','source_facts',facts,'pay_vector',pay,'charge_vector',charge,'total_pay_pence',0,
    'calculated_comparison_charge_pence',0,'source_validation_charge_pence',null,'invoice_presentation_charge_pence',0,
    'vat_rate_pct',rate,'vat_amount',difference::numeric/100,'total_inc_vat',difference::numeric/100,'price_check_result','NOT_APPLICABLE',
    'price_check_fingerprint',null,'charge_acceptance_id',null,'mapping_rate_policy_fingerprint',encode(mapping,'hex'),'invoice_timesheet_id',identity_row.invoice_timesheet_id));
  insert into public.weekly_source_billing_movements(vat_rounding_revision_id,final_revision_id,finalisation_cycle_id,actual_client_id,candidate_id,contract_id,work_event_id,
    movement_role,prior_movement_id,source_profile_kind,source_line_kind,source_facts_json,canonical_pay_vector_json,canonical_charge_vector_json,
    total_pay_ex_vat,calculated_comparison_charge_pence,invoice_presentation_charge_pence,vat_rate_pct,vat_amount,total_inc_vat,
    price_check_result,mapping_rate_policy_fingerprint,invoice_timesheet_id,original_cycle_key,movement_economic_hash)
  values(revision.id,revision.id,revision.source_cycle_id,p_client_id,identity_row.candidate_id,identity_row.contract_id,identity_row.work_event_id,
    'POSITIVE',p_identity_movement_id,profile,'INVOICE_VAT_ROUNDING',facts,pay,charge,0,0,0,rate,difference::numeric/100,difference::numeric/100,
    'NOT_APPLICABLE',mapping,identity_row.invoice_timesheet_id,revision.source_cycle_id::text,h) returning public.weekly_source_billing_movements.id into id;
  return id;
end;
$function$;

create or replace function private.weekly_source_rebill_movement_insert_v1(p_transition_id uuid,p_final_revision_id uuid)
returns uuid language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare t public.weekly_source_state_transitions%rowtype; revision public.weekly_source_final_revisions%rowtype;
  current_line public.weekly_source_final_snapshot_lines%rowtype; prior_line public.weekly_source_final_snapshot_lines%rowtype;
  r public.weekly_source_row_resolutions%rowtype; e public.weekly_source_row_economic_snapshots%rowtype;
  rules jsonb; amounts jsonb; id uuid; facts jsonb; pay jsonb; charge jsonb; h bytea; mapping bytea; ts uuid; net bigint; vat bigint;
begin
  select * into strict t from public.weekly_source_state_transitions where weekly_source_state_transitions.id=p_transition_id;
  select * into strict revision from public.weekly_source_final_revisions where weekly_source_final_revisions.id=p_final_revision_id;
  rules:=private.weekly_source_frozen_rule_values_v1(revision.id);
  if rules is null or rules->>'invoice_sender'<>'CLIENT_SELF_BILL' or rules->>'self_bill_source' is distinct from 'SAME_FILE' then return null; end if;
  if t.final_revision_id<>revision.id or t.outcome<>'NO_CHANGE' or t.source_profile_kind<>'HEALTHROSTER_ACTUAL_ROWS' then
    raise exception 'WEEKLY_SOURCE_REBILL_SCOPE_INVALID' using errcode='55000'; end if;
  select * into strict current_line from public.weekly_source_final_snapshot_lines where weekly_source_final_snapshot_lines.id=t.new_snapshot_line_id;
  select * into strict prior_line from public.weekly_source_final_snapshot_lines where weekly_source_final_snapshot_lines.id=t.previous_snapshot_line_id;
  if current_line.work_event_id<>prior_line.work_event_id or current_line.external_event_identity is not distinct from prior_line.external_event_identity
    or current_line.external_event_identity is null or prior_line.external_event_identity is null then
    raise exception 'WEEKLY_SOURCE_REBILL_REKEY_NOT_PROVED' using errcode='55000'; end if;
  select m.id into id from public.weekly_source_billing_movements m where m.transition_id=t.id and m.source_line_kind='SOURCE_REBILL_LINE';
  if found then return id; end if;
  if not coalesce((private.weekly_source_shift_on_self_bill_v1(current_line.work_event_id,revision.source_cycle_id)->>'s25_pay_selected')::boolean,false)
    or not exists(select 1 from public.weekly_source_billing_movements prior_movement
      join public.weekly_source_final_revisions prior_revision on prior_revision.id=prior_movement.final_revision_id and prior_revision.state='CURRENT'
      join public.weekly_source_cycles prior_cycle on prior_cycle.id=prior_movement.finalisation_cycle_id
      join public.weekly_source_invoice_line_bindings binding on binding.billing_movement_id=prior_movement.id and binding.state='CURRENT'
      where prior_movement.work_event_id=current_line.work_event_id and prior_movement.actual_client_id=current_line.client_id
        and prior_movement.placement_state in ('PLACED','ISSUED')
        and prior_cycle.source_group_id=(select source_group_id from public.weekly_source_cycles where weekly_source_cycles.id=revision.source_cycle_id)
        and prior_cycle.finalisation_week_ending<(select finalisation_week_ending from public.weekly_source_cycles where weekly_source_cycles.id=revision.source_cycle_id)) then return null; end if;
  amounts:=private.weekly_source_invoice_line_amounts_v1(current_line.row_resolution_id,revision.id);
  select * into strict r from public.weekly_source_row_resolutions where weekly_source_row_resolutions.id=current_line.row_resolution_id;
  select * into strict e from public.weekly_source_row_economic_snapshots where row_resolution_id=r.id;
  ts:=private.weekly_source_finalisation_lineage_assert_v1(r.id,revision.source_cycle_id);
  net:=(amounts->>'net_pence')::bigint; vat:=(amounts->>'vat_pence')::bigint;
  pay:=private.weekly_source_invoice_only_vector_v1('PAY',0); charge:=private.weekly_source_invoice_only_vector_v1('CHARGE',net);
  mapping:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_MAPPING_RATE_POLICY_V1',jsonb_build_object(
    'row_resolution_id',r.id,'work_event_id',r.work_event_id,'candidate_id',r.candidate_id,'client_id',r.client_id,'contract_id',r.contract_id,
    'contract_and_rate_fingerprint',encode(e.contract_and_rate_fingerprint,'hex'),'effective_policy_fingerprint',encode(e.effective_policy_fingerprint,'hex'),
    'invoice_vat_policy_fingerprint',encode(e.invoice_vat_policy_fingerprint,'hex'),'calculation_fingerprint',encode(e.calculation_fingerprint,'hex')));
  facts:=jsonb_build_object('schema_version','WEEKLY_SOURCE_REBILL_FACTS_V1','snapshot_line_id',current_line.id,'prior_snapshot_line_id',prior_line.id,
    'upload_row_id',current_line.upload_row_id,'bound_row_resolution_id',r.id,'old_request_id',prior_line.external_event_identity,
    'new_request_id',current_line.external_event_identity,'rule_set_version_id',revision.rule_set_version_id,
    'invoice_hold',amounts->'invoice_hold','hold_release_id',amounts->'hold_release_id');
  h:=private.weekly_source_billing_movement_hash_v1(jsonb_build_object('transition_id',t.id,'final_revision_id',revision.id,'finalisation_cycle_id',revision.source_cycle_id,
    'actual_client_id',r.client_id,'candidate_id',r.candidate_id,'contract_id',r.contract_id,'work_event_id',r.work_event_id,'movement_role','POSITIVE',
    'source_profile_kind',t.source_profile_kind,'source_line_kind','SOURCE_REBILL_LINE','source_facts',facts,'pay_vector',pay,'charge_vector',charge,'total_pay_pence',0,
    'calculated_comparison_charge_pence',e.calculated_charge_pence,'source_validation_charge_pence',case when (amounts->>'source_amount')::boolean then net else null end,
    'invoice_presentation_charge_pence',net,'vat_rate_pct',(amounts->>'vat_rate_pct')::numeric,'vat_amount',vat::numeric/100,'total_inc_vat',(net+vat)::numeric/100,
    'price_check_result',amounts->>'price_check_result','price_check_fingerprint',case when (amounts->>'source_amount')::boolean then encode(e.calculation_fingerprint,'hex') else null end,
    'charge_acceptance_id',amounts->'charge_acceptance_id','mapping_rate_policy_fingerprint',encode(mapping,'hex'),'invoice_timesheet_id',ts));
  insert into public.weekly_source_billing_movements(transition_id,final_revision_id,finalisation_cycle_id,actual_client_id,candidate_id,contract_id,work_event_id,
    movement_role,source_profile_kind,source_line_kind,source_facts_json,canonical_pay_vector_json,canonical_charge_vector_json,total_pay_ex_vat,
    calculated_comparison_charge_pence,source_validation_charge_pence,invoice_presentation_charge_pence,vat_rate_pct,vat_amount,total_inc_vat,price_check_result,
    price_check_fingerprint,charge_acceptance_id,mapping_rate_policy_fingerprint,invoice_timesheet_id,original_cycle_key,movement_economic_hash)
  values(t.id,revision.id,revision.source_cycle_id,r.client_id,r.candidate_id,r.contract_id,r.work_event_id,'POSITIVE',t.source_profile_kind,'SOURCE_REBILL_LINE',
    facts,pay,charge,0,e.calculated_charge_pence,case when (amounts->>'source_amount')::boolean then net else null end,net,(amounts->>'vat_rate_pct')::numeric,
    vat::numeric/100,(net+vat)::numeric/100,amounts->>'price_check_result',case when (amounts->>'source_amount')::boolean then e.calculation_fingerprint else null end,
    nullif(amounts->>'charge_acceptance_id','')::uuid,mapping,ts,revision.source_cycle_id::text,h) returning public.weekly_source_billing_movements.id into id;
  return id;
end;
$function$;

create or replace function public.weekly_source_invoice_hold_release_atomic_v1(p_request jsonb)
returns jsonb language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare actor uuid; row_id uuid; expected_hash bytea; expected_version bigint; kind text; choice text;
  s public.weekly_source_upload_rows%rowtype; u public.weekly_source_uploads%rowtype; c public.weekly_source_cycles%rowtype;
  g public.weekly_source_groups%rowtype; scope_row public.weekly_source_report_scopes%rowtype; rules jsonb; v_client_id uuid; existing public.weekly_source_invoice_hold_releases%rowtype;
  net bigint; vat bigint; gross bigint; h bytea; id uuid;
begin
  perform private._weekly_source_settings_assert_request_v1(p_request,array['actor_user_id','upload_row_id','source_cycle_id','report_scope_id','expected_source_row_hash','expected_authority_scope_version','hold_kind','action'],'WEEKLY_SOURCE_INVOICE_HOLD_REQUEST_INVALID');
  begin actor:=(p_request->>'actor_user_id')::uuid; row_id:=(p_request->>'upload_row_id')::uuid;
    expected_hash:=private.weekly_source_finalisation_hex32_v1(p_request->>'expected_source_row_hash','WEEKLY_SOURCE_INVOICE_HOLD_HASH_INVALID');
    expected_version:=(p_request->>'expected_authority_scope_version')::bigint;
    kind:=p_request->>'hold_kind'; choice:=coalesce(p_request->>'action',case when kind='VAT_FIT' then 'BILL_AT_NET_PLUS_20' else 'BILL_AS_FILE' end);
  exception when others then raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_REQUEST_INVALID' using errcode='22023'; end;
  perform private._weekly_source_settings_require_admin_v1(actor);
  if kind is null or choice is null or kind not in ('VAT_FIT','DUPLICATE_REQUEST_ID') or choice not in ('BILL_AT_NET_PLUS_20','BILL_AS_FILE')
    or (kind='DUPLICATE_REQUEST_ID' and choice<>'BILL_AS_FILE') then raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_CHOICE_INVALID' using errcode='22023'; end if;
  select * into strict s from public.weekly_source_upload_rows where weekly_source_upload_rows.id=row_id;
  select * into strict u from public.weekly_source_uploads where weekly_source_uploads.id=s.upload_id;
  select * into strict g from public.weekly_source_groups where weekly_source_groups.id=(select source_group_id from public.weekly_source_cycles where weekly_source_cycles.id=u.source_cycle_id) for update;
  select * into strict c from public.weekly_source_cycles where weekly_source_cycles.id=u.source_cycle_id for update;
  if u.report_scope_id is not null then
    select * into strict scope_row from public.weekly_source_report_scopes where weekly_source_report_scopes.id=u.report_scope_id for update;
    if scope_row.source_cycle_id<>c.id or scope_row.source_group_id<>g.id or scope_row.current_complete_upload_id is distinct from u.id
      or s.row_finalisation_state<>'SOURCE_REPORT_LINE' then raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_REPORT_SCOPE_INVALID' using errcode='55000'; end if;
  elsif c.current_complete_upload_id is distinct from u.id and u.state<>'CORRECTION_READY' then
    raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_SCOPE_STALE' using errcode='40001'; end if;
  if u.report_scope_id is distinct from nullif(p_request->>'report_scope_id','')::uuid or c.id is distinct from nullif(p_request->>'source_cycle_id','')::uuid or s.normalised_row_hash is distinct from expected_hash
    or coalesce(scope_row.version,c.version) is distinct from expected_version or u.state not in ('CURRENT','CORRECTION_READY') or not g.active or g.source_family<>'ROSTER' then
    raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_SCOPE_STALE' using errcode='40001'; end if;
  perform private._weekly_source_settings_deployment_v1(g.agency_id,g.environment);
  v_client_id:=coalesce(scope_row.client_id,c.scope_client_id,nullif(u.file_metadata_json->>'client_id','')::uuid);
  if v_client_id is null or (nullif(u.file_metadata_json->>'client_id','')::uuid is not null and nullif(u.file_metadata_json->>'client_id','')::uuid<>v_client_id) or (c.scope_client_id is not null and nullif(u.file_metadata_json->>'client_id','')::uuid is distinct from c.scope_client_id) then
    raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_CLIENT_SCOPE_INVALID' using errcode='55000'; end if;
  perform private.weekly_source_office_authority_v1(actor,'FINALISE_WEEK',g.id,v_client_id,c.finalisation_week_ending);
  rules:=private.weekly_source_effective_rule_set_v1(v_client_id,c.id)->'values';
  if rules is null or rules->>'invoice_sender'<>'CLIENT_SELF_BILL' or rules->>'invoice_amount_basis'<>'SOURCE_AMOUNT'
    or s.source_money_parse_state<>'VALID' or s.source_shift_charge_pence is null or s.source_gross_pence is null or s.source_vat_pence is null then
    raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_NOT_ELIGIBLE' using errcode='55000'; end if;
  if kind='VAT_FIT' and (rules->>'source_amount_basis'<>'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY'
    or abs(5::numeric*s.source_gross_pence-6::numeric*s.source_shift_charge_pence)<=5) then
    raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_NOT_ELIGIBLE' using errcode='55000'; end if;
  if kind='DUPLICATE_REQUEST_ID' and (u.report_scope_id is null or s.row_finalisation_state<>'SOURCE_REPORT_LINE' or rules->>'self_bill_source' is distinct from 'SEPARATE_FINAL_REPORT' or rules->>'source_amount_basis' is distinct from 'REPORT_LINE_NET') then raise exception 'WEEKLY_SOURCE_INVOICE_DUPLICATE_HOLD_NOT_PROVED' using errcode='55000'; end if;
  if kind='DUPLICATE_REQUEST_ID' and private.weekly_source_self_bill_report_duplicate_prior_v1(s.id) is distinct from true then
    raise exception 'WEEKLY_SOURCE_INVOICE_DUPLICATE_HOLD_NOT_PROVED' using errcode='55000'; end if;
  net:=s.source_shift_charge_pence;
  vat:=case when choice='BILL_AT_NET_PLUS_20' then round(net::numeric*20/100)::bigint else s.source_vat_pence end;
  gross:=case when choice='BILL_AT_NET_PLUS_20' then net+vat else s.source_gross_pence end;
  select * into existing from public.weekly_source_invoice_hold_releases where upload_row_id=s.id and hold_kind=kind;
  if found then
    if existing.action<>choice or existing.net_pence<>net or existing.vat_pence<>vat or existing.gross_pence<>gross then
      raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_RELEASE_REPLAY_CONFLICT' using errcode='40001'; end if;
    return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_INVOICE_HOLD_RELEASE_V1','status','RELEASED','idempotent',true,
      'release_id',existing.id,'upload_row_id',s.id,'hold_kind',kind,'action',choice,'net_pence',net,'vat_pence',vat,'gross_pence',gross);
  end if;
  -- First-invoice purpose authority: admission shares the cycle lock. Preserve
  -- every original movement, snapshot and manifest value and their hashes.
  if exists(select 1 from public.weekly_source_billing_movements m where
      (m.source_facts_json->>'upload_row_id'=s.id::text or m.self_bill_report_row_id=s.id)
      and m.movement_role in ('POSITIVE','REPLACEMENT','STANDALONE_NEGATIVE')
      and (m.placement_state<>'UNPLACED' or exists(select 1 from public.weekly_source_invoice_line_bindings b
        where b.billing_movement_id=m.id and b.state='CURRENT')
        or exists(select 1 from public.weekly_source_invoice_placements p join public.invoices i on i.id=p.invoice_id
          where p.billing_movement_id=m.id and i.status::text='ISSUED')))
    or exists(select 1 from public.weekly_source_final_snapshot_lines line join public.weekly_source_final_revisions r on r.id=line.final_revision_id
      where line.upload_row_id=s.id and r.state='PREPARED') then
    raise exception 'WEEKLY_SOURCE_INVOICE_HOLD_ALREADY_PLACED' using errcode='55000'; end if;
  h:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_INVOICE_HOLD_RELEASE_V1',jsonb_build_object('upload_row_id',s.id,
    'source_row_hash',encode(s.normalised_row_hash,'hex'),'hold_kind',kind,'action',choice,'net_pence',net,'vat_pence',vat,'gross_pence',gross,'actor_user_id',actor));
  insert into public.weekly_source_invoice_hold_releases(upload_row_id,hold_kind,action,net_pence,vat_pence,gross_pence,actor_user_id,release_hash)
    values(s.id,kind,choice,net,vat,gross,actor,h) returning public.weekly_source_invoice_hold_releases.id into id;
  return jsonb_build_object('ok',true,'contract','WEEKLY_SOURCE_INVOICE_HOLD_RELEASE_V1','status','RELEASED','idempotent',false,
    'release_id',id,'upload_row_id',s.id,'hold_kind',kind,'action',choice,'net_pence',net,'vat_pence',vat,'gross_pence',gross);
end;
$function$;

-- Called only by the root's already-authorised full Office workspace reader.
-- This reuses the existing warning rows/selection command shape; Ignore is an
-- immutable acceptance receipt and never changes source invoice or pay money.
create or replace function private.weekly_source_source_amount_warnings_v1(p_cycle_id uuid,p_publication_id uuid)
returns jsonb language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare c public.weekly_source_cycles%rowtype; p public.weekly_source_projection_publications%rowtype;
  u public.weekly_source_uploads%rowtype; rules jsonb; v_client_id uuid; v_generation bigint; rows jsonb; keys jsonb;
  count_rows integer; accepted_count integer; workspace_version text; proof text;
begin
  select * into c from public.weekly_source_cycles where weekly_source_cycles.id=p_cycle_id;
  select * into p from public.weekly_source_projection_publications where weekly_source_projection_publications.id=p_publication_id and source_cycle_id=c.id
    and report_scope_id is null and authority_scope_kind='CYCLE' and state='CURRENT';
  if c.id is null or p.id is null or c.current_projection_publication_id is distinct from p.id then return null; end if;
  select * into u from public.weekly_source_uploads where weekly_source_uploads.id=p.upload_id and source_cycle_id=c.id
    and id=c.current_complete_upload_id and state='CURRENT' and report_scope_id is null;
  if u.id is null then return null; end if;
  v_client_id:=coalesce(c.scope_client_id,nullif(u.file_metadata_json->>'client_id','')::uuid);
  rules:=private.weekly_source_effective_rule_set_v1(v_client_id,c.id)->'values';
  if rules is null or rules->>'invoice_sender' is distinct from 'CLIENT_SELF_BILL'
    or rules->>'invoice_amount_basis' is distinct from 'SOURCE_AMOUNT'
    or rules->>'source_amount_basis' is distinct from 'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY' then return null; end if;
  v_generation:=coalesce(p.projection_generation,p.authority_scope_version);
  with warnings as (
    select charge.id,source_row.id as source_row_id,source_row.work_date,
      coalesce(candidate.display_name,candidate.tms_ref,source_row.source_candidate_identity,'Candidate') as candidate_name,
      charge.source_shift_charge_pence,charge.calculated_segment_charge_pence,charge.source_charge_difference_pence,
      concat_ws(' · ',contract.role,contract.band,contract.display_site) as contract_label,acceptance.id as acceptance_id
    from public.weekly_source_charge_checks charge
    join public.weekly_source_upload_rows source_row on source_row.id=charge.upload_row_id and source_row.upload_id=u.id
    join public.weekly_source_row_resolutions resolution on resolution.id=charge.row_resolution_id
      and resolution.upload_row_id=source_row.id and resolution.generation=v_generation and resolution.client_id=v_client_id and resolution.mapping_state='RESOLVED'
    join public.weekly_source_row_economic_snapshots economic on economic.row_resolution_id=resolution.id
      and economic.calculation_fingerprint=charge.charge_calculation_fingerprint
    join public.candidates candidate on candidate.id=resolution.candidate_id
    join public.contracts contract on contract.id=resolution.contract_id and contract.client_id=resolution.client_id and contract.candidate_id=resolution.candidate_id
    left join public.weekly_source_charge_acceptances acceptance on acceptance.charge_check_id=charge.id
      and acceptance.acceptance_kind='ACCEPTED_DISPARITY' and acceptance.acceptance_policy_fingerprint=private.weekly_source_source_amount_acceptance_policy_fingerprint_v1()
      and acceptance.source_upload_id=u.id and acceptance.source_upload_hash=u.content_sha256
      and acceptance.upload_row_id=source_row.id and acceptance.row_resolution_id=resolution.id and acceptance.contract_id=resolution.contract_id
      and acceptance.source_row_fingerprint=resolution.source_row_fingerprint and acceptance.contract_and_rate_fingerprint=economic.contract_and_rate_fingerprint
      and acceptance.effective_policy_fingerprint=economic.effective_policy_fingerprint and acceptance.charge_calculation_fingerprint=economic.calculation_fingerprint
    where charge.generation=v_generation and charge.comparison_profile_version='SOURCE_INVOICE_MONEY_PENCE_V1'
      and charge.comparison_result='SOURCE_AMOUNT_DIFFERS' and charge.phase_severity='INFORMATION' and charge.blocker_code is null
  ) select count(*)::integer,count(acceptance_id)::integer,
    coalesce(jsonb_agg(jsonb_build_object('warning_key','charge-check:'||id::text,'source_row_id',source_row_id,
      'candidate',candidate_name,'day_date',to_char(work_date,'Dy FMDD Mon YYYY'),
      'source_charge','£'||to_char(source_shift_charge_pence::numeric/100,'FM9999999990.00'),
      'calculated_charge','£'||to_char(calculated_segment_charge_pence::numeric/100,'FM9999999990.00'),
      'difference',case when source_charge_difference_pence<0 then '-£' else '£' end||to_char(abs(source_charge_difference_pence)::numeric/100,'FM9999999990.00'),
      'contract',contract_label,'warning','Rate card expired or wrong Contract rate','warning_tone','warning',
      'status',jsonb_build_object('text',case when acceptance_id is null then '! No hold · Rate difference' else 'Ignored · No hold' end,
        'tone','warning','subtext','The file amount remains the invoice authority','subtone','info'),
      'action_label',case when acceptance_id is null then 'Ignore flag' else 'Ignored' end,
      'accept_eligible',acceptance_id is null,'detail_rows','[]'::jsonb) order by work_date,candidate_name,id),'[]'::jsonb),
    coalesce(jsonb_agg('charge-check:'||id::text order by id) filter(where acceptance_id is null),'[]'::jsonb)
    into count_rows,accepted_count,rows,keys from warnings;
  if count_rows=0 then return null; end if;
  workspace_version:=private.weekly_source_office_workspace_version_v1(c.id,p.id);
  proof:=encode(private.weekly_source_sha256_jsonb_v1('SOURCE_AMOUNT_RATE_WARNING_SELECTION_V1',jsonb_build_object(
    'source_cycle_id',c.id,'projection_publication_id',p.id,'projection_generation',v_generation,
    'workspace_version',workspace_version,'eligible_warning_keys',keys)),'hex');
  return jsonb_build_object('contract','WEEKLY_SOURCE_SOURCE_AMOUNT_WARNINGS_V1','phase','INFORMATION',
    'rows',rows,'total_count',count_rows,'accepted_count',accepted_count,'ready_count',accepted_count,'hard_blocker_count',0,
    'notice',null,'acceptance',case when jsonb_array_length(keys)>0 then jsonb_build_object('enabled',true,
      'action','ACCEPT_NHSP_SOURCE_CHARGES','payload',jsonb_build_object('source_cycle_id',c.id,'projection_publication_id',p.id),
      'selection',jsonb_build_object('key','warning_keys','proof_key','selection_proof','proof',proof)) else null end);
end;
$function$;

create or replace function private.weekly_source_invoice_placement_identity_guard_v1()
returns trigger language plpgsql security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare m public.weekly_source_billing_movements%rowtype;
begin
  select * into strict m from public.weekly_source_billing_movements where weekly_source_billing_movements.id=new.billing_movement_id;
  if new.source_shift_group_id is distinct from m.work_event_id
    or (new.source_shift_group_id is null and not (m.source_line_kind='INVOICE_VAT_ROUNDING'
      or (m.source_line_kind='SELF_BILL_REPORT_LINE' and m.movement_role='STANDALONE_NEGATIVE'))) then
    raise exception 'WEEKLY_SOURCE_INVOICE_PLACEMENT_IDENTITY_INVALID' using errcode='55000'; end if;
  return new;
end;
$function$;
drop trigger if exists weekly_source_invoice_placement_identity_guard on public.weekly_source_invoice_placements;
create trigger weekly_source_invoice_placement_identity_guard before insert or update of billing_movement_id,source_shift_group_id
  on public.weekly_source_invoice_placements for each row execute function private.weekly_source_invoice_placement_identity_guard_v1();

create or replace function private.weekly_source_frozen_rule_values_v1(p_final_revision_id uuid)
returns jsonb language sql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
  select version.values_json from public.weekly_source_final_revisions revision
    join public.weekly_source_cycles cycle on cycle.id=revision.source_cycle_id
    join public.weekly_source_groups source_group on source_group.id=cycle.source_group_id
    join public.weekly_source_rule_set_versions version on version.id=revision.rule_set_version_id
    join public.weekly_source_rule_sets rules on rules.id=version.rule_set_id
      and rules.agency_id=source_group.agency_id and rules.environment=source_group.environment
  where revision.id=p_final_revision_id and source_group.source_family='ROSTER';
$function$;

-- The single shared four-fact selector. The root integrator composes the report
-- arm into this complete function; callers consume the fact they need.
create or replace function private.weekly_source_shift_on_self_bill_v1(p_work_event_id uuid,p_cycle_or_report uuid)
returns jsonb language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare c public.weekly_source_cycles%rowtype; event_fact public.weekly_work_events%rowtype; scope_fact public.weekly_source_report_scopes%rowtype;
  rules jsonb; selected boolean:=false; bound boolean:=false; ready boolean:=false;
  r public.weekly_source_row_resolutions%rowtype; s public.weekly_source_upload_rows%rowtype; u public.weekly_source_uploads%rowtype;
  profile text; count_rows integer; ids uuid[]; v_selection_upload_id uuid; v_selection_publication_id uuid; preparing_count integer;
begin
  -- NULL is the existing legacy/no-Source arm. An explicit but missing or
  -- foreign mutation scope is an error, never four false admission facts.
  if p_work_event_id is null or p_cycle_or_report is null then
    return jsonb_build_object('on_accepted_report',false,'bound_to_finalised_export',false,'invoice_ready',false,'s25_pay_selected',false); end if;
  select * into event_fact from public.weekly_work_events where id=p_work_event_id;
  if not found then raise exception 'WEEKLY_SOURCE_SELF_BILL_EVENT_NOT_FOUND' using errcode='22023'; end if;
  select * into c from public.weekly_source_cycles where weekly_source_cycles.id=p_cycle_or_report;
  if not found then
    select * into scope_fact from public.weekly_source_report_scopes where id=p_cycle_or_report;
    if not found then raise exception 'WEEKLY_SOURCE_SELF_BILL_SCOPE_NOT_FOUND' using errcode='22023'; end if;
    select * into strict c from public.weekly_source_cycles where id=scope_fact.source_cycle_id;
    if scope_fact.client_id is distinct from event_fact.client_id or scope_fact.source_group_id is distinct from c.source_group_id
      or not exists(select 1 from public.weekly_source_groups source_group where source_group.id=c.source_group_id
        and source_group.agency_id=scope_fact.agency_id and source_group.environment=scope_fact.environment) then
      raise exception 'WEEKLY_SOURCE_SELF_BILL_SCOPE_MISMATCH' using errcode='22023'; end if;
  end if;
  if event_fact.first_source_group_id is distinct from c.source_group_id
    or (c.scope_client_id is not null and c.scope_client_id is distinct from event_fact.client_id) then
    raise exception 'WEEKLY_SOURCE_SELF_BILL_SCOPE_MISMATCH' using errcode='22023'; end if;
  rules:=private.weekly_source_effective_rule_set_v1(event_fact.client_id,c.id)->'values';
  if rules->>'invoice_sender'='CLIENT_SELF_BILL' and rules->>'self_bill_source'='SEPARATE_FINAL_REPORT' then
    if scope_fact.id is not null and scope_fact.self_bill_report_id is null then
      raise exception 'WEEKLY_SOURCE_SELF_BILL_REPORT_SCOPE_REQUIRED' using errcode='22023'; end if;
    return private.weekly_source_report_shift_facts_v1(event_fact.id,p_cycle_or_report);
  end if;
  if scope_fact.id is not null and scope_fact.self_bill_report_id is not null then
    raise exception 'WEEKLY_SOURCE_SELF_BILL_REPORT_RULE_MISMATCH' using errcode='22023'; end if;
  if rules is null or rules->>'invoice_sender' is distinct from 'CLIENT_SELF_BILL' or rules->>'self_bill_source' is distinct from 'SAME_FILE' then
    return jsonb_build_object('on_accepted_report',false,'bound_to_finalised_export',false,'invoice_ready',false,'s25_pay_selected',false); end if;
  v_selection_upload_id:=c.current_complete_upload_id; v_selection_publication_id:=c.current_projection_publication_id;
  select count(*) into preparing_count from public.weekly_final_source_correction_sessions correction
    where correction.source_cycle_id=c.id and correction.authority_scope_kind='CYCLE' and correction.state='PREPARING'
      and correction.expected_current_final_revision_id=c.current_final_revision_id;
  if preparing_count>1 then raise exception 'WEEKLY_SOURCE_SELF_BILL_CORRECTION_SCOPE_AMBIGUOUS' using errcode='55000'; end if;
  if preparing_count=1 then
    -- Only native PREPARE has this state. PREPARED stays hidden and ordinary
    -- readers retain the committed current pointer.
    select correction.replacement_correction_upload_id,correction.replacement_projection_publication_id
      into v_selection_upload_id,v_selection_publication_id from public.weekly_final_source_correction_sessions correction
      where correction.source_cycle_id=c.id and correction.authority_scope_kind='CYCLE' and correction.state='PREPARING'
        and correction.expected_current_final_revision_id=c.current_final_revision_id;
  end if;
  select array_agg(resolution.id),count(*) into ids,count_rows from public.weekly_source_row_resolutions resolution
    join public.weekly_source_upload_rows source_row on source_row.id=resolution.upload_row_id
    join public.weekly_source_uploads upload on upload.id=source_row.upload_id
    join public.weekly_source_projection_publications publication on publication.id=v_selection_publication_id and publication.upload_id=upload.id
      and publication.source_cycle_id=c.id and publication.report_scope_id is null and publication.authority_scope_kind='CYCLE'
      and publication.state=case when preparing_count=1 then 'CORRECTION_READY' else 'CURRENT' end
    join public.weekly_work_events actual_event on actual_event.id=resolution.work_event_id and actual_event.client_id=resolution.client_id
      and actual_event.candidate_id=resolution.candidate_id and actual_event.first_source_group_id=c.source_group_id
    where upload.source_cycle_id=c.id and upload.report_scope_id is null
      and upload.state=case when preparing_count=1 then 'CORRECTION_READY' else 'CURRENT' end
      and (c.scope_client_id is null or c.scope_client_id=resolution.client_id) and resolution.work_event_id=p_work_event_id and upload.id=v_selection_upload_id
      and resolution.generation=coalesce(publication.projection_generation,publication.authority_scope_version)
      and resolution.mapping_state='RESOLVED';
  if count_rows>1 then raise exception 'WEEKLY_SOURCE_SELF_BILL_SHIFT_AMBIGUOUS' using errcode='55000'; end if;
  if count_rows=1 then
    select * into strict r from public.weekly_source_row_resolutions where weekly_source_row_resolutions.id=ids[1];
    select * into strict s from public.weekly_source_upload_rows where weekly_source_upload_rows.id=r.upload_row_id;
    select * into strict u from public.weekly_source_uploads where weekly_source_uploads.id=s.upload_id;
    select profile_code into strict profile from public.weekly_source_format_profiles where weekly_source_format_profiles.id=u.source_format_profile_id;
    if profile is distinct from rules->>'checking_profile_kind' then return jsonb_build_object('on_accepted_report',false,'bound_to_finalised_export',false,'invoice_ready',false,'s25_pay_selected',false); end if;
    bound:=s.row_finalisation_state='SOURCE_WORKED' or (profile='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1' and s.row_finalisation_state='NOT_APPLICABLE');
    selected:=bound and s.work_date<=c.finalisation_week_ending and (profile='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1'
      or (s.finalised_at_utc is not null and nullif(btrim(s.finalised_by),'') is not null and s.finalised_at_utc<=c.cutoff_at_utc));
    ready:=selected;
    if selected and rules->>'invoice_amount_basis'='SOURCE_AMOUNT' and rules->>'source_amount_basis'='EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY' then
      ready:=s.source_money_parse_state='VALID' and s.source_shift_charge_pence is not null and s.source_gross_pence is not null and s.source_vat_pence is not null
        and (abs(5::numeric*s.source_gross_pence-6::numeric*s.source_shift_charge_pence)<=5
          or exists(select 1 from public.weekly_source_invoice_hold_releases receipt where receipt.upload_row_id=s.id and receipt.hold_kind='VAT_FIT'));
    end if;
  end if;
  return jsonb_build_object('on_accepted_report',false,'bound_to_finalised_export',coalesce(bound,false),
    'invoice_ready',coalesce(ready,false),'s25_pay_selected',coalesce(selected,false));
end;
$function$;

create or replace function private.weekly_source_invoice_only_vector_v1(p_kind text,p_total_pence bigint)
returns jsonb language plpgsql immutable set search_path to 'pg_catalog','pg_temp'
as $function$
begin
  if p_kind is null or p_kind not in ('PAY','CHARGE') or p_total_pence is null or abs(p_total_pence::numeric)>999999999999
    or (p_kind='PAY' and p_total_pence<>0) then raise exception 'WEEKLY_SOURCE_INVOICE_VECTOR_INVALID' using errcode='22023'; end if;
  return jsonb_build_object('schema_version','WEEKLY_SOURCE_RATE_VECTOR_V1','kind',p_kind,
    'source_mode','HEALTHROSTER_WEEKLY','rate_method','SPLIT_RATE_WINDOWS','row_sign',case when p_total_pence<0 then -1 else 1 end,
    'paid_minutes',0,'break_minutes',0,'hours',jsonb_build_object('day',0,'night',0,'sat',0,'sun',0,'bh',0),
    'rates',jsonb_build_object('day',0,'night',0,'sat',0,'sun',0,'bh',0),'total_pence',p_total_pence);
end;
$function$;

create or replace function private.weekly_source_invoice_line_amounts_v1(p_row_resolution_id uuid,p_final_revision_id uuid)
returns jsonb language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare r public.weekly_source_row_resolutions%rowtype; s public.weekly_source_upload_rows%rowtype;
  e public.weekly_source_row_economic_snapshots%rowtype; revision public.weekly_source_final_revisions%rowtype;
  rules jsonb; release_row public.weekly_source_invoice_hold_releases%rowtype;
  net bigint; vat bigint; gross bigint; rate numeric; exact_net numeric; price text; held boolean:=false; fingerprint bytea;
  acceptance_id uuid; check_id uuid;
begin
  select * into strict r from public.weekly_source_row_resolutions where weekly_source_row_resolutions.id=p_row_resolution_id;
  select * into strict s from public.weekly_source_upload_rows where weekly_source_upload_rows.id=r.upload_row_id;
  select * into strict revision from public.weekly_source_final_revisions where weekly_source_final_revisions.id=p_final_revision_id;
  select * into strict e from public.weekly_source_row_economic_snapshots where row_resolution_id=r.id;
  if r.mapping_state<>'RESOLVED' or e.source_mode<>'HEALTHROSTER_WEEKLY' or e.row_sign<>1
    or not exists(select 1 from public.weekly_source_uploads u where u.id=s.upload_id and u.id=revision.upload_id
      and u.source_cycle_id=revision.source_cycle_id)
    or private.weekly_source_finalisation_economic_assert_v1(r.id,revision.source_cycle_id,'HEALTHROSTER_WEEKLY') is distinct from e.id then
    raise exception 'WEEKLY_SOURCE_INVOICE_MONEY_SCOPE_INVALID' using errcode='55000';
  end if;
  rules:=private.weekly_source_frozen_rule_values_v1(revision.id);
  rate:=case when e.invoice_vat_chargeable then e.invoice_vat_rate_pct else 0 end;
  net:=e.calculated_charge_pence; price:='NOT_APPLICABLE'; vat:=round(net::numeric*rate/100)::bigint; gross:=net+vat;
  if rules is not null and rules->>'invoice_sender'='CLIENT_SELF_BILL' then
    if rules->>'invoice_amount_basis'='SOURCE_AMOUNT' and rules->>'source_amount_basis'='REPORT_LINE_NET' then
      -- Checking exports carry pay-only canonical economics. The report owner
      -- alone reads report line money and applies its report VAT method.
      null;
    elsif rules->>'invoice_amount_basis'='SOURCE_AMOUNT' then
      if s.source_money_parse_state<>'VALID' or s.source_qualification_profile_version<>'SOURCE_INVOICE_MONEY_PENCE_V1'
        or s.source_shift_charge_pence is null or s.source_gross_pence is null or s.source_vat_pence is null
        or s.source_shift_charge_pence<0 or s.source_shift_charge_pence+s.source_vat_pence<>s.source_gross_pence
        or e.calculated_charge_pence<=0 then raise exception 'WEEKLY_SOURCE_INVOICE_SOURCE_MONEY_INVALID' using errcode='55000'; end if;
      net:=s.source_shift_charge_pence;
      select comparison_result,id into strict price,check_id from public.weekly_source_charge_checks
        where upload_row_id=s.id and row_resolution_id=r.id and generation=r.generation
          and comparison_profile_version='SOURCE_INVOICE_MONEY_PENCE_V1'
          and source_shift_charge_pence=s.source_shift_charge_pence and calculated_segment_charge_pence=e.calculated_charge_pence
          and charge_calculation_fingerprint=e.calculation_fingerprint and blocker_code is null;
      if price is distinct from (case when net=e.calculated_charge_pence then 'EXACT'
        when abs(net-e.calculated_charge_pence)=1 then 'SOURCE_ROUNDING_EQUIVALENT' else 'SOURCE_AMOUNT_DIFFERS' end) then
        raise exception 'WEEKLY_SOURCE_INVOICE_SOURCE_PRICE_STALE' using errcode='40001'; end if;
      if price='SOURCE_AMOUNT_DIFFERS' then
        select a.id into acceptance_id from public.weekly_source_charge_acceptances a join public.weekly_source_uploads u on u.id=s.upload_id
          where a.charge_check_id=check_id and a.upload_row_id=s.id and a.row_resolution_id=r.id and a.source_upload_id=u.id
            and a.contract_id=r.contract_id and a.acceptance_kind='ACCEPTED_DISPARITY' and a.source_upload_hash=u.content_sha256
            and a.source_row_fingerprint=r.source_row_fingerprint and a.contract_and_rate_fingerprint=e.contract_and_rate_fingerprint
            and a.effective_policy_fingerprint=e.effective_policy_fingerprint and a.charge_calculation_fingerprint=e.calculation_fingerprint
            and a.acceptance_policy_fingerprint=private.weekly_source_source_amount_acceptance_policy_fingerprint_v1();
        if found then price:='ACCEPTED_DISPARITY'; end if;
      end if;
      fingerprint:=private.weekly_source_invoice_economics_fingerprint_v1(jsonb_build_object(
        'source_shift_charge_pence',s.source_shift_charge_pence,'source_gross_pence',s.source_gross_pence,'source_vat_pence',s.source_vat_pence),rules->>'line_vat_method',rate);
      held:=rules->>'source_amount_basis'='EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY'
        and abs(5::numeric*s.source_gross_pence-6::numeric*s.source_shift_charge_pence)>5;
      select * into release_row from public.weekly_source_invoice_hold_releases where upload_row_id=s.id and hold_kind='VAT_FIT';
      if found then net:=release_row.net_pence; vat:=release_row.vat_pence; gross:=release_row.gross_pence; held:=false;
      elsif rules->>'line_vat_method' in ('LINE_GROSS_MINUS_NET_FROM_SOURCE','LINE_VAT_FROM_REPORT') then
        vat:=s.source_vat_pence; gross:=s.source_gross_pence;
      else vat:=round(net::numeric*rate/100)::bigint; gross:=net+vat; end if;
    elsif rules->>'line_vat_method'='ROUND_TOTAL_FROM_EXACT_NET' then
      exact_net:=(e.minutes_day::numeric*e.charge_day+e.minutes_night::numeric*e.charge_night
        +e.minutes_sat::numeric*e.charge_sat+e.minutes_sun::numeric*e.charge_sun+e.minutes_bh::numeric*e.charge_bh)/60;
      gross:=round(exact_net*(1+rate/100)*100)::bigint; vat:=gross-net;
    elsif rules->>'line_vat_method'<>'ROUND_EACH_LINE_FROM_NET' then
      raise exception 'WEEKLY_SOURCE_INVOICE_LINE_VAT_SCOPE_INVALID' using errcode='55000';
    end if;
  end if;
  return jsonb_build_object('net_pence',net,'vat_pence',vat,'gross_pence',gross,'vat_rate_pct',rate,
    'price_check_result',price,'charge_acceptance_id',acceptance_id,'source_amount',rules is not null and rules->>'invoice_sender'='CLIENT_SELF_BILL' and rules->>'invoice_amount_basis'='SOURCE_AMOUNT'
      and rules->>'source_amount_basis'='EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY',
    'pay_only',rules is not null and rules->>'invoice_sender'='CLIENT_SELF_BILL' and rules->>'source_amount_basis'='REPORT_LINE_NET',
    'invoice_hold',case when held then 'VAT_FIT' else null end,'hold_release_id',release_row.id,
    'invoice_economics_sha256',case when fingerprint is null then null else encode(fingerprint,'hex') end,
    'rule_set_version_id',revision.rule_set_version_id);
end;
$function$;

create or replace function private.weekly_source_source_amount_acceptance_policy_fingerprint_v1()
returns bytea language sql immutable set search_path to 'pg_catalog','pg_temp'
as $function$
  select private.weekly_source_sha256_jsonb_v1('SOURCE_AMOUNT_CHARGE_ACCEPTANCE_POLICY_V1',jsonb_build_object(
    'accepted_results',jsonb_build_array('SOURCE_AMOUNT_DIFFERS'),'admission_kinds',jsonb_build_array('ACCEPTED_DISPARITY'),
    'invoice_authority','EXACT_SOURCE_PENCE','candidate_pay_authority','CLOUDTMS_CALCULATION'));
$function$;

create or replace function private.weekly_source_invoice_window_cycle_v1(p_upload_row_id uuid,p_source_group_id uuid)
returns uuid language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare s public.weekly_source_upload_rows%rowtype; ids uuid[];
begin
  select * into strict s from public.weekly_source_upload_rows where weekly_source_upload_rows.id=p_upload_row_id;
  if s.finalised_at_utc is null then return null; end if;
  select array_agg(c.id order by c.id) into ids from public.weekly_source_cycles c
    where c.source_group_id=p_source_group_id and c.state in ('OPEN','FINALISABLE','FINALISING')
      and s.work_date<=c.finalisation_week_ending
      and c.cutoff_at_utc>=s.finalised_at_utc
      and s.finalised_at_utc>coalesce((select max(prior.cutoff_at_utc) from public.weekly_source_cycles prior
        where prior.source_group_id=c.source_group_id and prior.cutoff_at_utc<c.cutoff_at_utc),
        ((c.cutoff_at_utc at time zone 'Europe/London')-interval '7 days') at time zone 'Europe/London')
      and (c.scope_client_id is null or c.scope_client_id=(select r.client_id from public.weekly_source_row_resolutions r
        where r.upload_row_id=s.id order by r.generation desc,r.created_at_utc desc limit 1));
  -- Parent and Client sibling cycles share cutoffs; retain the actual Client
  -- cycle where present and never choose a different Client's window.
  if cardinality(ids)>1 then
    select array_agg(c.id) into ids from public.weekly_source_cycles c where c.id=any(ids) and c.scope_client_id is not null;
  end if;
  if cardinality(ids)>1 then raise exception 'WEEKLY_SOURCE_INVOICE_WINDOW_AMBIGUOUS' using errcode='55000'; end if;
  return ids[1];
end;
$function$;

create or replace function private.weekly_source_invoice_correction_insert_v1(p_transition_id uuid,p_work_event_id uuid,p_final_revision_id uuid)
returns jsonb language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare t public.weekly_source_state_transitions%rowtype; revision public.weekly_source_final_revisions%rowtype;
  current_line public.weekly_source_final_snapshot_lines%rowtype; prior_line public.weekly_source_final_snapshot_lines%rowtype;
  base public.weekly_source_billing_movements%rowtype; rules jsonb; amounts jsonb; cycle_id uuid; window_cycle uuid;
  adjustment_net bigint; adjustment_vat bigint; net bigint; vat bigint; gross bigint; v_role text; pair_id uuid:=gen_random_uuid(); base_money jsonb;
  facts jsonb; pay_vector jsonb; charge_vector jsonb; h bytea; ids uuid[]:='{}'::uuid[]; id uuid; existing_count integer;
begin
  select * into strict t from public.weekly_source_state_transitions where weekly_source_state_transitions.id=p_transition_id;
  select * into strict revision from public.weekly_source_final_revisions where weekly_source_final_revisions.id=p_final_revision_id;
  if t.final_revision_id<>revision.id or t.work_event_id<>p_work_event_id or t.outcome<>'NO_CHANGE'
    or t.source_profile_kind<>'HEALTHROSTER_ACTUAL_ROWS' or revision.state not in ('CURRENT','PREPARED') then
    raise exception 'WEEKLY_SOURCE_INVOICE_CORRECTION_SCOPE_INVALID' using errcode='55000'; end if;
  rules:=private.weekly_source_frozen_rule_values_v1(revision.id);
  if rules is null or rules->>'invoice_sender'<>'CLIENT_SELF_BILL' or rules->>'invoice_amount_basis'<>'SOURCE_AMOUNT'
    or rules->>'invoice_membership'<>'FINALISED_IN_CYCLE_WINDOW' then return jsonb_build_object('movement_count',0); end if;
  select * into strict current_line from public.weekly_source_final_snapshot_lines where weekly_source_final_snapshot_lines.id=t.new_snapshot_line_id;
  select * into strict prior_line from public.weekly_source_final_snapshot_lines where weekly_source_final_snapshot_lines.id=t.previous_snapshot_line_id;
  if current_line.invoice_economics_sha256 is null or current_line.invoice_economics_sha256 is not distinct from prior_line.invoice_economics_sha256 then
    return jsonb_build_object('movement_count',0); end if;
  select array_agg(m.id order by m.movement_role),count(*) into ids,existing_count from public.weekly_source_billing_movements m
    where m.transition_id=t.id and m.source_line_kind='SOURCE_INVOICE_CORRECTION' and m.movement_role in ('REVERSAL','REPLACEMENT');
  if existing_count=2 then return jsonb_build_object('movement_count',0,'idempotent',true,'movement_ids',to_jsonb(ids));
  elsif existing_count<>0 then raise exception 'WEEKLY_SOURCE_INVOICE_CORRECTION_PARTIAL_PAIR' using errcode='55000'; end if;
  cycle_id:=revision.source_cycle_id;
  window_cycle:=private.weekly_source_invoice_window_cycle_v1(current_line.upload_row_id,
    (select source_group_id from public.weekly_source_cycles where weekly_source_cycles.id=cycle_id));
  if window_cycle is null then return jsonb_build_object('movement_count',0,'information_code','SOURCE_AMOUNT_CHANGED_OUTSIDE_SELF_BILL_WINDOW'); end if;
  if window_cycle<>cycle_id then
    raise exception 'WEEKLY_SOURCE_INVOICE_CORRECTION_WINDOW_SCOPE_UNSUPPORTED' using errcode='55000',detail=jsonb_build_object(
      'contract','WEEKLY_SOURCE_INVOICE_CORRECTION_WINDOW_ROUTING_V1','window_cycle_id',window_cycle,'current_cycle_id',cycle_id,
      'final_revision_id',revision.id,'transition_id',t.id,'work_event_id',p_work_event_id,'actual_client_id',current_line.client_id,
      'current_upload_id',revision.upload_id,'current_upload_row_id',current_line.upload_row_id,'prior_upload_row_id',prior_line.upload_row_id,
      'finalised_at_utc',(select s.finalised_at_utc from public.weekly_source_upload_rows s where s.id=current_line.upload_row_id),
      'window_cutoff_at_utc',(select c.cutoff_at_utc from public.weekly_source_cycles c where c.id=window_cycle),
      'current_cutoff_at_utc',(select c.cutoff_at_utc from public.weekly_source_cycles c where c.id=cycle_id),
      'window_previous_cutoff_at_utc',(select max(prior.cutoff_at_utc) from public.weekly_source_cycles prior where prior.source_group_id=(select source_group_id from public.weekly_source_cycles where weekly_source_cycles.id=window_cycle)
        and prior.cutoff_at_utc<(select cutoff_at_utc from public.weekly_source_cycles where weekly_source_cycles.id=window_cycle)))::text;
  end if;
  amounts:=private.weekly_source_invoice_line_amounts_v1(current_line.row_resolution_id,revision.id);
  select m.* into base from public.weekly_source_billing_movements m
    join public.weekly_source_final_revisions r on r.id=m.final_revision_id and r.state='CURRENT'
    join public.weekly_source_cycles c on c.id=m.finalisation_cycle_id
    where m.work_event_id=p_work_event_id and m.movement_role in ('POSITIVE','REPLACEMENT')
      and m.source_line_kind in ('SOURCE_ORDINARY','SOURCE_REPLACEMENT') and m.placement_state<>'VOIDED_BY_CORRECT_FINAL'
      and c.source_group_id=(select source_group_id from public.weekly_source_cycles where weekly_source_cycles.id=cycle_id)
      and m.actual_client_id=current_line.client_id
    order by c.finalisation_week_ending desc,r.finalised_at_utc desc,m.created_at_utc desc,m.id desc limit 1;
  if not found then raise exception 'WEEKLY_SOURCE_INVOICE_CORRECTION_BASE_MISSING' using errcode='55000'; end if;
  base_money:=private.weekly_source_invoice_movement_money_v1(base.id);
  select coalesce(sum(m.invoice_presentation_charge_pence),0)::bigint,coalesce(sum((private.weekly_source_invoice_movement_money_v1(m.id)->>'vat_pence')::numeric),0)::bigint into adjustment_net,adjustment_vat
    from public.weekly_source_billing_movements m join public.weekly_source_final_revisions r on r.id=m.final_revision_id and r.state='CURRENT'
    join public.weekly_source_cycles c on c.id=m.finalisation_cycle_id
    where m.work_event_id=p_work_event_id and m.source_line_kind='SOURCE_INVOICE_CORRECTION' and m.placement_state<>'VOIDED_BY_CORRECT_FINAL'
      and c.source_group_id=(select source_group_id from public.weekly_source_cycles where weekly_source_cycles.id=cycle_id) and m.actual_client_id=current_line.client_id;
  pay_vector:=private.weekly_source_invoice_only_vector_v1('PAY',0); ids:='{}'::uuid[];
  foreach v_role in array array['REVERSAL','REPLACEMENT'] loop
    net:=case when v_role='REVERSAL' then -(base.invoice_presentation_charge_pence+adjustment_net) else (amounts->>'net_pence')::bigint end;
    vat:=case when v_role='REVERSAL' then -((base_money->>'vat_pence')::bigint+adjustment_vat) else (amounts->>'vat_pence')::bigint end; gross:=net+vat;
    facts:=jsonb_build_object('schema_version','WEEKLY_SOURCE_INVOICE_CORRECTION_FACTS_V1','snapshot_line_id',current_line.id,
      'prior_snapshot_line_id',prior_line.id,'upload_row_id',current_line.upload_row_id,'bound_row_resolution_id',current_line.row_resolution_id,
      'rule_set_version_id',revision.rule_set_version_id,'invoice_economics_sha256',encode(current_line.invoice_economics_sha256,'hex'),
      'hold_release_id',case when v_role='REPLACEMENT' then amounts->'hold_release_id' else 'null'::jsonb end,
      'invoice_hold',case when v_role='REPLACEMENT' then amounts->'invoice_hold' else 'null'::jsonb end,'invoice_window_cycle_id',window_cycle,
      'correction_presentation','FULL_REVERSAL_REPLACEMENT');
    charge_vector:=private.weekly_source_invoice_only_vector_v1('CHARGE',net);
    h:=private.weekly_source_billing_movement_hash_v1(jsonb_build_object('transition_id',t.id,'final_revision_id',revision.id,
      'finalisation_cycle_id',cycle_id,'actual_client_id',base.actual_client_id,'candidate_id',base.candidate_id,
      'contract_id',base.contract_id,'work_event_id',p_work_event_id,'movement_role',v_role,'correction_unit_id',pair_id,
      'prior_movement_id',base.id,'source_profile_kind',t.source_profile_kind,'source_line_kind','SOURCE_INVOICE_CORRECTION',
      'source_facts',facts,'pay_vector',pay_vector,'charge_vector',charge_vector,'total_pay_pence',0,
      'calculated_comparison_charge_pence',0,'source_validation_charge_pence',net,'invoice_presentation_charge_pence',net,
      'vat_rate_pct',(amounts->>'vat_rate_pct')::numeric,'vat_amount',vat::numeric/100,'total_inc_vat',gross::numeric/100,
      'price_check_result','NOT_APPLICABLE','price_check_fingerprint',null,'charge_acceptance_id',null,
      'mapping_rate_policy_fingerprint',encode(base.mapping_rate_policy_fingerprint,'hex'),'invoice_timesheet_id',base.invoice_timesheet_id));
    insert into public.weekly_source_billing_movements(transition_id,final_revision_id,finalisation_cycle_id,actual_client_id,candidate_id,contract_id,work_event_id,
      movement_role,correction_unit_id,prior_movement_id,source_profile_kind,source_line_kind,source_facts_json,canonical_pay_vector_json,canonical_charge_vector_json,
      total_pay_ex_vat,calculated_comparison_charge_pence,source_validation_charge_pence,invoice_presentation_charge_pence,vat_rate_pct,vat_amount,total_inc_vat,
      price_check_result,mapping_rate_policy_fingerprint,invoice_timesheet_id,original_cycle_key,movement_economic_hash)
    values(t.id,revision.id,cycle_id,base.actual_client_id,base.candidate_id,base.contract_id,p_work_event_id,v_role,pair_id,base.id,t.source_profile_kind,
      'SOURCE_INVOICE_CORRECTION',facts,pay_vector,charge_vector,0,0,net,net,(amounts->>'vat_rate_pct')::numeric,vat::numeric/100,gross::numeric/100,
      'NOT_APPLICABLE',base.mapping_rate_policy_fingerprint,base.invoice_timesheet_id,cycle_id::text,h) returning public.weekly_source_billing_movements.id into id;
    ids:=array_append(ids,id);
  end loop;
  return jsonb_build_object('movement_count',2,'movement_ids',to_jsonb(ids));
end;
$function$;

create or replace function private.weekly_source_invoice_correction_unwind_v1(p_transition_id uuid,p_work_event_id uuid,p_final_revision_id uuid)
returns uuid language plpgsql volatile security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare t public.weekly_source_state_transitions%rowtype; reversal public.weekly_source_billing_movements%rowtype;
  latest public.weekly_source_billing_movements%rowtype; net bigint; vat bigint; id uuid; facts jsonb; pay jsonb; charge jsonb; h bytea;
begin
  select * into strict t from public.weekly_source_state_transitions where weekly_source_state_transitions.id=p_transition_id;
  if t.final_revision_id<>p_final_revision_id or t.work_event_id<>p_work_event_id or t.outcome not in ('AMEND','CANCEL') then
    raise exception 'WEEKLY_SOURCE_INVOICE_UNWIND_SCOPE_INVALID' using errcode='55000'; end if;
  select m.id into id from public.weekly_source_billing_movements m where m.transition_id=t.id and m.movement_role='CORRECTION_UNWIND';
  if found then return id; end if;
  select * into strict reversal from public.weekly_source_billing_movements where transition_id=t.id and movement_role='REVERSAL' and source_line_kind='GENERATED_HISTORICAL_REVERSAL';
  select coalesce(sum(m.invoice_presentation_charge_pence),0)::bigint,coalesce(sum((private.weekly_source_invoice_movement_money_v1(m.id)->>'vat_pence')::numeric),0)::bigint into net,vat
    from public.weekly_source_billing_movements m join public.weekly_source_final_revisions r on r.id=m.final_revision_id and r.state='CURRENT'
    join public.weekly_source_cycles c on c.id=m.finalisation_cycle_id
    where m.work_event_id=p_work_event_id and m.source_line_kind='SOURCE_INVOICE_CORRECTION' and m.placement_state<>'VOIDED_BY_CORRECT_FINAL'
      and m.actual_client_id=reversal.actual_client_id and c.source_group_id=(select source_group_id from public.weekly_source_cycles where weekly_source_cycles.id=t.finalisation_cycle_id);
  if net=0 and vat=0 then return null; end if;
  select m.* into strict latest from public.weekly_source_billing_movements m
    join public.weekly_source_final_revisions r on r.id=m.final_revision_id and r.state='CURRENT'
    join public.weekly_source_cycles c on c.id=m.finalisation_cycle_id
    where m.work_event_id=p_work_event_id and m.source_line_kind='SOURCE_INVOICE_CORRECTION' and m.movement_role='REPLACEMENT'
      and m.placement_state<>'VOIDED_BY_CORRECT_FINAL' and m.actual_client_id=reversal.actual_client_id
      and c.source_group_id=(select source_group_id from public.weekly_source_cycles where weekly_source_cycles.id=t.finalisation_cycle_id)
    order by c.finalisation_week_ending desc,r.finalised_at_utc desc,m.created_at_utc desc,m.id desc limit 1;
  pay:=private.weekly_source_invoice_only_vector_v1('PAY',0); charge:=private.weekly_source_invoice_only_vector_v1('CHARGE',-net);
  facts:=jsonb_build_object('schema_version','WEEKLY_SOURCE_INVOICE_CORRECTION_UNWIND_V1','pay_reversal_movement_id',reversal.id,
    'prior_correction_replacement_id',latest.id,'outstanding_net_pence',net,'outstanding_vat_pence',vat,'correction_presentation','FULL_REVERSAL_REPLACEMENT');
  h:=private.weekly_source_billing_movement_hash_v1(jsonb_build_object('transition_id',t.id,'final_revision_id',p_final_revision_id,
    'finalisation_cycle_id',t.finalisation_cycle_id,'actual_client_id',reversal.actual_client_id,'candidate_id',reversal.candidate_id,
    'contract_id',reversal.contract_id,'work_event_id',p_work_event_id,'movement_role','CORRECTION_UNWIND','correction_unit_id',reversal.correction_unit_id,
    'prior_movement_id',latest.id,'source_profile_kind',reversal.source_profile_kind,'source_line_kind','SOURCE_INVOICE_CORRECTION',
    'source_facts',facts,'pay_vector',pay,'charge_vector',charge,'total_pay_pence',0,'calculated_comparison_charge_pence',0,
    'source_validation_charge_pence',-net,'invoice_presentation_charge_pence',-net,'vat_rate_pct',reversal.vat_rate_pct,
    'vat_amount',-vat::numeric/100,'total_inc_vat',-(net+vat)::numeric/100,'price_check_result','NOT_APPLICABLE',
    'price_check_fingerprint',null,'charge_acceptance_id',null,'mapping_rate_policy_fingerprint',encode(latest.mapping_rate_policy_fingerprint,'hex'),
    'invoice_timesheet_id',reversal.invoice_timesheet_id));
  insert into public.weekly_source_billing_movements(transition_id,final_revision_id,finalisation_cycle_id,actual_client_id,candidate_id,contract_id,work_event_id,
    movement_role,correction_unit_id,prior_movement_id,source_profile_kind,source_line_kind,source_facts_json,canonical_pay_vector_json,canonical_charge_vector_json,
    total_pay_ex_vat,calculated_comparison_charge_pence,source_validation_charge_pence,invoice_presentation_charge_pence,vat_rate_pct,vat_amount,total_inc_vat,
    price_check_result,mapping_rate_policy_fingerprint,invoice_timesheet_id,original_cycle_key,movement_economic_hash)
  values(t.id,p_final_revision_id,t.finalisation_cycle_id,reversal.actual_client_id,reversal.candidate_id,reversal.contract_id,p_work_event_id,
    'CORRECTION_UNWIND',reversal.correction_unit_id,latest.id,reversal.source_profile_kind,'SOURCE_INVOICE_CORRECTION',facts,pay,charge,
    0,0,-net,-net,reversal.vat_rate_pct,-vat::numeric/100,-(net+vat)::numeric/100,'NOT_APPLICABLE',latest.mapping_rate_policy_fingerprint,
    reversal.invoice_timesheet_id,t.finalisation_cycle_id::text,h) returning public.weekly_source_billing_movements.id into id;
  return id;
end;
$function$;

create or replace function private.weekly_source_invoice_source_amount_movement_valid_v1(p_movement_id uuid)
returns boolean language plpgsql stable security definer set search_path to 'pg_catalog','pg_temp'
as $function$
declare m public.weekly_source_billing_movements%rowtype; t public.weekly_source_state_transitions%rowtype;
  sn public.weekly_source_final_snapshot_lines%rowtype; source_row public.weekly_source_upload_rows%rowtype; resolution public.weekly_source_row_resolutions%rowtype;
  economic public.weekly_source_row_economic_snapshots%rowtype; charge public.weekly_source_charge_checks%rowtype;
  upload public.weekly_source_uploads%rowtype; rules jsonb; expected_price text;
begin
  select * into m from public.weekly_source_billing_movements where weekly_source_billing_movements.id=p_movement_id;
  if not found or m.source_profile_kind<>'HEALTHROSTER_ACTUAL_ROWS' or m.transition_id is null
    or m.source_validation_charge_pence is null or m.invoice_presentation_charge_pence is distinct from m.source_validation_charge_pence then return false; end if;
  select * into t from public.weekly_source_state_transitions where weekly_source_state_transitions.id=m.transition_id;
  if not found or t.final_revision_id<>m.final_revision_id or t.work_event_id<>m.work_event_id then return false; end if;
  rules:=private.weekly_source_frozen_rule_values_v1(m.final_revision_id);
  if rules is null or rules->>'invoice_sender' is distinct from 'CLIENT_SELF_BILL'
    or rules->>'invoice_amount_basis' is distinct from 'SOURCE_AMOUNT'
    or rules->>'source_amount_basis' is distinct from 'EXPORT_ACTUAL_COST_INC_VAT_NET_AGENCY_WORKER_PAY' then return false; end if;
  if m.source_line_kind='SOURCE_INVOICE_CORRECTION' then
    return m.total_pay_ex_vat=0 and m.canonical_pay_vector_json->>'total_pence'='0'
      and m.source_facts_json->>'schema_version' in ('WEEKLY_SOURCE_INVOICE_CORRECTION_FACTS_V1','WEEKLY_SOURCE_INVOICE_CORRECTION_UNWIND_V1')
      and ((m.movement_role in ('REVERSAL','REPLACEMENT') and t.outcome='NO_CHANGE')
        or (m.movement_role='CORRECTION_UNWIND' and t.outcome in ('AMEND','CANCEL')))
      and m.price_check_result='NOT_APPLICABLE' and m.price_check_fingerprint is null and m.charge_acceptance_id is null;
  end if;
  if not ((m.source_line_kind='SOURCE_ORDINARY' and t.outcome='ADD' and m.movement_role='POSITIVE')
    or (m.source_line_kind='SOURCE_REPLACEMENT' and t.outcome='AMEND' and m.movement_role='REPLACEMENT')
    or (m.source_line_kind='SOURCE_REBILL_LINE' and t.outcome='NO_CHANGE' and m.movement_role='POSITIVE' and m.total_pay_ex_vat=0)) then return false; end if;
  select * into sn from public.weekly_source_final_snapshot_lines where weekly_source_final_snapshot_lines.id=t.new_snapshot_line_id;
  if not found or sn.final_revision_id<>m.final_revision_id or sn.client_id<>m.actual_client_id
    or sn.candidate_id<>m.candidate_id or sn.contract_id<>m.contract_id or sn.work_event_id<>m.work_event_id
    or sn.upload_row_id::text is distinct from m.source_facts_json->>'upload_row_id' then return false; end if;
  -- Historical admission validates the frozen producer facts, not today's
  -- Client policy/rates or a later Ignore receipt for this old movement.
  select * into resolution from public.weekly_source_row_resolutions where weekly_source_row_resolutions.id=sn.row_resolution_id;
  select * into source_row from public.weekly_source_upload_rows where weekly_source_upload_rows.id=sn.upload_row_id;
  select * into upload from public.weekly_source_uploads where weekly_source_uploads.id=source_row.upload_id;
  select * into economic from public.weekly_source_row_economic_snapshots where row_resolution_id=resolution.id;
  select * into charge from public.weekly_source_charge_checks where upload_row_id=source_row.id and row_resolution_id=resolution.id
    and generation=resolution.generation and comparison_profile_version='SOURCE_INVOICE_MONEY_PENCE_V1';
  if resolution.id is null or source_row.id is null or economic.id is null or charge.id is null
    or resolution.work_event_id is distinct from m.work_event_id or resolution.client_id is distinct from m.actual_client_id
    or resolution.candidate_id is distinct from m.candidate_id or resolution.contract_id is distinct from m.contract_id
    or source_row.source_money_parse_state<>'VALID' or source_row.source_qualification_profile_version<>'SOURCE_INVOICE_MONEY_PENCE_V1'
    or source_row.source_shift_charge_pence is null or source_row.source_gross_pence is null or source_row.source_vat_pence is null
    or source_row.source_shift_charge_pence+source_row.source_vat_pence<>source_row.source_gross_pence
    or m.invoice_presentation_charge_pence is distinct from source_row.source_shift_charge_pence
    or m.calculated_comparison_charge_pence is distinct from economic.calculated_charge_pence
    or m.price_check_fingerprint is distinct from economic.calculation_fingerprint
    or charge.charge_calculation_fingerprint is distinct from economic.calculation_fingerprint
    or charge.source_shift_charge_pence is distinct from source_row.source_shift_charge_pence
    or charge.calculated_segment_charge_pence is distinct from economic.calculated_charge_pence then return false; end if;
  expected_price:=case when source_row.source_shift_charge_pence=economic.calculated_charge_pence then 'EXACT'
    when abs(source_row.source_shift_charge_pence-economic.calculated_charge_pence)=1 then 'SOURCE_ROUNDING_EQUIVALENT' else 'SOURCE_AMOUNT_DIFFERS' end;
  if charge.comparison_result<>expected_price then return false; end if;
  if m.price_check_result=expected_price and m.charge_acceptance_id is null then return true; end if;
  return expected_price='SOURCE_AMOUNT_DIFFERS' and m.price_check_result='ACCEPTED_DISPARITY' and exists(
    select 1 from public.weekly_source_charge_acceptances acceptance where acceptance.id=m.charge_acceptance_id
      and acceptance.acceptance_kind='ACCEPTED_DISPARITY' and acceptance.charge_check_id=charge.id
      and acceptance.source_upload_id=upload.id and acceptance.source_upload_hash=upload.content_sha256
      and acceptance.upload_row_id=source_row.id and acceptance.row_resolution_id=resolution.id and acceptance.contract_id=resolution.contract_id
      and acceptance.source_row_fingerprint=resolution.source_row_fingerprint and acceptance.contract_and_rate_fingerprint=economic.contract_and_rate_fingerprint
      and acceptance.effective_policy_fingerprint=economic.effective_policy_fingerprint and acceptance.charge_calculation_fingerprint=economic.calculation_fingerprint
      and acceptance.acceptance_policy_fingerprint=private.weekly_source_source_amount_acceptance_policy_fingerprint_v1());
end;
$function$;

drop trigger if exists weekly_source_immutable_record_guard on public.weekly_source_invoice_hold_releases;
create trigger weekly_source_immutable_record_guard before update or delete on public.weekly_source_invoice_hold_releases
  for each row execute function private._weekly_source_immutable_record_guard_v1();
drop trigger if exists weekly_source_immutable_truncate_guard on public.weekly_source_invoice_hold_releases;
create trigger weekly_source_immutable_truncate_guard before truncate on public.weekly_source_invoice_hold_releases
  for each statement execute function private._weekly_source_immutable_record_guard_v1();

alter function private.weekly_source_invoice_source_amount_movement_valid_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_invoice_source_amount_movement_valid_v1(uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_source_amount_warnings_v1(uuid,uuid) owner to postgres;
revoke all on function private.weekly_source_source_amount_warnings_v1(uuid,uuid) from public,anon,authenticated,service_role;
alter function private.weekly_source_billing_movement_hash_v1(jsonb) owner to postgres;
alter function private.weekly_source_frozen_rule_values_v1(uuid) owner to postgres;
alter function private.weekly_source_shift_on_self_bill_v1(uuid,uuid) owner to postgres;
alter function private.weekly_source_invoice_only_vector_v1(text,bigint) owner to postgres;
alter function private.weekly_source_invoice_line_amounts_v1(uuid,uuid) owner to postgres;
alter function private.weekly_source_source_amount_acceptance_policy_fingerprint_v1() owner to postgres;
alter function private.weekly_source_invoice_window_cycle_v1(uuid,uuid) owner to postgres;
alter function private.weekly_source_invoice_correction_insert_v1(uuid,uuid,uuid) owner to postgres;
alter function private.weekly_source_invoice_correction_unwind_v1(uuid,uuid,uuid) owner to postgres;
alter function private.weekly_source_invoice_movement_money_v1(uuid,uuid) owner to postgres;
alter function private.weekly_source_invoice_presentation_release_authority_v1(uuid[],uuid) owner to postgres;
alter function private.weekly_source_invoice_presentation_source_hash_v1(uuid[],uuid) owner to postgres;
alter function private.weekly_source_vat_rounding_movement_insert_v1(uuid,uuid,uuid) owner to postgres;
alter function private.weekly_source_rebill_movement_insert_v1(uuid,uuid) owner to postgres;
alter function private.weekly_source_invoice_placement_identity_guard_v1() owner to postgres;
alter function public.weekly_source_invoice_hold_release_atomic_v1(jsonb) owner to postgres;
revoke all on function private.weekly_source_billing_movement_hash_v1(jsonb),private.weekly_source_frozen_rule_values_v1(uuid),
  private.weekly_source_invoice_only_vector_v1(text,bigint),private.weekly_source_invoice_line_amounts_v1(uuid,uuid),
  private.weekly_source_invoice_window_cycle_v1(uuid,uuid),private.weekly_source_invoice_correction_insert_v1(uuid,uuid,uuid),
  private.weekly_source_invoice_correction_unwind_v1(uuid,uuid,uuid) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invoice_movement_money_v1(uuid,uuid),private.weekly_source_shift_on_self_bill_v1(uuid,uuid) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_invoice_presentation_release_authority_v1(uuid[],uuid),private.weekly_source_invoice_presentation_source_hash_v1(uuid[],uuid) from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_source_amount_acceptance_policy_fingerprint_v1() from public,anon,authenticated,service_role;
revoke all on function private.weekly_source_vat_rounding_movement_insert_v1(uuid,uuid,uuid),private.weekly_source_rebill_movement_insert_v1(uuid,uuid),
  private.weekly_source_invoice_placement_identity_guard_v1() from public,anon,authenticated,service_role;
revoke all on function public.weekly_source_invoice_hold_release_atomic_v1(jsonb) from public,anon,authenticated,service_role;
grant execute on function public.weekly_source_invoice_hold_release_atomic_v1(jsonb) to service_role;
notify pgrst,'reload schema';
commit;
