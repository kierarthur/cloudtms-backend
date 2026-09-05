-- Banking Pay Create Draft certified-finance INSERT_ITEMS handoff.
-- This verifier proves identity, fail-closed handoff and unchanged execution
-- metadata only. Existing finance owners remain the sole economic authority.
\set ON_ERROR_STOP on

do $catalogue$
declare
  routine_signature regprocedure :=
    'public.pay_batch_insert_items_from_preview(uuid,uuid,uuid,jsonb)'::regprocedure;
  routine_definition text;
  identity_alias_declaration text;
  deferred_alias_declaration text;
  routine_owner name;
  routine_security_definer boolean;
  routine_volatility "char";
  routine_parallel "char";
  routine_config text[];
  visible_alias text;
begin
  select pg_get_functiondef(routine_signature),
         owner_role.rolname,
         routine.prosecdef,
         routine.provolatile,
         routine.proparallel,
         routine.proconfig
  into strict routine_definition,
              routine_owner,
              routine_security_definer,
              routine_volatility,
              routine_parallel,
              routine_config
  from pg_proc as routine
  join pg_roles as owner_role on owner_role.oid=routine.proowner
  where routine.oid=routine_signature;

  identity_alias_declaration:=substring(
    routine_definition
    from 'v_certified_finance_identity_aliases constant text\[\][[:space:]]*:= ARRAY\[([^]]*)\]::text\[\];'
  );
  deferred_alias_declaration:=substring(
    routine_definition
    from 'v_deferred_finance_aliases constant text\[\][[:space:]]*:= ARRAY\[([^]]*)\]::text\[\];'
  );

  if identity_alias_declaration is null or deferred_alias_declaration is null then
    raise exception 'BANKING_PAY_DRAFT_FINANCE_HANDOFF_ALIAS_DECLARATION_MISSING';
  end if;

  foreach visible_alias in array array[
    'OVERPAYMENT_RECOVERY',
    'MANUAL_DEBT_RECOVERY',
    'PAYMENT_ADVANCE_REPAYMENT',
    'LOAN_PAYOUT',
    'UNDERPAYMENT_PAYMENT',
    'MANUAL_CREDIT_ADJUSTMENT_PAYMENT'
  ]::text[] loop
    if (length(identity_alias_declaration)-length(replace(identity_alias_declaration,quote_literal(visible_alias),'')))
         / length(quote_literal(visible_alias)) <> 1 then
      raise exception 'BANKING_PAY_DRAFT_FINANCE_HANDOFF_VISIBLE_ALIAS_CARDINALITY_CHANGED:%',visible_alias;
    end if;
  end loop;

  foreach visible_alias in array array[
    'OVERPAYMENT_RECOVERY',
    'PAYMENT_ADVANCE_REPAYMENT',
    'LOAN_PAYOUT',
    'UNDERPAYMENT_PAYMENT',
    'MANUAL_CREDIT_ADJUSTMENT_PAYMENT'
  ]::text[] loop
    if (length(deferred_alias_declaration)-length(replace(deferred_alias_declaration,quote_literal(visible_alias),'')))
         / length(quote_literal(visible_alias)) <> 1 then
      raise exception 'BANKING_PAY_DRAFT_FINANCE_HANDOFF_DEFERRED_ALIAS_CARDINALITY_CHANGED:%',visible_alias;
    end if;
  end loop;

  if position(quote_literal('MANUAL_DEBT_RECOVERY') in deferred_alias_declaration)>0 then
    raise exception 'BANKING_PAY_DRAFT_FINANCE_HANDOFF_NON_DEFERRED_ALIAS_ACCEPTED';
  end if;

  if position(quote_literal('LOAN_REPAYMENT') in identity_alias_declaration)>0
     or position(quote_literal('MANUAL_CREDIT_PAYOUT') in identity_alias_declaration)>0
     or position(quote_literal('LOAN_REPAYMENT') in deferred_alias_declaration)>0
     or position(quote_literal('MANUAL_CREDIT_PAYOUT') in deferred_alias_declaration)>0 then
    raise exception 'BANKING_PAY_DRAFT_FINANCE_HANDOFF_HIDDEN_ALIAS_ACCEPTED';
  end if;

  if position('tmp_pay_batch_insert_items_certified_finance' in routine_definition)=0
     or position('DRAFT_FINANCE_CONSTITUENT_HANDOFF_INVALID' in routine_definition)=0
     or position('DRAFT_FINANCE_PREEXISTING_ITEM_LINK_MISMATCH' in routine_definition)=0
     or position('MANUAL_DEBT_RECOVERY remains on' in routine_definition)=0
     or position('finance_row.visible_alias = ''MANUAL_DEBT_RECOVERY''' in routine_definition)=0
     or position('PAYMENT_ADVANCE_REPAYMENT'' THEN ''LOAN_REPAYMENT' in routine_definition)=0
     or position('MANUAL_CREDIT_ADJUSTMENT_PAYMENT'' THEN ''MANUAL_CREDIT_PAYOUT' in routine_definition)=0
     or position('pay_batch_apply_finance_adjustments' in routine_definition)>0 then
    raise exception 'BANKING_PAY_DRAFT_FINANCE_HANDOFF_DEFINITION_DRIFT';
  end if;

  if routine_owner<>current_user
     or routine_security_definer is not true
     or routine_volatility<>'v'
     or routine_parallel<>'u'
     or routine_config is distinct from array['search_path=public']::text[]
     or has_function_privilege('anon',routine_signature,'EXECUTE')
     or has_function_privilege('authenticated',routine_signature,'EXECUTE')
     or not has_function_privilege('service_role',routine_signature,'EXECUTE') then
    raise exception 'BANKING_PAY_DRAFT_FINANCE_HANDOFF_METADATA_OR_ACL_DRIFT';
  end if;

  if routine_definition~*'pg_catalog\.(coalesce|nullif|least|greatest)[[:space:]]*\('
     or routine_definition~*'(^|[^[:alnum:]_])(min|max)[[:space:]]*\([[:space:]]*[^)]*uuid' then
    raise exception 'BANKING_PAY_DRAFT_FINANCE_HANDOFF_UNSAFE_POSTGRES_CONSTRUCT';
  end if;
end;
$catalogue$;
