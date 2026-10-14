-- Mandatory NEW/UPGRADE prospective verifier. SELECT/catalogue-only body.
-- This is not a native race/financial qualification or a canonical body seal.
do $a0_transaction_fence_contract$
declare v_oid oid;v_count integer;f record;v_body text;r record;
begin
  v_oid:=to_regclass('private.timesheet_correction_a0_transaction_fences');
  if v_oid is null or not exists(select 1 from pg_catalog.pg_class c where c.oid=v_oid
    and c.relkind='r' and c.relrowsecurity and c.relowner=(current_user::regrole)::oid)
    or exists(select 1 from pg_catalog.pg_class c cross join lateral pg_catalog.aclexplode(
      coalesce(c.relacl,pg_catalog.acldefault('r',c.relowner))) a where c.oid=v_oid and a.grantee<>c.relowner)
    or exists(select 1 from pg_catalog.pg_trigger t where t.tgrelid=v_oid)
    or exists(select 1 from pg_catalog.pg_constraint c where c.conrelid=v_oid and c.contype='f') then
    raise exception 'CORRECTION_A0_PRIVATE_TOKEN_TABLE_CONTRACT_INVALID' using errcode='55000';end if;
  if not exists(select 1 from pg_catalog.pg_constraint c where c.conrelid=v_oid and c.contype='u'
    and c.conname='timesheet_correction_a0_fences_identity_key' and c.convalidated and not c.condeferrable
    and exists(select 1 from pg_catalog.pg_index i where i.indexrelid=c.conindid and i.indisunique
      and i.indisvalid and i.indisready and i.indnullsnotdistinct)) then
    raise exception 'CORRECTION_A0_TOKEN_UNIQUE_ARBITER_INVALID' using errcode='55000';end if;
  if current_setting('block_size')::integer<>8192 then
    raise exception 'CORRECTION_A0_TOKEN_INDEX_PROFILE_UNPROVED' using errcode='55000';end if;
  if not exists(select 1 from pg_catalog.pg_collation c where c.oid='pg_catalog.default'::regcollation
    and c.collisdeterministic) then
    raise exception 'CORRECTION_A0_JSONB_ARBITER_EQUALITY_UNPROVED' using errcode='55000';end if;
  select count(*) into v_count from pg_catalog.pg_attribute a join pg_catalog.pg_class c on c.oid=a.attrelid
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace join pg_catalog.pg_collation co on co.oid=a.attcollation
    where n.nspname='public' and not a.attisdropped and a.attnum>0 and a.atttypid='text'::regtype
      and co.collisdeterministic and (c.relname,a.attname) in(values('timesheets','booking_id'),('invoice_lines','booking_id'),
        ('timesheets','occupant_key_norm'),('candidates','key_norm'));
  if v_count<>4 then raise exception 'CORRECTION_A0_NATIVE_EQUALITY_CHANGED' using errcode='55000';end if;
  for f in select to_regprocedure(s.signature) as oid,s.signature from(values
    ('private.timesheet_correction_a0_fence_tokens_v1(uuid[],uuid[],text[],text[],jsonb[])'),
    ('private.timesheet_correction_a0_fence_acquire_v1(jsonb,boolean)'),
    ('private.timesheet_correction_a0_family_tokens_retain_v1(uuid,uuid[])'),
    ('private.timesheet_correction_a0_request_tokens_retain_v1(private.timesheet_correction_requests)')) s(signature)
  loop
    if f.oid is null or not exists(select 1 from pg_catalog.pg_proc p where p.oid=f.oid
      and p.prosecdef and p.proowner=(current_user::regrole)::oid)
      or exists(select 1 from pg_catalog.pg_proc p cross join lateral pg_catalog.aclexplode(
        coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) a where p.oid=f.oid and a.grantee<>p.proowner)
      or has_function_privilege('anon',f.oid,'EXECUTE') or has_function_privilege('authenticated',f.oid,'EXECUTE')
      or has_function_privilege('service_role',f.oid,'EXECUTE') then
      raise exception 'CORRECTION_A0_TOKEN_PRIVATE_HELPER_CONTRACT_INVALID' using errcode='55000';end if;
  end loop;
  select pg_get_functiondef(to_regprocedure('private.timesheet_correction_a0_fence_acquire_v1(jsonb,boolean)')) into v_body;
  if position('for share nowait' in lower(v_body))=0 or position('do update set touch_bit=not f.touch_bit' in lower(v_body))=0
    or position('do nothing returning 1 into v_inserted' in lower(v_body))=0
    or position('set_config(''lock_timeout'',''1ms'',true)' in v_body)=0 then
    raise exception 'CORRECTION_A0_TOKEN_PROTOCOL_DECLARATION_INVALID' using errcode='55000';end if;
  for r in select * from(values
    ('timesheet_validations','zzzz_healthroster_a0_reference_before_v1','private.timesheet_correction_a0_reference_before_v1()'),
    ('invoice_lines','aaa_healthroster_a0_reference_before_v1','private.timesheet_correction_a0_reference_before_v1()'),
    ('timesheets_financials','aaa_healthroster_a0_financial_before_v1','private.timesheet_correction_a0_reference_before_v1()'),
    ('timesheets','aaa_healthroster_a0_booking_before_v1','private.timesheet_correction_a0_booking_before_v1()'),
    ('candidates','zzzz_healthroster_a0_candidate_before_v1','private.timesheet_correction_a0_candidate_before_v1()')) t(table_name,trigger_name,signature)
  loop
    if not exists(select 1 from pg_catalog.pg_trigger t where t.tgrelid=to_regclass('public.'||r.table_name)
      and t.tgname=r.trigger_name and t.tgfoid=to_regprocedure(r.signature) and t.tgenabled in('O','A')
      and (t.tgtype&1)=1 and(t.tgtype&2)=2 and(t.tgtype&4)=4 and(t.tgtype&8)=8 and(t.tgtype&16)=16) then
      raise exception 'CORRECTION_A0_ALL_MUTATION_TRIGGER_CONTRACT_INVALID' using errcode='55000';end if;
  end loop;
  -- Preserve the genuine existing NEW/UPGRADE isolation envelopes. No call to
  -- a native business owner or touch helper is made by this catalogue verifier.
end;
$a0_transaction_fence_contract$;
\ir 14102026_1030_a0_transaction_fence_reviewed_bodies.sqlinc
