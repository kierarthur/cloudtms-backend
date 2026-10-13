-- Data-free owner declarations, exact body hashes and early refusal only.
-- No positive component/CAS/transport/release qualification or activation.
\set ON_ERROR_STOP on
begin;
do $automatic_declarations$
declare r record; p pg_catalog.pg_proc%rowtype;
begin
  for r in select * from (values
    ('private.weekly_source_staged_source_documents_v1(uuid)','5885da5f42fd5720abafc25e2db83ea0f31a72eda91853dc412ec635d999b426'),
    ('private.weekly_source_staged_publication_context_v1(uuid,uuid)','50abe98b4a7a89d5b0a8aec4577cbd5859b32e9b9232d2eecd85924645eaeea6'),
    ('private.weekly_source_approved_detail_evidence_v1(uuid,jsonb)','9c24f87d60218ff52700e5e31320b2a343c6b5edf51689780908c22705c51e43'),
    ('private.weekly_source_pay_query_local_receipt_event_v1(uuid,uuid,uuid,uuid)','2141c11ae0a472c7154b5a90a1bdaf9dc9fa8365c13d09b160bb68d95ad1e03d'),
    ('private.weekly_source_pay_query_c1_receipt_event_v1(uuid,uuid,uuid,uuid)','4b8a308150ecb3aeb0c5ddcfda81e03626aa6b03454441cb699f3a7c6aa5ef6f'),
    ('private.weekly_source_pay_query_next_receipt_event_v1(uuid,uuid,uuid,uuid)','e307b64f3c4a2be37eb3137ced47f0036bef187f2235c2687adbfd94b1bb5223'),
    ('private.weekly_source_staged_prior_wait_receipt_v1(uuid,uuid)','34b636779b3fa99785d15004fe4c47ce61bedef6b01cda8560040ad3d2858cab'),
    ('private.weekly_source_expense_authority_evidence_v1(uuid,jsonb)','a6e8f983d20fdd210a41b018b07309f5177a9fea3c2a130dccaad84f5dae4eec'),
    ('private.weekly_source_approved_expense_evidence_v1(uuid,jsonb)','61cb875c397d5fee968d805f3b489831af3ab2f0e5c04ef65c72eaf802130b8e'),
    ('private.weekly_source_staged_entitlement_components_v1(uuid,uuid)','227b134b376348d95260dd9f37537604310cffb2aaff4b3ad05b39674a539b84'),
    ('private.weekly_source_automatic_reconcile_delta_assert_v1(uuid,uuid)','f0e596041a2a0a225192a4b79ea887bd96806dd9045ec8635d0927756b85d9da')
  ) expected(signature,body_sha256) loop
    select * into p from pg_catalog.pg_proc where oid=pg_catalog.to_regprocedure(r.signature);
    if not found or p.proowner<>current_user::regrole::oid or not p.prosecdef or p.provolatile<>'s'
      or p.prokind<>'f' or p.pronargdefaults<>0 or p.prorettype<>'jsonb'::regtype::oid
      or p.proconfig is distinct from array['search_path=pg_catalog, pg_temp']::text[]
      or pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(p.prosrc,'UTF8')),'hex')<>r.body_sha256
      or pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE')
      or pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE')
      or pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE')
      or exists(select 1 from pg_catalog.aclexplode(coalesce(p.proacl,pg_catalog.acldefault('f',p.proowner))) a
        where a.grantee=0 and a.privilege_type='EXECUTE') then
      raise exception 'AUTOMATIC_NATIVE_PRIVATE_DECLARATION_FAILED' using errcode='55000',detail=r.signature;
    end if;
  end loop;
  -- Full ordinary receipt wrappers and Local retain their reviewed body bytes.
  for r in select * from (values
    ('public.weekly_exceptional_pay_complete_local_v1(jsonb)','b784c6b55a7255f34f2d46aaf10ed5ad98aceccd22ba400207343bf1c0c6f5cf'),
    ('private.weekly_source_pay_query_local_receipt_v1(uuid,uuid,uuid)','fc492a7e92c911ef618bf26808b9ec9fb483a2d5745e217d76def3f0672a70f7'),
    ('private.weekly_source_pay_query_c1_receipt_v1(uuid,uuid,uuid)','37cb2de5cf4038903079a965171540cb08be6277d3f8ba038bcf9643ca2d3923'),
    ('private.weekly_source_pay_query_next_receipt_v1(uuid,uuid,uuid)','037b250753c5230362737684061bef25e9f1e3673155326784b76240213be711')
  ) expected(signature,body_sha256) loop
    if not exists(select 1 from pg_catalog.pg_proc retained_proc where retained_proc.oid=pg_catalog.to_regprocedure(r.signature)
      and pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(retained_proc.prosrc,'UTF8')),'hex')=r.body_sha256) then
      raise exception 'AUTOMATIC_NATIVE_EXISTING_OWNER_BODY_CHANGED' using errcode='55000',detail=r.signature;
    end if;
  end loop;
end;
$automatic_declarations$;
do $automatic_early_refusals$
declare refused boolean; missing_id constant uuid:='13102026-2206-4000-8000-000000000001';
begin
  refused:=false;
  begin
    perform private.weekly_source_staged_publication_context_v1(null,null);
  exception when sqlstate '22023' then
    if sqlerrm<>'WEEKLY_SOURCE_STAGED_OWNERSHIP_UNAVAILABLE' then raise; end if; refused:=true;
  end;
  if not refused then raise exception 'AUTOMATIC_NATIVE_NULL_STAGE_NOT_REFUSED'; end if;
  refused:=false;
  begin
    perform private.weekly_source_automatic_reconcile_delta_assert_v1(null,null);
  exception when sqlstate '22023' then
    if sqlerrm<>'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' then raise; end if; refused:=true;
  end;
  if not refused then raise exception 'AUTOMATIC_NATIVE_NULL_DELTA_NOT_REFUSED'; end if;
  -- Prove the missing selector before calling SELECT STRICT; never touch a real request.
  if exists(select 1 from public.weekly_exceptional_c1_publication_requests where id=missing_id) then
    raise exception 'AUTOMATIC_NATIVE_VERIFIER_SENTINEL_COLLISION'; end if;
  refused:=false;
  begin
    perform private.weekly_source_staged_publication_context_v1(missing_id,missing_id);
  exception when no_data_found then refused:=true; end;
  if not refused then raise exception 'AUTOMATIC_NATIVE_MISSING_STAGE_NOT_REFUSED'; end if;
  refused:=false;
  begin
    perform private.weekly_source_automatic_reconcile_delta_assert_v1(missing_id,missing_id);
  exception when no_data_found then refused:=true; end;
  if not refused then raise exception 'AUTOMATIC_NATIVE_MISSING_DELTA_NOT_REFUSED'; end if;
  -- Presence is never readiness. Only exercise the proven missing-dependency arm.
  if pg_catalog.to_regprocedure('private.weekly_source_automatic_reconcile_producer_ready_v1(uuid)') is null then
    refused:=false;
    begin
      perform private.weekly_source_automatic_reconcile_capability_assert_v1(null);
    exception when sqlstate '55000' then
      if sqlerrm<>'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_DELTA_UNAVAILABLE' then raise; end if; refused:=true;
    end;
    if not refused then raise exception 'AUTOMATIC_NATIVE_MISSING_CAPABILITY_NOT_REFUSED'; end if;
  end if;
  if pg_catalog.to_regprocedure('private.weekly_source_bulk_producer_ready_v1(uuid)') is null
    or pg_catalog.to_regprocedure('private.weekly_source_bulk_source_cas_v1(uuid,uuid[])') is null then
    refused:=false;
    begin
      perform private.weekly_source_bulk_source_cas_assert_v1(null,'{}'::uuid[]);
    exception when sqlstate '55000' then
      if sqlerrm<>'WEEKLY_SOURCE_BULK_PRODUCER_UNAVAILABLE' then raise; end if; refused:=true;
    end;
    if not refused then raise exception 'AUTOMATIC_NATIVE_MISSING_BULK_NOT_REFUSED'; end if;
  end if;
end;
$automatic_early_refusals$;
rollback;
