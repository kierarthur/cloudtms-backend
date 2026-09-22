-- Verifies the installed non-Candidate/MyTMS browser-isolation phase without
-- reading or mutating application rows.

do $general_browser_isolation_verification$
declare
  v_count integer;
  v_hash text;
  v_service_missing integer;
  v_browser_executable integer;
  v_browser_executable_identities text;
begin
  with protected_tables(name) as (
    values
      ('candidate_job_titles'),('candidates'),('client_settings'),('clients'),
      ('contract_weeks'),('contracts'),('mail_outbox'),('settings_defaults'),
      ('timesheet_evidence'),('timesheets'),('timesheets_financials'),
      ('candidate_app_accounts'),('candidate_app_sessions'),
      ('candidate_approval_requests'),('candidate_auth_challenges'),
      ('candidate_daily_availability_days'),
      ('candidate_daily_command_receipts'),('candidate_daily_rota_days'),
      ('candidate_daily_rota_generations'),
      ('candidate_daily_sheet_projection_outbox'),('candidate_notifications'),
      ('candidate_submission_components'),('candidate_submission_workflows'),
      ('invoice_document_versions')
  ), targets as (
    select
      c.relname,
      c.relrowsecurity,
      pg_catalog.has_table_privilege('service_role',c.oid,'SELECT') as svc_select,
      pg_catalog.has_table_privilege('service_role',c.oid,'INSERT') as svc_insert,
      pg_catalog.has_table_privilege('service_role',c.oid,'UPDATE') as svc_update,
      pg_catalog.has_table_privilege('service_role',c.oid,'DELETE') as svc_delete,
      pg_catalog.has_table_privilege('service_role',c.oid,'TRUNCATE') as svc_truncate,
      pg_catalog.has_table_privilege('service_role',c.oid,'REFERENCES') as svc_references,
      pg_catalog.has_table_privilege('service_role',c.oid,'TRIGGER') as svc_trigger,
      pg_catalog.has_table_privilege('anon',c.oid,'SELECT,INSERT,UPDATE,DELETE') as anon_access,
      pg_catalog.has_table_privilege('authenticated',c.oid,'SELECT,INSERT,UPDATE,DELETE') as auth_access
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public'
      and c.relkind in ('r','p')
      and c.relname not ilike '%candidate%'
      and not exists (select 1 from protected_tables p where p.name=c.relname)
  )
  select
    pg_catalog.count(*),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      relname||'|'||relrowsecurity::text||'|'||
      svc_select::text||svc_insert::text||svc_update::text||svc_delete::text||
      svc_truncate::text||svc_references::text||svc_trigger::text||'|'||
      anon_access::text||'|'||auth_access::text,
      E'\n' order by relname
    ),''))
  into v_count,v_hash
  from targets;

  -- Plan 6 adds 93 source-reconciliation relations. Seven are Candidate-
  -- named and remain under the dedicated Candidate isolation verifier; the
  -- other 86 extend this non-Candidate inventory. The sealed hash was
  -- regenerated only after proving zero anon/authenticated table access.
  -- Plan 6.2 Gate 1 adds four more non-Candidate relations, taking the
  -- inventory from 212 to 216: weekly_source_entitlement_decision_bundles,
  -- weekly_source_entitlement_heads, weekly_source_entitlement_head_components
  -- and weekly_source_pending_entitlement_bundles. The set difference against a
  -- pre-Plan-6.2 build was proved to be exactly those four rows, with nothing
  -- removed and no existing row changed, and each carries enabled and forced
  -- RLS, only the cloudtms_miget_service_owner_all policy, and no table
  -- privilege for anon, authenticated or service_role.
  -- Decision D8 then adds a fifth, weekly_source_root_authorisations, taking
  -- the inventory from 216 to 217. The set difference against a pre-Plan-6.2
  -- build was re-measured for this change and is exactly those five relations,
  -- with nothing removed and no existing row changed; the new one carries
  -- enabled and forced RLS, only the cloudtms_miget_service_owner_all policy,
  -- no table-level or column-level grant to any grantee but the owner, and no
  -- privilege for anon, authenticated or service_role.
  -- The approved source-rate-disparity journey adds the immutable Office
  -- acceptance ledger weekly_source_charge_acceptances, taking the inventory
  -- from 217 to 218. Removing that exact row from the installed 218-row census
  -- reproduces the prior seal 4621263ae3113627625f576c5563ff7c,
  -- proving that no earlier relation changed. The added table has enabled and
  -- forced RLS, only the established cloudtms_miget_service_owner_all policy,
  -- no anon/authenticated/service_role table privilege and no non-owner column
  -- privilege.
  if v_count<>218 or v_hash<>'9cdd16f38d3e11259fc4cd9f83f99b83' then
    raise exception 'GENERAL_RELATION_ISOLATION_VERIFICATION_FAILED:count=% hash=%',
      v_count,v_hash;
  end if;

  with targets as (
    select
      c.relname,
      ('security_invoker=true'=any(coalesce(c.reloptions,array[]::text[]))) as invoker,
      pg_catalog.has_table_privilege('service_role',c.oid,'SELECT') as svc_select,
      pg_catalog.has_table_privilege('anon',c.oid,'SELECT') as anon_select,
      pg_catalog.has_table_privilege('authenticated',c.oid,'SELECT') as auth_select
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind='v'
      and c.relname in (
        'timesheets_hr_view','v_contract_weeks_enriched','v_finance_cases_register',
        'v_legacy_contract_rate_lines_flat','v_mailshot_resolution_graph',
        'v_mailshot_src_client','v_mailshot_src_contract','v_mailshot_src_invoice',
        'v_mailshot_src_system','v_mailshot_src_timesheet','v_mailshot_src_umbrella',
        'v_outbox_unified','v_rates_client_defaults_enabled',
        'v_timesheets_daily_match','v_timesheets_details','v_timesheets_funnel',
        'v_timesheets_summary','v_timesheets_summary_base','v_ts_invoice_precheck',
        'vw_picker_clients'
      )
  )
  select
    pg_catalog.count(*),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      relname||'|'||invoker::text||'|'||svc_select::text||'|'||
      anon_select::text||'|'||auth_select::text,
      E'\n' order by relname
    ),''))
  into v_count,v_hash
  from targets;

  if v_count<>20 or v_hash<>'f7b3b9ccf07dd052c65b98932af9a76c' then
    raise exception 'GENERAL_VIEW_ISOLATION_VERIFICATION_FAILED:count=% hash=%',
      v_count,v_hash;
  end if;

  with targets as (
    select
      n.nspname||'.'||p.proname||'('||coalesce((
        select pg_catalog.string_agg(
          type_namespace.nspname||'.'||argument_type.typname,
          ',' order by argument.argument_ordinal
        )
        from pg_catalog.unnest(p.proargtypes::oid[]) with ordinality
          as argument(type_oid,argument_ordinal)
        join pg_catalog.pg_type argument_type on argument_type.oid=argument.type_oid
        join pg_catalog.pg_namespace type_namespace on type_namespace.oid=argument_type.typnamespace
      ),'')||')' as signature,
      pg_catalog.has_function_privilege('service_role',p.oid,'EXECUTE') as svc_execute,
      pg_catalog.has_function_privilege('anon',p.oid,'EXECUTE') as anon_execute,
      pg_catalog.has_function_privilege('authenticated',p.oid,'EXECUTE') as auth_execute
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid=p.pronamespace
    where n.nspname='public'
      and p.prosecdef
      and p.proname<>'cloudtms_data_api_mfa_gate'
      and p.proname not ilike '%candidate%'
      and p.proname not in (
        'timesheet_break_entry_effective_get_v1',
        'daily_zero_shifts_review_create_v1'
      )
  )
  select
    pg_catalog.count(*),
    pg_catalog.count(*) filter (where not svc_execute),
    pg_catalog.count(*) filter (where anon_execute or auth_execute),
    pg_catalog.string_agg(signature,',' order by signature)
      filter (where anon_execute or auth_execute),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      signature||'|'||svc_execute::text||'|'||anon_execute::text||'|'||auth_execute::text,
      E'\n' order by signature
    ),''))
  into v_count,v_service_missing,v_browser_executable,
       v_browser_executable_identities,v_hash
  from targets;

  -- The settled Workbench certificate boundary adds eleven non-Candidate
  -- SECURITY DEFINER RPCs. The row-backed Draft V8 consumer then adds thirteen
  -- exact service-only RPCs. The later Candidate and expense-carrier releases
  -- remain additive. The pending-expense Timesheet deletion boundary adds its
  -- service-only preview and confirmed-apply RPCs. The unified Outbox projection
  -- adds one service-only RPC for complete cross-source sorting and membership.
  -- The unpaid-cancellation communication boundary adds one service-only notice
  -- reconciliation RPC and preserves zero direct browser execution.
  -- The planned-week Candidate delete boundary adds three non-Candidate-named,
  -- service-only RPCs for preview, rejection and guarded deletion.
  -- The owner-internal reference issuer and established shared-session opener
  -- remain withheld, so browser execution stays zero.
  -- The combined hash includes the Plan 6 Weekly Source RPC inventory. Its
  -- additive service-only functions preserve the pre-existing count of
  -- unrelated service omissions and expose no browser-executable identity.
  -- Plan 6.2 Gate 8 adds exactly two more non-Candidate SECURITY DEFINER
  -- entry points, taking the inventory from 764 to 766:
  -- public.weekly_source_mode_a_dispatch_atomic_v1(jsonb) and
  -- public.weekly_source_mode_a_reference_apply_atomic_v1(jsonb). The set
  -- difference against a pre-Plan-6.2 build was proved to be exactly those two
  -- rows, with nothing removed and no existing row changed. Both are
  -- service-only with a fixed search_path, both are registered in
  -- private._weekly_source_acl_service_rpc_contract_v1(), the count of
  -- unrelated service omissions is unchanged at 74, and browser execution
  -- stays zero.
  -- MOVED at the Plan 6.2 final seals pass (WP-15d), 18 September 2026, on the
  -- express instruction of HANDOVER 2 round-5 Part C: "The browser-isolation
  -- RPC seal must also move to the independently verified inventory.
  -- browser_executable = 0 proves the security property but does not permit a
  -- stale inventory seal. Do not ship 766/74."
  --
  -- Measured on a full NEW build from empty (241 migrations, 645 repeatables):
  --   count=782  service_missing=75  browser_executable=0
  --   hash=83c87f3491dddb2c8c1383843392db7d
  -- Part C names 778/75; that figure was taken at the round-5 request and four
  -- further service RPCs have landed since, so the seal moves to the value
  -- measured now, not to the one quoted then. Both are recorded in
  -- IMPL\reports\WP-15d_REPORT.md.
  --
  -- Proof, by the method of IMPL\reports\WP-15a_REPORT.md section 6.3: the same
  -- inventory query was run against ws62_wp15d_base, a clone of the
  -- pre-Plan-6.2 ws62_template, which reproduced the original sealed
  -- 764/74/9eae90be87c49a28ed5e7069f65d93fd exactly. The set difference against
  -- it is 18 rows present in NEW, 0 absent from NEW and 0 changed, so nothing
  -- was removed and no existing routine's privileges moved. Every one of the 18
  -- is prosecdef, owned by postgres and carries a fixed search_path; 17 hold
  -- exactly one foreign grant, service_role EXECUTE, and are registered in
  -- private._weekly_source_acl_service_rpc_contract_v1(). The eighteenth,
  -- public.tsfin_weekly_source_hours_v1(uuid), is deliberately granted to
  -- nobody and is the whole of the 74 -> 75 move in service_missing; it is
  -- reached only from inside public.tsfin_report_timesheets_v2. anon and
  -- authenticated hold EXECUTE on none of the 18, so browser_executable stays
  -- 0 and the security property is unchanged.
  --
  -- MOVED AGAIN by WP-44 (notification defects), 18 September 2026, for the one
  -- service-only RPC that finding F2 requires:
  -- public.weekly_source_message_dispatch_snapshot_failure_atomic_v1(jsonb).
  -- Without it a TRANSIENT failure of the Candidate push snapshot step was
  -- recorded as a PERMANENT suppression carrying a fabricated control-plane
  -- snapshot identity, and the notification could never be re-claimed.
  --
  -- Measured on two full NEW builds from empty of the same tree, one without
  -- the new routine and one with it (242 migrations, 647 repeatables):
  --   before  count=782  service_missing=75  browser_executable=0
  --           hash=83c87f3491dddb2c8c1383843392db7d   (reproduces the seal above)
  --   after   count=783  service_missing=75  browser_executable=0
  --           hash=f03f6dec444e8694119fab411ee04f6a
  -- The set difference between those two inventories is exactly one row present
  -- in the later build, 0 absent from it and 0 changed:
  --   public.weekly_source_message_dispatch_snapshot_failure_atomic_v1(pg_catalog.jsonb)
  --     |svc_execute=true|anon_execute=false|auth_execute=false
  -- It is prosecdef, owned by postgres, VOLATILE, carries the fixed
  -- search_path 'public, private, pg_catalog, pg_temp', holds exactly one
  -- foreign grant (service_role EXECUTE, acl postgres=X/postgres
  -- service_role=X/postgres) and is registered in
  -- private._weekly_source_acl_service_rpc_contract_v1(). anon and
  -- authenticated hold EXECUTE on it in neither build, so browser_executable
  -- stays 0, service_missing is unchanged at 75, and the security property this
  -- verifier exists to protect is unchanged. Recorded in
  -- IMPL\reports\WP-44_NOTIFICATION_DEFECTS.md and handed to WP-15d in
  -- IMPL\handoffs\WP-44_NEEDS.md for the final seals pass.
  --
  -- MOVED AGAIN by WP-59 (the correction-session exit), 19 September 2026, for
  -- the one service-only RPC that WP-50 finding F3 requires:
  -- public.weekly_source_correct_final_cancel_atomic_v1(jsonb).  Without it an
  -- abandoned Correct-final-source correction session could never leave the
  -- states inside weekly_final_source_correction_sessions_active_uq, and one
  -- person walking away held that Trust and cutoff out of service for ever.
  --
  -- Moved under ruling B1: a measured figure may be sealed only when it is
  -- re-measured on the exact candidate, the membership proof shows the expected
  -- additions with zero unexplained removals or changes, and
  -- browser_executable = 0.  All three were established, not relayed:
  --
  -- CONTROL 1 -- a full NEW build from empty of the same tree WITHOUT the new
  -- routine, banking_modal_v2_release5901_20260919, ran this verifier in its own
  -- build and PASSED at the then-pinned 783 / f03f6dec444e8694119fab411ee04f6a.
  --
  -- CONTROL 2 -- on the candidate build, the routine was dropped inside a
  -- rolled-back transaction and this verifier's OWN inventory query was re-run
  -- against the resulting catalogue.  It returned 783 /
  -- f03f6dec444e8694119fab411ee04f6a exactly, which is what proves the
  -- measurement method here is the verifier's and not the package's.
  --
  --   control    count=783  service_missing=75  browser_executable=0
  --              hash=f03f6dec444e8694119fab411ee04f6a   (reproduces the seal above)
  --   candidate  count=784  service_missing=75  browser_executable=0
  --              hash=81b65eaa75eeb742b54f17c01040d979
  --
  -- MEMBERSHIP PROOF, row by row against that control:
  --   additions=1  removals=0  changes=0  browser_executable=0
  --   ADDED public.weekly_source_correct_final_cancel_atomic_v1(pg_catalog.jsonb)
  --         |svc=true|anon=false|auth=false
  -- It is prosecdef, owned by postgres, VOLATILE, carries the fixed
  -- search_path 'public, private, pg_catalog, pg_temp', holds exactly one
  -- foreign grant (service_role EXECUTE) and is registered in
  -- private._weekly_source_acl_service_rpc_contract_v1().  anon and
  -- authenticated hold EXECUTE on it in neither measurement, so
  -- browser_executable stays 0, service_missing is unchanged at 75, and the
  -- security property this verifier exists to protect is unchanged.
  --
  -- RE-MEASURED AND MOVED AGAIN within the same WP-59 window, to 785.  Between
  -- WP-59's first measurement and its final build from empty, the package that
  -- owns the WP-50 F2 withdraw-admission owner landed
  -- public.weekly_source_invoice_withdraw_admission_atomic_v1(jsonb) into the
  -- same tree.  The tree now carries two additions, not one, and a pin of 784
  -- would redden every build.  Ruling B1's words are "seal the final measured
  -- membership", so the FINAL membership is what is sealed here.
  --
  -- THREE-WAY CONTROL, on banking_modal_v2_release5905_20260919, a build from
  -- empty, inside one rolled-back transaction, using this verifier's own
  -- inventory query at each step:
  --   tree (both new routines) count=785 service_missing=75 browser_executable=0
  --                            hash=f07a574367b85de4c57642e28744f69b
  --   minus WP-59's routine    count=784 service_missing=75 browser_executable=0
  --                            hash=b61c219afae37e7629a673fe4b9c801a
  --   minus BOTH (the control) count=783 service_missing=75 browser_executable=0
  --                            hash=f03f6dec444e8694119fab411ee04f6a
  -- The control reproduces WP-44's seal exactly, which is what proves the
  -- measurement method is this verifier's own and not the package's.
  --
  -- MEMBERSHIP PROOF against that control: additions=2, removals=0, changes=0,
  -- browser_executable=0 at every step.
  --   ADDED public.weekly_source_correct_final_cancel_atomic_v1(pg_catalog.jsonb)
  --         |svc=true|anon=false|auth=false        (WP-59, this package)
  --   ADDED public.weekly_source_invoice_withdraw_admission_atomic_v1(pg_catalog.jsonb)
  --         |svc=true|anon=false|auth=false        (the F2 withdraw-admission package)
  -- Both are registered in private._weekly_source_acl_service_rpc_contract_v1()
  -- and in this repository's independent expected set, each by its own owner.
  -- WP-59 seals the MEMBERSHIP measured here; it makes no claim about the other
  -- package's design, only that the row it adds is service-only and reachable
  -- by neither anon nor authenticated.
  --
  -- WAS 783 / f03f6dec444e8694119fab411ee04f6a (sealed by WP-44).
  -- THEN 784 / 81b65eaa75eeb742b54f17c01040d979 (WP-59's own routine alone).
  -- IS  785 / f07a574367b85de4c57642e28744f69b (the final measured membership).
  -- Recorded in IMPL\reports\WP-59_CORRECTION_SESSION_EXIT.md.
  --
  -- RE-MEASURED on the Stage 9 final candidate, 19 September 2026, after the
  -- owner-approved Office and completed-Timesheet-copy work.  This is a
  -- controlled membership change, not a count-only re-pin:
  --
  --   previous seal count=785 service_missing=75 browser_executable=0
  --                 hash=f07a574367b85de4c57642e28744f69b
  --   current tree  count=788 service_missing=75 browser_executable=0
  --                 hash=65782ff06a913aeb6ad0afa34842ec82
  --
  -- The previous membership is reproduced exactly from the current installed
  -- catalogue when the four named current additions are removed and the one
  -- explicitly retired whole-invoice withdrawal RPC is restored as the same
  -- service-only row it held in the prior seal:
  --
  --   REMOVED (approved retirement)
  --     public.weekly_source_invoice_withdraw_admission_atomic_v1(pg_catalog.jsonb)
  --       |svc=true|anon=false|auth=false
  --   ADDED (Office source-charge warning acceptance)
  --     public.weekly_source_charge_accept_atomic_v1(pg_catalog.jsonb)
  --       |svc=true|anon=false|auth=false
  --   ADDED (completed-Timesheet informational-copy delivery)
  --     public.weekly_source_completed_pack_copy_commit_atomic_v1(pg_catalog.jsonb)
  --       |svc=true|anon=false|auth=false
  --     public.weekly_source_completed_pack_copy_due_list_v1(pg_catalog.jsonb)
  --       |svc=true|anon=false|auth=false
  --     public.weekly_source_completed_pack_copy_status_sync_v1(pg_catalog.jsonb)
  --       |svc=true|anon=false|auth=false
  --
  -- Three controls were run against the PostgreSQL 17.11 NEW build using this
  -- verifier's own target query and ordering.  Removing only the three copy
  -- RPCs returns 785 / fb4b817bcbfce6b61345fd0ad0d64d69.  Removing the charge
  -- acceptance RPC as well returns the unchanged WP-44 base at
  -- 783 / f03f6dec444e8694119fab411ee04f6a.  Adding back the correction-session
  -- exit and the retired withdrawal signature reproduces the previous final
  -- seal exactly at 785 / f07a574367b85de4c57642e28744f69b.
  --
  -- Every added RPC is SECURITY DEFINER, owned by the release owner, has a
  -- fixed search_path, is executable by service_role and by neither anon nor
  -- authenticated, and is present in both sides of the independent Weekly
  -- Source ACL contract.  Therefore browser_executable remains zero and the
  -- unrelated service_missing figure remains 75.
  --
  -- Stage 7's protected TEST upgrade then proved the complete installed
  -- catalogue includes the approved invoice/report projection introduced by
  -- 17092026_1200_weekly_source_audit_and_export_v1.sql:
  --
  --   public.weekly_source_invoice_report_rows_v1(pg_catalog.jsonb)
  --     |svc=true|anon=false|auth=false
  --
  -- This is the sole 788 -> 789 membership change.  It is registered in the
  -- Weekly Source ACL contract and preserves both the historical unrelated
  -- service_missing count and zero browser-executable routines.
  --
  -- Stage 8's NHSP Trust-to-Client report-scope owner adds exactly one further
  -- service-only routine:
  --
  --   public.weekly_source_nhsp_report_scope_resolve_atomic_v1(pg_catalog.jsonb)
  --     |svc=true|anon=false|auth=false
  --
  -- The protected TEST upgrade measured 790 / 75 / 0 with hash
  -- 13bbb357c119f705b01b7336d72997b1.  The routine is registered in both
  -- independent Weekly Source ACL sets, owned by the release owner, has a
  -- fixed search_path, and preserves zero browser-executable routines.
  --
  -- The service-view bridge introduced for the approved-hours projection adds
  -- exactly one further service-only wrapper:
  --
  --   public.timesheet_settings_authority_frozen_get_v1(pg_catalog.uuid)
  --     |svc=true|anon=false|auth=false
  --
  -- The protected TEST release rehearsal measured 791 / 75 / 0 with hash
  -- 46bd8e9cf5bf76e053b0d06bb973ed4e.  The wrapper is SECURITY DEFINER,
  -- owned by the release owner, has a fixed search_path, and preserves both
  -- the historical unrelated service_missing count and zero browser-executable
  -- routines.
  if v_count<>791 or v_service_missing<>75 or v_browser_executable<>0
     or v_hash<>'46bd8e9cf5bf76e053b0d06bb973ed4e' then
    raise exception 'GENERAL_RPC_ISOLATION_VERIFICATION_FAILED:count=% service_missing=% browser_executable=% browser_executable_identities=% hash=%',
      v_count,v_service_missing,v_browser_executable,
      v_browser_executable_identities,v_hash;
  end if;

  with targets as (
    select
      c.relname,
      pg_catalog.has_sequence_privilege('service_role',c.oid,'USAGE') as svc_usage,
      pg_catalog.has_sequence_privilege('service_role',c.oid,'SELECT') as svc_select,
      pg_catalog.has_sequence_privilege('service_role',c.oid,'UPDATE') as svc_update,
      pg_catalog.has_sequence_privilege('anon',c.oid,'USAGE,SELECT,UPDATE') as anon_access,
      pg_catalog.has_sequence_privilege('authenticated',c.oid,'USAGE,SELECT,UPDATE') as auth_access
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relkind='S'
      and c.relname not ilike '%candidate%'
  )
  select
    pg_catalog.count(*),
    pg_catalog.md5(coalesce(pg_catalog.string_agg(
      relname||'|'||svc_usage::text||svc_select::text||svc_update::text||'|'||
      anon_access::text||'|'||auth_access::text,
      E'\n' order by relname
    ),''))
  into v_count,v_hash
  from targets;

  -- The authoritative audit order migration adds the service-only identity
  -- sequence behind audit_events.event_sequence.  It is not available to
  -- anon or authenticated and changes the sealed inventory from 8 to 9.
  if v_count<>9 or v_hash<>'7cd05e540b00e9ad067c6fc6d98e4b79' then
    raise exception 'GENERAL_SEQUENCE_ISOLATION_VERIFICATION_FAILED:count=% hash=%',
      v_count,v_hash;
  end if;

  with target_defaults as (
    select
      d.defaclrole,
      d.defaclobjtype::text as object_type,
      case when x.grantee=0 then 'PUBLIC' else gr.rolname end as grantee,
      x.privilege_type,
      x.is_grantable
    from pg_catalog.pg_default_acl d
    join pg_catalog.pg_namespace n on n.oid=d.defaclnamespace
    join pg_catalog.pg_roles r on r.oid=d.defaclrole
    cross join lateral pg_catalog.aclexplode(d.defaclacl) x
    left join pg_catalog.pg_roles gr on gr.oid=x.grantee
    where n.nspname='public'
      and d.defaclobjtype in ('r','f','S')
      and r.rolname=current_user
  ), required_service_privileges as (
    select
      d.defaclobjtype::text as object_type,
      x.privilege_type
    from pg_catalog.pg_default_acl d
    join pg_catalog.pg_namespace n on n.oid=d.defaclnamespace
    join pg_catalog.pg_roles r on r.oid=d.defaclrole
    cross join lateral pg_catalog.aclexplode(
      pg_catalog.acldefault(d.defaclobjtype,d.defaclrole)
    ) x
    where n.nspname='public'
      and d.defaclobjtype in ('r','f','S')
      and r.rolname=current_user
      and x.grantee=d.defaclrole
  )
  select
    (select pg_catalog.count(distinct object_type) from target_defaults),
    (select pg_catalog.count(*) from required_service_privileges required
      where not exists (
        select 1 from target_defaults installed
        where installed.object_type=required.object_type
          and installed.grantee='service_role'
          and installed.privilege_type=required.privilege_type
      )),
    (select pg_catalog.count(*) from target_defaults
      where grantee in ('PUBLIC','anon','authenticated'))
  into v_count,v_service_missing,v_browser_executable;

  if v_count<>3 or v_service_missing<>0 or v_browser_executable<>0 then
    raise exception 'GENERAL_DEFAULT_ACL_VERIFICATION_FAILED:kinds=% service_missing=% browser_entries=%',
      v_count,v_service_missing,v_browser_executable;
  end if;

  if not pg_catalog.has_function_privilege(
    'anon','public.cloudtms_data_api_mfa_gate()','EXECUTE'
  ) or not pg_catalog.has_function_privilege(
    'authenticated','public.cloudtms_data_api_mfa_gate()','EXECUTE'
  ) then
    raise exception 'GENERAL_MFA_PRE_REQUEST_EXECUTE_CONTRACT_CHANGED';
  end if;
end
$general_browser_isolation_verification$;
