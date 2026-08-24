-- One-time LEGACY_UPGRADE bridge for three historical trigger functions that
-- were installed outside public.schema_migrations on the original platform.
-- These no-op definitions exist only while the protected migration history is
-- replayed with application traffic disabled. Current repeatables must replace
-- all three before contract verification and atomic adoption are permitted.

\set ON_ERROR_STOP on

begin;

do $legacy_upgrade$
declare
  v_name text;
begin
  foreach v_name in array array[
    'pay_workbench_mark_candidate_dirty',
    'pay_workbench_mark_finance_case_dirty',
    'pay_workbench_mark_contract_client_dirty'
  ]
  loop
    if to_regprocedure(format('public.%I()', v_name)) is null then
      execute format($ddl$
        create function public.%I()
        returns trigger
        language plpgsql
        set search_path to 'public'
        as $function$
        begin
          perform 'CLOUDTMS_LEGACY_TRANSITION_SHIM';
          if TG_OP = 'DELETE' then
            return OLD;
          end if;
          return NEW;
        end
        $function$
      $ddl$, v_name);
    end if;
  end loop;
end
$legacy_upgrade$;

commit;
