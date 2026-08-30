-- LEGACY_UPGRADE-only proof that the immutable 8 August migration repairing one
-- exact TEST Workbench job is not applicable to LIVE. The release engine runs
-- this proof atomically with the legacy ledger acknowledgement and does not
-- execute the TEST-fixture mutation against LIVE.

do $legacy_nonapplicability$
begin
  if exists (
    select 1
    from public.banking_pay_workbench_jobs
    where id = '8d441fff-4153-413c-a522-72b6903a754f'::uuid
       or candidate_id = 'bfdc14ec-82a6-566c-b6d5-bf760ecaf030'::uuid
  ) then
    raise exception 'LEGACY_SCOPE_GENERATION_TEST_FIXTURE_PRESENT_IN_LIVE';
  end if;
end
$legacy_nonapplicability$;
