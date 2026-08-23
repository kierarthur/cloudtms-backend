begin;

set local role service_role;

select
  private._candidate_manager_email_claim_route_current_v1(
    'f2000000-0000-4000-8000-000000000001'::uuid,
    'f2000000-0000-4000-8000-000000000002'::uuid,
    1,
    repeat('0',64),
    'f2000000-0000-4000-8000-000000000003'::uuid,
    'f2000000-0000-4000-8000-000000000004'::uuid,
    1
  ) is false as unknown_route_fails_closed;

rollback;

select 'PASS'::text as candidate_manager_email_claim_route_guard_verification;
