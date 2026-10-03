-- The resumed historical H1/H2 closure replays the old pay-cache trigger
-- definition after Stage 2 A35 was already ledgered. CREATE OR REPLACE resets
-- that function's planner-local configuration, so restore the exact reviewed
-- A35 setting after all historical function reassertions. This changes no
-- function body, payment decision, provider call or external side effect.
\ir 26092026_0207_banking_pay_stage2_session_plan_cache_mode_v1.sql

-- The adjacent Stage 2 preview and recovery definitions were also restored
-- after A34's ledgered ALTERs. Reassert its six exact jit settings; the two-
-- argument recovery overload and the other owners retain the same values.
BEGIN;
ALTER FUNCTION public.pay_preview_candidate_build_finance_case_baseline(jsonb,uuid) SET jit = off;
ALTER FUNCTION private.pay_workbench_recovery_selection_overlay_apply_v1(uuid,uuid,jsonb) SET jit = off;
ALTER FUNCTION public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(uuid,uuid) SET jit = off;
ALTER FUNCTION public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(uuid,uuid,jsonb) SET jit = off;
ALTER FUNCTION private.pay_workbench_modal_blocked_presentation_v2(jsonb,jsonb) SET jit = off;
ALTER FUNCTION public.pay_workbench_draft_certificate_constituent_ref_page_v8(uuid,integer,integer,text) SET jit = off;
COMMIT;
