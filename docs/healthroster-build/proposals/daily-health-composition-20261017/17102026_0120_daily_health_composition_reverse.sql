-- Additive reverse only. No native historical routine/table is changed.
drop function private.daily_rate_correction_health_result_basis_v1(uuid,uuid);
drop function private.daily_rate_correction_health_accept_basis_v1(jsonb);
drop function private.daily_rate_correction_health_review_prepare_v1(jsonb);
drop function private.daily_rate_correction_health_basis_v1(jsonb,boolean);
drop function private.daily_rate_correction_health_actor_require_v1(uuid,boolean);
