-- Explicit write-path validator for the two client correction settings.
--
-- Deliberately not a trigger. A client that later ceases to be
-- import-authoritative must retain its stored values; the resolver ignores
-- them while the client is ineligible and uses them again only if eligibility
-- is restored. Callers use this helper only when explicitly submitting either
-- correction-setting field.

create or replace function public._ctms_assert_import_correction_settings_write_v1(
  p_is_nhsp boolean,
  p_autoprocess_hr boolean,
  p_no_timesheet_required boolean,
  p_reversal_complete_financials_date public.correction_financials_date_basis_enum,
  p_reversal_replacement_financials_date public.correction_financials_date_basis_enum
)
returns void
language plpgsql
immutable
security definer
set search_path to 'public', 'pg_temp'
as $function$
begin
  if p_reversal_complete_financials_date is null
     and p_reversal_replacement_financials_date is null then
    return;
  end if;

  if not (
    coalesce(p_is_nhsp, false)
    or (
      coalesce(p_autoprocess_hr, false)
      and coalesce(p_no_timesheet_required, false)
    )
  ) then
    raise exception 'CORRECTION_POLICY_NOT_AVAILABLE_FOR_CLIENT'
      using errcode = '22023',
            detail = jsonb_build_object(
              'code', 'CORRECTION_POLICY_NOT_AVAILABLE_FOR_CLIENT',
              'eligibility', 'is_nhsp OR (autoprocess_hr AND no_timesheet_required)',
              'retained_values_rule', 'stored values may remain but are ignored while ineligible'
            )::text;
  end if;
end;
$function$;

comment on function public._ctms_assert_import_correction_settings_write_v1(
  boolean,
  boolean,
  boolean,
  public.correction_financials_date_basis_enum,
  public.correction_financials_date_basis_enum
) is
  'Validates explicit writes of import-authoritative correction settings without blocking later eligibility changes or retained inactive values.';

revoke all on function public._ctms_assert_import_correction_settings_write_v1(
  boolean,
  boolean,
  boolean,
  public.correction_financials_date_basis_enum,
  public.correction_financials_date_basis_enum
) from public, anon, authenticated, service_role;
