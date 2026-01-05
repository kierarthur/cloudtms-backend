create or replace function public.trg_tsfin_client_hospitals_wakeup()
returns trigger
language plpgsql
as $function$
begin
  /*
    SAFETY: Do NOT enqueue timesheets whose current TSFIN snapshot is locked by invoice
    or already paid.

    Trigger is already wired:
      trg_tsfin_client_hospitals_wakeup_aiu
        AFTER INSERT OR UPDATE OF hospital_name_norm, client_id
        ON public.client_hospitals
        EXECUTE FUNCTION public.trg_tsfin_client_hospitals_wakeup()
  */

  with alias_norms as (
    select distinct lower(btrim(x)) as norm
    from jsonb_array_elements_text(
      case
        when tg_op = 'INSERT' then coalesce(new.hospital_name_norm, '[]'::jsonb)
        else coalesce(new.hospital_name_norm, '[]'::jsonb) || coalesce(old.hospital_name_norm, '[]'::jsonb)
      end
    ) as t(x)
    where x is not null and btrim(x) <> ''
  )
  insert into public.ts_financials_outbox (timesheet_id, reason)
  select distinct ts.timesheet_id, 'CONTEXT_CHANGED'::ts_fin_reason_enum
  from public.timesheets ts
  join alias_norms a
    on ts.hospital_norm = a.norm
  left join public.timesheets_financials tf
    on tf.timesheet_id = ts.timesheet_id
   and tf.is_current = true
  where ts.is_current = true
    and ts.authorised_at_server is not null
    and ts.revoked_at is null
    and (
      tf.timesheet_id is null
      or (tf.locked_by_invoice_id is null and tf.paid_at_utc is null)
    )
  on conflict (timesheet_id, reason)
  do update set next_attempt_at = null;

  return new;
end;
$function$;
