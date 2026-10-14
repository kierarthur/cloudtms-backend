-- Exact member evidence remains inside the existing invoice support store.
create or replace function private._invoice_support_member_guard_v1()
returns trigger language plpgsql security definer
set search_path to 'pg_catalog','public','pg_temp'
as $function$
begin
  if tg_op='UPDATE' and new.member_rows_json is distinct from old.member_rows_json
    and exists(select 1 from public.invoices i where i.id=old.invoice_id
      and i.status in('ISSUED','PAID')) then
    raise exception 'ISSUED_INVOICE_SUPPORT_MEMBER_IMMUTABLE' using errcode='55000';
  end if;
  if tg_op='DELETE' and old.member_rows_json<>'{}'::jsonb
    and exists(select 1 from public.invoices i where i.id=old.invoice_id
      and i.status in('ISSUED','PAID')) then
    raise exception 'ISSUED_INVOICE_SUPPORT_MEMBER_IMMUTABLE' using errcode='55000';
  end if;
  if tg_op<>'DELETE' and exists(select 1 from jsonb_each(new.member_rows_json) m
    where not pg_input_is_valid(m.key,'uuid') or jsonb_typeof(m.value)<>'array') then
    raise exception 'INVOICE_SUPPORT_MEMBER_SHAPE_INVALID' using errcode='22023';
  end if;
  if tg_op='DELETE' then return old; end if;
  return new;
end;
$function$;
drop trigger if exists trg_invoice_support_member_guard_v1 on public.invoice_hr_source_rows;
create trigger trg_invoice_support_member_guard_v1
before update or delete or insert on public.invoice_hr_source_rows
for each row execute function private._invoice_support_member_guard_v1();
alter function private._invoice_support_member_guard_v1() owner to postgres;
revoke all on function private._invoice_support_member_guard_v1() from public,anon,authenticated,service_role;
