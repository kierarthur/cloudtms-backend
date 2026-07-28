-- Allow the Import Review grouped timesheet-query workflow to use its
-- dedicated outbox type. Safe to rerun: an already-compatible constraint
-- is left untouched.

do $migration$
declare
  v_constraint_definition text;
begin
  select pg_get_constraintdef(c.oid)
    into v_constraint_definition
  from pg_constraint c
  where c.conrelid = 'public.mail_outbox'::regclass
    and c.conname = 'mail_outbox_type_check'
    and c.contype = 'c';

  if v_constraint_definition is null
     or position('TIMESHEET_QUERY' in v_constraint_definition) = 0 then
    if v_constraint_definition is not null then
      alter table public.mail_outbox
        drop constraint mail_outbox_type_check;
    end if;

    alter table public.mail_outbox
      add constraint mail_outbox_type_check
      check (
        type = any (
          array[
            'INVOICE'::text,
            'REMITTANCE'::text,
            'TSO_FAILURE'::text,
            'BROADCAST'::text,
            'TIMESHEET_QR'::text,
            'TIMESHEET_REFUSAL'::text,
            'TIMESHEET_GENERAL'::text,
            'TIMESHEET_QUERY'::text
          ]
        )
        or type = 'MAILSHOT_EMAIL'::text
      ) not valid;

    alter table public.mail_outbox
      validate constraint mail_outbox_type_check;
  end if;
end
$migration$;

