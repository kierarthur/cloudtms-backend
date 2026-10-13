-- Repeatable CloudTMS function/view authority: banking_pay_next_job_lifecycle_clock_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- PF-06 D01 supporting trigger on the existing private job only. Counts and
-- business transitions stay owned by their existing routines. Clock changes
-- commit or roll back with that same row, and completed replays retain clocks.
create or replace function private.bpay_next_job_lifecycle_clock_v1()
returns trigger language plpgsql security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_now timestamptz := pg_catalog.clock_timestamp();
begin
  if TG_OP='INSERT' then
    NEW.created_at_utc:=v_now;
    NEW.started_at_utc:=case when NEW.status='LEASED' then v_now else null end;
    NEW.finished_at_utc:=case when NEW.status in ('DONE','FAILED') then v_now else null end;
  else
    NEW.created_at_utc:=OLD.created_at_utc;
    NEW.started_at_utc:=OLD.started_at_utc;
    NEW.finished_at_utc:=OLD.finished_at_utc;
    if OLD.created_at_utc is not null and OLD.started_at_utc is null
       and OLD.status<>'LEASED' and NEW.status='LEASED' then
      NEW.started_at_utc:=v_now;
    end if;
    if OLD.created_at_utc is not null and OLD.finished_at_utc is null and OLD.status not in ('DONE','FAILED')
       and NEW.status in ('DONE','FAILED') then
      NEW.finished_at_utc:=v_now;
    end if;
  end if;
  return NEW;
end
$function$;

alter function private.bpay_next_job_lifecycle_clock_v1() owner to postgres;
revoke all on function private.bpay_next_job_lifecycle_clock_v1() from public,anon,authenticated,service_role;
drop trigger if exists bpay_next_job_lifecycle_clock_v1 on private.bpay_next_job;
create trigger bpay_next_job_lifecycle_clock_v1 before insert or update
on private.bpay_next_job for each row
execute function private.bpay_next_job_lifecycle_clock_v1();

commit;
