-- App-Ready corrective gate: remove direct browser authority from the exact
-- Candidate/MyTMS data-plane relation set. CloudTMS Workers retain service-role
-- access; Candidate business RPC semantics and unrelated data-plane objects
-- remain unchanged.

do $preflight$
declare
  v_name text;
  v_kind "char";
begin
  foreach v_name in array array[
    'candidate_job_titles','candidates','client_settings','clients',
    'contract_weeks','contracts','mail_outbox','settings_defaults',
    'timesheet_evidence','timesheets','timesheets_financials'
  ] loop
    select c.relkind into v_kind
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on n.oid=c.relnamespace
    where n.nspname='public' and c.relname=v_name;
    if v_kind is distinct from 'r'::"char" then
      raise exception 'CANDIDATE_MYTMS_RELATION_PREFLIGHT_FAILED:%', v_name;
    end if;
  end loop;

  select c.relkind into v_kind
  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on n.oid=c.relnamespace
  where n.nspname='public' and c.relname='candidate_activity_rollup';
  if v_kind is distinct from 'v'::"char" then
    raise exception 'CANDIDATE_MYTMS_VIEW_PREFLIGHT_FAILED:candidate_activity_rollup';
  end if;
end
$preflight$;

alter table public.candidate_job_titles enable row level security;
alter table public.candidates enable row level security;
alter table public.client_settings enable row level security;
alter table public.clients enable row level security;
alter table public.contract_weeks enable row level security;
alter table public.contracts enable row level security;
alter table public.mail_outbox enable row level security;
alter table public.settings_defaults enable row level security;
alter table public.timesheet_evidence enable row level security;
alter table public.timesheets enable row level security;
alter table public.timesheets_financials enable row level security;

revoke all on table
  public.candidate_activity_rollup,
  public.candidate_job_titles,
  public.candidates,
  public.client_settings,
  public.clients,
  public.contract_weeks,
  public.contracts,
  public.mail_outbox,
  public.settings_defaults,
  public.timesheet_evidence,
  public.timesheets,
  public.timesheets_financials
from public, anon, authenticated;

-- The view must evaluate with the caller's authority. Browser roles have no
-- view grant and no underlying-table grant or policy; service_role retains the
-- existing Worker-only read path.
alter view public.candidate_activity_rollup set (security_invoker=true);

grant select,insert,update,delete on table
  public.candidate_job_titles,
  public.candidates,
  public.client_settings,
  public.clients,
  public.contract_weeks,
  public.contracts,
  public.mail_outbox,
  public.settings_defaults,
  public.timesheet_evidence,
  public.timesheets,
  public.timesheets_financials
to service_role;
grant select on table public.candidate_activity_rollup to service_role;

revoke all on function public.candidate_delete_apply(uuid,uuid,text)
  from public, anon, authenticated;
revoke all on function public.candidate_delete_eligibility(uuid)
  from public, anon, authenticated;
revoke all on function public.candidate_list_ids(jsonb)
  from public, anon, authenticated;
revoke all on function public.candidate_picker_search(text,integer,integer,boolean)
  from public, anon, authenticated;

grant execute on function public.candidate_delete_apply(uuid,uuid,text)
  to service_role;
grant execute on function public.candidate_delete_eligibility(uuid)
  to service_role;
grant execute on function public.candidate_list_ids(jsonb)
  to service_role;
grant execute on function public.candidate_picker_search(text,integer,integer,boolean)
  to service_role;

alter function public.candidate_picker_search(text,integer,integer,boolean)
  set search_path to pg_catalog, public;

comment on view public.candidate_activity_rollup is
  'Worker-only Candidate activity projection. SECURITY INVOKER and browser-role ACL denial are required by the MyTMS App-Ready isolation gate.';
