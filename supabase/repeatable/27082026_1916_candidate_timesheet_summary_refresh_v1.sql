-- Repeatable CloudTMS function/view authority: candidate_timesheet_summary_refresh_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

create or replace function private._candidate_timesheet_summary_revision_upsert_v1(
  p_identity_kind text,p_identity_id uuid,p_current_timesheet_id uuid,p_contract_week_id uuid
)
returns void
language plpgsql
security definer
set search_path=pg_catalog,public,private,extensions,pg_temp
as $function$
declare
  v_kind text:=upper(btrim(coalesce(p_identity_kind,'')));
  v_seq bigint;
begin
  if p_identity_id is null or v_kind not in ('TIMESHEET','CONTRACT_WEEK') then return; end if;
  if v_kind='TIMESHEET' and p_current_timesheet_id is null then return; end if;

  v_seq:=nextval('private.candidate_timesheet_summary_revision_seq'::regclass);
  insert into private.candidate_timesheet_summary_revisions as revision_row(
    identity_kind,identity_id,current_timesheet_id,contract_week_id,revision_seq,changed_at_utc
  ) values (
    v_kind,p_identity_id,p_current_timesheet_id,p_contract_week_id,v_seq,clock_timestamp()
  )
  on conflict(identity_kind,identity_id) do update set
    current_timesheet_id=excluded.current_timesheet_id,
    contract_week_id=excluded.contract_week_id,
    revision_seq=excluded.revision_seq,
    changed_at_utc=excluded.changed_at_utc;
end;
$function$;

create or replace function private._candidate_timesheet_summary_revision_touch_workflow_v1(p_workflow_id uuid)
returns void
language plpgsql
security definer
set search_path=pg_catalog,public,private,extensions,pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_current_timesheet_id uuid;
begin
  if p_workflow_id is null then return; end if;
  select workflow_row.* into v_workflow
  from public.candidate_submission_workflows as workflow_row
  where workflow_row.id=p_workflow_id;
  if not found then return; end if;

  v_current_timesheet_id:=coalesce(v_workflow.target_timesheet_id,v_workflow.anchor_timesheet_id);
  if v_workflow.contract_week_id is not null then
    perform private._candidate_timesheet_summary_revision_upsert_v1(
      'CONTRACT_WEEK',v_workflow.contract_week_id,v_current_timesheet_id,v_workflow.contract_week_id
    );
  end if;
  if v_workflow.anchor_timesheet_id is not null then
    perform private._candidate_timesheet_summary_revision_upsert_v1(
      'TIMESHEET',v_workflow.anchor_timesheet_id,v_current_timesheet_id,v_workflow.contract_week_id
    );
  end if;
  if v_workflow.target_timesheet_id is not null
     and v_workflow.target_timesheet_id is distinct from v_workflow.anchor_timesheet_id then
    perform private._candidate_timesheet_summary_revision_upsert_v1(
      'TIMESHEET',v_workflow.target_timesheet_id,v_current_timesheet_id,v_workflow.contract_week_id
    );
  end if;
end;
$function$;

create or replace function private._candidate_timesheet_summary_workflow_revision_trg_v1()
returns trigger
language plpgsql
security definer
set search_path=pg_catalog,public,private,extensions,pg_temp
as $function$
declare
  v_current_timesheet_id uuid;
  v_contract_week_id uuid;
  v_identity_id uuid;
  v_timesheet_ids uuid[];
  v_contract_week_ids uuid[];
begin
  if tg_op='INSERT' then
    v_current_timesheet_id:=coalesce(new.target_timesheet_id,new.anchor_timesheet_id);
    v_contract_week_id:=new.contract_week_id;
    v_timesheet_ids:=array_remove(array[new.anchor_timesheet_id,new.target_timesheet_id]::uuid[],null);
    v_contract_week_ids:=array_remove(array[new.contract_week_id]::uuid[],null);
  elsif tg_op='UPDATE' then
    v_current_timesheet_id:=coalesce(new.target_timesheet_id,new.anchor_timesheet_id);
    v_contract_week_id:=coalesce(new.contract_week_id,old.contract_week_id);
    v_timesheet_ids:=array_remove(array[
      old.anchor_timesheet_id,old.target_timesheet_id,new.anchor_timesheet_id,new.target_timesheet_id
    ]::uuid[],null);
    v_contract_week_ids:=array_remove(array[old.contract_week_id,new.contract_week_id]::uuid[],null);
  else
    v_current_timesheet_id:=coalesce(old.target_timesheet_id,old.anchor_timesheet_id);
    v_contract_week_id:=old.contract_week_id;
    v_timesheet_ids:=array_remove(array[old.anchor_timesheet_id,old.target_timesheet_id]::uuid[],null);
    v_contract_week_ids:=array_remove(array[old.contract_week_id]::uuid[],null);
  end if;

  for v_identity_id in
    select distinct identity_id from unnest(v_timesheet_ids) as ids(identity_id)
  loop
    perform private._candidate_timesheet_summary_revision_upsert_v1(
      'TIMESHEET',v_identity_id,coalesce(v_current_timesheet_id,v_identity_id),v_contract_week_id
    );
  end loop;
  for v_identity_id in
    select distinct identity_id from unnest(v_contract_week_ids) as ids(identity_id)
  loop
    perform private._candidate_timesheet_summary_revision_upsert_v1(
      'CONTRACT_WEEK',v_identity_id,v_current_timesheet_id,v_identity_id
    );
  end loop;

  if tg_op='DELETE' then return old; end if;
  return new;
end;
$function$;

create or replace function private._candidate_timesheet_summary_approval_revision_trg_v1()
returns trigger
language plpgsql
security definer
set search_path=pg_catalog,public,private,extensions,pg_temp
as $function$
declare
  v_workflow_id uuid;
begin
  v_workflow_id:=case when tg_op='DELETE' then old.workflow_id else new.workflow_id end;
  perform private._candidate_timesheet_summary_revision_touch_workflow_v1(v_workflow_id);
  if tg_op='DELETE' then return old; end if;
  return new;
end;
$function$;

create or replace function private._candidate_timesheet_summary_mail_revision_trg_v1()
returns trigger
language plpgsql
security definer
set search_path=pg_catalog,public,private,extensions,pg_temp
as $function$
declare
  v_scope jsonb;
  v_workflow_text text;
begin
  v_scope:=case
    when tg_op='DELETE' then coalesce(old.payment_scope_json,'{}'::jsonb)
    else coalesce(new.payment_scope_json,'{}'::jsonb)
  end;
  v_workflow_text:=nullif(btrim(coalesce(
    v_scope->>'candidate_workflow_id',v_scope->>'candidate_manager_workflow_id',''
  )), '');
  if v_workflow_text is not null and pg_input_is_valid(v_workflow_text,'uuid') then
    perform private._candidate_timesheet_summary_revision_touch_workflow_v1(v_workflow_text::uuid);
  end if;
  if tg_op='DELETE' then return old; end if;
  return new;
end;
$function$;

create or replace function public.candidate_timesheet_summary_cursor_v1()
returns jsonb
language sql
stable
security definer
set search_path=pg_catalog,public,private,extensions,pg_temp
as $function$
  select jsonb_build_object(
    'cursor',coalesce(max(revision_row.revision_seq),0),
    'observed_at_utc',statement_timestamp()
  )
  from private.candidate_timesheet_summary_revisions as revision_row;
$function$;

drop trigger if exists candidate_timesheet_summary_workflow_revision_trg
  on public.candidate_submission_workflows;
create trigger candidate_timesheet_summary_workflow_revision_trg
after insert or update or delete on public.candidate_submission_workflows
for each row execute function private._candidate_timesheet_summary_workflow_revision_trg_v1();

drop trigger if exists candidate_timesheet_summary_approval_revision_trg
  on public.candidate_approval_requests;
create trigger candidate_timesheet_summary_approval_revision_trg
after insert or update or delete on public.candidate_approval_requests
for each row execute function private._candidate_timesheet_summary_approval_revision_trg_v1();

drop trigger if exists candidate_timesheet_summary_mail_revision_trg
  on public.mail_outbox;
drop trigger if exists candidate_timesheet_summary_mail_insert_revision_trg
  on public.mail_outbox;
drop trigger if exists candidate_timesheet_summary_mail_update_revision_trg
  on public.mail_outbox;
drop trigger if exists candidate_timesheet_summary_mail_delete_revision_trg
  on public.mail_outbox;
create trigger candidate_timesheet_summary_mail_insert_revision_trg
after insert on public.mail_outbox
for each row
when (
  new.payment_scope_json ? 'candidate_workflow_id'
  or new.payment_scope_json ? 'candidate_manager_workflow_id'
)
execute function private._candidate_timesheet_summary_mail_revision_trg_v1();
create trigger candidate_timesheet_summary_mail_update_revision_trg
after update of payment_scope_json,status,provider_status,sent_at,failed_at,
  attempt_lease_token,attempt_lease_expires_at_utc on public.mail_outbox
for each row
when (
  new.payment_scope_json ? 'candidate_workflow_id'
  or new.payment_scope_json ? 'candidate_manager_workflow_id'
  or old.payment_scope_json ? 'candidate_workflow_id'
  or old.payment_scope_json ? 'candidate_manager_workflow_id'
)
execute function private._candidate_timesheet_summary_mail_revision_trg_v1();
create trigger candidate_timesheet_summary_mail_delete_revision_trg
after delete on public.mail_outbox
for each row
when (
  old.payment_scope_json ? 'candidate_workflow_id'
  or old.payment_scope_json ? 'candidate_manager_workflow_id'
)
execute function private._candidate_timesheet_summary_mail_revision_trg_v1();

alter function private._candidate_timesheet_summary_revision_upsert_v1(text,uuid,uuid,uuid) owner to postgres;
alter function private._candidate_timesheet_summary_revision_touch_workflow_v1(uuid) owner to postgres;
alter function private._candidate_timesheet_summary_workflow_revision_trg_v1() owner to postgres;
alter function private._candidate_timesheet_summary_approval_revision_trg_v1() owner to postgres;
alter function private._candidate_timesheet_summary_mail_revision_trg_v1() owner to postgres;
alter function public.candidate_timesheet_summary_cursor_v1() owner to postgres;

revoke all on function private._candidate_timesheet_summary_revision_upsert_v1(text,uuid,uuid,uuid)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_timesheet_summary_revision_touch_workflow_v1(uuid)
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_timesheet_summary_workflow_revision_trg_v1()
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_timesheet_summary_approval_revision_trg_v1()
  from public,anon,authenticated,service_role;
revoke all on function private._candidate_timesheet_summary_mail_revision_trg_v1()
  from public,anon,authenticated,service_role;
revoke all on function public.candidate_timesheet_summary_cursor_v1()
  from public,anon,authenticated;
grant execute on function public.candidate_timesheet_summary_cursor_v1() to service_role;

comment on function public.candidate_timesheet_summary_cursor_v1() is
  'Returns only the current Candidate-driven Timesheet Summary revision cursor for a race-safe initial Summary snapshot.';

notify pgrst,'reload schema';

commit;
