-- PF-04 D01: one bounded terminal temporary-row page; permanent origins retained.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_terminal_cleanup_v1(p_limit integer)
returns jsonb language plpgsql volatile security invoker
set search_path=pg_catalog,private
as $function$
declare
  v_clock private.bpay_next_terminal_cleanup_clock%rowtype;
  v_page record;
  v_job private.bpay_next_job%rowtype;
  v_run private.bpay_next_pay_run%rowtype;
  v_visited integer:=0;
  v_deleted integer:=0;
  v_changed integer;
begin
  if p_limit is null or p_limit<1 or p_limit>100 then
    raise exception using errcode='22023',message='BPAY_NEXT_RUNTIME_REQUEST_INVALID';
  end if;
  select c.* into v_clock from private.bpay_next_terminal_cleanup_clock c
    where c.id=1 for update skip locked;
  if not found then
    return pg_catalog.jsonb_build_object('lane',null,'rows_visited','0',
      'rows_deleted','0','rows_retained','0','busy',true);
  end if;
  if v_clock.lane='SELECTION' then
    -- Scan only a PK page, including nonterminal rows. Advance past retained
    -- origins so an old referenced row cannot starve later temporary rows.
    for v_page in select p.run_id,p.page_no from private.bpay_next_selection_page p
      where v_clock.selection_run_cursor is null or
        (p.run_id,p.page_no)>(v_clock.selection_run_cursor,v_clock.selection_page_cursor)
      order by p.run_id,p.page_no limit p_limit
    loop
      v_visited:=v_visited+1;
      v_clock.selection_run_cursor:=v_page.run_id;
      v_clock.selection_page_cursor:=v_page.page_no;
      -- Header before child, like the existing upload owner. Contended scopes
      -- are retained this sweep; no waiting on a money owner or lock inversion.
      select r.* into v_run from private.bpay_next_pay_run r
        where r.id=v_page.run_id for update skip locked;
      if not found or v_run.status not in ('COMPLETE','CANCELLED') then continue;end if;
      -- Keep all accepted selection/choice/snapshot/pin/frozen rows. Only this
      -- closed upload buffer is temporary; late upload retries refuse normally.
      delete from private.bpay_next_selection_page p
        where p.run_id=v_page.run_id and p.page_no=v_page.page_no;
      get diagnostics v_changed=ROW_COUNT;
      v_deleted:=v_deleted+v_changed;
    end loop;
    if v_visited<p_limit then
      v_clock.selection_run_cursor:=null;v_clock.selection_page_cursor:=null;
    end if;
    update private.bpay_next_terminal_cleanup_clock
      set lane='JOB',selection_run_cursor=v_clock.selection_run_cursor,
        selection_page_cursor=v_clock.selection_page_cursor where id=1;
  else
    for v_job in select j.* from private.bpay_next_job j
      where v_clock.job_cursor is null or j.id>v_clock.job_cursor
      order by j.id limit p_limit for update skip locked
    loop
      v_visited:=v_visited+1;v_clock.job_cursor:=v_job.id;
      if v_job.status<>'DONE' or v_job.lease_nonce is not null or v_job.lease_until_utc is not null
        or v_job.job_kind not in ('PAYE_NET_ENTRY','TRANSFER_BUILD','CSV_SETTLEMENT','CSV_RETURN',
          'CASH_REISSUE','SIMPLE_CANCEL','POSITION_APPLY','PREPARE','REISSUE_CANCEL',
          'INTERNAL_SETTLEMENT','PREPARATION_EXPIRY','CASE_CREATE','CASE_WRITE_OFF') then continue;end if;
      if not exists(select 1 from private.bpay_next_command c
        join private.bpay_next_enrollment_clock e on e.id=1
        where c.id=v_job.command_id and c.agency_sequence=v_job.command_sequence
          and c.module_epoch=v_job.module_epoch and c.status='COMPLETE'
          and c.sealed_at_utc is not null and c.expected_member_count is not null
          and c.expected_member_count=c.enrolled_member_count
          and e.last_enrolled_sequence>=c.agency_sequence) then continue;end if;
      -- Exact retained references, including the installed-unused F25 schema.
      -- Never remove a dependent row to make its original job deletable.
      if exists(select 1 from private.bpay_next_case_allocation_state s where s.job_id=v_job.id)
        or exists(select 1 from private.bpay_next_preparation_expiry_worker w
          where w.job_id=v_job.id or w.original_job_id=v_job.id)
        or exists(select 1 from private.bpay_next_work_collection w where w.last_job_id=v_job.id)
        or exists(select 1 from private.bpay_next_destination_posted_leg l where l.job_id=v_job.id)
        or exists(select 1 from private.bpay_next_run_work_query_pin_v2 p where p.prepare_job_id=v_job.id)
        or exists(select 1 from private.bpay_next_effective_work_cut c where c.prepare_job_id=v_job.id)
      then continue;end if;
      delete from private.bpay_next_job j where j.id=v_job.id and j.status='DONE';
      get diagnostics v_changed=ROW_COUNT;
      v_deleted:=v_deleted+v_changed;
    end loop;
    if v_visited<p_limit then v_clock.job_cursor:=null;end if;
    update private.bpay_next_terminal_cleanup_clock set lane='SELECTION',job_cursor=v_clock.job_cursor where id=1;
  end if;
  return pg_catalog.jsonb_build_object('lane',v_clock.lane,'rows_visited',v_visited::text,
    'rows_deleted',v_deleted::text,'rows_retained',(v_visited-v_deleted)::text,'busy',false);
end
$function$;

create or replace function public.bpay_next_terminal_cleanup_service_v1(p_limit integer)
returns jsonb language plpgsql volatile security definer
set search_path=pg_catalog,private
as $function$
begin
  if coalesce(pg_catalog.current_setting('request.jwt.claim.role',true),
    nullif(pg_catalog.current_setting('request.jwt.claims',true),'')::jsonb->>'role','')<>'service_role' then
    raise exception using errcode='42501',message='BPAY_NEXT_RUNTIME_SERVICE_FORBIDDEN';
  end if;
  if not exists(select 1 from private.bpay_next_module_control m
    where m.id=1 and m.active_owner='NEXT' for share) then
    raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';
  end if;
  return private.bpay_next_terminal_cleanup_v1(p_limit);
end
$function$;
alter function private.bpay_next_terminal_cleanup_v1(integer) owner to postgres;
revoke all on function private.bpay_next_terminal_cleanup_v1(integer) from public,anon,authenticated,service_role;
alter function public.bpay_next_terminal_cleanup_service_v1(integer) owner to postgres;
revoke all on function public.bpay_next_terminal_cleanup_service_v1(integer) from public,anon,authenticated,service_role;
grant execute on function public.bpay_next_terminal_cleanup_service_v1(integer) to service_role;
notify pgrst, 'reload schema';
commit;
