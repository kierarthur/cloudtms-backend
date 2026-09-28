-- Repeatable CloudTMS function/view authority: weekly_source_workbench_inventory_projection_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

create or replace function private.weekly_source_workbench_inventory_track_v1()
returns trigger language plpgsql security definer set search_path='' as $function$
declare
  v_head uuid;
  v_old_emit integer:=0;
  v_new_emit integer:=0;
  v_old_bad integer:=0;
  v_new_bad integer:=0;
begin
  if tg_relid='public.weekly_source_entitlement_heads'::regclass then
    insert into private.weekly_source_workbench_inventory_v1(head_id,complete)
      values(new.id,true);
    return new;
  end if;
  if tg_op='UPDATE' and new.head_id is distinct from old.head_id then
    raise exception 'WEEKLY_SOURCE_COMPONENT_HEAD_CHANGE_REFUSED' using errcode='23514';
  end if;
  v_head:=case when tg_op='DELETE' then old.head_id else new.head_id end;
  -- The original owner already locks its head before component writes. This
  -- metadata row is below that head in the shared lock order; it never seeks
  -- a head/root lock after taking its own lock.
  perform 1 from private.weekly_source_workbench_inventory_v1
    where head_id=v_head and complete for update;
  if not found then
    raise exception 'WEEKLY_SOURCE_WORKBENCH_INVENTORY_NOT_INITIALISED' using errcode='55000';
  end if;
  if tg_op<>'INSERT' then
    v_old_emit:=(old.component_kind='WORKED_TIME' or round(case when old.exclude_from_pay then 0::numeric else coalesce(old.pay_ex_vat,0) end,2)<>0)::integer;
    v_old_bad:=(old.component_kind is null or old.component_kind not in ('WORKED_TIME','ADDITIONAL_UNIT','EXPENSE')
      or (old.component_kind='ADDITIONAL_UNIT' and nullif(btrim(coalesce(old.additional_code_raw,'')),'') is null)
      or (old.component_kind='EXPENSE' and nullif(btrim(coalesce(old.expense_code,'')),'') is null))::integer;
  end if;
  if tg_op<>'DELETE' then
    v_new_emit:=(new.component_kind='WORKED_TIME' or round(case when new.exclude_from_pay then 0::numeric else coalesce(new.pay_ex_vat,0) end,2)<>0)::integer;
    v_new_bad:=(new.component_kind is null or new.component_kind not in ('WORKED_TIME','ADDITIONAL_UNIT','EXPENSE')
      or (new.component_kind='ADDITIONAL_UNIT' and nullif(btrim(coalesce(new.additional_code_raw,'')),'') is null)
      or (new.component_kind='EXPENSE' and nullif(btrim(coalesce(new.expense_code,'')),'') is null))::integer;
  end if;
  update private.weekly_source_workbench_inventory_v1 set
    component_count=component_count+case tg_op when 'INSERT' then 1 when 'DELETE' then -1 else 0 end,
    emittable_count=emittable_count+v_new_emit-v_old_emit,
    invalid_count=invalid_count+v_new_bad-v_old_bad
   where head_id=v_head;
  if tg_op='DELETE' then return old; end if;
  return new;
end;
$function$;
alter function private.weekly_source_workbench_inventory_track_v1() owner to postgres;
revoke all on function private.weekly_source_workbench_inventory_track_v1() from public,anon,authenticated,service_role;
drop trigger if exists weekly_source_workbench_inventory_head on public.weekly_source_entitlement_heads;
create trigger weekly_source_workbench_inventory_head after insert on public.weekly_source_entitlement_heads
 for each row execute function private.weekly_source_workbench_inventory_track_v1();
drop trigger if exists weekly_source_workbench_inventory_component on public.weekly_source_entitlement_head_components;
create trigger weekly_source_workbench_inventory_component after insert or update or delete on public.weekly_source_entitlement_head_components
 for each row execute function private.weekly_source_workbench_inventory_track_v1();

-- Release-only, resumable upgrade preparation. Each call admits at most 25
-- physical components plus one lookahead. No caller-supplied cursor, resetting,
-- population array or loop. Component mutation while a head is being seeded
-- fails closed through the trigger above. Complete heads replay without work.
create or replace function private.weekly_source_workbench_inventory_seed_page_v1(p_head_id uuid)
returns table(complete boolean,component_count bigint,last_ordinal integer)
language plpgsql security definer set search_path='' as $function$
declare
  v_expected integer;
  v_state private.weekly_source_workbench_inventory_v1%rowtype;
  v_count bigint; v_emit bigint; v_bad bigint; v_last integer; v_more boolean;
begin
  select h.component_count into v_expected from public.weekly_source_entitlement_heads h
   where h.id=p_head_id for no key update;
  if not found then raise exception 'WEEKLY_SOURCE_HEAD_NOT_FOUND' using errcode='22023'; end if;
  insert into private.weekly_source_workbench_inventory_v1(head_id) values(p_head_id) on conflict do nothing;
  select i.* into v_state from private.weekly_source_workbench_inventory_v1 i where i.head_id=p_head_id for update;
  if not v_state.complete then
    with raw as materialized (
      select c.component_ordinal,c.component_kind,c.exclude_from_pay,c.pay_ex_vat,c.additional_code_raw,c.expense_code
       from public.weekly_source_entitlement_head_components c
       where c.head_id=p_head_id and c.component_ordinal>v_state.bootstrap_last_ordinal
       order by c.component_ordinal limit 26
    ), page as materialized (select * from raw order by component_ordinal limit 25)
    select count(*),count(*) filter(where component_kind='WORKED_TIME' or round(case when exclude_from_pay then 0::numeric else coalesce(pay_ex_vat,0) end,2)<>0),
      count(*) filter(where component_kind is null or component_kind not in ('WORKED_TIME','ADDITIONAL_UNIT','EXPENSE')
        or (component_kind='ADDITIONAL_UNIT' and nullif(btrim(coalesce(additional_code_raw,'')),'') is null)
        or (component_kind='EXPENSE' and nullif(btrim(coalesce(expense_code,'')),'') is null)),
      coalesce(max(component_ordinal),v_state.bootstrap_last_ordinal),(select count(*)>25 from raw)
     into v_count,v_emit,v_bad,v_last,v_more from page;
    if not v_more and v_state.component_count+v_count<>v_expected then
      raise exception 'WEEKLY_SOURCE_HEAD_INVENTORY_MISMATCH' using errcode='23514';
    end if;
    update private.weekly_source_workbench_inventory_v1 i set
      component_count=i.component_count+v_count,emittable_count=i.emittable_count+v_emit,
      invalid_count=i.invalid_count+v_bad,bootstrap_last_ordinal=v_last,complete=not v_more
      where i.head_id=p_head_id;
  end if;
  return query select i.complete,i.component_count,i.bootstrap_last_ordinal
    from private.weekly_source_workbench_inventory_v1 i where i.head_id=p_head_id;
end;
$function$;
alter function private.weekly_source_workbench_inventory_seed_page_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_workbench_inventory_seed_page_v1(uuid) from public,anon,authenticated,service_role;

-- Release runner invokes one bounded page per transaction. Old readers remain
-- installed until this returns true. New heads are tracked transactionally by
-- the already installed head/component triggers, including IDs below this cut.
create or replace function private.weekly_source_workbench_inventory_install_page_v1()
returns boolean language plpgsql security definer set search_path='' as $function$
declare
  v_state private.weekly_source_workbench_inventory_install_v1%rowtype;
  v_head uuid;
  v_done boolean;
begin
  if not exists(select 1 from pg_catalog.pg_trigger where tgrelid='public.weekly_source_entitlement_heads'::regclass
      and tgname='weekly_source_workbench_inventory_head' and tgenabled in ('O','A')
      and tgfoid='private.weekly_source_workbench_inventory_track_v1()'::regprocedure)
    or not exists(select 1 from pg_catalog.pg_trigger where tgrelid='public.weekly_source_entitlement_head_components'::regclass
      and tgname='weekly_source_workbench_inventory_component' and tgenabled in ('O','A')
      and tgfoid='private.weekly_source_workbench_inventory_track_v1()'::regprocedure)
    or not exists(select 1 from pg_catalog.pg_trigger where tgrelid='public.weekly_source_entitlement_head_components'::regclass
      and tgname='ws_inventory_changed_v1' and tgenabled in ('O','A')
      and tgfoid='private.weekly_source_inventory_changed_v1()'::regprocedure) then
    raise exception 'WEEKLY_SOURCE_WORKBENCH_TRACKING_NOT_INSTALLED' using errcode='55000';
  end if;
  insert into private.weekly_source_workbench_inventory_install_v1(singleton,scan_through)
    values(true,(select id from public.weekly_source_entitlement_heads order by id desc limit 1))
    on conflict do nothing;
  select * into v_state from private.weekly_source_workbench_inventory_install_v1 where singleton for update;
  if v_state.complete then return true; end if;
  if v_state.last_head is null then
    select id into v_head from public.weekly_source_entitlement_heads
      where id<=v_state.scan_through order by id limit 1 for no key update;
  else
    select id into v_head from public.weekly_source_entitlement_heads
      where id>v_state.last_head and id<=v_state.scan_through
      order by id limit 1 for no key update;
  end if;
  if not found then
    update private.weekly_source_workbench_inventory_install_v1 set complete=true,page_calls=page_calls+1 where singleton;
    return true;
  end if;
  select s.complete into v_done from private.weekly_source_workbench_inventory_seed_page_v1(v_head) s;
  update private.weekly_source_workbench_inventory_install_v1
    set last_head=case when v_done then v_head else last_head end,page_calls=page_calls+1 where singleton;
  return false;
end;
$function$;
alter function private.weekly_source_workbench_inventory_install_page_v1() owner to postgres;
revoke all on function private.weekly_source_workbench_inventory_install_page_v1() from public,anon,authenticated,service_role;

commit;
