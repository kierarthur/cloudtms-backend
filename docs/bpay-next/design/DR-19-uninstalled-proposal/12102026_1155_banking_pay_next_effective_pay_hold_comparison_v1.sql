-- DR19/BD143: existing native hold and accepted guard successors only.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_pay_hold_control_trigger_v1()
returns trigger language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare v_bookings text[];v_work_ids uuid[];v_candidate uuid;v_work private.bpay_next_work%rowtype;
 v_canonical record;v_held boolean;v_reason text;v_sheet uuid;v_financial uuid;v_effective_change boolean;
begin
 if (select active_owner from private.bpay_next_module_control where id=1) is distinct from 'NEXT' then return new;end if;
 if tg_table_schema<>'public' or tg_table_name not in ('timesheets','timesheets_financials') then
  raise exception using errcode='23514',message='BPAY_NEXT_PAY_HOLD_TRIGGER_RELATION_INVALID';end if;
 if tg_table_name='timesheets' then
  if tg_op='UPDATE' then
   if (old.is_current,old.booking_id,old.timesheet_id) is not distinct from (new.is_current,new.booking_id,new.timesheet_id)
     or not(old.is_current or new.is_current) then return new;end if;
   v_bookings:=array[old.booking_id,new.booking_id];
  else
   if new.is_current is not true then return new;end if;
   v_bookings:=array[new.booking_id];
  end if;
 else
  if tg_op='UPDATE' then
   if (old.is_current,old.timesheet_id,old.pay_on_hold,old.pay_on_hold_reason)
     is not distinct from (new.is_current,new.timesheet_id,new.pay_on_hold,new.pay_on_hold_reason)
     or not(old.is_current or new.is_current) then return new;end if;
   select array_agg(distinct t.booking_id) into v_bookings from public.timesheets t
    where t.is_current and t.timesheet_id=any(array[old.timesheet_id,new.timesheet_id]);
  else
   if new.is_current is not true then return new;end if;
   select array_agg(t.booking_id) into v_bookings from public.timesheets t
    where t.is_current and t.timesheet_id=new.timesheet_id;
  end if;
 end if;
 select array_agg(w.id order by w.id) into v_work_ids from private.bpay_next_work w where w.booking_id=any(v_bookings);
 if v_work_ids is null then return new;end if;
 -- Native mutation already owns its public row. No public lock is acquired
 -- after the established Candidate -> Work private order.
 for v_candidate in select distinct w.candidate_id from private.bpay_next_work w
   where w.id=any(v_work_ids) order by w.candidate_id loop
  perform 1 from private.bpay_next_worker_control where candidate_id=v_candidate for update;
  -- Owner11Oct A9: public edits can precede NEXT control enrolment.
  -- No private projection has changed; let the native edit finish.
  if not found then return new;end if;
 end loop;
 perform 1 from private.bpay_next_work where id=any(v_work_ids) order by id for update;
 for v_work in select w.* from private.bpay_next_work w where w.id=any(v_work_ids) order by w.id loop
  select t.timesheet_id,tf.id financial_id,tf.pay_on_hold,tf.pay_on_hold_reason into v_canonical
    from public.timesheets t left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current
    where t.booking_id=v_work.booking_id and t.is_current;
  v_sheet:=v_canonical.timesheet_id;v_financial:=v_canonical.financial_id;
  v_held:=coalesce(v_canonical.pay_on_hold,false);
  v_reason:=case when v_held then v_canonical.pay_on_hold_reason end;
  v_effective_change:=(v_work.pay_hold,v_work.pay_hold_reason) is distinct from (v_held,v_reason);
  -- Existing Work state can precede control enrolment. The actual canonical
  -- financial-row transition still proves set/clear/re-set without upgrading
  -- the accepted choice's Work SHARE lock or treating rotation as a hold.
  if tg_table_name='timesheets_financials' and tg_op='UPDATE' then
   if old.is_current and new.is_current and old.timesheet_id=new.timesheet_id
     and v_canonical.financial_id=new.id and v_sheet=new.timesheet_id
     and (coalesce(old.pay_on_hold,false),case when old.pay_on_hold then old.pay_on_hold_reason end)
       is distinct from (coalesce(new.pay_on_hold,false),case when new.pay_on_hold then new.pay_on_hold_reason end) then
    v_effective_change:=true;
   end if;
  end if;
  if (v_work.pay_hold_physical_timesheet_id,v_work.pay_hold_financial_id,v_work.pay_hold,v_work.pay_hold_reason)
    is not distinct from (v_sheet,v_financial,v_held,v_reason) and not v_effective_change then continue;end if;
  update private.bpay_next_work set pay_hold_control_version=pay_hold_control_version+1,
    effective_pay_hold_revision=effective_pay_hold_revision+case when v_effective_change then 1 else 0 end,
    pay_hold_changed_at_utc=clock_timestamp(),pay_hold_physical_timesheet_id=v_sheet,pay_hold_financial_id=v_financial,
    pay_hold=v_held,pay_hold_reason=v_reason where id=v_work.id;
  update private.bpay_next_worker_control set financial_view_revision=financial_view_revision+1 where candidate_id=v_work.candidate_id;
  -- The existing wrapper owns contribution replacement and ORIGIN/Processing.
  perform private.bpay_next_workbench_refresh_work_v1(v_work.id);
 end loop;
 return new;
end
$function$;

create or replace function private.bpay_next_work_choice_guard_v1()
returns trigger language plpgsql security definer
set search_path=pg_catalog,private
as $function$
declare
  v_run private.bpay_next_pay_run%rowtype;v_choice private.bpay_next_work_choice%rowtype;
  v_page private.bpay_next_component_selection_page%rowtype;v_count integer;
begin
  if tg_op='DELETE' then
    raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_IMMUTABLE';
  end if;
  if tg_op='UPDATE' and tg_table_name<>'bpay_next_work_choice' then
    raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_IMMUTABLE';
  end if;
  select r.* into strict v_run from private.bpay_next_pay_run r
    where r.id=new.run_id for update;
  if v_run.status<>'PREPARING' or v_run.selection_state<>'OPEN' then
    raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_NOT_OPEN';
  end if;
  if tg_table_name='bpay_next_work_choice' and tg_op='INSERT' then
    -- Original choice construction only, under the exact Work lock. This also
    -- covers retained manifest callers; no delayed capture can refill it.
    perform 1 from private.bpay_next_work where id=new.work_id for share;
    select w.effective_pay_hold_revision,w.pay_hold_control_version,w.pay_hold_changed_at_utc,t.timesheet_id,tf.id,
      coalesce(tf.pay_on_hold,false),case when tf.pay_on_hold then tf.pay_on_hold_reason end
      into new.effective_pay_hold_revision,new.pay_hold_control_version,new.pay_hold_changed_at_utc,new.pay_hold_physical_timesheet_id,
        new.pay_hold_financial_id,new.pay_hold,new.pay_hold_reason
      from private.bpay_next_work w left join public.timesheets t on t.booking_id=w.booking_id and t.is_current
      left join public.timesheets_financials tf on tf.timesheet_id=t.timesheet_id and tf.is_current where w.id=new.work_id;
  end if;
  if tg_op='UPDATE' then
    if tg_table_name='bpay_next_work_choice' and
      (new.effective_pay_hold_revision,new.pay_hold_control_version,new.pay_hold_changed_at_utc,new.pay_hold_physical_timesheet_id,
        new.pay_hold_financial_id,new.pay_hold,new.pay_hold_reason) is distinct from
      (old.effective_pay_hold_revision,old.pay_hold_control_version,old.pay_hold_changed_at_utc,old.pay_hold_physical_timesheet_id,
        old.pay_hold_financial_id,old.pay_hold,old.pay_hold_reason) then
      raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_IMMUTABLE';end if;
    if (old.selection_state='SEALED' and (to_jsonb(new)-'segment_page_count'-'segment_count'-'segment_sealed_count')
        is distinct from (to_jsonb(old)-'segment_page_count'-'segment_count'-'segment_sealed_count'))
      or new.run_id<>old.run_id or new.work_id<>old.work_id
      or new.expected_revision_id is distinct from old.expected_revision_id
      or new.selection_mode<>old.selection_mode
      or new.page_count<old.page_count or new.component_count<old.component_count then
      raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_IMMUTABLE';
    end if;
    if (old.segment_sealed_count is not null and (new.segment_page_count,new.segment_count,new.segment_sealed_count)
      is distinct from (old.segment_page_count,old.segment_count,old.segment_sealed_count)) or new.segment_control_sequence is distinct from old.segment_control_sequence
      or new.segment_page_count<old.segment_page_count or new.segment_count<old.segment_count then
      raise exception using errcode='55000',message='BPAY_NEXT_SEGMENT_SELECTION_IMMUTABLE';
    end if;
    if new.segment_page_count<>old.segment_page_count or new.segment_count<>old.segment_count then
      if new.segment_page_count<>old.segment_page_count+1 or new.segment_count<=old.segment_count
        or new.segment_count-old.segment_count>100 or not exists(select 1 from private.bpay_next_segment_selection_page p
          where p.run_id=new.run_id and p.work_id=new.work_id and p.page_no=new.segment_page_count
            and p.first_member_no=old.segment_count+1 and p.item_count=new.segment_count-old.segment_count)
        or (select count(*) from private.bpay_next_selected_segment s where s.run_id=new.run_id and s.work_id=new.work_id
          and s.selection_no>old.segment_count and s.selection_no<=new.segment_count)<>new.segment_count-old.segment_count then
        raise exception using errcode='23514',message='BPAY_NEXT_SEGMENT_PAGE_COUNT_MISMATCH';
      end if;
    end if;
    if new.page_count<>old.page_count or new.component_count<>old.component_count then
      select p.* into strict v_page from private.bpay_next_component_selection_page p
        where p.run_id=new.run_id and p.work_id=new.work_id and p.page_no=new.page_count;
      select count(*)::integer into v_count from private.bpay_next_selected_component c
        where c.run_id=new.run_id and c.work_id=new.work_id and c.page_no=new.page_count;
      if new.page_count<>old.page_count+1 or v_page.first_selection_no<>old.component_count+1
        or new.component_count<>old.component_count+v_page.item_count or v_count<>v_page.item_count
        or new.selection_state<>old.selection_state then
        raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_PAGE_COUNT_MISMATCH';
      end if;
    end if;
  elsif tg_table_name='bpay_next_work_choice' then
    if new.page_count<>0 or new.component_count<>0
      or (new.selection_mode='SUBSET' and new.selection_state<>'OPEN') then
      raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_PAGE_COUNT_MISMATCH';
    end if;
  else
    select c.* into strict v_choice from private.bpay_next_work_choice c
      where c.run_id=new.run_id and c.work_id=new.work_id;
    if v_choice.selection_mode<>'SUBSET' or v_choice.selection_state<>'OPEN'
      or new.page_no<>v_choice.page_count+1 then
      raise exception using errcode='55000',message='BPAY_NEXT_CHOICE_NOT_OPEN';
    end if;
    if tg_table_name='bpay_next_component_selection_page' then
      if new.first_selection_no<>v_choice.component_count+1 then
        raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_PAGE_COUNT_MISMATCH';
      end if;
    else
      select p.* into strict v_page from private.bpay_next_component_selection_page p
        where p.run_id=new.run_id and p.work_id=new.work_id and p.page_no=new.page_no;
      if new.expected_revision_id<>v_choice.expected_revision_id or new.page_item_no>v_page.item_count
        or new.selection_no<>v_page.first_selection_no+new.page_item_no-1 then
        raise exception using errcode='23514',message='BPAY_NEXT_CHOICE_PAGE_COUNT_MISMATCH';
      end if;
    end if;
  end if;
  return new;
end
$function$;
alter function private.bpay_next_pay_hold_control_trigger_v1() owner to postgres;
revoke all on function private.bpay_next_pay_hold_control_trigger_v1() from public,anon,authenticated,service_role;
alter function private.bpay_next_work_choice_guard_v1() owner to postgres;
revoke all on function private.bpay_next_work_choice_guard_v1() from public,anon,authenticated,service_role;
notify pgrst,'reload schema';
commit;
