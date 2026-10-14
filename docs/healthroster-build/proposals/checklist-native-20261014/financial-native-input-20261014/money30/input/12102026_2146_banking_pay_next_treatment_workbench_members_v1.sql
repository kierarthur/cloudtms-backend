-- WB-29 / BD155: exact native Source approval qualification only.
-- Actual current/applied/selected/Position/job identities and all money remain native.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_workbench_work_members_v1(p_work_id uuid,p_revision_id uuid)
returns table(component_key text,approved_line_id uuid,approved_segment_id uuid,segment_key text,
  component_kind text,tax_treatment text,work_date date,approved_quantity numeric,unit_label text,
  approved_amount numeric,realised_amount numeric,held_amount numeric,source_pay_channel text,
  parent_complete boolean,segment_attribution_complete boolean)
language plpgsql security definer set search_path=pg_catalog,private
as $function$
declare v_revision private.bpay_next_work_revision%rowtype;v_approval uuid;v_event uuid;
begin
  perform private.bpay_next_workbench_require_next_v1();
  select x.* into v_revision from private.bpay_next_work_revision x where x.id=p_revision_id and x.work_id=p_work_id;
  v_approval:=p_revision_id;v_event:=v_revision.source_event_id;
  if v_revision.payment_treatment_approval_revision_id is not null and v_revision.sealed_at_utc is not null then
    v_approval:=private.bpay_next_treatment_approval_revision_v1(v_revision.id);
    select x.source_event_id into v_event from private.bpay_next_work_revision x where x.id=v_approval and x.work_id=p_work_id;
  end if;
  return query
  with source_books as materialized (
    select p.component_key,book.qualification_state,book.approval_revision_id,book.approved_line_id,
      book.source_channel,book.approved_source_ex_vat,book.native_realised_paye,book.native_realised_umbrella,
      book.native_held_paye,book.native_held_umbrella
    from private.bpay_next_work w join private.bpay_next_work_revision r on r.id=p_revision_id and r.work_id=w.id
    join private.bpay_next_position p on p.work_id=w.id and p.source_book_state='READY' and p.source_book_revision>0
    left join private.bpay_next_approved_line a on a.revision_id=r.id and a.component_key=p.component_key
    join private.bpay_next_source_component_binding b on b.canonical_work_id=w.id and b.canonical_component_key=p.component_key
    join private.bpay_next_source_component_binding_version v on v.id=b.current_binding_version_id
      and v.approval_work_id=w.id and v.approval_revision_id=v_approval and v.approval_component_key=p.component_key
    left join private.bpay_next_approved_line original_class on original_class.id=coalesce(v.classification_line_id,b.canonical_line_id)
    cross join lateral private.bpay_next_bound_source_snapshot_v1(v.id,p.source_book_revision) book
    where w.id=p_work_id and w.work_kind='SOURCE' and coalesce(a.component_kind,original_class.component_kind)='WORK' and (r.source_head_id is not null
      or (r.source_kind='SOURCE' and v_event is not null and r.financial_snapshot_id is not null))
  ), keys as (
    select a.component_key from private.bpay_next_approved_line a where a.revision_id=p_revision_id
    union select p.component_key from private.bpay_next_position p where p.work_id=p_work_id
  ), parents as (
    select k.component_key,a.id line_id,coalesce(a.component_kind,original_class.component_kind) component_kind,
      case when a.id is not null then a.tax_treatment else original_class.tax_treatment end tax_treatment,
      case when a.id is not null then a.work_date else original_class.work_date end work_date,a.approved_quantity,a.unit_label,
      p.approved_source_ex_vat a,p.realised_source_ex_vat c,p.held_source_ex_vat h,p.source_basis_channel,
      p.realised_target_ex_vat,p.realised_target_vat,p.realised_target_inc_vat,
      p.held_target_ex_vat,p.held_target_vat,p.held_target_inc_vat,
      (p.work_id is not null and p.applied_revision_id=p_revision_id
        and p.approved_source_ex_vat=coalesce(a.source_pay_ex_vat,0)
        and (a.id is not null or p.approved_source_ex_vat=0)
        and (w.work_kind<>'SOURCE' or a.id is not null or original_class.id is not null)
        and (not (w.work_kind='SOURCE' and coalesce(a.component_kind,original_class.component_kind)='WORK' and (r.source_head_id is not null
          or (r.source_kind='SOURCE' and v_event is not null and r.financial_snapshot_id is not null)))
          or (book.qualification_state='READY' and book.approval_revision_id=v_approval
            and book.approved_line_id is not distinct from aa.id and book.source_channel=r.source_pay_channel
            and (book.approved_source_ex_vat,book.native_realised_paye,book.native_held_paye,book.native_realised_umbrella,book.native_held_umbrella)
              is not distinct from (p.approved_source_ex_vat,
                case when r.source_pay_channel='PAYE' then p.realised_source_ex_vat else 0::numeric end,
                case when r.source_pay_channel='PAYE' then p.held_source_ex_vat else 0::numeric end,
                case when r.source_pay_channel='UMBRELLA' then p.realised_source_ex_vat else 0::numeric end,
                case when r.source_pay_channel='UMBRELLA' then p.held_source_ex_vat else 0::numeric end)))) complete,
      (exists(select 1 from private.bpay_next_approved_segment s where s.revision_id=p_revision_id and s.component_key=k.component_key)
        or exists(select 1 from private.bpay_next_work_segment_origin_share s where s.work_id=p_work_id and s.component_key=k.component_key)) segmented
    from keys k left join private.bpay_next_approved_line a on a.revision_id=p_revision_id and a.component_key=k.component_key
    left join private.bpay_next_approved_line aa on aa.revision_id=v_approval and aa.component_key=k.component_key
    left join private.bpay_next_position p on p.work_id=p_work_id and p.component_key=k.component_key
    join private.bpay_next_work w on w.id=p_work_id
    left join private.bpay_next_work_revision r on r.id=p_revision_id and r.work_id=w.id
    left join private.bpay_next_source_component_binding binding on binding.canonical_work_id=w.id and binding.canonical_component_key=k.component_key
    left join private.bpay_next_source_component_binding_version version on version.id=binding.current_binding_version_id
    left join private.bpay_next_approved_line original_class on original_class.id=coalesce(version.classification_line_id,binding.canonical_line_id)
    left join source_books book on book.component_key=k.component_key
  ), segment_keys as (
    select s.component_key,s.segment_key from private.bpay_next_approved_segment s where s.revision_id=p_revision_id
    union select s.component_key,s.segment_key from private.bpay_next_work_segment_origin_share s where s.work_id=p_work_id
  ), members as (
    select p.*,s.segment_key,seg.id segment_id,seg.work_date segment_date,seg.approved_hours,
      coalesce(seg.approved_source_ex_vat,0) segment_a,b.realised_source_ex_vat segment_c,b.active_held_source_ex_vat segment_h
    from parents p join segment_keys s on s.component_key=p.component_key
    left join private.bpay_next_approved_segment seg on seg.revision_id=p_revision_id
      and seg.component_key=s.component_key and seg.segment_key=s.segment_key
    cross join lateral private.bpay_next_work_segment_gross_balance_v1(p_work_id,s.component_key,s.segment_key) b
  ), totals as (
    select m.component_key,sum(m.segment_a) a,sum(m.segment_c) c,sum(m.segment_h) h from members m group by m.component_key
  ), target_credit as (
    select e.component_key,sum(e.target_ex_vat) ex,sum(e.target_vat) vat,sum(e.target_inc_vat) inc
    from private.bpay_next_work_segment_effect_share e where e.work_id=p_work_id group by e.component_key
  ), target_hold as (
    select s.component_key,sum(s.target_reserved_ex_vat) ex,sum(s.target_reserved_vat) vat,sum(s.target_reserved_inc_vat) inc
    from private.bpay_next_work_segment_origin_share s join private.bpay_next_hold h on h.id=s.hold_id and h.status='ACTIVE'
    where s.work_id=p_work_id group by s.component_key
  )
  select m.component_key,m.line_id,m.segment_id,m.segment_key,m.component_kind,m.tax_treatment,
    m.segment_date,m.approved_hours,'hours'::text,m.segment_a,m.segment_c,m.segment_h,m.source_basis_channel,
    coalesce(m.complete,false),
    ((t.a,t.c,t.h,coalesce(c.ex,0),coalesce(c.vat,0),coalesce(c.inc,0),coalesce(h.ex,0),coalesce(h.vat,0),coalesce(h.inc,0))
      is not distinct from (m.a,m.c,m.h,m.realised_target_ex_vat,m.realised_target_vat,m.realised_target_inc_vat,
        m.held_target_ex_vat,m.held_target_vat,m.held_target_inc_vat))
    from members m join totals t on t.component_key=m.component_key
      left join target_credit c on c.component_key=m.component_key left join target_hold h on h.component_key=m.component_key
  union all
  select p.component_key,p.line_id,null::uuid,null::text,p.component_kind,p.tax_treatment,p.work_date,
    p.approved_quantity,p.unit_label,p.a,p.c,p.h,p.source_basis_channel,coalesce(p.complete,false),true
    from parents p where not p.segmented;
end
$function$;


alter function private.bpay_next_workbench_work_members_v1(uuid,uuid) owner to postgres;
revoke all on function private.bpay_next_workbench_work_members_v1(uuid,uuid) from public,anon,authenticated,service_role;
commit;
