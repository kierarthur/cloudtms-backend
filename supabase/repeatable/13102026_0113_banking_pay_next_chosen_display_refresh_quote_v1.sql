-- WB-29 Ready chosen display; BD168. Shared native arithmetic and Source summaries unchanged.
\set ON_ERROR_STOP on
begin;

create or replace function private.bpay_next_current_component_resolution_quote_v1(
 p_work_id uuid,p_revision_id uuid,p_approved_line_id uuid,p_pay_date date,
 p_selected_leaf_keys text[],p_through_segment_key text,p_projection_refresh boolean
) returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare
 w private.bpay_next_work%rowtype;r private.bpay_next_work_revision%rowtype;
 a private.bpay_next_approved_line%rowtype;pos private.bpay_next_position%rowtype;
 g private.bpay_next_workbench_group%rowtype;c public.candidates%rowtype;u public.umbrellas%rowtype;
 md private.bpay_next_workbench_candidate_metadata%rowtype;v_fvr bigint;v_date jsonb;
 d private.bpay_next_component_resolution%rowtype;first_d private.bpay_next_component_resolution%rowtype;active record;
 v_target text;v_umbrella uuid;v_vat boolean;v_line uuid;v_seen integer:=0;
 v_refs jsonb:='[]';v_versions jsonb:='[]';v_decision_keys text[]:='{}';v_key text;
 v_count bigint;v_bad boolean;v_approval_count bigint;v_approval_a numeric;v_selected_count bigint;
 v_segmented boolean;v_member jsonb;v_members jsonb[]:=array[]::jsonb[];v_bytes bigint:=0;
 v_a numeric:=0;v_r numeric:=0;v_h numeric:=0;v_included numeric:=0;v_excluded numeric:=0;
 v_recovery numeric:=0;v_overhang numeric:=0;v_basis jsonb;v_required_included numeric;
 v_working jsonb;v_result jsonb;leaf record;v_native_version text;
begin
 if p_projection_refresh is null or p_work_id is null or p_revision_id is null or p_approved_line_id is null or p_pay_date is null
  or not isfinite(p_pay_date) or p_selected_leaf_keys is null
  or cardinality(p_selected_leaf_keys) not between 1 and 10000
  or array_ndims(p_selected_leaf_keys) is distinct from 1
  or exists(select 1 from unnest(p_selected_leaf_keys) x where x is null or octet_length(x) not between 1 and 8192)
  or (select count(distinct x) from unnest(p_selected_leaf_keys) x)<>cardinality(p_selected_leaf_keys)
  or octet_length(to_jsonb(p_selected_leaf_keys)::text)>8388608 then
  raise exception using errcode='22023',message='BPAY_NEXT_RESOLUTION_IDENTITY_INVALID';end if;
 select x.* into w from private.bpay_next_work x where x.id=p_work_id;
 select x.* into r from private.bpay_next_work_revision x where x.id=p_revision_id and x.work_id=w.id;
 select x.* into a from private.bpay_next_approved_line x where x.id=p_approved_line_id and x.revision_id=r.id;
 select x.* into pos from private.bpay_next_position x where x.work_id=w.id and x.component_key=a.component_key;
 select x.* into c from public.candidates x where x.id=w.candidate_id;
 select x.* into u from public.umbrellas x where x.id=c.umbrella_id;
 select x.* into g from private.bpay_next_workbench_group x where x.work_id=w.id;
 select x.* into md from private.bpay_next_workbench_candidate_metadata x where x.candidate_id=w.candidate_id;
 select x.financial_view_revision into v_fvr from private.bpay_next_worker_control x where x.candidate_id=w.candidate_id;
 v_date:=public.pay_banking_official_date_context_v1(null::timestamptz);
 v_target:=upper(c.pay_method);v_umbrella:=case when v_target='UMBRELLA' then u.id end;
 v_vat:=case when v_target='UMBRELLA' then u.vat_chargeable end;
 if w.id is null or r.id is null or a.id is null or pos.work_id is null or c.id is null or g.work_id is null
  or md.candidate_id is null or v_fvr is null or c.active is not true
  or v_target is null or v_target not in ('PAYE','UMBRELLA')
  or (v_target='UMBRELLA' and (u.id is null or u.enabled is not true or u.vat_chargeable is null))
  or w.approval_state is distinct from 'APPROVED' or w.current_revision_id is distinct from r.id
  or w.applied_revision_id is distinct from r.id or pos.applied_revision_id is distinct from r.id
  or r.approved_at_utc is null or r.sealed_at_utc is null or a.tax_treatment is distinct from 'TAXABLE'
  or pos.approved_source_ex_vat is distinct from a.source_pay_ex_vat
  or g.group_kind is distinct from 'WORK' or g.candidate_id is distinct from c.id
  or g.current_revision_id is distinct from r.id or g.target_pay_channel is distinct from v_target
  or g.source_pay_channel is distinct from r.source_pay_channel or g.complete is not true or g.dirty is not false
  or g.progress_state is distinct from 'CURRENT' or g.financial_view_revision is distinct from v_fvr
  or g.metadata_version is distinct from md.metadata_version
  or (not p_projection_refresh and (md.continuation_pending is not false
    or md.adopted_metadata_version is distinct from md.metadata_version))
  or v_date->>'ok' is distinct from 'true' or (v_date->>'current_official_pay_date')::date is distinct from p_pay_date then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
 -- Physical inputs are bounded before filtering any eligibility or selected rows.
 with native as materialized(select x.* from private.bpay_next_approved_segment x
   where x.revision_id=r.id and x.component_key=a.component_key order by x.segment_key limit 10001)
 select count(*),coalesce(sum(case when pay_excluded then 0 else approved_source_ex_vat end),0),
  coalesce(bool_or(approved_line_id is distinct from a.id),false)
 into v_approval_count,v_approval_a,v_bad from native;
 if v_approval_count>10000 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
 if v_bad then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
 with native as materialized(select x.* from private.bpay_next_workbench_item x
   where x.work_id=w.id and x.component_key=a.component_key order by x.segment_key,x.portion_kind,x.id limit 50001)
 select count(*),coalesce(bool_or(group_key is distinct from g.group_key or candidate_id is distinct from c.id
   or revision_id is distinct from r.id or approved_line_id is distinct from a.id
   or member_kind not in ('WORK_COMPONENT','WORK_SEGMENT') or amount_known is not true or progress_state is distinct from 'CURRENT'
   or financial_view_revision is distinct from v_fvr or projection_revision is distinct from g.projection_revision
   or metadata_version is distinct from md.metadata_version or source_pay_channel is distinct from r.source_pay_channel
   or target_pay_channel is distinct from v_target),false),coalesce(bool_or(member_kind='WORK_SEGMENT'),false)
 into v_count,v_bad,v_segmented from native;
 if v_count>50000 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
 v_segmented:=v_segmented or v_approval_count>0;
 if v_bad or (v_segmented and v_approval_a is distinct from pos.approved_source_ex_vat)
  or exists(select 1 from private.bpay_next_workbench_item x where x.work_id=w.id and x.component_key=a.component_key
    and x.member_kind is distinct from case when v_segmented then 'WORK_SEGMENT' else 'WORK_COMPONENT' end) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
 -- Exact native READY keys only. Need checks precede the no-saved-decision return.
 v_selected_count:=0;
 for leaf in select i.* from private.bpay_next_workbench_item i where i.work_id=w.id and i.component_key=a.component_key
   and ('WORK:'||w.id::text||':'||i.component_key||':'||i.member_kind||':'||i.member_key)=any(p_selected_leaf_keys)
   and i.portion_kind in ('AVAILABLE','ZERO') order by i.member_key loop
  v_selected_count:=v_selected_count+1;
  if iSfinite(p_pay_date) is not true or leaf.section is distinct from 'READY' or leaf.selectable is not true
   or leaf.is_selected is not false or leaf.amount_known is not true or leaf.direction is distinct from 'PAYMENT'
   or coalesce(leaf.pay_hold,false) or leaf.snooze_id is not null
   or (v_segmented and not exists(select 1 from private.bpay_next_approved_segment s where s.id=leaf.approved_segment_id
     and s.revision_id=r.id and s.approved_line_id=a.id and s.component_key=a.component_key and s.segment_key=leaf.segment_key
     and not s.pay_excluded)) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
  if leaf.included_source_ex_vat>0 and private.bpay_next_component_resolution_need_v1(w.id,r.id,a.id,
    leaf.approved_segment_id,v_target,v_umbrella,v_vat,p_pay_date) is not null then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_SOURCE_BASIS_UNAVAILABLE';end if;
 end loop;
 if v_selected_count<>cardinality(p_selected_leaf_keys) then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
 -- Complete native A/R/H coverage: all portions plus exhausted current approvals.
 with portions as materialized(select i.segment_key,count(*) n,
   sum(i.approved_source_ex_vat) a,sum(i.realised_source_ex_vat) r,sum(i.active_reserved_source_ex_vat) h,
   sum(i.residual_source_ex_vat) residual,sum(i.included_source_ex_vat+i.excluded_source_ex_vat) positive,
   sum(i.recovery_source_ex_vat) recovery,sum(i.held_overhang_source_ex_vat) overhang,
   sum(case when ('WORK:'||w.id::text||':'||i.component_key||':'||i.member_kind||':'||i.member_key)=any(p_selected_leaf_keys)
     and i.portion_kind='AVAILABLE' then i.included_source_ex_vat else 0 end) included
   from private.bpay_next_workbench_item i where i.work_id=w.id and i.component_key=a.component_key group by i.segment_key),
 native as materialized(select s.* from private.bpay_next_approved_segment s
   where s.revision_id=r.id and s.component_key=a.component_key),
 keys as materialized(select segment_key from portions union select segment_key from native)
 select count(*) into v_count from (select segment_key from keys limit 10001) bounded;
 if v_count>10000 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
 for v_member in
  with portions as materialized(select i.segment_key,count(*) n,
    sum(i.approved_source_ex_vat) a,sum(i.realised_source_ex_vat) r,sum(i.active_reserved_source_ex_vat) h,
    sum(i.residual_source_ex_vat) residual,sum(i.included_source_ex_vat+i.excluded_source_ex_vat) positive,
    sum(i.recovery_source_ex_vat) recovery,sum(i.held_overhang_source_ex_vat) overhang,
    sum(case when ('WORK:'||w.id::text||':'||i.component_key||':'||i.member_kind||':'||i.member_key)=any(p_selected_leaf_keys)
      and i.portion_kind='AVAILABLE' then i.included_source_ex_vat else 0 end) included
    from private.bpay_next_workbench_item i where i.work_id=w.id and i.component_key=a.component_key group by i.segment_key),
  native as materialized(select s.* from private.bpay_next_approved_segment s
    where s.revision_id=r.id and s.component_key=a.component_key),
  keys as materialized(select segment_key from portions union select segment_key from native)
  select jsonb_build_object('segment_key',k.segment_key,'approved_segment_id',s.id,
    'approved_source_ex_vat',case when p.n is null then case when s.pay_excluded then 0 else s.approved_source_ex_vat end else p.a end,
    'realised_source_ex_vat',case when p.n is null then case when s.pay_excluded then 0 else s.approved_source_ex_vat end else p.r end,
    'held_source_ex_vat',coalesce(p.h,0),'included_source_ex_vat',coalesce(p.included,0),
    'excluded_source_ex_vat',coalesce(p.positive-p.included,0),'recovery_source_ex_vat',coalesce(p.recovery,0),
    'held_overhang_source_ex_vat',coalesce(p.overhang,0),'native_source_ex_vat',case when s.pay_excluded then 0 else s.approved_source_ex_vat end,
    'projected_residual',coalesce(p.residual,0),
    'hours',jsonb_build_array(s.hours_day,s.hours_night,s.hours_sat,s.hours_sun,s.hours_bh),
    'rates',jsonb_build_array(s.pay_day,s.pay_night,s.pay_sat,s.pay_sun,s.pay_bh))
  from keys k left join portions p on p.segment_key is not distinct from k.segment_key
   left join native s on s.segment_key=k.segment_key order by k.segment_key nulls first
 loop
  if ((v_member->>'approved_segment_id') is not null
      and (v_member->>'approved_source_ex_vat')::numeric is distinct from (v_member->>'native_source_ex_vat')::numeric)
   or (v_segmented and v_member->>'approved_segment_id' is null and (v_member->>'approved_source_ex_vat')::numeric<>0)
   or (v_member->>'approved_source_ex_vat')::numeric-(v_member->>'realised_source_ex_vat')::numeric
      -(v_member->>'held_source_ex_vat')::numeric is distinct from (v_member->>'projected_residual')::numeric
   or (v_member->>'included_source_ex_vat')::numeric+(v_member->>'excluded_source_ex_vat')::numeric
      -(v_member->>'recovery_source_ex_vat')::numeric-(v_member->>'held_overhang_source_ex_vat')::numeric
      is distinct from (v_member->>'projected_residual')::numeric then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
  v_a:=v_a+(v_member->>'approved_source_ex_vat')::numeric;v_r:=v_r+(v_member->>'realised_source_ex_vat')::numeric;
  v_h:=v_h+(v_member->>'held_source_ex_vat')::numeric;v_included:=v_included+(v_member->>'included_source_ex_vat')::numeric;
  v_excluded:=v_excluded+(v_member->>'excluded_source_ex_vat')::numeric;v_recovery:=v_recovery+(v_member->>'recovery_source_ex_vat')::numeric;
  v_overhang:=v_overhang+(v_member->>'held_overhang_source_ex_vat')::numeric;
  v_bytes:=v_bytes+octet_length(v_member::text)+2;
  if v_bytes>8388608 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
  if v_segmented then v_members:=array_append(v_members,v_member);end if;
 end loop;
 if (v_a,v_r,v_h) is distinct from (pos.approved_source_ex_vat,pos.realised_source_ex_vat,pos.held_source_ex_vat)
  or v_a-v_r-v_h is distinct from v_included+v_excluded-v_recovery-v_overhang then
  raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
 -- Same101 physical mapped-boundary discovery as the accepted owner, without a Run.
 for d in select x.* from private.bpay_next_component_resolution x where x.work_id=w.id and x.state='ACTIVE'
   and (x.based_on_revision_id=r.id or x.published_revision_id=r.id)
   and (case when x.based_on_revision_id=r.id then x.approved_line_id else x.published_line_id end)=a.id
   order by case when x.based_on_revision_id=r.id then x.approved_line_id else x.published_line_id end,
     x.rate_family,x.rate_code,x.id limit 101 loop
  v_seen:=v_seen+1;
  if v_seen>100 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
  v_line:=case when d.based_on_revision_id=r.id then d.approved_line_id else d.published_line_id end;
  if v_line is distinct from a.id or (d.candidate_id,d.target_pay_channel,d.target_umbrella_id,d.target_umbrella_vat_chargeable,
    d.valuation_policy_id,d.valuation_date) is distinct from(c.id,v_target,v_umbrella,v_vat,r.valuation_policy_id,p_pay_date) then continue;end if;
  select x.* into active from private.bpay_next_component_resolution_active_v1(w.id,r.id,a.id,d.rate_family,d.rate_code,
    v_target,v_umbrella,v_vat,r.valuation_policy_id,p_pay_date) x;
  if active.id is distinct from d.id or active.decision_version is distinct from d.decision_version then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_BASIS_CHANGED';end if;
  v_key:=jsonb_build_array(d.rate_family,d.rate_code)::text;
  if v_key=any(v_decision_keys) then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_COMPATIBILITY_AMBIGUOUS';end if;
  v_decision_keys:=array_append(v_decision_keys,v_key);
  if cardinality(v_decision_keys)>5 then raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_BUCKET_MISSING';end if;
  if first_d.id is null then first_d:=d;end if;
  v_refs:=v_refs||jsonb_build_array(jsonb_build_object('approved_line_id',a.id,'rate_family',d.rate_family,
    'rate_code',d.rate_code,'decision_id',d.id,'decision_version',d.decision_version::text));
  select x.xmin::text into v_native_version from private.bpay_next_component_resolution x where x.id=d.id;
  v_versions:=v_versions||jsonb_build_array(jsonb_build_array('RATE_DECISION',d.id::text,d.decision_version::text,v_native_version));
 end loop;
 if first_d.id is null then return null;end if;
 v_basis:=jsonb_build_object('approved_source_ex_vat',v_a,'realised_source_ex_vat',v_r,
   'held_source_ex_vat',v_h,'source_basis_channel',pos.source_basis_channel);
 v_required_included:=v_included;
 if p_through_segment_key is not null then
  if not v_segmented or not exists(select 1 from unnest(v_members) x where x->>'segment_key'=p_through_segment_key
    and (x->>'included_source_ex_vat')::numeric>0 and x->>'approved_segment_id' is not null) then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
  select jsonb_build_object('approved_source_ex_vat',sum((x->>'approved_source_ex_vat')::numeric),
    'realised_source_ex_vat',sum((x->>'realised_source_ex_vat')::numeric),'held_source_ex_vat',sum((x->>'held_source_ex_vat')::numeric),
    'source_basis_channel',pos.source_basis_channel),sum((x->>'included_source_ex_vat')::numeric)
   into v_basis,v_required_included from unnest(v_members) x where x->>'segment_key'<=p_through_segment_key;
 end if;
 v_working:=jsonb_build_object('work_id',w.id,'revision_id',r.id,'approved_line_id',a.id,'pay_date',p_pay_date,
  'valuation_policy_id',r.valuation_policy_id,'refs',v_refs,'members',to_jsonb(v_members),
  'included_source_ex_vat',v_included,'current_decision_versions',v_versions);
 if octet_length(v_working::text)>8388608 then raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
 v_result:=private.bpay_next_component_resolution_input_quote_v1(first_d,r.id,v_basis,v_required_included,null,p_through_segment_key,v_working);
 return v_result;
end
$function$;

create or replace function private.bpay_next_current_component_resolution_quote_v1(
 p_work_id uuid,p_revision_id uuid,p_approved_line_id uuid,p_pay_date date,
 p_selected_leaf_keys text[],p_through_segment_key text default null
) returns jsonb language sql stable security invoker set search_path=pg_catalog,private
as $function$
 select private.bpay_next_current_component_resolution_quote_v1(
  p_work_id,p_revision_id,p_approved_line_id,p_pay_date,p_selected_leaf_keys,p_through_segment_key,false)
$function$;

create or replace function private.bpay_next_workbench_refresh_group_display_v1(p_group_key text)
returns void language plpgsql security definer set search_path=pg_catalog,private,public
as $function$
declare
 g private.bpay_next_workbench_group%rowtype;w private.bpay_next_work%rowtype;
 r private.bpay_next_work_revision%rowtype;c public.candidates%rowtype;u public.umbrellas%rowtype;
 v_date_context jsonb;v_pay_date date;v_sensitive boolean:=false;v_component record;
 v_keys text[];v_source numeric;v_quote jsonb;v_delta numeric:=0;
begin
  perform private.bpay_next_workbench_require_next_v1();
  if p_group_key is null then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_GROUP_INVALID';end if;
  select x.* into g from private.bpay_next_workbench_group x where x.group_key=p_group_key;
  -- Native projection owners hold Candidate then Work and prove Source A/R/H
  -- before this display-only replacement. No summary/delta money changes.
  if g.group_kind='WORK' and g.complete and not g.dirty and g.progress_state='CURRENT' then
    select x.* into strict w from private.bpay_next_work x where x.id=g.work_id;
    select x.* into strict r from private.bpay_next_work_revision x where x.id=g.current_revision_id and x.work_id=w.id;
    select x.* into strict c from public.candidates x where x.id=g.candidate_id;
    select x.* into u from public.umbrellas x where x.id=c.umbrella_id;
    v_date_context:=public.pay_banking_official_date_context_v1(null::timestamptz);
    if v_date_context->>'ok' is distinct from 'true' or v_date_context->>'current_official_pay_date' is null then
      raise exception using errcode='55000',message='BPAY_NEXT_WORKBENCH_DATE_UNAVAILABLE';end if;
    v_pay_date:=(v_date_context->>'current_official_pay_date')::date;
    -- EXISTS uses native Work/revision mappings. It cannot cap the complete
    -- number of components or mistake an incompatible older date for timeless.
    v_sensitive:=exists(select 1 from private.bpay_next_component_resolution d where d.work_id=w.id and d.state='ACTIVE'
      and (d.based_on_revision_id=r.id or d.published_revision_id=r.id));
    if not v_sensitive then
      v_sensitive:=exists(select 1 from (select distinct i.approved_line_id,i.approved_segment_id
        from private.bpay_next_workbench_item i where i.group_key=g.group_key and i.revision_id=r.id
          and i.tax_treatment='TAXABLE' and i.section in ('READY','ACTION')
          and i.approved_line_id is not null) native
        where private.bpay_next_component_resolution_need_v1(w.id,r.id,native.approved_line_id,native.approved_segment_id,
          g.target_pay_channel,case when g.target_pay_channel='UMBRELLA' then u.id end,
          case when g.target_pay_channel='UMBRELLA' then u.vat_chargeable end,null::date) is not null);
    end if;
    for v_component in select distinct i.approved_line_id,i.component_key
      from private.bpay_next_workbench_item i where i.group_key=g.group_key and i.revision_id=r.id
        and i.tax_treatment='TAXABLE' and i.section='READY' and i.selectable and i.amount_known and not i.is_selected
        and i.direction='PAYMENT' and i.portion_kind in ('AVAILABLE','ZERO')
        and exists(select 1 from private.bpay_next_component_resolution d where d.work_id=w.id and d.state='ACTIVE'
          and (d.based_on_revision_id=r.id or d.published_revision_id=r.id)
          and (case when d.based_on_revision_id=r.id then d.approved_line_id else d.published_line_id end)=i.approved_line_id)
      order by i.component_key,i.approved_line_id loop
      with native as materialized(select i.* from private.bpay_next_workbench_item i
        where i.group_key=g.group_key and i.revision_id=r.id and i.approved_line_id=v_component.approved_line_id
          and i.component_key=v_component.component_key and i.section='READY' and i.selectable and i.amount_known
          and not i.is_selected and i.direction='PAYMENT' and i.portion_kind in ('AVAILABLE','ZERO')
        order by i.member_kind,i.member_key,i.id limit 10001)
      select array_agg('WORK:'||w.id::text||':'||i.component_key||':'||i.member_kind||':'||i.member_key
        order by i.member_kind,i.member_key,i.id),sum(i.included_source_ex_vat)
        into v_keys,v_source from native i;
      if cardinality(v_keys)>10000 then
        raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_ACCEPTED_SCOPE_TOO_LARGE';end if;
      v_quote:=private.bpay_next_current_component_resolution_quote_v1(w.id,r.id,v_component.approved_line_id,
        v_pay_date,v_keys,null,true);
      if v_quote is not null then
        if jsonb_typeof(v_quote->'component_quote') is distinct from 'object'
          or v_quote->'component_quote'->>'ex' is null then
          raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_POSITION_INCOMPATIBLE';end if;
        v_delta:=v_delta+(v_quote->'component_quote'->>'ex')::numeric-v_source;
      end if;
    end loop;
  end if;
  delete from private.bpay_next_workbench_group_display where parent_group_key=p_group_key;
-- WB15 own-list display projection. Values never enter a financial command.
insert into private.bpay_next_workbench_group_display(
  presentation_key,presentation_kind,presentation_id,parent_group_key,candidate_id,case_component_id,
  section,client_id,week_ending_date,client_display_name,payment_label,timesheet_reference,
  target_pay_channel,has_deduction,selectable,reason_code,reason_priority,group_amount,ready_net_add_amount,
  gross_deduction_balance,net_deduction_balance,waiting_amount,waiting_net_add_amount,
  accepted_run_id,accepted_command_id,accepted_module_epoch,snooze_until_date,eligible_from_date,pay_hold_reason
)
with members as (
  select g.group_key parent_group_key,g.candidate_id,g.client_id,g.week_ending_date,g.target_pay_channel,g.certified_zero,
    case when g.group_kind='WORK' then 'WORK' when fc.case_kind='OVERPAYMENT' or i.case_component_id is null then 'CASE' else 'ROW' end presentation_kind,
    case when g.group_kind='WORK' then g.work_id when fc.case_kind='OVERPAYMENT' or i.case_component_id is null then g.case_id else i.case_component_id end presentation_id,
    case when g.group_kind='CASE' and fc.case_kind<>'OVERPAYMENT' then i.case_component_id end case_component_id,
    case when g.group_kind='WORK' then i.client_display_name else cl.name end client_display_name,
    case when g.group_kind='WORK' then 'Timesheet' when fc.case_kind='OVERPAYMENT' then 'Overpayment recovery'
      when cc.case_subtype in ('LOAN','PAYMENT_ADVANCE') then case when cc.direction='DEDUCTION' then 'Payment Advance repayment' else 'Payment Advance' end
      when cc.case_subtype='MANUAL_CREDIT' then 'Manual Credit' when cc.case_subtype='UNDERPAYMENT' then 'Underpayment'
      when cc.case_subtype='MANUAL_DEBT' then 'Manual Debt' else 'Payment needs attention' end payment_label,
    i.id,i.section item_section,i.direction,i.payroll_stage,i.portion_kind,i.member_kind,i.reason_code,i.reason_priority,
    i.is_selected,i.selectable,i.amount_known,i.selected_amount_known,i.timesheet_reference,i.included_source_ex_vat,
    i.gross_due_source_ex_vat,i.nominal_source_ex_vat,i.deduction_balance_source_ex_vat,
    i.active_reserved_source_ex_vat,i.selected_source_ex_vat,i.accepted_run_id,i.accepted_command_id,i.accepted_module_epoch,
    i.snooze_until_date,i.eligible_from_date,i.pay_hold_reason,
    (i.reason_code='MANUAL_COLLECTION_UNAVAILABLE' or not i.amount_known and not(g.certified_zero and i.member_kind='GROUP_EVIDENCE')
      or i.is_selected and not i.selected_amount_known) amount_unavailable,
    case when i.is_selected then i.selected_source_ex_vat
      when i.portion_kind='RESERVED' then i.active_reserved_source_ex_vat
      when i.direction='PAYMENT' and i.section='READY' then i.included_source_ex_vat
      when i.direction='PAYMENT' and i.section='WAITING' and i.payroll_stage='GROSS_ADD' then i.gross_due_source_ex_vat
      else i.nominal_source_ex_vat end display_value,
    case when i.is_selected and i.accepted_run_id is not null and i.accepted_command_id is not null and i.accepted_module_epoch is not null
      and exists(select 1 from private.bpay_next_run_command rc join private.bpay_next_command cmd on cmd.id=rc.command_id
        join private.bpay_next_pay_run pr on pr.id=rc.run_id where rc.run_id=i.accepted_run_id and rc.command_id=i.accepted_command_id
          and cmd.command_kind='PREPARE' and cmd.module_epoch=i.accepted_module_epoch and pr.selection_state='SEALED'
          and pr.status in ('PREPARING','REVIEW','DRAFT','EXECUTING'))
      then pg_catalog.jsonb_build_array(i.accepted_run_id::text,i.accepted_command_id::text,i.accepted_module_epoch::text) end accepted_pair
  from private.bpay_next_workbench_group g join private.bpay_next_workbench_item i on i.group_key=g.group_key
  left join private.bpay_next_finance_case fc on fc.id=g.case_id
  left join private.bpay_next_case_component cc on cc.id=i.case_component_id and cc.case_id=g.case_id
  left join public.clients cl on cl.id=g.client_id
  where g.group_key=p_group_key
), scoped as (
  select m.*,sec.section from members m cross join lateral(values(m.item_section),('ALL'::text)) sec(section)
), reduced as (
  select presentation_kind,presentation_id,parent_group_key,candidate_id,case_component_id,section,client_id,week_ending_date,target_pay_channel,
    min(client_display_name::text) client_display_name,min(payment_label::text) payment_label,min(timesheet_reference::text) timesheet_reference,
    bool_or(direction='DEDUCTION') has_deduction,bool_or(selectable) selectable,
    (array_agg(reason_code order by reason_priority,id))[1] reason_code,min(reason_priority::smallint) reason_priority,
    bool_or(amount_unavailable) amount_unavailable,
    coalesce(sum(case when direction='DEDUCTION' then -display_value when direction='PAYMENT' then display_value when reason_code='MANUAL_COLLECTION' and direction='INFORMATIONAL' then display_value else 0 end),0) group_amount,
    coalesce(sum(included_source_ex_vat) filter(where item_section='READY' and direction='PAYMENT' and payroll_stage='NET_ADD'),0) ready_net_add_amount,
    coalesce(sum(deduction_balance_source_ex_vat) filter(where direction='DEDUCTION' and payroll_stage='GROSS_DEDUCT'),0) gross_deduction_balance,
    coalesce(sum(deduction_balance_source_ex_vat) filter(where direction='DEDUCTION' and payroll_stage='NET_DEDUCT'),0) net_deduction_balance,
    coalesce(sum(gross_due_source_ex_vat) filter(where item_section='WAITING' and direction='PAYMENT' and payroll_stage='GROSS_ADD' and portion_kind<>'RESERVED'),0) waiting_amount,
    coalesce(sum(nominal_source_ex_vat) filter(where item_section='WAITING' and direction='PAYMENT' and payroll_stage='NET_ADD' and portion_kind<>'RESERVED'),0) waiting_net_add_amount,
    array_agg(distinct accepted_pair) filter(where accepted_pair is not null) accepted_pairs,
    min(snooze_until_date::date) snooze_until_date,min(eligible_from_date::date) eligible_from_date,min(pay_hold_reason::text) pay_hold_reason
  from scoped group by presentation_kind,presentation_id,parent_group_key,candidate_id,case_component_id,section,client_id,week_ending_date,target_pay_channel
)
select presentation_kind||':'||presentation_id::text,presentation_kind,presentation_id,parent_group_key,candidate_id,case_component_id,
  section,client_id,week_ending_date,client_display_name,payment_label,timesheet_reference,
  target_pay_channel,has_deduction,selectable,reason_code,reason_priority,
  case when not amount_unavailable then group_amount end,case when not amount_unavailable then ready_net_add_amount end,
  case when not amount_unavailable then gross_deduction_balance end,case when not amount_unavailable then net_deduction_balance end,
  case when not amount_unavailable then waiting_amount end,case when not amount_unavailable then waiting_net_add_amount end,
  case when cardinality(accepted_pairs)=1 then (accepted_pairs[1]->>0)::uuid end,
  case when cardinality(accepted_pairs)=1 then (accepted_pairs[1]->>1)::uuid end,
  case when cardinality(accepted_pairs)=1 then (accepted_pairs[1]->>2)::bigint end,
  snooze_until_date,eligible_from_date,pay_hold_reason
from reduced;
  if g.group_kind='WORK' and g.complete and not g.dirty and g.progress_state='CURRENT' then
    update private.bpay_next_workbench_group_display set group_amount=group_amount+v_delta
      where parent_group_key=p_group_key and presentation_kind='WORK' and section='READY' and group_amount is not null;
    -- Certification follows the complete replacement, never partial paging.
    update private.bpay_next_workbench_group set chosen_display_date_sensitive=v_sensitive,
      chosen_display_pay_date=case when v_sensitive then v_pay_date end where group_key=p_group_key;
  end if;
end
$function$;

alter function private.bpay_next_current_component_resolution_quote_v1(uuid,uuid,uuid,date,text[],text,boolean) owner to postgres;
revoke all on function private.bpay_next_current_component_resolution_quote_v1(uuid,uuid,uuid,date,text[],text,boolean) from public,anon,authenticated,service_role;
alter function private.bpay_next_current_component_resolution_quote_v1(uuid,uuid,uuid,date,text[],text) owner to postgres;
revoke all on function private.bpay_next_current_component_resolution_quote_v1(uuid,uuid,uuid,date,text[],text) from public,anon,authenticated,service_role;
alter function private.bpay_next_workbench_refresh_group_display_v1(text) owner to postgres;
revoke all on function private.bpay_next_workbench_refresh_group_display_v1(text) from public,anon,authenticated,service_role;
commit;
