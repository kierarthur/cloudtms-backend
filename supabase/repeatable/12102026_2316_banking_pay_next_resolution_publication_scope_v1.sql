-- WB29 / BD162: finite native publication input and Source seed qualification.
-- W2 preholds its complete Candidate/Work/Position/Case scope before calling.
-- No seed, publication, history sum or money change is performed here.

\set ON_ERROR_STOP on

begin;

create or replace function private.bpay_next_resolution_revision_census_v1(p_revision_ids uuid[])
returns void language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare v_id uuid;v_row record;v_count integer:=0;v_bytes bigint:=0;
 r private.bpay_next_work_revision%rowtype;
begin
 if cardinality(p_revision_ids) not between 1 and 200 or p_revision_ids is null
  or array_position(p_revision_ids,null::uuid) is not null then
  raise exception using errcode='22023',message='BPAY_NEXT_RESOLUTION_REQUEST_INVALID';end if;
 for v_id in select distinct id from unnest(p_revision_ids) id order by id loop
  select x.* into r from private.bpay_next_work_revision x where x.id=v_id;
  if r.id is null or r.approved_at_utc is null or r.sealed_at_utc is null then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_STALE';end if;
  -- Physical immutable-tree prefix first, with no bucket/intent filtering.
  -- LIMIT is an overflow sentinel; it never supplies a partial clone.
  for v_row in
   select payload from (
    select to_jsonb(x) payload from private.bpay_next_work_revision x where x.id=v_id
    union all select to_jsonb(x) from private.bpay_next_approved_line x where x.revision_id=v_id
    union all select to_jsonb(x) from private.bpay_next_rate_detail x
     join private.bpay_next_approved_line l on l.id=x.approved_line_id where l.revision_id=v_id
    union all select to_jsonb(x) from private.bpay_next_rate_schedule x where x.revision_id=v_id
    union all select to_jsonb(x) from private.bpay_next_shift_detail x
     join private.bpay_next_approved_line l on l.id=x.approved_line_id where l.revision_id=v_id
    union all select to_jsonb(x) from private.bpay_next_break_detail x
     join private.bpay_next_shift_detail s on s.id=x.shift_detail_id
     join private.bpay_next_approved_line l on l.id=s.approved_line_id where l.revision_id=v_id
    union all select to_jsonb(x) from private.bpay_next_approved_segment x where x.revision_id=v_id
    union all select to_jsonb(x) from private.bpay_next_revision_worker_basis x where x.revision_id=v_id
   ) tree limit 10001
  loop
   v_count:=v_count+1;v_bytes:=v_bytes+octet_length(v_row.payload::text);
   if v_count>10000 or v_bytes>8388608 then
    raise exception using errcode='54000',message='BPAY_NEXT_RESOLUTION_SCOPE_TOO_LARGE';end if;
  end loop;
  if (select count(*) from private.bpay_next_approved_line l where l.revision_id=v_id) is distinct from r.expected_line_count::bigint
   or (select count(*) from private.bpay_next_rate_schedule s where s.revision_id=v_id) is distinct from r.expected_rate_schedule_count::bigint
   or exists(select 1 from private.bpay_next_approved_line l where l.revision_id=v_id
    and (select count(*) from private.bpay_next_rate_detail d where d.approved_line_id=l.id)<>l.expected_rate_detail_count)
   or not exists(select 1 from private.bpay_next_revision_worker_basis b where b.revision_id=v_id) then
   raise exception using errcode='23514',message='BPAY_NEXT_TREATMENT_SOURCE_CHANGED';end if;
 end loop;
end
$function$;

create or replace function private.bpay_next_resolution_source_seed_plan_v1(
 p_work_id uuid,p_component_key text,p_expected_revision_id uuid
) returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private
as $function$
declare w private.bpay_next_work%rowtype;p private.bpay_next_position%rowtype;
 b private.bpay_next_source_component_binding%rowtype;e private.bpay_next_financial_effect%rowtype;
 h private.bpay_next_hold%rowtype;v_snapshot record;v_count integer;v_ids uuid[];
 v_effect uuid;v_hold uuid;v_pointer uuid;
begin
 select x.* into w from private.bpay_next_work x where x.id=p_work_id;
 select x.* into p from private.bpay_next_position x where x.work_id=p_work_id and x.component_key=p_component_key;
 if w.id is null or w.work_kind is distinct from 'SOURCE'
  or w.current_revision_id is distinct from p_expected_revision_id or w.applied_revision_id is distinct from p_expected_revision_id
  or p.applied_revision_id is distinct from p_expected_revision_id then
  raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_SEED_BASIS_UNAVAILABLE';end if;
 select x.* into b from private.bpay_next_source_component_binding x
  where x.canonical_work_id=p_work_id and x.canonical_component_key=p_component_key for share;
 if b.current_binding_version_id is null then
  raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_SEED_BASIS_UNAVAILABLE';end if;
 select x.* into strict v_snapshot from private.bpay_next_bound_source_snapshot_v1(
  b.current_binding_version_id,case when p.source_book_state='READY' then p.source_book_revision else 1 end) x;
 if p.source_book_state='READY' then
  if v_snapshot.qualification_state is distinct from 'READY'
   or (v_snapshot.candidate_id,v_snapshot.canonical_work_id,v_snapshot.canonical_component_key)
    is distinct from (w.candidate_id,w.id,p_component_key) then
   raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_SEED_BASIS_UNAVAILABLE';end if;
  return jsonb_build_object('seed_required',false,'work_id',p_work_id,'component_key',p_component_key,
   'expected_revision_id',p_expected_revision_id,'binding_version_id',b.current_binding_version_id,
   'source_book_revision',p.source_book_revision::text,'effect_id',null,'hold_id',null);
 end if;
 if v_snapshot.qualification_state is distinct from 'BOOK_UNBOUND' then
  raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_SEED_BASIS_UNAVAILABLE';end if;
 if p.realised_source_ex_vat<0 or p.held_source_ex_vat<0 then
  raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_SIGNED_SEED_UNAVAILABLE';end if;
 if p.realised_source_ex_vat>0 then
  select c.original_effect_id into v_pointer from private.bpay_next_work_collection c
   where c.work_id=p_work_id and c.component_key=p_component_key;
  if v_pointer is not null then
   select x.* into e from private.bpay_next_financial_effect x where x.id=v_pointer;
   if (e.work_id,e.component_key,e.effect_kind,e.source_disposition_ex_vat)
    is not distinct from (p_work_id,p_component_key,'PAYROLL_SETTLED'::text,p.realised_source_ex_vat) then
    v_effect:=e.id;
   end if;
  end if;
  if v_effect is null then
   -- Bound the whole physical component history before amount/kind filters.
   select array_agg(x.id) into v_ids from (select id from private.bpay_next_financial_effect
    where work_id=p_work_id and component_key=p_component_key order by occurred_at_utc,id limit 101) x;
   if coalesce(cardinality(v_ids),0)>100 then
    raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_SEED_BASIS_UNAVAILABLE';end if;
   select count(*),min(x.id::text)::uuid into v_count,v_effect from private.bpay_next_financial_effect x
    where x.id=any(v_ids) and x.effect_kind='PAYROLL_SETTLED' and x.source_disposition_ex_vat=p.realised_source_ex_vat;
   if v_count<>1 then raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_TYPED_WITNESS_REQUIRED';end if;
  end if;
 end if;
 if p.held_source_ex_vat>0 then
  -- ACTIVE membership is the native partial-index predicate, not a money filter.
  select array_agg(x.id) into v_ids from (select id from private.bpay_next_hold
   where work_id=p_work_id and component_key=p_component_key and status='ACTIVE' order by id limit 101) x;
  if coalesce(cardinality(v_ids),0)>100 then
   raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_SEED_BASIS_UNAVAILABLE';end if;
  select count(*),min(x.id::text)::uuid into v_count,v_hold from private.bpay_next_hold x
   where x.id=any(v_ids) and x.source_reserved_ex_vat=p.held_source_ex_vat;
  if v_count<>1 then raise exception using errcode='55000',message='BPAY_NEXT_SOURCE_BOOK_TYPED_WITNESS_REQUIRED';end if;
 end if;
 -- Preserve every actual paid/frozen/settlement and ZERO predicate in1246.
 -- The scalar matches above additionally cover the whole maintained R/H.
 perform 1 from private.bpay_next_source_book_seed_proof_v1(p_work_id,p_component_key,p_expected_revision_id,v_effect,v_hold);
 return jsonb_build_object('seed_required',true,'work_id',p_work_id,'component_key',p_component_key,
  'expected_revision_id',p_expected_revision_id,'binding_version_id',b.current_binding_version_id,
  'source_book_revision',null,'effect_id',v_effect,'hold_id',v_hold);
end
$function$;

alter function private.bpay_next_resolution_revision_census_v1(uuid[]) owner to postgres;
alter function private.bpay_next_resolution_source_seed_plan_v1(uuid,text,uuid) owner to postgres;
revoke all on function private.bpay_next_resolution_revision_census_v1(uuid[]),
 private.bpay_next_resolution_source_seed_plan_v1(uuid,text,uuid) from public,anon,authenticated,service_role;

commit;
