-- FC-02 full 0101 successor: native raw row sort operand; native label joins, original clear point, bounded history/count windows; no money/projection.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_snooze_management_page_v1(p_actor uuid,p_args jsonb)
returns jsonb language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare v_scope jsonb;v_context jsonb;v_after jsonb;v_key jsonb;v_last jsonb;v_rows jsonb:='[]';
 v_row jsonb;v_rec record;v_epoch bigint;v_agency uuid;v_revision private.bpay_next_workbench_revision%rowtype;
 v_clock bigint;v_limit integer;v_sort text;v_complete boolean:=true;v_seen integer:=0;v_total integer:=0;v_expenses integer:=0;v_value text;
begin
 if p_actor is null or not exists(select 1 from public.tms_users u where u.id=p_actor and u.is_active and u.role::text='admin') then
  raise exception using errcode='42501',message='BPAY_NEXT_WORKBENCH_FORBIDDEN';end if;
 select m.owner_epoch into v_epoch from private.bpay_next_module_control m where m.id=1 and m.active_owner='NEXT';
 if v_epoch is null then raise exception using errcode='55000',message='BPAY_NEXT_MODULE_NOT_ACTIVE';end if;
 if p_args is null or jsonb_typeof(p_args)<>'object' or not(p_args?&array['candidate_id','client_id','scope_kind','filter','snooze_id','sort','after','limit','view','count_only'])
  or p_args-array['candidate_id','client_id','scope_kind','filter','snooze_id','sort','after','limit','view','count_only']<>'{}'::jsonb or octet_length(p_args::text)>32768
  or p_args->>'scope_kind' is null or p_args->>'scope_kind' not in ('ALL','WORK','CASE')
  or p_args->>'filter' is null or p_args->>'filter' not in ('ACTIVE','INDEFINITE','DATED')
  or p_args->>'sort' is null or p_args->>'sort' not in ('CREATED_ASC','CREATED_DESC','CANDIDATE_ASC','CANDIDATE_DESC','TIMESHEET_ASC','TIMESHEET_DESC','WHAT_ASC','WHAT_DESC','UNTIL_ASC','UNTIL_DESC','NOTE_ASC','NOTE_DESC')
  or p_args->>'view' is null or p_args->>'view' not in ('ALL','ACTIVE','SNOOZED','HISTORY') or jsonb_typeof(p_args->'count_only') is distinct from 'boolean'
  or ((p_args->>'count_only')::boolean and (p_args->'snooze_id'<>'null'::jsonb or p_args->>'sort' not in ('CREATED_ASC','CREATED_DESC')))
  or jsonb_typeof(p_args->'limit') is distinct from 'number' or p_args->>'limit'!~'^[1-9][0-9]?$' then
  raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
 if exists(select 1 from unnest(array['candidate_id','client_id','snooze_id']) q(k) where p_args->k<>'null'::jsonb
  and (jsonb_typeof(p_args->k)<>'string' or p_args->>k!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$')) then
  raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
 v_limit:=(p_args->>'limit')::integer;v_sort:=p_args->>'sort';
 if v_limit>50 or (p_args->'snooze_id'<>'null'::jsonb and p_args->'after'<>'null'::jsonb) then
  raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
 select d.installation_id into strict v_agency from private.cloudtms_database_identity d where d.singleton;
 select x.* into strict v_revision from private.bpay_next_workbench_revision x where x.id=1;
 select x.last_sequence into strict v_clock from private.bpay_next_command_clock x where x.id=1;
 v_scope:=p_args-array['after','limit'];
 v_context:=jsonb_build_object('actor_user_id',p_actor,'agency_id',v_agency,'owner_epoch',v_epoch::text,
  'workbench_revision',v_revision.workbench_revision::text,'adoption_revision',v_revision.adoption_revision::text,'control_cut',v_clock::text,'scope',v_scope);
 v_after:=nullif(p_args->'after','null'::jsonb);
 if v_after is not null then
  if jsonb_typeof(v_after)<>'object' or not(v_after?&array['context','key']) or v_after-array['context','key']<>'{}'::jsonb
   or v_after->'context' is distinct from v_context then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
  v_key:=v_after->'key';
  if jsonb_typeof(v_key)<>'object' or not(v_key?&array['created_at_utc','id','sort_value']) or v_key-array['created_at_utc','id','sort_value']<>'{}'::jsonb
   or jsonb_typeof(v_key->'sort_value') not in ('string','null') or octet_length(coalesce(v_key->>'sort_value',''))>11000
   or (v_sort like 'CREATED_%' and v_key->'sort_value'<>'null'::jsonb)
   or jsonb_typeof(v_key->'id')<>'string' or v_key->>'id'!~'^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$'
   or jsonb_typeof(v_key->'created_at_utc')<>'string' or v_key->>'created_at_utc'!~'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,6})?(Z|[+]00:00)$'
   or not isfinite((v_key->>'created_at_utc')::timestamptz) then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
  if v_key->>'sort_value' is not null then
   v_value:=convert_from(decode(v_key->>'sort_value','base64'),'UTF8');
   if replace(encode(convert_to(v_value,'UTF8'),'base64'),chr(10),'') is distinct from v_key->>'sort_value' or octet_length(v_value)>8192 then raise exception using errcode='22023',message='BPAY_NEXT_WORKBENCH_REQUEST_INVALID';end if;
   v_key:=jsonb_set(v_key,'{sort_value}',to_jsonb(v_value));
  end if;
 end if;
 -- Exact inactive control remains reachable only to check an original CLEAR.
 -- Keep the ordinary active-list seek predicate literal for its partial index.
 if p_args->'snooze_id'<>'null'::jsonb then
  v_row:=private.bpay_next_snooze_management_row_v1((p_args->>'snooze_id')::uuid);
  if v_row is not null and (p_args->'candidate_id'='null'::jsonb or v_row->'candidate_id'=p_args->'candidate_id')
   and (p_args->'client_id'='null'::jsonb or v_row->'client_id'=p_args->'client_id')
   and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(v_row->>'scope_kind'='CASE'))
   and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(v_row->'until_date'='null'::jsonb)) then
   v_rows:=jsonb_build_array(v_row||jsonb_build_object('sort_key',null));end if;
  if octet_length(v_rows::text)>110000 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
  return jsonb_build_object('ok',true,'kind','SNOOZES','context',v_context,'rows',v_rows,'scanned_count',jsonb_array_length(v_rows)::text,'counts',null,'complete',true,'next_cursor',null);
 end if;
 for v_rec in
 select q.* from (
(select s.id,s.created_at_utc,s.scope_kind,null::text as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and s.active=true and p_args->>'view' in ('ALL','SNOOZED') and v_sort='CREATED_ASC' and (v_key is null or s.created_at_utc > (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid))
 order by s.created_at_utc ASC,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,s.note collate "C" as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and s.active=true and p_args->>'view' in ('ALL','SNOOZED') and v_sort='NOTE_ASC' and (v_key is null or (s.note collate "C" > (v_key->>'sort_value') collate "C" or (s.note collate "C" is not distinct from (v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))
 order by s.note collate "C" ASC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,s.until_date::text as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and s.active=true and p_args->>'view' in ('ALL','SNOOZED') and v_sort='UNTIL_ASC' and s.until_date is not null and (v_key is null or ((s.until_date is null)::integer>(v_key->'sort_value'='null'::jsonb)::integer or ((s.until_date is null)=(v_key->'sort_value'='null'::jsonb) and (s.until_date > (v_key->>'sort_value')::date or (s.until_date is not distinct from (v_key->>'sort_value')::date and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))))
 order by (s.until_date is null),s.until_date ASC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,s.until_date::text as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and s.active=true and p_args->>'view' in ('ALL','SNOOZED') and v_sort='UNTIL_ASC' and s.until_date is null and (v_key is null or ((s.until_date is null)::integer>(v_key->'sort_value'='null'::jsonb)::integer or ((s.until_date is null)=(v_key->'sort_value'='null'::jsonb) and (s.until_date > (v_key->>'sort_value')::date or (s.until_date is not distinct from (v_key->>'sort_value')::date and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))))
 order by s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,null::text as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and s.active=true and p_args->>'view' in ('ALL','SNOOZED') and v_sort='CREATED_DESC' and (v_key is null or s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid))
 order by s.created_at_utc DESC,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,s.note collate "C" as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and s.active=true and p_args->>'view' in ('ALL','SNOOZED') and v_sort='NOTE_DESC' and (v_key is null or (s.note collate "C" < (v_key->>'sort_value') collate "C" or (s.note collate "C" is not distinct from (v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))
 order by s.note collate "C" DESC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,s.until_date::text as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and s.active=true and p_args->>'view' in ('ALL','SNOOZED') and v_sort='UNTIL_DESC' and s.until_date is not null and (v_key is null or ((s.until_date is null)::integer>(v_key->'sort_value'='null'::jsonb)::integer or ((s.until_date is null)=(v_key->'sort_value'='null'::jsonb) and (s.until_date < (v_key->>'sort_value')::date or (s.until_date is not distinct from (v_key->>'sort_value')::date and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))))
 order by (s.until_date is null),s.until_date DESC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,s.until_date::text as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and s.active=true and p_args->>'view' in ('ALL','SNOOZED') and v_sort='UNTIL_DESC' and s.until_date is null and (v_key is null or ((s.until_date is null)::integer>(v_key->'sort_value'='null'::jsonb)::integer or ((s.until_date is null)=(v_key->'sort_value'='null'::jsonb) and (s.until_date < (v_key->>'sort_value')::date or (s.until_date is not distinct from (v_key->>'sort_value')::date and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))))
 order by s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,null::text as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and s.active=false and p_args->>'view' in ('ALL','HISTORY') and v_sort='CREATED_ASC' and (v_key is null or s.created_at_utc > (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid))
 order by s.created_at_utc ASC,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,s.note collate "C" as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and s.active=false and p_args->>'view' in ('ALL','HISTORY') and v_sort='NOTE_ASC' and (v_key is null or (s.note collate "C" > (v_key->>'sort_value') collate "C" or (s.note collate "C" is not distinct from (v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))
 order by s.note collate "C" ASC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,s.until_date::text as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and s.active=false and p_args->>'view' in ('ALL','HISTORY') and v_sort='UNTIL_ASC' and s.until_date is not null and (v_key is null or ((s.until_date is null)::integer>(v_key->'sort_value'='null'::jsonb)::integer or ((s.until_date is null)=(v_key->'sort_value'='null'::jsonb) and (s.until_date > (v_key->>'sort_value')::date or (s.until_date is not distinct from (v_key->>'sort_value')::date and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))))
 order by (s.until_date is null),s.until_date ASC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,s.until_date::text as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and s.active=false and p_args->>'view' in ('ALL','HISTORY') and v_sort='UNTIL_ASC' and s.until_date is null and (v_key is null or ((s.until_date is null)::integer>(v_key->'sort_value'='null'::jsonb)::integer or ((s.until_date is null)=(v_key->'sort_value'='null'::jsonb) and (s.until_date > (v_key->>'sort_value')::date or (s.until_date is not distinct from (v_key->>'sort_value')::date and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))))
 order by s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,null::text as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and s.active=false and p_args->>'view' in ('ALL','HISTORY') and v_sort='CREATED_DESC' and (v_key is null or s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid))
 order by s.created_at_utc DESC,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,s.note collate "C" as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and s.active=false and p_args->>'view' in ('ALL','HISTORY') and v_sort='NOTE_DESC' and (v_key is null or (s.note collate "C" < (v_key->>'sort_value') collate "C" or (s.note collate "C" is not distinct from (v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))
 order by s.note collate "C" DESC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,s.until_date::text as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and s.active=false and p_args->>'view' in ('ALL','HISTORY') and v_sort='UNTIL_DESC' and s.until_date is not null and (v_key is null or ((s.until_date is null)::integer>(v_key->'sort_value'='null'::jsonb)::integer or ((s.until_date is null)=(v_key->'sort_value'='null'::jsonb) and (s.until_date < (v_key->>'sort_value')::date or (s.until_date is not distinct from (v_key->>'sort_value')::date and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))))
 order by (s.until_date is null),s.until_date DESC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,s.until_date::text as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and s.active=false and p_args->>'view' in ('ALL','HISTORY') and v_sort='UNTIL_DESC' and s.until_date is null and (v_key is null or ((s.until_date is null)::integer>(v_key->'sort_value'='null'::jsonb)::integer or ((s.until_date is null)=(v_key->'sort_value'='null'::jsonb) and (s.until_date < (v_key->>'sort_value')::date or (s.until_date is not distinct from (v_key->>'sort_value')::date and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))))
 order by s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,lower(r.candidate_display_name) collate "C" as sort_value from private.bpay_next_work_revision r join private.bpay_next_work w on w.id=r.work_id join private.bpay_next_snooze s on s.work_id=w.id and s.candidate_id=w.candidate_id left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind<>'CASE' and r.sealed_at_utc is not null and w.current_revision_id=r.id and v_sort='CANDIDATE_ASC' and (v_key is null or left(lower(r.candidate_display_name),512) collate "C" >= left((v_key->>'sort_value'),512) collate "C") and (v_key is null or (lower(r.candidate_display_name) collate "C" > (v_key->>'sort_value') collate "C" or (lower(r.candidate_display_name) collate "C" is not distinct from (v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))
 order by left(lower(r.candidate_display_name),512) collate "C" ASC,lower(r.candidate_display_name) collate "C" ASC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,r.week_ending_date::text as sort_value from private.bpay_next_work_revision r join private.bpay_next_work w on w.id=r.work_id join private.bpay_next_snooze s on s.work_id=w.id and s.candidate_id=w.candidate_id left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind<>'CASE' and r.sealed_at_utc is not null and w.current_revision_id=r.id and v_sort='TIMESHEET_ASC' and (v_key is null or (r.week_ending_date > (v_key->>'sort_value')::date or (r.week_ending_date is not distinct from (v_key->>'sort_value')::date and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))
 order by r.week_ending_date ASC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,lower(r.candidate_display_name) collate "C" as sort_value from private.bpay_next_work_revision r join private.bpay_next_work w on w.id=r.work_id join private.bpay_next_snooze s on s.work_id=w.id and s.candidate_id=w.candidate_id left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind<>'CASE' and r.sealed_at_utc is not null and w.current_revision_id=r.id and v_sort='CANDIDATE_DESC' and (v_key is null or left(lower(r.candidate_display_name),512) collate "C" <= left((v_key->>'sort_value'),512) collate "C") and (v_key is null or (lower(r.candidate_display_name) collate "C" < (v_key->>'sort_value') collate "C" or (lower(r.candidate_display_name) collate "C" is not distinct from (v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))
 order by left(lower(r.candidate_display_name),512) collate "C" DESC,lower(r.candidate_display_name) collate "C" DESC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,r.week_ending_date::text as sort_value from private.bpay_next_work_revision r join private.bpay_next_work w on w.id=r.work_id join private.bpay_next_snooze s on s.work_id=w.id and s.candidate_id=w.candidate_id left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind<>'CASE' and r.sealed_at_utc is not null and w.current_revision_id=r.id and v_sort='TIMESHEET_DESC' and (v_key is null or (r.week_ending_date < (v_key->>'sort_value')::date or (r.week_ending_date is not distinct from (v_key->>'sort_value')::date and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))
 order by r.week_ending_date DESC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,lower(r.candidate_display_name) collate "C" as sort_value from private.bpay_next_work_revision r join private.bpay_next_work w on w.id=r.work_id join private.bpay_next_snooze s on s.work_id=w.id and s.candidate_id=w.candidate_id left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind<>'CASE' and r.sealed_at_utc is not null and w.current_revision_id is null and w.applied_revision_id=r.id and v_sort='CANDIDATE_ASC' and (v_key is null or left(lower(r.candidate_display_name),512) collate "C" >= left((v_key->>'sort_value'),512) collate "C") and (v_key is null or (lower(r.candidate_display_name) collate "C" > (v_key->>'sort_value') collate "C" or (lower(r.candidate_display_name) collate "C" is not distinct from (v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))
 order by left(lower(r.candidate_display_name),512) collate "C" ASC,lower(r.candidate_display_name) collate "C" ASC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,r.week_ending_date::text as sort_value from private.bpay_next_work_revision r join private.bpay_next_work w on w.id=r.work_id join private.bpay_next_snooze s on s.work_id=w.id and s.candidate_id=w.candidate_id left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind<>'CASE' and r.sealed_at_utc is not null and w.current_revision_id is null and w.applied_revision_id=r.id and v_sort='TIMESHEET_ASC' and (v_key is null or (r.week_ending_date > (v_key->>'sort_value')::date or (r.week_ending_date is not distinct from (v_key->>'sort_value')::date and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))
 order by r.week_ending_date ASC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,lower(r.candidate_display_name) collate "C" as sort_value from private.bpay_next_work_revision r join private.bpay_next_work w on w.id=r.work_id join private.bpay_next_snooze s on s.work_id=w.id and s.candidate_id=w.candidate_id left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind<>'CASE' and r.sealed_at_utc is not null and w.current_revision_id is null and w.applied_revision_id=r.id and v_sort='CANDIDATE_DESC' and (v_key is null or left(lower(r.candidate_display_name),512) collate "C" <= left((v_key->>'sort_value'),512) collate "C") and (v_key is null or (lower(r.candidate_display_name) collate "C" < (v_key->>'sort_value') collate "C" or (lower(r.candidate_display_name) collate "C" is not distinct from (v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))
 order by left(lower(r.candidate_display_name),512) collate "C" DESC,lower(r.candidate_display_name) collate "C" DESC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,r.week_ending_date::text as sort_value from private.bpay_next_work_revision r join private.bpay_next_work w on w.id=r.work_id join private.bpay_next_snooze s on s.work_id=w.id and s.candidate_id=w.candidate_id left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind<>'CASE' and r.sealed_at_utc is not null and w.current_revision_id is null and w.applied_revision_id=r.id and v_sort='TIMESHEET_DESC' and (v_key is null or (r.week_ending_date < (v_key->>'sort_value')::date or (r.week_ending_date is not distinct from (v_key->>'sort_value')::date and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))
 order by r.week_ending_date DESC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,lower(r.candidate_display_name) collate "C" as sort_value from private.bpay_next_work_revision r join private.bpay_next_work w on w.id=r.work_id join private.bpay_next_snooze s on s.work_id=w.id and s.candidate_id=w.candidate_id left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind<>'CASE' and r.sealed_at_utc is not null and w.current_revision_id is null and w.applied_revision_id is null and s.expected_revision_id=r.id and v_sort='CANDIDATE_ASC' and (v_key is null or left(lower(r.candidate_display_name),512) collate "C" >= left((v_key->>'sort_value'),512) collate "C") and (v_key is null or (lower(r.candidate_display_name) collate "C" > (v_key->>'sort_value') collate "C" or (lower(r.candidate_display_name) collate "C" is not distinct from (v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))
 order by left(lower(r.candidate_display_name),512) collate "C" ASC,lower(r.candidate_display_name) collate "C" ASC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,r.week_ending_date::text as sort_value from private.bpay_next_work_revision r join private.bpay_next_work w on w.id=r.work_id join private.bpay_next_snooze s on s.work_id=w.id and s.candidate_id=w.candidate_id left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind<>'CASE' and r.sealed_at_utc is not null and w.current_revision_id is null and w.applied_revision_id is null and s.expected_revision_id=r.id and v_sort='TIMESHEET_ASC' and (v_key is null or (r.week_ending_date > (v_key->>'sort_value')::date or (r.week_ending_date is not distinct from (v_key->>'sort_value')::date and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))
 order by r.week_ending_date ASC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,lower(r.candidate_display_name) collate "C" as sort_value from private.bpay_next_work_revision r join private.bpay_next_work w on w.id=r.work_id join private.bpay_next_snooze s on s.work_id=w.id and s.candidate_id=w.candidate_id left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind<>'CASE' and r.sealed_at_utc is not null and w.current_revision_id is null and w.applied_revision_id is null and s.expected_revision_id=r.id and v_sort='CANDIDATE_DESC' and (v_key is null or left(lower(r.candidate_display_name),512) collate "C" <= left((v_key->>'sort_value'),512) collate "C") and (v_key is null or (lower(r.candidate_display_name) collate "C" < (v_key->>'sort_value') collate "C" or (lower(r.candidate_display_name) collate "C" is not distinct from (v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))
 order by left(lower(r.candidate_display_name),512) collate "C" DESC,lower(r.candidate_display_name) collate "C" DESC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,r.week_ending_date::text as sort_value from private.bpay_next_work_revision r join private.bpay_next_work w on w.id=r.work_id join private.bpay_next_snooze s on s.work_id=w.id and s.candidate_id=w.candidate_id left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind<>'CASE' and r.sealed_at_utc is not null and w.current_revision_id is null and w.applied_revision_id is null and s.expected_revision_id=r.id and v_sort='TIMESHEET_DESC' and (v_key is null or (r.week_ending_date < (v_key->>'sort_value')::date or (r.week_ending_date is not distinct from (v_key->>'sort_value')::date and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))
 order by r.week_ending_date DESC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,k.candidate_sort_name collate "C" as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind='CASE' and k.candidate_id=s.candidate_id and v_sort='CANDIDATE_ASC' and (v_key is null or (k.candidate_sort_name collate "C" > (v_key->>'sort_value') collate "C" or (k.candidate_sort_name collate "C" is not distinct from (v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))
 order by left(k.candidate_sort_name,512) collate "C" ASC,k.candidate_sort_name collate "C" ASC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,k.candidate_sort_name collate "C" as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind='CASE' and k.candidate_id=s.candidate_id and v_sort='CANDIDATE_DESC' and (v_key is null or (k.candidate_sort_name collate "C" < (v_key->>'sort_value') collate "C" or (k.candidate_sort_name collate "C" is not distinct from (v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))))
 order by left(k.candidate_sort_name,512) collate "C" DESC,k.candidate_sort_name collate "C" DESC,s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,null::text as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and s.active=true and p_args->>'view' in ('ALL','SNOOZED') and s.scope_kind='CASE' and v_sort in ('TIMESHEET_ASC','TIMESHEET_DESC') and (v_key is null or v_key->'sort_value'<>'null'::jsonb or (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))
 order by s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,null::text as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and s.active=false and p_args->>'view' in ('ALL','HISTORY') and s.scope_kind='CASE' and v_sort in ('TIMESHEET_ASC','TIMESHEET_DESC') and (v_key is null or v_key->'sort_value'<>'null'::jsonb or (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid)))
 order by s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,'Whole Timesheet'::text collate "C" as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind='WHOLE_TIMESHEET' and s.case_kind is null and v_sort in ('WHAT_ASC','WHAT_DESC') and (v_key is null or v_key->'sort_value'='null'::jsonb and false or (v_sort='WHAT_ASC' and 'Whole Timesheet'::text collate "C">(v_key->>'sort_value') collate "C") or (v_sort='WHAT_DESC' and 'Whole Timesheet'::text collate "C"<(v_key->>'sort_value') collate "C") or ('Whole Timesheet'::text collate "C"=(v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid))))
 order by s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,'Shift'::text collate "C" as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind='SEGMENT' and s.case_kind is null and v_sort in ('WHAT_ASC','WHAT_DESC') and (v_key is null or v_key->'sort_value'='null'::jsonb and false or (v_sort='WHAT_ASC' and 'Shift'::text collate "C">(v_key->>'sort_value') collate "C") or (v_sort='WHAT_DESC' and 'Shift'::text collate "C"<(v_key->>'sort_value') collate "C") or ('Shift'::text collate "C"=(v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid))))
 order by s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,'All expenses and mileage'::text collate "C" as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind='EXPENSES_ALL' and s.case_kind is null and v_sort in ('WHAT_ASC','WHAT_DESC') and (v_key is null or v_key->'sort_value'='null'::jsonb and false or (v_sort='WHAT_ASC' and 'All expenses and mileage'::text collate "C">(v_key->>'sort_value') collate "C") or (v_sort='WHAT_DESC' and 'All expenses and mileage'::text collate "C"<(v_key->>'sort_value') collate "C") or ('All expenses and mileage'::text collate "C"=(v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid))))
 order by s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,'Mileage'::text collate "C" as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id left join private.bpay_next_work_revision r on r.id=coalesce(w.current_revision_id,w.applied_revision_id,s.expected_revision_id) and r.work_id=s.work_id left join private.bpay_next_approved_line a on a.id=coalesce((select x.id from private.bpay_next_approved_line x where x.revision_id=r.id and x.component_key=s.component_key),s.expense_approved_line_id)
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind='EXPENSE_LINE' and s.case_kind is null and (case when a.component_kind='MILEAGE' then 'Mileage' when a.component_key like '%TRAVEL%' then 'Travel' when a.component_key like '%ACCOMMODATION%' then 'Accommodation' else 'Expense' end)='Mileage' and v_sort in ('WHAT_ASC','WHAT_DESC') and (v_key is null or v_key->'sort_value'='null'::jsonb and false or (v_sort='WHAT_ASC' and 'Mileage'::text collate "C">(v_key->>'sort_value') collate "C") or (v_sort='WHAT_DESC' and 'Mileage'::text collate "C"<(v_key->>'sort_value') collate "C") or ('Mileage'::text collate "C"=(v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid))))
 order by s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,'Travel'::text collate "C" as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id left join private.bpay_next_work_revision r on r.id=coalesce(w.current_revision_id,w.applied_revision_id,s.expected_revision_id) and r.work_id=s.work_id left join private.bpay_next_approved_line a on a.id=coalesce((select x.id from private.bpay_next_approved_line x where x.revision_id=r.id and x.component_key=s.component_key),s.expense_approved_line_id)
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind='EXPENSE_LINE' and s.case_kind is null and (case when a.component_kind='MILEAGE' then 'Mileage' when a.component_key like '%TRAVEL%' then 'Travel' when a.component_key like '%ACCOMMODATION%' then 'Accommodation' else 'Expense' end)='Travel' and v_sort in ('WHAT_ASC','WHAT_DESC') and (v_key is null or v_key->'sort_value'='null'::jsonb and false or (v_sort='WHAT_ASC' and 'Travel'::text collate "C">(v_key->>'sort_value') collate "C") or (v_sort='WHAT_DESC' and 'Travel'::text collate "C"<(v_key->>'sort_value') collate "C") or ('Travel'::text collate "C"=(v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid))))
 order by s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,'Accommodation'::text collate "C" as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id left join private.bpay_next_work_revision r on r.id=coalesce(w.current_revision_id,w.applied_revision_id,s.expected_revision_id) and r.work_id=s.work_id left join private.bpay_next_approved_line a on a.id=coalesce((select x.id from private.bpay_next_approved_line x where x.revision_id=r.id and x.component_key=s.component_key),s.expense_approved_line_id)
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind='EXPENSE_LINE' and s.case_kind is null and (case when a.component_kind='MILEAGE' then 'Mileage' when a.component_key like '%TRAVEL%' then 'Travel' when a.component_key like '%ACCOMMODATION%' then 'Accommodation' else 'Expense' end)='Accommodation' and v_sort in ('WHAT_ASC','WHAT_DESC') and (v_key is null or v_key->'sort_value'='null'::jsonb and false or (v_sort='WHAT_ASC' and 'Accommodation'::text collate "C">(v_key->>'sort_value') collate "C") or (v_sort='WHAT_DESC' and 'Accommodation'::text collate "C"<(v_key->>'sort_value') collate "C") or ('Accommodation'::text collate "C"=(v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid))))
 order by s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,'Expense'::text collate "C" as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id left join private.bpay_next_work_revision r on r.id=coalesce(w.current_revision_id,w.applied_revision_id,s.expected_revision_id) and r.work_id=s.work_id left join private.bpay_next_approved_line a on a.id=coalesce((select x.id from private.bpay_next_approved_line x where x.revision_id=r.id and x.component_key=s.component_key),s.expense_approved_line_id)
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind='EXPENSE_LINE' and s.case_kind is null and (case when a.component_kind='MILEAGE' then 'Mileage' when a.component_key like '%TRAVEL%' then 'Travel' when a.component_key like '%ACCOMMODATION%' then 'Accommodation' else 'Expense' end)='Expense' and v_sort in ('WHAT_ASC','WHAT_DESC') and (v_key is null or v_key->'sort_value'='null'::jsonb and false or (v_sort='WHAT_ASC' and 'Expense'::text collate "C">(v_key->>'sort_value') collate "C") or (v_sort='WHAT_DESC' and 'Expense'::text collate "C"<(v_key->>'sort_value') collate "C") or ('Expense'::text collate "C"=(v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid))))
 order by s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,'Payment Advance repayment'::text collate "C" as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind='CASE' and s.case_kind ='LOAN' and v_sort in ('WHAT_ASC','WHAT_DESC') and (v_key is null or v_key->'sort_value'='null'::jsonb and false or (v_sort='WHAT_ASC' and 'Payment Advance repayment'::text collate "C">(v_key->>'sort_value') collate "C") or (v_sort='WHAT_DESC' and 'Payment Advance repayment'::text collate "C"<(v_key->>'sort_value') collate "C") or ('Payment Advance repayment'::text collate "C"=(v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid))))
 order by s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,'Payment Advance repayment'::text collate "C" as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind='CASE' and s.case_kind ='ADVANCE' and v_sort in ('WHAT_ASC','WHAT_DESC') and (v_key is null or v_key->'sort_value'='null'::jsonb and false or (v_sort='WHAT_ASC' and 'Payment Advance repayment'::text collate "C">(v_key->>'sort_value') collate "C") or (v_sort='WHAT_DESC' and 'Payment Advance repayment'::text collate "C"<(v_key->>'sort_value') collate "C") or ('Payment Advance repayment'::text collate "C"=(v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid))))
 order by s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,'Manual Debt recovery'::text collate "C" as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind='CASE' and s.case_kind ='MANUAL_DEBT' and v_sort in ('WHAT_ASC','WHAT_DESC') and (v_key is null or v_key->'sort_value'='null'::jsonb and false or (v_sort='WHAT_ASC' and 'Manual Debt recovery'::text collate "C">(v_key->>'sort_value') collate "C") or (v_sort='WHAT_DESC' and 'Manual Debt recovery'::text collate "C"<(v_key->>'sort_value') collate "C") or ('Manual Debt recovery'::text collate "C"=(v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid))))
 order by s.created_at_utc desc,s.id limit v_limit+1)
 union all
(select s.id,s.created_at_utc,s.scope_kind,'Overpayment recovery'::text collate "C" as sort_value from private.bpay_next_snooze s
 left join private.bpay_next_work w on w.id=s.work_id
 left join private.bpay_next_case_list_key k on k.case_id=s.case_id
 where (p_args->'candidate_id'='null'::jsonb or s.candidate_id=(p_args->>'candidate_id')::uuid)
 and (p_args->'client_id'='null'::jsonb or coalesce(w.client_id,k.client_id)=(p_args->>'client_id')::uuid)
 and (p_args->>'scope_kind'='ALL' or (p_args->>'scope_kind'='CASE')=(s.scope_kind='CASE'))
 and (p_args->>'filter'='ACTIVE' or (p_args->>'filter'='INDEFINITE')=(s.until_date is null)) and (p_args->>'view'='ALL' or (p_args->>'view'='SNOOZED' and s.active) or (p_args->>'view'='HISTORY' and not s.active)) and s.scope_kind='CASE' and s.case_kind ='OVERPAYMENT' and v_sort in ('WHAT_ASC','WHAT_DESC') and (v_key is null or v_key->'sort_value'='null'::jsonb and false or (v_sort='WHAT_ASC' and 'Overpayment recovery'::text collate "C">(v_key->>'sort_value') collate "C") or (v_sort='WHAT_DESC' and 'Overpayment recovery'::text collate "C"<(v_key->>'sort_value') collate "C") or ('Overpayment recovery'::text collate "C"=(v_key->>'sort_value') collate "C" and (s.created_at_utc < (v_key->>'created_at_utc')::timestamptz or (s.created_at_utc=(v_key->>'created_at_utc')::timestamptz and s.id>(v_key->>'id')::uuid))))
 order by s.created_at_utc desc,s.id limit v_limit+1)
 ) q order by (q.sort_value is null),case when v_sort like '%_ASC' then q.sort_value end collate "C" asc,case when v_sort like '%_DESC' then q.sort_value end collate "C" desc,case when v_sort='CREATED_ASC' then q.created_at_utc end asc,case when v_sort<>'CREATED_ASC' then q.created_at_utc end desc,q.id limit v_limit+1
 loop
  v_seen:=v_seen+1;if v_seen>v_limit then v_complete:=false;exit;end if;
  if (p_args->>'count_only')::boolean then
   v_total:=v_total+case when v_rec.scope_kind<>'CASE' then 1 else 0 end;
   v_expenses:=v_expenses+case when v_rec.scope_kind in ('EXPENSE_LINE','EXPENSES_ALL') then 1 else 0 end;
   v_last:=jsonb_build_object('created_at_utc',to_char(v_rec.created_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'id',v_rec.id,'sort_value',v_rec.sort_value);continue;
  end if;
  v_row:=private.bpay_next_snooze_management_row_v1(v_rec.id);
  if v_row is null then raise exception using errcode='23514',message='BPAY_NEXT_WORKBENCH_CURSOR_STALE';end if;
  v_row:=v_row||jsonb_build_object('sort_key',jsonb_build_object('created_at_utc',to_char(v_rec.created_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'id',v_rec.id,'sort_value',v_rec.sort_value));
  if octet_length((v_rows||jsonb_build_array(v_row))::text)>110000 then
   if jsonb_array_length(v_rows)=0 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
   v_complete:=false;exit;end if;
  v_rows:=v_rows||jsonb_build_array(v_row);v_last:=jsonb_build_object('created_at_utc',to_char(v_rec.created_at_utc at time zone 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'id',v_rec.id,'sort_value',v_rec.sort_value);
 end loop;
 if v_last->>'sort_value' is not null then v_last:=jsonb_set(v_last,'{sort_value}',to_jsonb(replace(encode(convert_to(v_last->>'sort_value','UTF8'),'base64'),chr(10),'')));end if;
 if not v_complete and octet_length(jsonb_build_object('context',v_context,'key',v_last)::text)>16384 then raise exception using errcode='54000',message='BPAY_NEXT_WORKBENCH_RESPONSE_TOO_LARGE';end if;
 return jsonb_build_object('ok',true,'kind','SNOOZES','context',v_context,'rows',v_rows,'scanned_count',case when (p_args->>'count_only')::boolean then least(v_seen,v_limit)::text else jsonb_array_length(v_rows)::text end,
  'counts',case when (p_args->>'count_only')::boolean then jsonb_build_object('timesheet_snoozes_count',v_total::text,'timesheet_expense_snoozes_count',v_expenses::text) else null end,'complete',v_complete,
  'next_cursor',case when v_complete then null else jsonb_build_object('context',v_context,'key',v_last) end);
end
$function$;

alter function private.bpay_next_snooze_management_page_v1(uuid,jsonb) owner to postgres;
revoke all on function private.bpay_next_snooze_management_page_v1(uuid,jsonb) from public,anon,authenticated,service_role;
commit;
