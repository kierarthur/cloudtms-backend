-- FC-02 SCR114: full0058 successors; native counters for supplied Candidate Case headers, no money recomputation.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_case_list_refresh_key_v1(p_case_id uuid)
returns void language plpgsql volatile security invoker set search_path=pg_catalog,private,public
as $function$
declare v_origin jsonb;v_collection private.bpay_next_work_collection%rowtype;
begin
 select c.* into v_collection from private.bpay_next_work_collection c where c.case_id=p_case_id;
 if found then
  v_origin:=private.bpay_next_payment_client_origin_v1(v_collection.original_revision_id);
 end if;
 insert into private.bpay_next_case_list_key(case_id,candidate_id,created_at_utc,candidate_sort_name,
  candidate_display_name,candidate_reference,case_subtype,case_type_label,client_id,client_display_name,client_association,list_status,list_original,list_outstanding,list_weekly_due,list_funded,list_recovered,list_written_off)
 select c.id,c.candidate_id,r0.case_created_at_utc,
  lower(coalesce(m.candidate_sort_name,nullif(btrim(ca.display_name),''),nullif(btrim(concat_ws(' ',ca.first_name,ca.last_name)),''),ca.id::text)),
  coalesce(m.candidate_sort_name,nullif(btrim(ca.display_name),''),nullif(btrim(concat_ws(' ',ca.first_name,ca.last_name)),''),ca.id::text),
  coalesce(m.tms_ref,ca.tms_ref),r.case_subtype,
  case r.case_subtype when 'LOAN' then 'Payment Advance' when 'PAYMENT_ADVANCE' then 'Payment Advance'
   when 'OVERPAYMENT' then 'Overpayment' when 'UNDERPAYMENT' then 'Underpayment'
   when 'MANUAL_CREDIT' then 'Manual Credit' when 'MANUAL_DEBT' then 'Manual Debt' else 'Unmapped case' end,
  case when v_origin->>'classification'='CLASSIFIED' then (v_origin->>'client_id')::uuid end,
  v_origin->>'client_display_name',
  case when v_collection.id is null then 'UNASSOCIATED' when v_origin->>'classification'='CLASSIFIED' then 'FROZEN' else 'UNKNOWN' end,c.status,c.principal_approved,
  (case when c.case_kind in ('LOAN','ADVANCE') then c.principal_funded else c.principal_approved end)-c.principal_recovered-c.principal_written_off-c.principal_paid_credit,r.weekly_due_source_ex_vat,c.principal_funded,c.principal_recovered,c.principal_written_off
 from private.bpay_next_finance_case c join public.candidates ca on ca.id=c.candidate_id
 left join private.bpay_next_case_rule r0 on r0.case_id=c.id and r0.candidate_id=c.candidate_id and r0.rule_revision=1
 left join private.bpay_next_case_rule r on r.id=c.current_rule_id and r.case_id=c.id and r.candidate_id=c.candidate_id
  and r.rule_revision=c.current_rule_revision
 left join private.bpay_next_workbench_candidate_metadata m on m.candidate_id=c.candidate_id
 where c.id=p_case_id
 on conflict(case_id) do update set candidate_id=excluded.candidate_id,created_at_utc=excluded.created_at_utc,
  candidate_sort_name=excluded.candidate_sort_name,candidate_display_name=excluded.candidate_display_name,candidate_reference=excluded.candidate_reference,
  case_subtype=excluded.case_subtype,case_type_label=excluded.case_type_label,client_id=excluded.client_id,
  client_display_name=excluded.client_display_name,client_association=excluded.client_association,list_status=excluded.list_status,list_original=excluded.list_original,
  list_outstanding=excluded.list_outstanding,list_weekly_due=excluded.list_weekly_due,list_funded=excluded.list_funded,list_recovered=excluded.list_recovered,list_written_off=excluded.list_written_off;
end
$function$;

create or replace function private.bpay_next_case_list_page_v1(p_actor_user_id uuid,p_kind text,p_args jsonb)
returns jsonb language plpgsql volatile security invoker set search_path=pg_catalog,private,public
as $function$
declare
 v_filters jsonb;v_context jsonb;v_after jsonb;v_sort text;v_limit integer;v_row record;v_value jsonb;
 v_previous_last jsonb;
 v_rows jsonb:='[]'::jsonb;v_complete boolean:=true;v_next jsonb;v_last jsonb;v_scanned integer:=0;v_matched integer:=0;
 v_summary jsonb:=jsonb_build_object('payment_advances_active_count','0','overpayments_active_count','0',
  'underpayments_active_count','0','manual_debt_adjustments_active_count','0','manual_credit_adjustments_count','0',
  'mixed_finance_cases_count','0','unresolved_finance_cases_count','0','stale_finance_cases_count','0','finance_cases_with_active_snooze_count','0');
 v_category text;v_gap_count integer:=0;v_key text;
 v_after_id uuid;v_after_date timestamptz;v_after_sort text;v_bytes integer:=24576;v_result jsonb;v_cursor_last jsonb;
 v_candidate uuid;v_client uuid;v_subtype text;v_view text;v_snooze text;v_hide boolean;
begin
 if p_kind not in ('CASE_LIST','CASE_SUMMARY') or p_args is null or jsonb_typeof(p_args)<>'object'
  or octet_length(p_args::text)>32768 or exists(select 1 from jsonb_object_keys(p_args) k where k not in ('filters','after','sort','limit'))
  or (p_kind='CASE_SUMMARY' and (p_args ? 'sort' or p_args ? 'limit')) then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
 if p_args ? 'filters' and (jsonb_typeof(p_args->'filters') is distinct from 'object'
  or exists(select 1 from jsonb_object_keys(p_args->'filters') k where k not in ('candidate_id','client_id','case_subtype','view','snooze','hide_completed'))) then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
 v_filters:=jsonb_build_object('candidate_id',null,'client_id',null,'case_subtype','ALL','view','ALL','snooze','ALL','hide_completed',true)
  ||coalesce(p_args->'filters','{}'::jsonb);
 if jsonb_typeof(v_filters->'candidate_id') not in ('string','null') or jsonb_typeof(v_filters->'client_id') not in ('string','null')
  or (v_filters->>'candidate_id' is not null and v_filters->>'candidate_id' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
  or (v_filters->>'client_id' is not null and v_filters->>'client_id' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')
  or jsonb_typeof(v_filters->'case_subtype') is distinct from 'string'
  or v_filters->>'case_subtype' not in ('ALL','LOAN','PAYMENT_ADVANCE','OVERPAYMENT','UNDERPAYMENT','MANUAL_CREDIT','MANUAL_DEBT','UNMAPPED')
  or jsonb_typeof(v_filters->'view') is distinct from 'string' or v_filters->>'view' not in ('ALL','ACTIVE','SNOOZED','HISTORY')
  or jsonb_typeof(v_filters->'snooze') is distinct from 'string' or v_filters->>'snooze' not in ('ALL','FINITE','INDEFINITE')
  or jsonb_typeof(v_filters->'hide_completed') is distinct from 'boolean' then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
 v_candidate:=(v_filters->>'candidate_id')::uuid;v_client:=(v_filters->>'client_id')::uuid;
 v_subtype:=v_filters->>'case_subtype';v_view:=v_filters->>'view';v_snooze:=v_filters->>'snooze';v_hide:=(v_filters->>'hide_completed')::boolean;
 v_sort:=coalesce(p_args->>'sort','CREATED_DESC');
 if v_sort not in ('CREATED_ASC','CREATED_DESC','CANDIDATE_ASC','CANDIDATE_DESC','TYPE_ASC','TYPE_DESC','STATUS_ASC','STATUS_DESC','ORIGINAL_ASC','ORIGINAL_DESC','OUTSTANDING_ASC','OUTSTANDING_DESC','WEEKLY_ASC','WEEKLY_DESC','FUNDED_ASC','FUNDED_DESC','RECOVERED_ASC','RECOVERED_DESC','WRITTEN_OFF_ASC','WRITTEN_OFF_DESC')
  or (p_args ? 'sort' and jsonb_typeof(p_args->'sort') is distinct from 'string') then
  raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
 if p_kind='CASE_LIST' then
  if p_args ? 'limit' and jsonb_typeof(p_args->'limit') is distinct from 'number' then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
  v_limit:=coalesce((p_args->>'limit')::integer,10);
  if v_limit not in (5,10,20,50) then raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
 else v_limit:=100;end if;
 v_context:=private.bpay_next_case_list_context_v1(p_actor_user_id);
 v_after:=nullif(p_args->'after','null'::jsonb);
 if v_after is not null then
  if jsonb_typeof(v_after) is distinct from 'object' or octet_length(v_after::text)>16384
   or (v_after-array['context','filters','sort','last']) is distinct from '{}'::jsonb
   or not(v_after ?& array['context','filters','sort','last']) then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
  if v_after->'context' is distinct from v_context or v_after->'filters' is distinct from v_filters
   or v_after->>'sort' is distinct from (case when p_kind='CASE_SUMMARY' then 'CASE_ID_ASC' else v_sort end) then
   raise exception using errcode='55000',message='BPAY_NEXT_CASE_LIST_CONTEXT_CHANGED';end if;
  v_last:=v_after->'last';
  if jsonb_typeof(v_last) is distinct from 'object' or (v_last-array['case_id','created_at_utc','sort_value']) is distinct from '{}'::jsonb
   or not(v_last ?& array['case_id','created_at_utc','sort_value']) or jsonb_typeof(v_last->'case_id') is distinct from 'string'
   or v_last->>'case_id' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
   or jsonb_typeof(v_last->'created_at_utc') not in ('string','null') or jsonb_typeof(v_last->'sort_value') not in ('string','null') then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
  v_after_id:=(v_last->>'case_id')::uuid;v_after_date:=(v_last->>'created_at_utc')::timestamptz;
  if v_last->>'sort_value' is not null then
   v_after_sort:=convert_from(decode(v_last->>'sort_value','base64'),'UTF8');
   if replace(encode(convert_to(v_after_sort,'UTF8'),'base64'),chr(10),'') is distinct from v_last->>'sort_value' then
    raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
   v_last:=jsonb_set(v_last,'{sort_value}',to_jsonb(v_after_sort));
  end if;
  if v_after_date is not null and not pg_catalog.isfinite(v_after_date) then raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
  if (p_kind='CASE_SUMMARY' and (v_after_date is not null or v_after_sort is not null))
   or (p_kind='CASE_LIST' and ((v_sort like 'CREATED_%' and v_after_sort is not null)
     or (v_sort not like 'CREATED_%' and (v_after_sort is null or octet_length(v_after_sort)>8192)))) then
   raise exception using errcode='22023',message='BPAY_NEXT_CASE_LIST_REQUEST_INVALID';end if;
 end if;
 -- Fifteen static, independently limited native index paths. Selection filters
 -- are applied after the raw window; no caller SQL or dynamic identifier.
 for v_row in select page.* from (
 (select k.*,null::text as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_SUMMARY' and (v_after_id is null or k.case_id>v_after_id)
  order by k.case_id asc limit v_limit+1)
 union all
 (select k.*,null::text as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='CREATED_ASC' and (v_after_id is null or ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc > v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id)))))
  order by (k.created_at_utc is null) asc,k.created_at_utc asc nulls last,k.case_id asc limit v_limit+1)
 union all
 (select k.*,null::text as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='CREATED_DESC' and (v_after_id is null or ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id)))))
  order by (k.created_at_utc is null) asc,k.created_at_utc desc nulls last,k.case_id asc limit v_limit+1)
 union all
 (select k.*,k.candidate_sort_name as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='CANDIDATE_ASC' and (v_after_id is null or (k.candidate_sort_name> v_after_sort collate "C" or (k.candidate_sort_name=v_after_sort collate "C" and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id)))))))
  order by left(k.candidate_sort_name,512) collate "C" asc,k.candidate_sort_name asc, (k.created_at_utc is null) asc,k.created_at_utc desc nulls last,k.case_id asc limit v_limit+1)
 union all
 (select k.*,k.candidate_sort_name as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='CANDIDATE_DESC' and (v_after_id is null or (k.candidate_sort_name< v_after_sort collate "C" or (k.candidate_sort_name=v_after_sort collate "C" and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id)))))))
  order by left(k.candidate_sort_name,512) collate "C" desc,k.candidate_sort_name desc, (k.created_at_utc is null) asc,k.created_at_utc desc nulls last,k.case_id asc limit v_limit+1)
 union all
 (select k.*,k.case_type_label as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='TYPE_ASC' and (v_after_id is null or (k.case_type_label> v_after_sort collate "C" or (k.case_type_label=v_after_sort collate "C" and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id)))))))
  order by k.case_type_label asc, (k.created_at_utc is null) asc,k.created_at_utc desc nulls last,k.case_id asc limit v_limit+1)
 union all
 (select k.*,k.case_type_label as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='TYPE_DESC' and (v_after_id is null or (k.case_type_label< v_after_sort collate "C" or (k.case_type_label=v_after_sort collate "C" and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id)))))))
  order by k.case_type_label desc, (k.created_at_utc is null) asc,k.created_at_utc desc nulls last,k.case_id asc limit v_limit+1)

 union all
 (select k.*,coalesce(k.list_status::text,'~') as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='STATUS_ASC' and (v_after_id is null or
   (k.list_status is null)::integer>(nullif(v_after_sort,'~') is null)::integer or ((k.list_status is null)=(nullif(v_after_sort,'~') is null) and
    (k.list_status > nullif(v_after_sort,'~') or (k.list_status is not distinct from nullif(v_after_sort,'~') and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id))))))))
  order by (k.list_status is null),k.list_status ASC nulls last,(k.created_at_utc is null),k.created_at_utc desc nulls last,k.case_id limit v_limit+1)

 union all
 (select k.*,coalesce(k.list_status::text,'~') as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='STATUS_DESC' and (v_after_id is null or
   (k.list_status is null)::integer>(nullif(v_after_sort,'~') is null)::integer or ((k.list_status is null)=(nullif(v_after_sort,'~') is null) and
    (k.list_status < nullif(v_after_sort,'~') or (k.list_status is not distinct from nullif(v_after_sort,'~') and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id))))))))
  order by (k.list_status is null),k.list_status DESC nulls last,(k.created_at_utc is null),k.created_at_utc desc nulls last,k.case_id limit v_limit+1)

 union all
 (select k.*,coalesce(k.list_original::text,'~') as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='ORIGINAL_ASC' and (v_after_id is null or
   (k.list_original is null)::integer>(nullif(v_after_sort,'~')::numeric is null)::integer or ((k.list_original is null)=(nullif(v_after_sort,'~')::numeric is null) and
    (k.list_original > nullif(v_after_sort,'~')::numeric or (k.list_original is not distinct from nullif(v_after_sort,'~')::numeric and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id))))))))
  order by (k.list_original is null),k.list_original ASC nulls last,(k.created_at_utc is null),k.created_at_utc desc nulls last,k.case_id limit v_limit+1)

 union all
 (select k.*,coalesce(k.list_original::text,'~') as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='ORIGINAL_DESC' and (v_after_id is null or
   (k.list_original is null)::integer>(nullif(v_after_sort,'~')::numeric is null)::integer or ((k.list_original is null)=(nullif(v_after_sort,'~')::numeric is null) and
    (k.list_original < nullif(v_after_sort,'~')::numeric or (k.list_original is not distinct from nullif(v_after_sort,'~')::numeric and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id))))))))
  order by (k.list_original is null),k.list_original DESC nulls last,(k.created_at_utc is null),k.created_at_utc desc nulls last,k.case_id limit v_limit+1)

 union all
 (select k.*,coalesce(k.list_outstanding::text,'~') as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='OUTSTANDING_ASC' and (v_after_id is null or
   (k.list_outstanding is null)::integer>(nullif(v_after_sort,'~')::numeric is null)::integer or ((k.list_outstanding is null)=(nullif(v_after_sort,'~')::numeric is null) and
    (k.list_outstanding > nullif(v_after_sort,'~')::numeric or (k.list_outstanding is not distinct from nullif(v_after_sort,'~')::numeric and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id))))))))
  order by (k.list_outstanding is null),k.list_outstanding ASC nulls last,(k.created_at_utc is null),k.created_at_utc desc nulls last,k.case_id limit v_limit+1)

 union all
 (select k.*,coalesce(k.list_outstanding::text,'~') as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='OUTSTANDING_DESC' and (v_after_id is null or
   (k.list_outstanding is null)::integer>(nullif(v_after_sort,'~')::numeric is null)::integer or ((k.list_outstanding is null)=(nullif(v_after_sort,'~')::numeric is null) and
    (k.list_outstanding < nullif(v_after_sort,'~')::numeric or (k.list_outstanding is not distinct from nullif(v_after_sort,'~')::numeric and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id))))))))
  order by (k.list_outstanding is null),k.list_outstanding DESC nulls last,(k.created_at_utc is null),k.created_at_utc desc nulls last,k.case_id limit v_limit+1)

 union all
 (select k.*,coalesce(k.list_weekly_due::text,'~') as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='WEEKLY_ASC' and (v_after_id is null or
   (k.list_weekly_due is null)::integer>(nullif(v_after_sort,'~')::numeric is null)::integer or ((k.list_weekly_due is null)=(nullif(v_after_sort,'~')::numeric is null) and
    (k.list_weekly_due > nullif(v_after_sort,'~')::numeric or (k.list_weekly_due is not distinct from nullif(v_after_sort,'~')::numeric and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id))))))))
  order by (k.list_weekly_due is null),k.list_weekly_due ASC nulls last,(k.created_at_utc is null),k.created_at_utc desc nulls last,k.case_id limit v_limit+1)

 union all
 (select k.*,coalesce(k.list_weekly_due::text,'~') as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='WEEKLY_DESC' and (v_after_id is null or
   (k.list_weekly_due is null)::integer>(nullif(v_after_sort,'~')::numeric is null)::integer or ((k.list_weekly_due is null)=(nullif(v_after_sort,'~')::numeric is null) and
    (k.list_weekly_due < nullif(v_after_sort,'~')::numeric or (k.list_weekly_due is not distinct from nullif(v_after_sort,'~')::numeric and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id))))))))
  order by (k.list_weekly_due is null),k.list_weekly_due DESC nulls last,(k.created_at_utc is null),k.created_at_utc desc nulls last,k.case_id limit v_limit+1)

 union all
 (select k.*,coalesce(k.list_funded::text,'~') as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='FUNDED_ASC' and (v_after_id is null or
   (k.list_funded is null)::integer>(nullif(v_after_sort,'~')::numeric is null)::integer or ((k.list_funded is null)=(nullif(v_after_sort,'~')::numeric is null) and
    (k.list_funded > nullif(v_after_sort,'~')::numeric or (k.list_funded is not distinct from nullif(v_after_sort,'~')::numeric and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id))))))))
  order by (k.list_funded is null),k.list_funded ASC nulls last,(k.created_at_utc is null),k.created_at_utc desc nulls last,k.case_id limit v_limit+1)

 union all
 (select k.*,coalesce(k.list_funded::text,'~') as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='FUNDED_DESC' and (v_after_id is null or
   (k.list_funded is null)::integer>(nullif(v_after_sort,'~')::numeric is null)::integer or ((k.list_funded is null)=(nullif(v_after_sort,'~')::numeric is null) and
    (k.list_funded < nullif(v_after_sort,'~')::numeric or (k.list_funded is not distinct from nullif(v_after_sort,'~')::numeric and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id))))))))
  order by (k.list_funded is null),k.list_funded DESC nulls last,(k.created_at_utc is null),k.created_at_utc desc nulls last,k.case_id limit v_limit+1)

 union all
 (select k.*,coalesce(k.list_recovered::text,'~') as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='RECOVERED_ASC' and (v_after_id is null or
   (k.list_recovered is null)::integer>(nullif(v_after_sort,'~')::numeric is null)::integer or ((k.list_recovered is null)=(nullif(v_after_sort,'~')::numeric is null) and
    (k.list_recovered > nullif(v_after_sort,'~')::numeric or (k.list_recovered is not distinct from nullif(v_after_sort,'~')::numeric and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id))))))))
  order by (k.list_recovered is null),k.list_recovered ASC nulls last,(k.created_at_utc is null),k.created_at_utc desc nulls last,k.case_id limit v_limit+1)

 union all
 (select k.*,coalesce(k.list_recovered::text,'~') as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='RECOVERED_DESC' and (v_after_id is null or
   (k.list_recovered is null)::integer>(nullif(v_after_sort,'~')::numeric is null)::integer or ((k.list_recovered is null)=(nullif(v_after_sort,'~')::numeric is null) and
    (k.list_recovered < nullif(v_after_sort,'~')::numeric or (k.list_recovered is not distinct from nullif(v_after_sort,'~')::numeric and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id))))))))
  order by (k.list_recovered is null),k.list_recovered DESC nulls last,(k.created_at_utc is null),k.created_at_utc desc nulls last,k.case_id limit v_limit+1)

 union all
 (select k.*,coalesce(k.list_written_off::text,'~') as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='WRITTEN_OFF_ASC' and (v_after_id is null or
   (k.list_written_off is null)::integer>(nullif(v_after_sort,'~')::numeric is null)::integer or ((k.list_written_off is null)=(nullif(v_after_sort,'~')::numeric is null) and
    (k.list_written_off > nullif(v_after_sort,'~')::numeric or (k.list_written_off is not distinct from nullif(v_after_sort,'~')::numeric and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id))))))))
  order by (k.list_written_off is null),k.list_written_off ASC nulls last,(k.created_at_utc is null),k.created_at_utc desc nulls last,k.case_id limit v_limit+1)

 union all
 (select k.*,coalesce(k.list_written_off::text,'~') as sort_value from private.bpay_next_case_list_key k
  where p_kind='CASE_LIST' and v_sort='WRITTEN_OFF_DESC' and (v_after_id is null or
   (k.list_written_off is null)::integer>(nullif(v_after_sort,'~')::numeric is null)::integer or ((k.list_written_off is null)=(nullif(v_after_sort,'~')::numeric is null) and
    (k.list_written_off < nullif(v_after_sort,'~')::numeric or (k.list_written_off is not distinct from nullif(v_after_sort,'~')::numeric and ((k.created_at_utc is null)::integer>(v_after_date is null)::integer or ((k.created_at_utc is null)=(v_after_date is null) and (k.created_at_utc < v_after_date or (k.created_at_utc is not distinct from v_after_date and k.case_id>v_after_id))))))))
  order by (k.list_written_off is null),k.list_written_off DESC nulls last,(k.created_at_utc is null),k.created_at_utc desc nulls last,k.case_id limit v_limit+1)
 ) page order by
 case when p_kind='CASE_SUMMARY' then page.case_id end asc,
 case when v_sort in ('CANDIDATE_ASC','TYPE_ASC','STATUS_ASC') then page.sort_value end collate "C" asc,
 case when v_sort in ('CANDIDATE_DESC','TYPE_DESC','STATUS_DESC') then page.sort_value end collate "C" desc,
 case when v_sort like 'ORIGINAL_%' or v_sort like 'OUTSTANDING_%' or v_sort like 'WEEKLY_%' or v_sort like 'FUNDED_%' or v_sort like 'RECOVERED_%' or v_sort like 'WRITTEN_OFF_%' then page.sort_value='~' else false end asc,
 case when v_sort in ('ORIGINAL_ASC','OUTSTANDING_ASC','WEEKLY_ASC','FUNDED_ASC','RECOVERED_ASC','WRITTEN_OFF_ASC') then nullif(page.sort_value,'~')::numeric end asc,
 case when v_sort in ('ORIGINAL_DESC','OUTSTANDING_DESC','WEEKLY_DESC','FUNDED_DESC','RECOVERED_DESC','WRITTEN_OFF_DESC') then nullif(page.sort_value,'~')::numeric end desc,
 (page.created_at_utc is null) asc,
 case when v_sort='CREATED_ASC' then page.created_at_utc end asc nulls last,
 case when v_sort<>'CREATED_ASC' then page.created_at_utc end desc nulls last,page.case_id asc
 loop
  if v_scanned=v_limit then v_complete:=false;exit;end if;
  v_scanned:=v_scanned+1;v_previous_last:=v_last;
  v_last:=jsonb_build_object('case_id',v_row.case_id,'created_at_utc',case when p_kind='CASE_SUMMARY' then null else v_row.created_at_utc end,
   'sort_value',case when p_kind='CASE_SUMMARY' then null else v_row.sort_value end);
  if (v_candidate is not null and v_row.candidate_id<>v_candidate)
   or (v_client is not null and v_row.client_id is distinct from v_client)
   or (v_subtype<>'ALL' and not(
    (v_subtype='UNMAPPED' and (v_row.case_subtype is null or v_row.case_subtype not in ('LOAN','PAYMENT_ADVANCE','OVERPAYMENT','UNDERPAYMENT','MANUAL_CREDIT','MANUAL_DEBT')))
    or (v_subtype='PAYMENT_ADVANCE' and v_row.case_subtype in ('LOAN','PAYMENT_ADVANCE'))
    or v_row.case_subtype=v_subtype)) then continue;end if;
  v_value:=private.bpay_next_case_list_row_v1(v_row.case_id,v_context);
  if p_kind='CASE_LIST' then
   v_value:=v_value||jsonb_build_object('sort_key',v_last);
   if octet_length(v_value::text)>90000 then raise exception using errcode='54000',message='BPAY_NEXT_CASE_LIST_ROW_TOO_LARGE';end if;
  end if;
  if (v_hide and v_value->>'status' in ('CLOSED','WRITTEN_OFF'))
   or (v_view='ACTIVE' and (v_value->>'status' not in ('OPEN','PAUSED') or v_value->'has_active_snooze' is distinct from 'false'::jsonb))
   or (v_view='HISTORY' and v_value->>'status' not in ('CLOSED','WRITTEN_OFF'))
   or (v_view='SNOOZED' and v_value->'has_active_snooze' is distinct from 'true'::jsonb)
   or (v_snooze='FINITE' and coalesce((v_value->>'active_snooze_count')::integer<=(v_value->>'indefinite_snooze_count')::integer,true))
   or (v_snooze='INDEFINITE' and coalesce((v_value->>'indefinite_snooze_count')::integer=0,true)) then continue;end if;
  if p_kind='CASE_LIST' then
   if v_bytes+octet_length(v_value::text)>120000 then
    if v_matched=0 then raise exception using errcode='54000',message='BPAY_NEXT_CASE_LIST_ROW_TOO_LARGE';end if;
    -- Reissue the current raw member rather than silently skip its output.
    v_last:=v_previous_last;v_scanned:=v_scanned-1;v_complete:=false;exit;
   end if;
   v_rows:=v_rows||jsonb_build_array(v_value);v_bytes:=v_bytes+octet_length(v_value::text);
  else
   v_category:=case v_value->>'case_subtype' when 'LOAN' then 'payment_advances_active_count' when 'PAYMENT_ADVANCE' then 'payment_advances_active_count'
    when 'OVERPAYMENT' then 'overpayments_active_count' when 'UNDERPAYMENT' then 'underpayments_active_count'
    when 'MANUAL_DEBT' then 'manual_debt_adjustments_active_count' when 'MANUAL_CREDIT' then 'manual_credit_adjustments_count' end;
   if v_category is not null then v_summary:=jsonb_set(v_summary,array[v_category],to_jsonb(((v_summary->>v_category)::integer+1)::text));end if;
   foreach v_category in array array['mixed_finance_cases_count','unresolved_finance_cases_count','stale_finance_cases_count','finance_cases_with_active_snooze_count']
   loop
    v_key:=case v_category when 'mixed_finance_cases_count' then 'is_mixed_case' when 'unresolved_finance_cases_count' then 'unresolved_taxable'
     when 'stale_finance_cases_count' then 'resolution_stale' else 'has_active_snooze' end;
    if v_value->v_key='true'::jsonb and v_summary->v_category<>'null'::jsonb then
     v_summary:=jsonb_set(v_summary,array[v_category],to_jsonb(((v_summary->>v_category)::integer+1)::text));
    elsif v_value->v_key='null'::jsonb then v_summary:=jsonb_set(v_summary,array[v_category],'null'::jsonb);end if;
   end loop;
   if v_value->>'gap_code' is not null then v_gap_count:=v_gap_count+1;end if;
  end if;
  v_matched:=v_matched+1;v_next:=v_last;
 end loop;
 if private.bpay_next_case_list_context_v1(p_actor_user_id) is distinct from v_context then
  raise exception using errcode='55000',message='BPAY_NEXT_CASE_LIST_CONTEXT_CHANGED';end if;
 v_cursor_last:=case when v_last->>'sort_value' is null then v_last else jsonb_set(v_last,'{sort_value}',
  to_jsonb(replace(encode(convert_to(v_last->>'sort_value','UTF8'),'base64'),chr(10),''))) end;
 v_next:=case when v_complete then null else jsonb_build_object('context',v_context,'filters',v_filters,
  'sort',case when p_kind='CASE_SUMMARY' then 'CASE_ID_ASC' else v_sort end,'last',v_cursor_last) end;
 if v_next is not null and octet_length(v_next::text)>16384 then
  raise exception using errcode='54000',message='BPAY_NEXT_CASE_LIST_RESPONSE_TOO_LARGE';end if;
 v_result:=case when p_kind='CASE_LIST' then jsonb_build_object('context',v_context,'filters',v_filters,'sort',v_sort,
  'rows',v_rows,'scanned_count',v_scanned::text,'complete',v_complete,'next_cursor',v_next)
 else jsonb_build_object('context',v_context,'filters',v_filters,'counts',v_summary,'scanned_count',v_scanned::text,
  'matched_count',v_matched::text,'gap_case_count',v_gap_count::text,'complete',v_complete,'next_cursor',v_next) end;
 if octet_length(v_result::text)>119936 then
  raise exception using errcode='54000',message='BPAY_NEXT_CASE_LIST_RESPONSE_TOO_LARGE';end if;
 return v_result;
end
$function$;

alter function private.bpay_next_case_list_refresh_key_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_case_list_refresh_key_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_case_list_page_v1(uuid,text,jsonb) owner to postgres;
revoke all on function private.bpay_next_case_list_page_v1(uuid,text,jsonb) from public,anon,authenticated,service_role;
commit;
