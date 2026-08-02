-- Contract-aware, consolidated query email workflow using the existing mail_outbox.

create or replace function public._timesheet_query_recipient_resolve_core_v1(p_client_id uuid,p_contract_id uuid default null)
returns jsonb language plpgsql stable security definer set search_path to 'public','extensions','pg_temp' as $function$
declare c public.clients%rowtype; k public.contracts%rowtype; v_scope text; v_key text; v_email text; v_hash text;
begin
  if p_client_id is null then raise exception 'TIMESHEET_QUERY_CLIENT_REQUIRED' using errcode='22023'; end if;
  select * into c from public.clients where id=p_client_id; if not found then raise exception 'TIMESHEET_QUERY_CLIENT_NOT_FOUND' using errcode='P0002'; end if;
  if p_contract_id is not null then select * into k from public.contracts where id=p_contract_id;
    if not found or k.client_id<>p_client_id then raise exception 'TIMESHEET_QUERY_CONTRACT_CLIENT_MISMATCH' using errcode='22023'; end if; end if;
  if p_contract_id is not null and coalesce(k.send_ts_queries_to_different_email,false) then
    v_scope:='CONTRACT_OVERRIDE'; v_key:='CONTRACT_OVERRIDE:'||k.id::text; v_email:=lower(btrim(coalesce(k.ts_queries_alt_email_address,'')));
    if length(v_email) not between 3 and 320 or v_email!~* '^[A-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?(?:\.[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?)+$' then
      raise exception 'TIMESHEET_QUERY_CONTRACT_OVERRIDE_INVALID' using errcode='22023'; end if;
    v_hash:=public._import_review_hash_v1(concat_ws('|','query-route-v1',v_key,v_email,k.updated_at));
  else
    v_scope:='CLIENT_DEFAULT'; v_key:='CLIENT_DEFAULT:'||c.id::text; v_email:=lower(btrim(coalesce(c.ts_queries_email,'')));
    if length(v_email) not between 3 and 320 or v_email!~* '^[A-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?(?:\.[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?)+$' then
      raise exception 'TIMESHEET_QUERY_CLIENT_EMAIL_INVALID' using errcode='22023'; end if;
    v_hash:=public._import_review_hash_v1(concat_ws('|','query-route-v1',v_key,v_email,c.rev,c.updated_at));
  end if;
  return jsonb_build_object('client_id',p_client_id,'contract_id',p_contract_id,'recipient_scope',v_scope,
    'recipient_scope_key',v_key,'recipient_email',v_email,'route_fingerprint',v_hash);
end $function$;

create or replace function public.timesheet_query_recipient_resolve_v1(p_client_id uuid,p_contract_id uuid default null)
returns jsonb language plpgsql stable security definer set search_path to 'public','pg_temp' as $function$
begin return public._timesheet_query_recipient_resolve_core_v1(p_client_id,p_contract_id); end $function$;

create or replace function public._import_review_html_escape_v1(p_value text)
returns text language sql immutable security definer set search_path to 'public','pg_temp' as $function$
  select replace(replace(replace(replace(replace(coalesce(p_value,''),'&','&amp;'),'<','&lt;'),'>','&gt;'),'"','&quot;'),'''','&#39;')
$function$;

create or replace function public.timesheet_query_email_enqueue_v1(
  p_import_id uuid,p_operation_id uuid,p_selected_action_ids jsonb,p_actor_user_id uuid default null,
  p_max_actions integer default 5000,p_max_groups integer default 100
)
returns jsonb language plpgsql security definer set search_path to 'public','extensions','pg_temp' as $function$
declare
  v_state public.import_review_states%rowtype; v_operation public.import_apply_operations%rowtype;
  v_ids text[]; v_db_ids text[]; v_group record; v_route jsonb;
  v_issue_ids uuid[]; v_issue_set_hash text; v_outbox_key text; v_delivery_id uuid; v_outbox_id uuid;
  v_html text; v_text text; v_subject text; v_results jsonb:='[]'; v_group_count integer:=0; v_reminder integer;
  v_new_delivery_count integer:=0; v_group_replay boolean; v_recipient_group_key text;
  v_attachments jsonb:='[]'::jsonb; v_greeting text; v_agency_name text;
begin
  perform public._import_review_assert_actor_v1(p_actor_user_id);
  if p_import_id is null or p_operation_id is null or jsonb_typeof(coalesce(p_selected_action_ids,'[]'))<>'array'
    or p_max_actions<1 or p_max_actions>5000 or p_max_groups<1 or p_max_groups>100
    or jsonb_array_length(coalesce(p_selected_action_ids,'[]'))>p_max_actions then
    raise exception 'TIMESHEET_QUERY_ENQUEUE_INPUT_INVALID' using errcode='22023'; end if;
  if exists(select 1 from jsonb_array_elements(coalesce(p_selected_action_ids,'[]'))x where jsonb_typeof(x)<>'string' or trim(both '"' from x::text)!~'^[0-9a-f]{64}$') then
    raise exception 'TIMESHEET_QUERY_ACTION_ID_INVALID' using errcode='22023'; end if;
  select coalesce(array_agg(distinct value order by value),array[]::text[]) into v_ids from jsonb_array_elements_text(coalesce(p_selected_action_ids,'[]'))value;
  if cardinality(v_ids)<>jsonb_array_length(coalesce(p_selected_action_ids,'[]')) then raise exception 'TIMESHEET_QUERY_ACTION_ID_DUPLICATE' using errcode='22023'; end if;
  select * into v_state from public.import_review_states where import_id=p_import_id for update;
  select * into v_operation from public.import_apply_operations
  where id=p_operation_id and import_id=p_import_id for update;
  if v_state.import_id is null
    or v_state.status not in ('IN_REVIEW','BLOCKED','READY','APPLIED')
    or v_state.last_operation_id is distinct from p_operation_id
    or v_operation.id is null
    or v_operation.committed_at_utc is null
    or v_operation.state not in ('SOURCE_COMMITTED_TSFIN_PENDING','COMPLETE') then
    raise exception 'TIMESHEET_QUERY_REVIEW_NOT_APPLIED' using errcode='55000'; end if;
  select coalesce(array_agg(value order by value),array[]::text[]) into v_db_ids
  from jsonb_array_elements_text(coalesce(v_operation.response_json->'post_commit_email_action_ids','[]'::jsonb)) value;
  if v_ids is distinct from v_db_ids then raise exception 'TIMESHEET_QUERY_SELECTED_ACTION_SET_MISMATCH' using errcode='40001'; end if;
  if exists(
    select 1
    from unnest(v_ids) x
    left join public.import_review_decisions d
      on d.action_id=x and d.import_id=p_import_id
    left join public.import_review_action_outcomes o
      on o.action_id=x and o.import_id=p_import_id and o.operation_id=p_operation_id
    where d.action_id is null or not d.selected
      or d.action_kind not in ('EMAIL_ISSUE','EMAIL_REMINDER')
      or o.action_id is null
  ) then
    raise exception 'TIMESHEET_QUERY_ACTION_STALE' using errcode='40001'; end if;

  create temporary table pg_temp.query_email_issues on commit drop as
  select d.*,coalesce(nullif(d.summary_json->>'issue_fingerprint',''),d.source_identity) issue_fingerprint,
    public._timesheet_query_recipient_resolve_core_v1(d.client_id,d.contract_id) route,
    evidence.evidence_json attachment_evidence
  from public.import_review_decisions d
  cross join lateral (select public._import_review_query_evidence_core_v1(d.timesheet_id) evidence_json) evidence
  where d.import_id=p_import_id and d.action_id=any(v_ids) order by d.action_id;
  if exists(select 1 from pg_temp.query_email_issues where route->>'recipient_scope_key' is distinct from summary_json->>'recipient_scope_key'
    or route->>'route_fingerprint' is distinct from summary_json->>'recipient_route_fingerprint') then
    raise exception 'TIMESHEET_QUERY_RECIPIENT_ROUTE_CHANGED' using errcode='40001'; end if;
  if exists(select 1 from pg_temp.query_email_issues
    where not coalesce((attachment_evidence->>'document_ready')::boolean,false)
      or attachment_evidence->>'evidence_fingerprint' is distinct from summary_json->>'attachment_fingerprint') then
    raise exception 'TIMESHEET_QUERY_ATTACHMENT_EVIDENCE_STALE' using errcode='40001'; end if;

  select coalesce(nullif(btrim(agency_name),''),'CloudTMS') into v_agency_name
  from public.settings_defaults order by agency_name nulls last limit 1;
  v_agency_name:=coalesce(v_agency_name,'CloudTMS');
  v_greeting:=case when (statement_timestamp() at time zone 'Europe/London')::time<time '12:00'
    then 'Good morning' else 'Good afternoon' end;

  -- Create/reuse issue identities without changing successful-send history.
  insert into public.hr_issue_emails(source_system,import_id,client_id,contract_id,timesheet_id,hr_row_id,staff_norm,work_date,
    reason_code,issue_fingerprint,last_sent_at,recipient_scope,recipient_scope_key,delivery_history_status)
  select hi.source_system::text,p_import_id,q.client_id,q.contract_id,q.timesheet_id,q.hr_row_id,q.summary_json->>'candidate_name',
    nullif(q.summary_json->>'work_date','')::date,coalesce(q.summary_json->>'reason_code','VALIDATION_MISMATCH'),q.issue_fingerprint,
    null,q.route->>'recipient_scope',q.route->>'recipient_scope_key','PENDING'
  from pg_temp.query_email_issues q join public.hr_imports hi on hi.id=p_import_id
  on conflict(issue_fingerprint) do update set contract_id=excluded.contract_id,recipient_scope=excluded.recipient_scope,
    recipient_scope_key=excluded.recipient_scope_key,updated_at=now();
  update pg_temp.query_email_issues q set issue_id=e.id from public.hr_issue_emails e where e.issue_fingerprint=q.issue_fingerprint;

  for v_group in
    select lower(route->>'recipient_email') recipient_email,
      case when count(distinct route->>'recipient_scope_key')=1 then min(route->>'recipient_scope') else 'CONTRACT_OVERRIDE' end recipient_scope,
      public._import_review_hash_v1(string_agg(distinct route->>'route_fingerprint','|' order by route->>'route_fingerprint')) route_fingerprint,
      count(distinct route->>'recipient_scope_key') business_route_count,
      count(*) issue_count,max(coalesce(e.sent_count,0))+case when bool_or(q.action_kind='EMAIL_REMINDER') then 1 else 0 end reminder_sequence
    from pg_temp.query_email_issues q join public.hr_issue_emails e on e.id=q.issue_id
    group by lower(route->>'recipient_email')
    order by lower(route->>'recipient_email')
  loop
    v_group_count:=v_group_count+1; if v_group_count>p_max_groups then raise exception 'TIMESHEET_QUERY_GROUP_LIMIT_EXCEEDED' using errcode='54000'; end if;
    v_recipient_group_key:='RECIPIENT_EMAIL:'||public._import_review_hash_v1(v_group.recipient_email);
    select array_agg(q.issue_id order by q.issue_id),
      public._import_review_hash_v1(string_agg(q.issue_fingerprint||':'||(q.attachment_evidence->>'evidence_fingerprint'),'|' order by q.issue_fingerprint)),
      coalesce((select jsonb_agg(jsonb_build_object(
        'r2_key',attachment_row.attachment_evidence->>'attachment_r2_key',
        'filename',attachment_row.attachment_evidence->>'attachment_filename'
      ) order by attachment_row.timesheet_id)
      from (
        select distinct on (q2.timesheet_id) q2.timesheet_id,q2.attachment_evidence
        from pg_temp.query_email_issues q2
        where lower(q2.route->>'recipient_email')=v_group.recipient_email
        order by q2.timesheet_id,q2.action_id
      ) attachment_row),'[]'::jsonb)
      into v_issue_ids,v_issue_set_hash,v_attachments
    from pg_temp.query_email_issues q
    where lower(q.route->>'recipient_email')=v_group.recipient_email;
    v_reminder:=v_group.reminder_sequence;
    v_outbox_key:='TIMESHEET_QUERY:'||public._import_review_hash_v1(concat_ws('|',p_operation_id,v_recipient_group_key,v_issue_set_hash,v_reminder));
    v_delivery_id:=null;v_outbox_id:=null;v_group_replay:=false;
    select d.id,d.mail_outbox_id into v_delivery_id,v_outbox_id from public.hr_issue_email_deliveries d where d.deterministic_outbox_key=v_outbox_key for update;
    if not found then
      v_new_delivery_count:=v_new_delivery_count+1;
      v_delivery_id:=gen_random_uuid();
      v_subject:=case when v_reminder>0 then 'Reminder: timesheet corrections required' else 'Timesheet corrections required' end;
      with lines as (
        select q.action_id||':'||row_number() over(partition by q.action_id order by cx.value->>'comparison_key') sort_key,
          coalesce(nullif(cl.name,''),nullif(q.summary_json->>'client_name',''),'Client') client_name,
          coalesce(q.contract_id::text,'client-default') contract_sort,
          case when q.contract_id is null then 'Client default'
            else coalesce(nullif(concat_ws(' · ',nullif(ct.display_site,''),nullif(ct.role,''),nullif(ct.band,'')),''),'Contract')
              ||case when ct.start_date is not null then ' ('||to_char(ct.start_date,'FMDD Mon YYYY')||'–'||to_char(ct.end_date,'FMDD Mon YYYY')||')' else '' end
          end contract_label,
          q.summary_json->>'candidate_name' candidate_name,
          coalesce(nullif(cx.value->>'work_date',''),nullif(q.summary_json->>'work_date','')) work_date,
          coalesce(nullif(cx.value->>'match_status',''),nullif(q.summary_json->>'reason_code','')) issue_code,
          concat_ws(' ',nullif(cx.value->>'timesheet_start','')||case when nullif(cx.value->>'timesheet_end','') is null then '' else '–'||(cx.value->>'timesheet_end') end,
            case when nullif(cx.value->>'timesheet_break_mins','') is not null then 'break '||(cx.value->>'timesheet_break_mins')||' min' end,
            case when nullif(cx.value->>'timesheet_start','') is null then concat_ws('–',q.summary_json->>'start_time',q.summary_json->>'end_time') end,
            case when nullif(q.summary_json->>'hours_worked','') is not null then (q.summary_json->>'hours_worked')||' hours' end) timesheet_detail,
          concat_ws(' ',nullif(cx.value->>'healthroster_start','')||case when nullif(cx.value->>'healthroster_end','') is null then '' else '–'||(cx.value->>'healthroster_end') end,
            case when nullif(cx.value->>'healthroster_break_mins','') is not null then 'break '||(cx.value->>'healthroster_break_mins')||' min' end) import_detail,
          case
            when nullif(cx.value->>'ref_before','') is not null
              and cx.value->>'ref_before'=cx.value->>'ref_after' then cx.value->>'ref_before'
            when nullif(cx.value->>'ref_before','') is not null and nullif(cx.value->>'ref_after','') is not null
              then (cx.value->>'ref_before')||' → '||(cx.value->>'ref_after')
            else coalesce(nullif(cx.value->>'ref_after',''),nullif(cx.value->>'ref_before',''))
          end reference_detail
        from pg_temp.query_email_issues q
        left join public.clients cl on cl.id=q.client_id
        left join public.contracts ct on ct.id=q.contract_id
        cross join lateral (
          select c.value from jsonb_array_elements(coalesce(q.summary_json->'comparisons','[]'::jsonb)) c(value)
          where coalesce(c.value->>'match_status','')<>'MATCH'
             or coalesce(c.value->>'ref_before','')<>coalesce(c.value->>'ref_after','')
          union all
          select '{}'::jsonb where not exists(
            select 1 from jsonb_array_elements(coalesce(q.summary_json->'comparisons','[]'::jsonb)) c2(value)
            where coalesce(c2.value->>'match_status','')<>'MATCH'
               or coalesce(c2.value->>'ref_before','')<>coalesce(c2.value->>'ref_after',''))
        ) cx
        where lower(q.route->>'recipient_email')=v_group.recipient_email
      ), rendered_rows as (
        select l.*,'<tr><td style="padding:8px;border:1px solid #dbe2ea">'||public._import_review_html_escape_v1(l.candidate_name)||'</td><td style="padding:8px;border:1px solid #dbe2ea;white-space:nowrap">'||
          public._import_review_html_escape_v1(case when l.work_date~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then to_char(l.work_date::date,'FMDD Mon YYYY') else l.work_date end)||'</td><td style="padding:8px;border:1px solid #dbe2ea">'||
          public._import_review_html_escape_v1(replace(initcap(lower(l.issue_code)),'_',' '))||'</td><td style="padding:8px;border:1px solid #dbe2ea">'||
          public._import_review_html_escape_v1(l.timesheet_detail)||'</td><td style="padding:8px;border:1px solid #dbe2ea">'||public._import_review_html_escape_v1(l.import_detail)||'</td><td style="padding:8px;border:1px solid #dbe2ea">'||
          public._import_review_html_escape_v1(l.reference_detail)||'</td></tr>' row_html
        from lines l
      ), contract_tables as (
        select client_name,contract_sort,contract_label,
          '<h4 style="margin:18px 0 8px;color:#334155;font-size:14px">'||public._import_review_html_escape_v1(contract_label)||'</h4>'||
          '<table role="table" cellspacing="0" cellpadding="0" style="width:100%;border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px"><thead><tr style="background:#eef2f7;color:#1e293b"><th style="padding:8px;border:1px solid #dbe2ea;text-align:left">Worker</th><th style="padding:8px;border:1px solid #dbe2ea;text-align:left">Date</th><th style="padding:8px;border:1px solid #dbe2ea;text-align:left">Issue</th><th style="padding:8px;border:1px solid #dbe2ea;text-align:left">Timesheet</th><th style="padding:8px;border:1px solid #dbe2ea;text-align:left">HealthRoster</th><th style="padding:8px;border:1px solid #dbe2ea;text-align:left">Reference</th></tr></thead><tbody>'||
          string_agg(row_html,'' order by sort_key)||'</tbody></table>' contract_html
        from rendered_rows group by client_name,contract_sort,contract_label
      ), client_sections as (
        select client_name,'<section style="margin:24px 0"><h3 style="margin:0 0 10px;color:#0f172a;font-size:17px">'||
          public._import_review_html_escape_v1(client_name)||'</h3>'||string_agg(contract_html,'' order by contract_label,contract_sort)||'</section>' client_html
        from contract_tables group by client_name
      )
      select '<div style="font-family:Arial,sans-serif;color:#0f172a;line-height:1.45"><p>'||
        public._import_review_html_escape_v1(v_greeting)||',</p><p>Please can you kindly make amendments on HealthRoster for the below shifts. The relevant timesheets have been attached to this email.</p>'||
        string_agg(client_html,'' order by client_name)||'<p>Many thanks,<br>'||
        public._import_review_html_escape_v1(v_agency_name)||'</p></div>' into v_html from client_sections;
      with lines as (
        select q.action_id||':'||row_number() over(partition by q.action_id order by cx.value->>'comparison_key') sort_key,
          coalesce(nullif(cl.name,''),nullif(q.summary_json->>'client_name',''),'Client') client_name,
          case when q.contract_id is null then 'Client default'
            else coalesce(nullif(concat_ws(' · ',nullif(ct.display_site,''),nullif(ct.role,''),nullif(ct.band,'')),''),'Contract') end contract_label,
          q.summary_json->>'candidate_name' candidate_name,
          coalesce(nullif(cx.value->>'work_date',''),nullif(q.summary_json->>'work_date','')) work_date,
          coalesce(nullif(cx.value->>'match_status',''),nullif(q.summary_json->>'reason_code','')) issue_code,
          concat_ws(' ',cx.value->>'timesheet_start',cx.value->>'timesheet_end',cx.value->>'healthroster_start',cx.value->>'healthroster_end',
            q.summary_json->>'start_time',q.summary_json->>'end_time',q.summary_json->>'hours_worked') detail
        from pg_temp.query_email_issues q
        left join public.clients cl on cl.id=q.client_id
        left join public.contracts ct on ct.id=q.contract_id
        cross join lateral (
          select c.value from jsonb_array_elements(coalesce(q.summary_json->'comparisons','[]'::jsonb)) c(value)
          where coalesce(c.value->>'match_status','')<>'MATCH'
             or coalesce(c.value->>'ref_before','')<>coalesce(c.value->>'ref_after','')
          union all
          select '{}'::jsonb where not exists(
            select 1 from jsonb_array_elements(coalesce(q.summary_json->'comparisons','[]'::jsonb)) c2(value)
            where coalesce(c2.value->>'match_status','')<>'MATCH'
               or coalesce(c2.value->>'ref_before','')<>coalesce(c2.value->>'ref_after',''))
        ) cx
        where lower(q.route->>'recipient_email')=v_group.recipient_email
      )
      select v_greeting||E',\n\nPlease can you kindly make amendments on HealthRoster for the below shifts. The relevant timesheets have been attached to this email.\n\n'||
        string_agg(concat_ws(' | ',l.client_name,l.contract_label,l.candidate_name,l.work_date,l.issue_code,l.detail),'\n' order by l.client_name,l.contract_label,l.sort_key)
        ||E'\n\nMany thanks,\n'||v_agency_name into v_text from lines l;
      if length(v_html)>262144 or length(v_text)>131072 then raise exception 'TIMESHEET_QUERY_BODY_LIMIT_EXCEEDED' using errcode='54000'; end if;
      insert into public.mail_outbox(type,"to",subject,body_html,body_text,attachments,status,created_by,reference,recipient_kind,recipient_id,
        context_kind,context_id,email_type,deterministic_outbox_key,payment_scope_json)
      values('TIMESHEET_QUERY',v_group.recipient_email,v_subject,v_html,v_text,v_attachments,'QUEUED'::public.mail_status_enum,p_actor_user_id,
        v_outbox_key,'TIMESHEET_QUERY_EMAIL',null,
        'TIMESHEET_QUERY_DELIVERY',v_delivery_id,'TIMESHEET_QUERY',v_outbox_key,'{}'::jsonb)
      on conflict do nothing;
      select id into v_outbox_id from public.mail_outbox where deterministic_outbox_key=v_outbox_key;
      if v_outbox_id is null then raise exception 'TIMESHEET_QUERY_OUTBOX_CLAIM_FAILED' using errcode='23505'; end if;
      insert into public.hr_issue_email_deliveries(id,import_id,operation_id,recipient_scope,recipient_scope_key,recipient_route_fingerprint,
        recipient_email,reminder_sequence,issue_set_fingerprint,deterministic_outbox_key,mail_outbox_id,status,created_by_user_id)
      values(v_delivery_id,p_import_id,p_operation_id,v_group.recipient_scope,v_recipient_group_key,v_group.route_fingerprint,
        v_group.recipient_email,v_reminder,v_issue_set_hash,v_outbox_key,v_outbox_id,'QUEUED',p_actor_user_id);
      insert into public.hr_issue_email_delivery_items(delivery_id,issue_id,action_id,issue_fingerprint)
      select v_delivery_id,q.issue_id,q.action_id,q.issue_fingerprint from pg_temp.query_email_issues q
      where lower(q.route->>'recipient_email')=v_group.recipient_email order by q.action_id;
    else
      v_group_replay:=true;
    end if;
    v_results:=v_results||jsonb_build_array(jsonb_build_object('delivery_id',v_delivery_id,'mail_outbox_id',v_outbox_id,
      'recipient_scope',v_group.recipient_scope,'recipient_scope_key',v_recipient_group_key,
      'business_route_count',v_group.business_route_count,'issue_count',v_group.issue_count,
      'reminder_sequence',v_reminder,'replay',v_group_replay));
  end loop;
  if v_new_delivery_count>0 then
    update public.import_review_states set follow_up_status='PENDING',state_version=state_version+1,
      updated_at_utc=now(),updated_by_user_id=p_actor_user_id where import_id=p_import_id returning * into v_state;
    insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json)
    values(p_import_id,v_state.state_version,p_operation_id,'QUERY_EMAILS_ENQUEUED',p_actor_user_id,
      jsonb_build_object('group_count',v_group_count,'new_delivery_count',v_new_delivery_count,'action_count',cardinality(v_ids)));
  else select * into v_state from public.import_review_states where import_id=p_import_id; end if;
  return jsonb_build_object('ok',true,'import_id',p_import_id,'operation_id',p_operation_id,'group_count',v_group_count,'groups',v_results,
    'new_delivery_count',v_new_delivery_count,'replay',v_group_count>0 and v_new_delivery_count=0,
    'follow_up_status',v_state.follow_up_status,'state_version',v_state.state_version);
end $function$;

create or replace function public._timesheet_query_email_delivery_mark_core_v1(
  p_mail_outbox_id uuid,p_provider_message_id text,p_provider_status text,p_accepted_at_utc timestamptz,p_actor_user_id uuid
)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
declare d public.hr_issue_email_deliveries%rowtype; o public.mail_outbox%rowtype; v_marked integer:=0; v_state public.import_review_states%rowtype; v_reconcile jsonb;
begin
  select * into o from public.mail_outbox where id=p_mail_outbox_id for update;
  select * into d from public.hr_issue_email_deliveries where mail_outbox_id=p_mail_outbox_id for update;
  if o.id is null or d.id is null then raise exception 'TIMESHEET_QUERY_DELIVERY_NOT_FOUND' using errcode='P0002'; end if;
  if upper(coalesce(o.status::text,''))<>'SENT' and o.sent_at is null then raise exception 'TIMESHEET_QUERY_OUTBOX_NOT_SENT' using errcode='55000'; end if;
  if nullif(btrim(coalesce(o.provider_message_id,'')),'') is not null and nullif(btrim(coalesce(p_provider_message_id,'')),'') is not null
    and o.provider_message_id<>btrim(p_provider_message_id) then raise exception 'TIMESHEET_QUERY_PROVIDER_ID_MISMATCH' using errcode='22023'; end if;
  if d.status='SENT' then return jsonb_build_object('ok',true,'replay',true,'delivery_id',d.id,'marked_issue_count',0); end if;
  update public.hr_issue_email_delivery_items set marked_sent_at_utc=coalesce(p_accepted_at_utc,o.sent_at,now())
  where delivery_id=d.id and marked_sent_at_utc is null;
  get diagnostics v_marked=row_count;
  update public.hr_issue_emails e set last_sent_at=coalesce(p_accepted_at_utc,o.sent_at,now()),sent_count=e.sent_count+1,
    last_successful_delivery_id=d.id,delivery_history_status='SENT_VERIFIED',updated_at=now()
  where e.id in (select di.issue_id from public.hr_issue_email_delivery_items di where di.delivery_id=d.id and di.marked_sent_at_utc is not null)
    and e.last_successful_delivery_id is distinct from d.id;
  update public.hr_issue_email_deliveries set status='SENT',provider_message_id=coalesce(nullif(btrim(p_provider_message_id),''),o.provider_message_id),
    provider_status=coalesce(nullif(btrim(p_provider_status),''),o.provider_status,'ACCEPTED'),accepted_at_utc=coalesce(p_accepted_at_utc,o.sent_at,now()),marked_at_utc=now(),updated_at_utc=now()
  where id=d.id returning * into d;
  if not exists(select 1 from public.hr_issue_email_deliveries x where x.import_id=d.import_id and x.status<>'SENT') then
    update public.import_apply_operations set response_json=response_json||jsonb_build_object('review_email_follow_up_status','COMPLETE'),updated_at_utc=now()
    where id=d.operation_id;
    v_reconcile:=public._import_review_follow_up_reconcile_core_v1(d.import_id,d.operation_id,p_actor_user_id);
    select * into v_state from public.import_review_states where import_id=d.import_id;
    insert into public.import_review_events(import_id,state_version,operation_id,event_code,actor_user_id,event_context_json)
    values(d.import_id,v_state.state_version,d.operation_id,'QUERY_EMAIL_FOLLOW_UP_COMPLETE',p_actor_user_id,jsonb_build_object('delivery_id',d.id));
  end if;
  return jsonb_build_object('ok',true,'replay',false,'delivery_id',d.id,'marked_issue_count',v_marked,'status',d.status,
    'follow_up_status',coalesce(v_reconcile->>'follow_up_status',(select s.follow_up_status from public.import_review_states s where s.import_id=d.import_id)));
end $function$;

create or replace function public.timesheet_query_email_delivery_mark_v1(
  p_mail_outbox_id uuid,p_provider_message_id text default null,p_provider_status text default null,
  p_accepted_at_utc timestamptz default null,p_actor_user_id uuid default null
)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
begin perform public._import_review_assert_actor_v1(p_actor_user_id);
  return public._timesheet_query_email_delivery_mark_core_v1(p_mail_outbox_id,p_provider_message_id,p_provider_status,p_accepted_at_utc,p_actor_user_id); end $function$;

create or replace function public.timesheet_query_email_delivery_reconcile_v1(
  p_after_delivery_id uuid default null,p_limit integer default 50,p_actor_user_id uuid default null
)
returns jsonb language plpgsql security definer set search_path to 'public','pg_temp' as $function$
declare v_limit integer:=least(greatest(coalesce(p_limit,50),1),100); r record; v_processed integer:=0; v_repaired integer:=0; v_skipped integer:=0; v_last uuid;
begin perform public._import_review_assert_actor_v1(p_actor_user_id);
  for r in select d.id,d.mail_outbox_id,o.provider_message_id,o.provider_status,o.sent_at
    from public.hr_issue_email_deliveries d join public.mail_outbox o on o.id=d.mail_outbox_id
    where d.status<>'SENT' and (o.status::text='SENT' or o.sent_at is not null) and (p_after_delivery_id is null or d.id>p_after_delivery_id)
    order by d.id limit v_limit
  loop v_processed:=v_processed+1;v_last:=r.id;
    begin perform public._timesheet_query_email_delivery_mark_core_v1(r.mail_outbox_id,r.provider_message_id,r.provider_status,r.sent_at,p_actor_user_id);v_repaired:=v_repaired+1;
    exception when others then v_skipped:=v_skipped+1; end;
  end loop;
  return jsonb_build_object('ok',true,'processed',v_processed,'repaired',v_repaired,'skipped',v_skipped,'next_cursor',case when v_processed=v_limit then v_last end,
    'has_more',exists(select 1 from public.hr_issue_email_deliveries d join public.mail_outbox o on o.id=d.mail_outbox_id
      where d.status<>'SENT' and (o.status::text='SENT' or o.sent_at is not null)
        and (v_last is null or d.id>v_last) limit 1));
end $function$;

revoke all on function public._timesheet_query_recipient_resolve_core_v1(uuid,uuid) from public,anon,authenticated,service_role;
revoke all on function public._import_review_html_escape_v1(text) from public,anon,authenticated,service_role;
revoke all on function public._timesheet_query_email_delivery_mark_core_v1(uuid,text,text,timestamptz,uuid) from public,anon,authenticated,service_role;
revoke all on function public.timesheet_query_recipient_resolve_v1(uuid,uuid) from public,anon,authenticated;
grant execute on function public.timesheet_query_recipient_resolve_v1(uuid,uuid) to service_role;
revoke all on function public.timesheet_query_email_enqueue_v1(uuid,uuid,jsonb,uuid,integer,integer) from public,anon,authenticated;
grant execute on function public.timesheet_query_email_enqueue_v1(uuid,uuid,jsonb,uuid,integer,integer) to service_role;
revoke all on function public.timesheet_query_email_delivery_mark_v1(uuid,text,text,timestamptz,uuid) from public,anon,authenticated;
grant execute on function public.timesheet_query_email_delivery_mark_v1(uuid,text,text,timestamptz,uuid) to service_role;
revoke all on function public.timesheet_query_email_delivery_reconcile_v1(uuid,integer,uuid) from public,anon,authenticated;
grant execute on function public.timesheet_query_email_delivery_reconcile_v1(uuid,integer,uuid) to service_role;
