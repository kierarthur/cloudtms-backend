-- F-25 pure typed QuerySync request digests, independent of live business rows.
-- Design: F-25-query-sync-completion-build.md. No input JSON is retained.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_query_matched_facts_digest_v1(p_contract uuid,p_family text,p_presence text,
 p_candidate_start timestamp without time zone,p_candidate_end timestamp without time zone,p_candidate_break integer,
 p_system_start timestamp without time zone,p_system_end timestamp without time zone,p_system_break integer)
returns bytea language sql immutable security invoker set search_path=pg_catalog,private
as $function$
 select sha256(convert_to(jsonb_build_array('BPAY_NEXT_QUERY_MATCHED_FACTS_V1',p_contract,p_family,p_presence,
  p_candidate_start,p_candidate_end,p_candidate_break,p_system_start,p_system_end,p_system_break)::text,'UTF8'))
$function$;

create or replace function private.bpay_next_query_sync_issue_digest_v1(p_issue jsonb)
returns bytea language plpgsql stable security invoker set search_path=pg_catalog,private
as $function$
declare work_id uuid;candidate_hash bytea;expected_text text;
begin
 if p_issue is null or jsonb_typeof(p_issue)<>'object' or exists(select 1 from jsonb_object_keys(p_issue) key where key not in
  ('work_event_id','candidate_timesheet_id','candidate_timesheet_revision','candidate_shift_fingerprint','source_row_id','source_work_event_link_id',
   'contract_id','issue_family','source_presence','candidate_start_at_local','candidate_end_at_local','candidate_break_minutes',
   'system_start_at_local','system_end_at_local','system_break_minutes','expected_material_comparison_fingerprint')) then
  raise exception using errcode='22023',message='WEEKLY_SOURCE_QUERY_ISSUE_INVALID';end if;
 work_id:=(p_issue->>'work_event_id')::uuid;
 if work_id is null then raise exception using errcode='22023',message='WEEKLY_SOURCE_QUERY_ISSUE_IDENTITY_INVALID';end if;
 candidate_hash:=case when nullif(p_issue->>'candidate_shift_fingerprint','') is null then null else
  private.weekly_source_query_hex32_v1(p_issue->>'candidate_shift_fingerprint','WEEKLY_SOURCE_CANDIDATE_SHIFT_HASH_INVALID') end;
 -- Original historic resolved-match branch deliberately bypasses the later
 -- expected-fingerprint parser. Retain its exact optional text in the complete
 -- input SHA; the actual original nonhistorical branch still validates it.
 expected_text:=nullif(p_issue->>'expected_material_comparison_fingerprint','');
 return sha256(convert_to(jsonb_build_array('BPAY_NEXT_QUERY_SYNC_ISSUE_V1',work_id,
   nullif(p_issue->>'candidate_timesheet_id','')::uuid,nullif(p_issue->>'candidate_timesheet_revision','')::integer,encode(candidate_hash,'hex'),
   nullif(p_issue->>'source_row_id','')::uuid,nullif(p_issue->>'source_work_event_link_id','')::uuid,nullif(p_issue->>'contract_id','')::uuid,
   upper(btrim(coalesce(p_issue->>'issue_family',''))),upper(btrim(coalesce(p_issue->>'source_presence',''))),
   nullif(p_issue->>'candidate_start_at_local','')::timestamp,nullif(p_issue->>'candidate_end_at_local','')::timestamp,
   nullif(p_issue->>'candidate_break_minutes','')::integer,nullif(p_issue->>'system_start_at_local','')::timestamp,
   nullif(p_issue->>'system_end_at_local','')::timestamp,nullif(p_issue->>'system_break_minutes','')::integer,expected_text)::text,'UTF8'));
end
$function$;

create or replace function private.bpay_next_query_sync_request_digest_v1(p_actor uuid,p_cycle uuid,p_publication uuid,p_work_ids uuid[],p_issue_sha256 bytea[])
returns bytea language plpgsql immutable security invoker set search_path=pg_catalog,private
as $function$
declare issues jsonb;
begin
 if p_actor is null or p_cycle is null or p_publication is null or p_work_ids is null or p_issue_sha256 is null
   or cardinality(p_work_ids)<>cardinality(p_issue_sha256) or cardinality(p_work_ids)>1000
   or array_position(p_work_ids,null) is not null or array_position(p_issue_sha256,null) is not null
   or (cardinality(p_work_ids)>0 and (array_ndims(p_work_ids)<>1 or array_lower(p_work_ids,1)<>1
     or array_ndims(p_issue_sha256)<>1 or array_lower(p_issue_sha256,1)<>1))
   or p_work_ids is distinct from coalesce((select array_agg(id order by id) from(select distinct id from unnest(p_work_ids) id) ids),'{}'::uuid[])
   or exists(select 1 from unnest(p_issue_sha256) digest where octet_length(digest)<>32) then
  raise exception using errcode='22023',message='BPAY_NEXT_QUERY_SYNC_REQUEST_DIGEST_INPUT_INVALID';end if;
 select coalesce(jsonb_agg(jsonb_build_array(p_work_ids[i],encode(p_issue_sha256[i],'hex')) order by i),'[]'::jsonb)
 into issues from generate_subscripts(p_work_ids,1) i;
 return sha256(convert_to(jsonb_build_array('BPAY_NEXT_QUERY_SYNC_REQUEST_V1',p_actor,p_cycle,p_publication,issues)::text,'UTF8'));
end
$function$;
alter function private.bpay_next_query_matched_facts_digest_v1(uuid,text,text,timestamp without time zone,timestamp without time zone,integer,timestamp without time zone,timestamp without time zone,integer) owner to postgres;
revoke all on function private.bpay_next_query_matched_facts_digest_v1(uuid,text,text,timestamp without time zone,timestamp without time zone,integer,timestamp without time zone,timestamp without time zone,integer) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_sync_issue_digest_v1(jsonb) owner to postgres;
revoke all on function private.bpay_next_query_sync_issue_digest_v1(jsonb) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_sync_request_digest_v1(uuid,uuid,uuid,uuid[],bytea[]) owner to postgres;
revoke all on function private.bpay_next_query_sync_request_digest_v1(uuid,uuid,uuid,uuid[],bytea[]) from public,anon,authenticated,service_role;
commit;
