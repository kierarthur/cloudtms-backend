-- F-25 original QuerySync true-tail immutable certificate assembly.
-- Design: F-25-query-sync-completion-build.md. No clock is taken here;
-- the actual outer caller alone finishes after its complete original tail.
\set ON_ERROR_STOP on
begin;
create or replace function private.bpay_next_query_sync_supplied_witnesses_v1(
 p_inputs private.bpay_next_query_sync_input_completion[],p_omissions private.bpay_next_query_sync_omission_completion[])
returns table(witness_ordinal bigint,witness_kind text,comparison_revision_id uuid,discrepancy_event_id uuid,evidence_sha256 bytea)
language sql stable security invoker set search_path=pg_catalog,private
as $function$
 with manifest as (
  select 'COMPARISON'::text kind,r.comparison_revision_id comparison,null::uuid event from unnest(p_inputs) r
  union select 'COMPARISON',r.prior_comparison_revision_id,null::uuid from unnest(p_inputs) r where r.outcome='CHANGED'
  union select 'COMPARISON',r.comparison_revision_id,null::uuid from unnest(p_omissions) r
  union select 'SOURCE_RECHECKED',null::uuid,r.recheck_event_id from unnest(p_inputs) r where r.outcome in ('NEW','CHANGED')
  union select 'RESOLVED_DECISION',null::uuid,r.resolved_decision_event_id from unnest(p_inputs) r where r.outcome='UNCHANGED_RESOLVED'
  union select 'SOURCE_MATCHED',null::uuid,r.resolved_event_id from unnest(p_omissions) r
 ) select row_number() over(order by case m.kind when 'COMPARISON' then 1 when 'SOURCE_RECHECKED' then 2 when 'RESOLVED_DECISION' then 3 when 'SOURCE_MATCHED' then 4 end,
   coalesce(m.comparison,m.event)),m.kind,m.comparison,m.event,private.bpay_next_query_original_evidence_digest_v1(m.kind,m.comparison,m.event)
 from manifest m
$function$;

create or replace function private.bpay_next_query_sync_expected_witnesses_v1(p_completion_id uuid)
returns table(witness_ordinal bigint,witness_kind text,comparison_revision_id uuid,discrepancy_event_id uuid,evidence_sha256 bytea)
language sql stable security invoker set search_path=pg_catalog,private
as $function$
 select w.* from private.bpay_next_query_sync_supplied_witnesses_v1(
   (select coalesce(array_agg(r order by r.work_event_id),'{}'::private.bpay_next_query_sync_input_completion[])
      from private.bpay_next_query_sync_input_completion r where r.completion_id=p_completion_id),
   (select coalesce(array_agg(r order by r.incident_id),'{}'::private.bpay_next_query_sync_omission_completion[])
      from private.bpay_next_query_sync_omission_completion r where r.completion_id=p_completion_id)) w
$function$;

create or replace function private.bpay_next_query_sync_complete_v1(p_producer_id uuid,p_actor uuid,p_cycle uuid,p_publication uuid,
 p_input_ids uuid[],p_issue_hashes bytea[],p_seen_work_ids uuid[],p_omitted_ids uuid[],
 p_new_count bigint,p_changed_count bigint,p_unchanged_count bigint,p_resolved_count bigint,
 p_inputs private.bpay_next_query_sync_input_completion[],p_omissions private.bpay_next_query_sync_omission_completion[])
returns bytea language plpgsql security invoker set search_path=pg_catalog,private,public
as $function$
declare owned private.bpay_next_query_producer%rowtype;header private.bpay_next_query_root_completion%rowtype;raw record;
 inputs_count bigint;omissions_count bigint;input_ids uuid[];issue_hashes bytea[];omitted_ids uuid[];
 new_count bigint;changed_count bigint;unchanged_count bigint;witness_count bigint;
begin
 if p_producer_id is null then return null;end if;
 select p.* into strict owned from private.bpay_next_query_producer p where p.id=p_producer_id and p.preparing_transaction_id=pg_current_xact_id() for update;
 if owned.owner_kind<>'QUERY_SYNC' or owned.phase<>'BUSINESS_OPEN' or owned.mode<>'STANDALONE_QUERY'
   or owned.owner_stack is distinct from array['QUERY_SYNC']::text[] or owned.completion_witness_id is not null
   or not exists(select 1 from private.bpay_next_input_transaction t where t.preparing_transaction_id=owned.preparing_transaction_id
     and t.module_epoch=owned.module_epoch and t.phase='OPEN')
   or p_inputs is null or p_omissions is null or cardinality(p_inputs)>1000 or cardinality(p_omissions)>1000
   or (cardinality(p_inputs)>0 and (array_ndims(p_inputs)<>1 or array_lower(p_inputs,1)<>1))
   or (cardinality(p_omissions)>0 and (array_ndims(p_omissions)<>1 or array_lower(p_omissions,1)<>1))
   or p_seen_work_ids is null or p_omitted_ids is null
   or array_position(p_seen_work_ids,null) is not null or array_position(p_omitted_ids,null) is not null
   or (cardinality(p_seen_work_ids)>0 and (array_ndims(p_seen_work_ids)<>1 or array_lower(p_seen_work_ids,1)<>1))
   or (cardinality(p_omitted_ids)>0 and (array_ndims(p_omitted_ids)<>1 or array_lower(p_omitted_ids,1)<>1)) then
  raise exception using errcode='55000',message='BPAY_NEXT_QUERY_SYNC_COMPLETION_OWNER_INVALID';end if;
 select count(*),coalesce(array_agg(r.work_event_id order by r.work_event_id),'{}'::uuid[]),
   coalesce(array_agg(r.input_issue_sha256 order by r.work_event_id),'{}'::bytea[]),
   count(*) filter(where r.outcome='NEW'),count(*) filter(where r.outcome='CHANGED'),count(*) filter(where r.outcome in ('UNCHANGED_OPEN','UNCHANGED_RESOLVED'))
 into inputs_count,input_ids,issue_hashes,new_count,changed_count,unchanged_count from unnest(p_inputs) r;
 select count(*),coalesce(array_agg(r.incident_id order by r.incident_id),'{}'::uuid[])
 into omissions_count,omitted_ids from unnest(p_omissions) r;
 if (input_ids,issue_hashes,omitted_ids,new_count,changed_count,unchanged_count,omissions_count) is distinct from
     (p_input_ids,p_issue_hashes,p_omitted_ids,p_new_count,p_changed_count,p_unchanged_count,p_resolved_count)
   or input_ids is distinct from (select coalesce(array_agg(x order by x),'{}'::uuid[]) from unnest(p_seen_work_ids) x)
   or cardinality(input_ids)<>(select count(distinct r.work_event_id) from unnest(p_inputs) r)
   or cardinality(omitted_ids)<>(select count(distinct r.incident_id) from unnest(p_omissions) r)
   or exists(select 1 from unnest(p_inputs) r where r.completion_id is distinct from owned.id or r.work_event_id is null
     or r.evidence_sha256 is distinct from private.bpay_next_query_sync_input_completion_digest_v1(r))
   or exists(select 1 from unnest(p_omissions) r where r.completion_id is distinct from owned.id or r.work_event_id is null
     or r.evidence_sha256 is distinct from private.bpay_next_query_sync_omission_completion_digest_v1(r))
   or owned.request_sha256 is distinct from private.bpay_next_query_sync_request_digest_v1(p_actor,p_cycle,p_publication,input_ids,issue_hashes) then
  raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_COMPLETION_ORIGINAL_SET_INVALID';end if;
 select * into strict raw from private.bpay_next_query_raw_stages_v1(owned.id);
 if raw.actual_stage_count is distinct from 3*new_count+2*changed_count+omissions_count then
  raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_COMPLETION_STAGE_COUNT_INVALID';end if;
 header.id:=owned.id;header.preparing_transaction_id:=owned.preparing_transaction_id;header.module_epoch:=owned.module_epoch;
 header.owner_kind:='QUERY_SYNC';header.root_variant:='QUERY_SYNC';header.request_sha256:=owned.request_sha256;header.child_action_sha256:=null;
 header.actor_user_id:=p_actor;header.source_cycle_id:=p_cycle;header.projection_publication_id:=p_publication;
 header.input_identity_count:=inputs_count;header.input_identities:=input_ids;
 header.input_identities_sha256:=sha256(convert_to(jsonb_build_array('BPAY_NEXT_QUERY_INPUT_IDENTITIES_V1',input_ids)::text,'UTF8'));
 header.omitted_incident_count:=omissions_count;header.omitted_incident_ids:=omitted_ids;
 header.omitted_incidents_sha256:=sha256(convert_to(jsonb_build_array('BPAY_NEXT_QUERY_OMITTED_INCIDENTS_V1',omitted_ids)::text,'UTF8'));
 header.new_incident_count:=new_count;header.changed_comparison_count:=changed_count;header.unchanged_incident_count:=unchanged_count;
 header.resolved_incident_count:=omissions_count;header.actual_stage_count:=raw.actual_stage_count;header.first_stage_ordinal:=raw.first_stage_ordinal;
 header.last_stage_ordinal:=raw.last_stage_ordinal;header.raw_stages_sha256:=raw.raw_stages_sha256;
 select sha256(convert_to(jsonb_build_array('BPAY_NEXT_QUERY_SYNC_INPUT_SET_V1',inputs_count,
    coalesce(jsonb_agg(jsonb_build_array(r.work_event_id,encode(private.bpay_next_query_sync_input_completion_digest_v1(r),'hex')) order by r.work_event_id),'[]'::jsonb))::text,'UTF8'))
 into header.query_input_completions_sha256 from unnest(p_inputs) r;
 select sha256(convert_to(jsonb_build_array('BPAY_NEXT_QUERY_SYNC_OMISSION_SET_V1',omissions_count,
    coalesce(jsonb_agg(jsonb_build_array(r.incident_id,encode(private.bpay_next_query_sync_omission_completion_digest_v1(r),'hex')) order by r.incident_id),'[]'::jsonb))::text,'UTF8'))
 into header.query_omission_completions_sha256 from unnest(p_omissions) r;
 select count(*),sha256(convert_to(coalesce(jsonb_agg(jsonb_build_array(w.witness_ordinal,w.witness_kind,w.comparison_revision_id,
   w.discrepancy_event_id,encode(w.evidence_sha256,'hex')) order by w.witness_ordinal),'[]'::jsonb)::text,'UTF8'))
 into witness_count,header.original_witnesses_sha256 from private.bpay_next_query_sync_supplied_witnesses_v1(p_inputs,p_omissions) w;
 if witness_count>4000 then raise exception using errcode='23514',message='BPAY_NEXT_QUERY_SYNC_WITNESS_SET_TOO_LARGE';end if;
 header.original_witness_count:=witness_count;header.completion_sha256:=private.bpay_next_query_root_completion_digest_v1(header);
 header.completed_at_utc:=transaction_timestamp();
 -- Compute all final immutable digests before the one header INSERT; no
 -- placeholder/header UPDATE can conceal a partial branch or omitted witness.
 insert into private.bpay_next_query_root_completion select (header).*;
 insert into private.bpay_next_query_sync_input_completion select r.* from unnest(p_inputs) r;
 insert into private.bpay_next_query_sync_omission_completion select r.* from unnest(p_omissions) r;
 insert into private.bpay_next_query_root_completion_witness(completion_id,witness_ordinal,witness_kind,comparison_revision_id,discrepancy_event_id,evidence_sha256)
  select header.id,w.witness_ordinal,w.witness_kind,w.comparison_revision_id,w.discrepancy_event_id,w.evidence_sha256
  from private.bpay_next_query_sync_supplied_witnesses_v1(p_inputs,p_omissions) w;
 perform private.bpay_next_query_root_completion_assert_v1(header.id);
 return header.completion_sha256;
end
$function$;
alter function private.bpay_next_query_sync_supplied_witnesses_v1(private.bpay_next_query_sync_input_completion[],private.bpay_next_query_sync_omission_completion[]) owner to postgres;
revoke all on function private.bpay_next_query_sync_supplied_witnesses_v1(private.bpay_next_query_sync_input_completion[],private.bpay_next_query_sync_omission_completion[]) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_sync_expected_witnesses_v1(uuid) owner to postgres;
revoke all on function private.bpay_next_query_sync_expected_witnesses_v1(uuid) from public,anon,authenticated,service_role;
alter function private.bpay_next_query_sync_complete_v1(uuid,uuid,uuid,uuid,uuid[],bytea[],uuid[],uuid[],bigint,bigint,bigint,bigint,private.bpay_next_query_sync_input_completion[],private.bpay_next_query_sync_omission_completion[]) owner to postgres;
revoke all on function private.bpay_next_query_sync_complete_v1(uuid,uuid,uuid,uuid,uuid[],bytea[],uuid[],uuid[],bigint,bigint,bigint,bigint,private.bpay_next_query_sync_input_completion[],private.bpay_next_query_sync_omission_completion[]) from public,anon,authenticated,service_role;
commit;
