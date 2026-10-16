import {execFileSync} from 'node:child_process';
import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(base,'../../../..');
const commit='90ffcf3f40644280fcdb5b791a62a86613896d6e';
const sha=s=>createHash('sha256').update(s).digest('hex'),inputs=[];
function source(file){const text=execFileSync('git',['show',`${commit}:${file}`],{cwd:root,encoding:'utf8',maxBuffer:8e6}).replace(/\r\n/g,'\n');
 inputs.push({source_commit:commit,path:file,sha256_normalized_lf:sha(text)});return text;}
function extract(text,name){const start=text.indexOf(`create or replace function ${name}(`);if(start<0)throw Error(name);
 const end=text.indexOf('$function$;',text.indexOf('as $function$',start));if(end<0)throw Error(name+' terminator');return text.slice(start,end+12)+'\n';}
function once(text,old,next){if(text.split(old).length!==2)throw Error('Unique source anchor: '+old.slice(0,120));return text.replace(old,next);}
function write(file,text){writeFileSync(path.join(base,'forward',file),text.trimEnd()+'\n');}
const projection=source('supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql');
const baseVersion=extract(projection,'private.weekly_source_office_workspace_version_v1');
let workspace=extract(source('supabase/repeatable/14102026_0044_weekly_source_protected_pay_authority_v2.sql'),'public.weekly_source_office_workspace_v1');
workspace=once(workspace,'  v_workspace_version text;','  v_workspace_version text;\n  v_manager_workspace_version text;\n  v_manager_action jsonb;');
workspace=once(workspace,'    v_ask_selection_proof:=pg_catalog.encode(',`    v_manager_workspace_version:=v_workspace_version;
    -- Only the actual contact/Admin Queries view reads contact history. Pay
    -- and unrelated tabs keep their original dependencies, rows and cursors.
    if not v_pay_view and v_tab='queries' then
      v_manager_action:=private.weekly_contact_source_manager_action_v1(v_actor,v_cycle.id,v_publication.id,v_filters);
      v_manager_workspace_version:=v_manager_action->>'workspace_version';
      v_manager_eligible_count:=(v_manager_action->>'eligible_group_count')::integer;
    end if;
    v_ask_selection_proof:=pg_catalog.encode(`);
workspace=once(workspace,`        'action','SEND_MANAGER_NOW','source_cycle_id',v_cycle.id,
        'projection_publication_id',v_publication.id,
        'workspace_version',v_workspace_version,'filters',v_filters,`,`        'action','SEND_MANAGER_NOW','source_cycle_id',v_cycle.id,
        'projection_publication_id',v_publication.id,
        'workspace_version',v_manager_workspace_version,'filters',v_filters,`);
workspace=once(workspace,`          'expected_workspace_version',v_workspace_version,'action','SEND_MANAGER_NOW',`,
 `          'expected_workspace_version',v_manager_workspace_version,'action','SEND_MANAGER_NOW',`);
write('28_native_contact_workspace.sqlinc',workspace);
let bulk=extract(projection,'public.weekly_source_office_bulk_query_action_atomic_v1');
bulk=once(bulk,'  v_current_version text;',`  v_current_version text;
  v_manager_action jsonb;
  v_contact_guard jsonb;
  v_candidate_included_count integer;
  v_candidate_excluded_count integer;
  v_candidate_exclusions jsonb;`);
bulk=once(bulk,`  select * into strict v_cycle from public.weekly_source_cycles
  where id=v_cycle_id for update;`,`  if v_action='SEND_MANAGER_NOW' then
    perform private.weekly_contact_source_manager_action_lock_v1(v_actor,v_cycle_id,v_publication_id,v_filters);
  elsif v_action='ASK_CANDIDATES' then
    perform private.weekly_contact_source_submission_lock_v1(v_cycle_id,null,'[]',v_publication_id);
  end if;
  select * into strict v_cycle from public.weekly_source_cycles
  where id=v_cycle_id for update;`);
bulk=once(bulk,`  if v_action='SEND_MANAGER_NOW' then
    perform private.weekly_contact_source_manager_action_lock_v1(v_actor,v_cycle_id,v_publication_id,v_filters);
  elsif v_action='ASK_CANDIDATES' then
    perform private.weekly_contact_source_submission_lock_v1(v_cycle_id,null,'[]',v_publication_id);
  end if;`,`  if v_action in('SEND_MANAGER_NOW','ASK_CANDIDATES') then
    perform private.weekly_contact_source_bulk_lock_v1(jsonb_set(jsonb_set(p_request,'{action}',to_jsonb(v_action)),
      '{selection}',v_selection||jsonb_build_object('mode',v_mode,'filters',v_filters)));
  end if;`);
bulk=once(bulk,'  v_current_version:=private.weekly_source_office_workspace_version_v1(v_cycle_id,v_publication_id);',
 `  if v_action='SEND_MANAGER_NOW' then
    v_manager_action:=private.weekly_contact_source_manager_action_v1(v_actor,v_cycle_id,v_publication_id,v_filters);
    v_current_version:=v_manager_action->>'workspace_version';
  else
    v_current_version:=private.weekly_source_office_workspace_version_v1(v_cycle_id,v_publication_id);
  end if;`);
bulk=once(bulk,`      if not v_group.manager_eligible or pg_catalog.cardinality(v_group.incident_ids)=0
         or pg_catalog.jsonb_array_length(v_group.missing_scopes)>0 then`,`      if not v_group.manager_eligible or pg_catalog.cardinality(v_group.incident_ids)=0
         or pg_catalog.jsonb_array_length(v_group.missing_scopes)>0
         or not exists(select 1 from jsonb_array_elements(v_manager_action->'groups') contact
           where contact->>'group_key'=v_group.group_key and contact->'eligible'='true'::jsonb) then`);
bulk=once(bulk,`      select query.manager_recipient_route_key,
        pg_catalog.array_agg(distinct incident_id order by incident_id) incident_ids`,`      select query.manager_recipient_route_key,contact.value->>'mode' contact_mode,
        pg_catalog.array_agg(distinct incident_id order by incident_id) incident_ids`);
bulk=once(bulk,`      cross join lateral pg_catalog.unnest(query.incident_ids) incident_id
      where query.group_key=any(v_selected_keys) and query.manager_eligible`,`      cross join lateral pg_catalog.unnest(query.incident_ids) incident_id
      join lateral jsonb_array_elements(v_manager_action->'groups') contact(value)
        on contact.value->>'group_key'=query.group_key and contact.value->'eligible'='true'::jsonb
      where query.group_key=any(v_selected_keys) and query.manager_eligible`);
bulk=once(bulk,`      group by query.manager_recipient_route_key
      order by pg_catalog.encode(query.manager_recipient_route_key,'hex')`,`      group by query.manager_recipient_route_key,contact.value->>'mode'
      order by pg_catalog.encode(query.manager_recipient_route_key,'hex'),contact.value->>'mode'`);
bulk=once(bulk,`      v_owner_result:=public.weekly_source_query_send_manager_now_atomic_v1(
        pg_catalog.jsonb_build_object('actor_user_id',v_actor,'source_cycle_id',v_cycle_id,
          'projection_publication_id',v_publication_id,'incident_ids',to_jsonb(v_route.incident_ids))
      );`,`      if v_route.contact_mode='ROUTINE_MANUAL_FOLLOWUP' then
        v_owner_result:=private.weekly_contact_source_manager_manual_step_v1(v_actor,v_cycle_id,v_publication_id,
          v_route.incident_ids,v_current_version);
      else
        v_owner_result:=public.weekly_source_query_send_manager_now_atomic_v1(
          pg_catalog.jsonb_build_object('actor_user_id',v_actor,'source_cycle_id',v_cycle_id,
            'projection_publication_id',v_publication_id,'incident_ids',to_jsonb(v_route.incident_ids))
        );
      end if;`);
bulk=once(bulk,"select query.manager_recipient_route_key,contact.value->>'mode' contact_mode,","select query.manager_recipient_route_key,");
bulk=once(bulk,`      group by query.manager_recipient_route_key,contact.value->>'mode'
      order by pg_catalog.encode(query.manager_recipient_route_key,'hex'),contact.value->>'mode'`,
`      group by query.manager_recipient_route_key
      order by pg_catalog.encode(query.manager_recipient_route_key,'hex')`);
bulk=once(bulk,`      if v_route.contact_mode='ROUTINE_MANUAL_FOLLOWUP' then
        v_owner_result:=private.weekly_contact_source_manager_manual_step_v1(v_actor,v_cycle_id,v_publication_id,
          v_route.incident_ids,v_current_version);
      else
        v_owner_result:=public.weekly_source_query_send_manager_now_atomic_v1(
          pg_catalog.jsonb_build_object('actor_user_id',v_actor,'source_cycle_id',v_cycle_id,
            'projection_publication_id',v_publication_id,'incident_ids',to_jsonb(v_route.incident_ids))
        );
      end if;`,
`      -- One complete same-signature native owner classifies the selected
      -- cohort union and authors one current body; no initial/followup split.
      v_owner_result:=public.weekly_source_query_send_manager_now_atomic_v1(
        pg_catalog.jsonb_build_object('actor_user_id',v_actor,'source_cycle_id',v_cycle_id,
          'projection_publication_id',v_publication_id,'incident_ids',to_jsonb(v_route.incident_ids))
      );`);
bulk=once(bulk,'      v_candidate_failed:=false;',`      v_candidate_failed:=false;
      v_candidate_included_count:=0;v_candidate_excluded_count:=0;v_candidate_exclusions:='[]';`);
const initialStart=bulk.indexOf('        select coalesce(pg_catalog.jsonb_agg(scope.value order by');
const initialEnd=bulk.indexOf('        for v_client in',initialStart);
if(initialStart<0||initialEnd<initialStart||!bulk.slice(initialStart,initialEnd).includes('public.weekly_source_timesheet_submission_request_start_atomic_v1'))
 throw Error('Exact original Candidate-wide missing-week start block');
bulk=bulk.slice(0,initialStart)+bulk.slice(initialEnd);
bulk=once(bulk,'          if v_client.has_missing_scope then continue; end if;',`          v_contact_guard:=private.weekly_contact_scope_guard_v1(v_candidate.candidate_id,v_client.client_id,'WORKER',clock_timestamp());
          if v_contact_guard->'allow' is distinct from 'true'::jsonb then
            v_candidate_excluded_count:=v_candidate_excluded_count+cardinality(v_client.group_keys);
            v_candidate_exclusions:=v_candidate_exclusions||jsonb_build_array(jsonb_build_object(
              'candidate_id',v_candidate.candidate_id,'client_id',v_client.client_id,'group_keys',to_jsonb(v_client.group_keys),
              'status','CONTACT_PAUSED','next_allowed_utc',v_contact_guard->'next_allowed_utc'));
            continue;
          end if;
          if v_client.has_missing_scope then
            select coalesce(jsonb_agg(scope.value order by(scope.value->>'week_ending')::date,
              (scope.value->>'contract_id')::uuid),'[]') into v_scopes from(
              select distinct on(value->>'week_ending',value->>'client_id',value->>'contract_id')value
              from private.weekly_source_office_query_groups_v1(v_cycle_id,v_publication_id,v_filters)query
              cross join lateral jsonb_array_elements(query.missing_scopes)value
              where query.group_key=any(v_selected_keys) and query.candidate_id=v_candidate.candidate_id and query.client_id=v_client.client_id
                and(query.outreach_eligible or not query.candidate_app_available)
              order by value->>'week_ending',value->>'client_id',value->>'contract_id',value::text)scope;
            v_owner_result:=private.weekly_contact_source_submission_partition_v1(jsonb_build_object('actor_user_id',v_actor,
              'source_cycle_id',v_cycle_id,'projection_publication_id',v_publication_id,'candidate_id',v_candidate.candidate_id,'scopes',v_scopes),false);
            if jsonb_array_length(v_owner_result->'exclusions')>0 then
              v_candidate_excluded_count:=v_candidate_excluded_count+cardinality(v_client.group_keys);
              v_candidate_exclusions:=v_candidate_exclusions||jsonb_build_array(jsonb_build_object(
                'candidate_id',v_candidate.candidate_id,'client_id',v_client.client_id,'group_keys',to_jsonb(v_client.group_keys),
                'status',v_owner_result->'exclusions'->0->'status','next_allowed_utc',v_owner_result->'exclusions'->0->'next_allowed_utc'));
            else
              v_candidate_results:=v_candidate_results||coalesce((select jsonb_agg(child->'receipt')
                from jsonb_array_elements(v_owner_result->'results')child),'[]');
              v_candidate_included_count:=v_candidate_included_count+cardinality(v_client.group_keys);
              v_candidate_issue_count:=v_candidate_issue_count+jsonb_array_length(v_scopes);
            end if;
            continue;
          end if;`);
bulk=once(bulk,`          v_candidate_results:=v_candidate_results||pg_catalog.jsonb_build_array(v_owner_result);
          v_candidate_issue_count:=v_candidate_issue_count+pg_catalog.cardinality(v_exact_incidents);`,
`          if v_owner_result->>'status' in('CREATED','UNCHANGED') then
            v_candidate_results:=v_candidate_results||pg_catalog.jsonb_build_array(v_owner_result);
            v_candidate_included_count:=v_candidate_included_count+cardinality(v_client.group_keys);
            v_candidate_issue_count:=v_candidate_issue_count+pg_catalog.cardinality(v_exact_incidents);
          else
            v_candidate_excluded_count:=v_candidate_excluded_count+cardinality(v_client.group_keys);
            v_candidate_exclusions:=v_candidate_exclusions||jsonb_build_array(jsonb_build_object(
              'candidate_id',v_candidate.candidate_id,'client_id',v_client.client_id,'group_keys',to_jsonb(v_client.group_keys),
              'status',v_owner_result->'status','next_allowed_utc',v_owner_result->'next_allowed_utc'));
          end if;`);
bulk=once(bulk,'        v_included_count:=v_included_count+pg_catalog.cardinality(v_candidate.group_keys);',
`        v_included_count:=v_included_count+v_candidate_included_count;
        v_excluded_count:=v_excluded_count+v_candidate_excluded_count;
        -- Existing bounded response presentation, with complete omitted count.
        -- It never truncates selected business work or its actual effects.
        select coalesce(jsonb_agg(value order by ordinal),'[]') into v_exclusions
          from jsonb_array_elements(v_exclusions||v_candidate_exclusions)with ordinality item(value,ordinal) where ordinal<=100;`);
bulk=once(bulk,'          if coalesce(pg_catalog.cardinality(v_exact_incidents),0)=0 then continue; end if;',
`          if coalesce(pg_catalog.cardinality(v_exact_incidents),0)=0 then
            v_candidate_excluded_count:=v_candidate_excluded_count+cardinality(v_client.group_keys);
            v_candidate_exclusions:=v_candidate_exclusions||jsonb_build_array(jsonb_build_object(
              'candidate_id',v_candidate.candidate_id,'client_id',v_client.client_id,'group_keys',to_jsonb(v_client.group_keys),
              'status','NO_ACTIONABLE_INCIDENTS'));
            continue;end if;`);
write('29_native_contact_bulk.sqlinc',bulk);
writeFileSync(path.join(base,'MANAGER_ACTION_OWNER_INPUTS.json'),JSON.stringify({version:'CONTACT_MANAGER_ACTION_OWNER_INPUTS_V1',inputs,
 unchanged_two_argument_version:{signature:'private.weekly_source_office_workspace_version_v1(uuid,uuid)',sha256_normalized_lf:sha(baseVersion),
 declaration_in_package:false},
 outputs:['28_native_contact_workspace.sqlinc','29_native_contact_bulk.sqlinc'].map(file=>({path:'forward/'+file,sha256:sha(readFileSync(path.join(base,'forward',file)))})),
 qualification:'Authored complete finite successors; existing public ABI unchanged; PostgreSQL/pay-isolation/concurrency NOT_RUN'},null,2)+'\n');
console.log(JSON.stringify({complete_native_owners:2,status:'SOURCE_ONLY_NATIVE_NOT_RUN'}));
