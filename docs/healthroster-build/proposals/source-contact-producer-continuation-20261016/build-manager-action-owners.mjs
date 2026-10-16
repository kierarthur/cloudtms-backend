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
bulk=once(bulk,'  v_current_version text;','  v_current_version text;\n  v_manager_action jsonb;');
bulk=once(bulk,`  select * into strict v_cycle from public.weekly_source_cycles
  where id=v_cycle_id for update;`,`  if v_action='SEND_MANAGER_NOW' then
    perform private.weekly_contact_source_manager_action_lock_v1(v_actor,v_cycle_id,v_publication_id,v_filters);
  end if;
  select * into strict v_cycle from public.weekly_source_cycles
  where id=v_cycle_id for update;`);
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
write('29_native_contact_bulk.sqlinc',bulk);
writeFileSync(path.join(base,'MANAGER_ACTION_OWNER_INPUTS.json'),JSON.stringify({version:'CONTACT_MANAGER_ACTION_OWNER_INPUTS_V1',inputs,
 unchanged_two_argument_version:{signature:'private.weekly_source_office_workspace_version_v1(uuid,uuid)',sha256_normalized_lf:sha(baseVersion),
 declaration_in_package:false},
 outputs:['28_native_contact_workspace.sqlinc','29_native_contact_bulk.sqlinc'].map(file=>({path:'forward/'+file,sha256:sha(readFileSync(path.join(base,'forward',file)))})),
 qualification:'Authored complete finite successors; existing public ABI unchanged; PostgreSQL/pay-isolation/concurrency NOT_RUN'},null,2)+'\n');
console.log(JSON.stringify({complete_native_owners:2,status:'SOURCE_ONLY_NATIVE_NOT_RUN'}));
