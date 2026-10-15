// Adjacent full source correction. Preserve 6346 and all original evidence.
import fs from'node:fs';import path from'node:path';import assert from'node:assert/strict';import{execFileSync}from'node:child_process';import{fileURLToPath}from'node:url';import{createHash}from'node:crypto';
import{scanManagedSql}from'../../../../scripts/cloudtms-managed-release-sql.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),commit='6346f699029201de90288514e4c0751d43e10553',file='docs/healthroster-build/proposals/checklist-native-process-publication-20261015/forward/wp_u3_process_publication.sqlinc',sha=b=>createHash('sha256').update(b).digest('hex');
const raw=(c,f)=>execFileSync('git',['cat-file','blob',c+':'+f],{cwd:repo,maxBuffer:12e6}),b=raw(commit,file);assert.deepEqual(b,fs.readFileSync(path.join(repo,file)));
const hunks=[{label:'complete native receipt source-unit and expense event frame locals',before:" facts jsonb;events jsonb:='[]'::jsonb;event_fact jsonb;proved boolean:=false;event_proved boolean;",after:" facts jsonb;events jsonb:='[]'::jsonb;event_fact jsonb;proved boolean:=false;event_proved boolean;\n source_units jsonb;source_expenses jsonb;expected_events jsonb;expected_event jsonb;event_matches bigint;"},
 {label:'derive ALL expected task events from the exact native immutable receipt source owners; no surviving join can erase a member',before:`  n:=0;proved:=true;
  for event in
   select distinct fs.work_event_id,fs.candidate_id,fs.client_id,fs.contract_id
   from public.weekly_source_final_snapshot_lines fs
   join public.weekly_source_row_timesheet_lineages l on l.upload_row_id=fs.upload_row_id
    and l.timesheet_id=root.timesheet_id
   where fs.final_revision_id=fr.id and fs.client_id=receipt.client_id
    and fs.contract_id=root.contract_id
  loop
   n:=n+1;event_proved:=false;current_receipt:=null;current_approval:=null;current_event:=null;`,after:`  source_units:=private.weekly_source_ordinary_projection_source_units_v1(fr.id,root.timesheet_id);
  source_expenses:=private.weekly_source_ordinary_projection_current_expenses_v1(root.timesheet_id,fr.id);
  facts:=facts||jsonb_build_object('native_source_units',source_units,'native_source_expenses',source_expenses);
  if source_units is distinct from receipt.source_unit_outcomes_json
   or source_expenses is distinct from receipt.source_expense_authorities_json
   or exists(select 1 from jsonb_array_elements(source_units||source_expenses) unit
    where jsonb_typeof(unit->'work_event_id') is distinct from 'string'
     or pg_input_is_valid(unit->>'work_event_id','uuid') is distinct from true) then
   return jsonb_build_object('proved',false,'native_facts',facts);end if;
  select coalesce(jsonb_agg(event_id order by event_id),'[]'::jsonb) into expected_events
   from (select distinct unit->>'work_event_id' event_id from jsonb_array_elements(source_units||source_expenses) unit) complete_events;
  facts:=facts||jsonb_build_object('expected_native_events',expected_events);
  n:=0;proved:=true;
  for expected_event in select value from jsonb_array_elements(expected_events)
  loop
   select count(*) into event_matches
   from public.weekly_source_final_snapshot_lines fs
   join public.weekly_source_row_resolutions rr on rr.id=fs.row_resolution_id and rr.upload_row_id=fs.upload_row_id
    and rr.work_event_id=fs.work_event_id and rr.candidate_id=fs.candidate_id and rr.client_id=fs.client_id
    and rr.contract_id=fs.contract_id and rr.mapping_state='RESOLVED'
   join public.weekly_source_row_timesheet_lineages l on l.row_resolution_id=rr.id
    and l.work_event_id=fs.work_event_id and l.candidate_id=fs.candidate_id and l.client_id=fs.client_id
    and l.contract_id=fs.contract_id and l.source_cycle_id=fr.source_cycle_id
    and l.family_booking_id=root.booking_id and l.week_ending_date=root.week_ending_date
    and l.timesheet_id=any(private.weekly_source_invoice_family_timesheet_ids_v1(root.timesheet_id))
   where fs.final_revision_id=fr.id and fs.work_event_id=(expected_event#>>'{}')::uuid
    and fs.client_id=receipt.client_id and fs.contract_id=root.contract_id
    and fs.candidate_id=(select c.candidate_id from public.contracts c where c.id=root.contract_id)
    and exists(select 1 from public.weekly_source_projection_publications p
     where p.upload_id=fr.upload_id and p.source_cycle_id=fr.source_cycle_id
      and p.authority_scope_kind=fr.authority_scope_kind and p.report_scope_id is not distinct from fr.report_scope_id
      and p.state in('CURRENT','SUPERSEDED') and rr.generation=coalesce(p.projection_generation,p.authority_scope_version));
   if event_matches<>1 then
    return jsonb_build_object('proved',false,'native_facts',facts||jsonb_build_object(
     'unproved_expected_event',expected_event,'exact_snapshot_lineage_matches',event_matches));end if;
   select fs.work_event_id,fs.candidate_id,fs.client_id,fs.contract_id into strict event
    from public.weekly_source_final_snapshot_lines fs where fs.final_revision_id=fr.id and fs.work_event_id=(expected_event#>>'{}')::uuid;
   n:=n+1;event_proved:=false;current_receipt:=null;current_approval:=null;current_event:=null;`},
 {label:'nonempty complete event census must all prove its current publication',before:"  proved:=proved and n>0;facts:=facts||jsonb_build_object('native_current_publications',events);",after:"  proved:=proved and n>0 and n=jsonb_array_length(expected_events);\n  facts:=facts||jsonb_build_object('native_current_publications',events);"}];
let s=b.toString();for(const h of hunks){assert.equal(s.split(h.before).length,2,h.label);s=s.replace(h.before,()=>h.after);}let reverse=s;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2,h.label);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,b.toString());
fs.mkdirSync(path.join(here,'input'),{recursive:true});fs.mkdirSync(path.join(here,'forward'),{recursive:true});fs.writeFileSync(path.join(here,'input/wp_u3_process_publication.sqlinc'),b);fs.writeFileSync(path.join(here,'forward/wp_u3_process_publication.sqlinc'),s);
const schemaFile='supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql',schema=raw(commit,schemaFile);const statements=scanManagedSql(schema.toString().replaceAll('\r\n','\n')).statements;const text=schema.toString().replaceAll('\r\n','\n');const schemaPins=[];
for(const table of ['weekly_source_row_timesheet_lineages','weekly_source_row_resolutions','weekly_source_final_snapshot_lines']){const p=statements.find(st=>new RegExp('create table public\\.'+table+'\\s*\\(','i').test(text.slice(st.start,st.end)));assert(p,table);const definition=text.slice(p.start,p.end),saved='input/'+table+'.sqlinc';fs.writeFileSync(path.join(here,saved),definition);schemaPins.push({table,source_commit:commit,source_file:schemaFile,source_file_sha256:sha(schema),saved_file:saved,definition_sha256:sha(definition)});}
fs.writeFileSync(path.join(here,'COMPLETE_FORWARD_AND_REVERSE.json'),JSON.stringify({source_only:true,prior_finding:'P1-PROCESS-LINEAGE-SCHEMA-AND-EVENT-COVERAGE',SQL_executed:false,source_commit:commit,source_path:file,input:'input/wp_u3_process_publication.sqlinc',output:'forward/wp_u3_process_publication.sqlinc',before_sha256:sha(b),after_sha256:sha(s),hunks,whole_reverse_equal:true,schema_pins:schemaPins},null,2)+'\n');console.log(JSON.stringify({source_only:true,whole_owner:1,reversible_hunks:3,exact_native_schema_tables:3,SQL_executed:false}));
