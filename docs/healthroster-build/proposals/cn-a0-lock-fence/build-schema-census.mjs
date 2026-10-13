import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];assert.ok(root);
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const read=p=>fs.readFileSync(path.join(root,p));
const contractFile='supabase/release/current-contract.json',bytes=read(contractFile),c=JSON.parse(bytes);
const requestFile='supabase/migrations/13102026_1127_healthroster_timesheet_correction_requests_v1.sql',request=read(requestFile);
const assignmentFile='supabase/migrations/13102026_1133_weekly_source_rule_sets_schema.sql',assignment=read(assignmentFile);
const fields=new Map(c.relations.map(r=>[r.schema+'.'+r.name,new Set((r.columns||[]).map(v=>v.name))]));
fields.set('private.timesheet_correction_requests',new Set([...request.toString().matchAll(/^\s{2}([a-z_]\w*)\s+(?:uuid|text|bigint|integer|date|jsonb|timestamptz)\b/gm)].map(m=>m[1])));
assert.match(assignment.toString(),/source_group_id uuid not null references public\.weekly_source_groups\(id\)/i);
fields.set('public.weekly_source_group_rule_assignments',new Set(['id','source_group_id']));
const explicit={
 'private.timesheet_correction_requests':['id','state','origin','reversed_financial_id','frozen_original_json','candidate_id','client_id','contract_id','root_timesheet_id','reversed_timesheet_id','replacement_timesheet_id','replacement_week_id'],
 'public.timesheets':['timesheet_id','candidate_workflow_id','booking_id','contract_id'],
 'public.weekly_source_group_clients':['source_group_id','client_id','id'],'public.weekly_source_groups':['id'],
 'public.clients':['id'],'public.candidates':['id','umbrella_id'],'public.umbrellas':['id'],'public.tms_users':['id'],
 'public.contracts':['id'],'public.weekly_source_group_rule_assignments':['source_group_id','id'],
 'public.weekly_source_client_policies':['client_id','id'],'public.client_settings':['client_id','id'],
 'public.candidate_submission_workflows':['target_timesheet_id','anchor_timesheet_id','id','contract_week_id'],
 'public.contract_weeks':['timesheet_id','id'],'public.candidate_approval_requests':['workflow_id','id'],
 'public.candidate_submission_components':['workflow_id','timesheet_id','id'],'public.timesheets_financials':['timesheet_id','id'],
 'public.nhsp_shifts':['timesheet_id','id'],'public.weekly_source_row_timesheet_lineages':['timesheet_id','id'],
 'public.candidate_expense_components':['owning_timesheet_id','workflow_id','expense_component_id'],
 'public.timesheet_evidence':['timesheet_id','id'],'public.timesheet_validations':['timesheet_id','id','status','reason_code','validated_at_utc','updated_at'],
 'public.invoice_lines':['timesheet_id','id']
};
const checks=Object.entries(explicit).map(([relation,columns])=>({relation,columns,missing:columns.filter(v=>!fields.get(relation)?.has(v)),
 source:relation.startsWith('private.')?requestFile:relation==='public.weekly_source_group_rule_assignments'?assignmentFile:contractFile}));
assert.deepEqual(checks.filter(r=>r.missing.length),[]);
const fence=fs.readFileSync(path.join(here,'13102026_2034_a0_native_lock_fence.sqlinc'),'utf8');
const fk=[...fence.matchAll(/\('(public\.\w+)','(\w+)','(\w+)','(public\.\w+)','(\w+)'\)/g)].map(m=>{
 const [_,child,name,childColumn,parent,parentColumn]=m;
 const table=c.relations.find(r=>r.schema+'.'+r.name===child),constraint=table?.constraints.find(k=>k.name===name);
 if(child==='public.weekly_source_group_rule_assignments')return {child,name,childColumn,parent,parentColumn,source:assignmentFile,installed:false};
 assert.ok(constraint&&constraint.type==='f'&&constraint.validated&&!constraint.deferrable,name);
 assert.ok(fields.get(child).has(childColumn)&&fields.get(parent).has(parentColumn));
 assert.ok(constraint.definition.includes('('+childColumn+')')&&constraint.definition.includes(parent.split('.')[1]+'('+parentColumn+')'),name);
 return {child,name,childColumn,parent,parentColumn,source:contractFile,definition:constraint.definition,installed:false};
});assert.equal(fk.length,16);
fs.writeFileSync(path.join(here,'SCHEMA_SOURCE_CENSUS.json'),JSON.stringify({
 contract:'HEALTHROSTER_A0_LOCK_SOURCE_SCHEMA_CENSUS_V1',status:'SOURCE_NAMES_ONLY_NOT_COMPILED_OR_INSTALLED',
 inputs:[{path:contractFile,sha256:sha(bytes)},{path:requestFile,sha256:sha(request)},{path:assignmentFile,sha256:sha(assignment)}],
 explicitNativeFieldChecks:checks,exactFkSourceChecks:fk,
 noTimesheetForeignKey:['public.timesheet_validations.timesheet_id','public.invoice_lines.timesheet_id'],
 physicalTimesheets:{candidate_id:fields.get('public.timesheets').has('candidate_id'),family_booking_id:fields.get('public.timesheets').has('family_booking_id'),booking_id:true,contract_id:true},
 limitation:'Reviewed explicit fields of the two new owners against exact current generated contract/new source migrations. Function-return record fields are bound to exact native reader bodies in WRITER_LOCK_CENSUS. This is not a PostgreSQL syntax/enum/catalogue/RI/role proof. Root must qualify every native dependency and all five writer holds before exposure.'
},null,2)+'\n');console.log(JSON.stringify({explicitRelations:checks.length,fields:checks.reduce((n,r)=>n+r.columns.length,0),namedFkChecks:fk.length,missing:0}));
