import {readFileSync,writeFileSync,readdirSync} from 'node:fs';
import {createHash} from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),file=path.join(base,'REGISTER_DELTA_WIP.json');
const register=JSON.parse(readFileSync(file,'utf8'));const sha=s=>createHash('sha256').update(s).digest('hex');
register.functions=[];
for(const name of readdirSync(path.join(base,'forward')).filter(name=>name.endsWith('.sqlinc')&&!name.endsWith('_acl.sqlinc')).sort()){
 const source='forward/'+name,sql=readFileSync(path.join(base,source),'utf8');
 for(const match of sql.matchAll(/create or replace function ((public|private)\.[a-z0-9_]+)\(([\s\S]*?)\)\s*returns([\s\S]*?)as \$function\$/gi)){
  const args=match[3].trim()?match[3].split(',').map(arg=>arg.trim().split(/\s+/)[1]).join(','):'';
  register.functions.push({signature:match[1]+'('+args+')',source,sha256:sha(sql),schema:match[2],
   owner:'audited logical postgres mapped only by root managed-release owner adapter',
   security_definer:/security definer/i.test(match[4]),fixed_search_path:/set search_path/i.test(match[4]),
   public:false,anon:false,authenticated:false,service_role_execute:match[2]==='public'});
 }
}
register.function_signature_count=register.functions.length;
register.deferred_integrity_triggers=[
 {table:'weekly_contact_source_intent_captures',trigger:'weekly_contact_source_intent_captures_lineage',source:'forward/19_capture_and_rebase_integrity.sqlinc'},
 {table:'weekly_contact_source_intent_capture_members',trigger:'weekly_contact_source_intent_capture_members_lineage',source:'forward/19_capture_and_rebase_integrity.sqlinc'},
 {table:'weekly_contact_source_due_event_rebases',trigger:'weekly_contact_source_due_event_rebases_lineage',source:'forward/19_capture_and_rebase_integrity.sqlinc'}];
register.stateful_row_guards=[{table:'weekly_contact_checking_scope_versions',trigger:'weekly_contact_checking_version_guard'},
 {table:'weekly_contact_source_ordinal_owners',trigger:'weekly_contact_source_ordinal_owner_guard'},
 {table:'weekly_contact_recheck_progress',trigger:'weekly_contact_recheck_progress_guard'}];
register.existing_table_identity_delta={source:'one-time/03_submission_cohort_identity.sqlinc',
 table:'weekly_timesheet_submission_requests',new_table:false,
 prior_generation_key:['source_cycle_id','candidate_id','request_generation'],
 generation_constraint:'weekly_timesheet_submission_requests_cohort_generation_uq',
 generation_key:['source_cycle_id','candidate_id','candidate_cohort_id','request_generation'],
 active_index:'weekly_timesheet_submission_requests_active_uq',prior_active_key:['source_cycle_id','candidate_id'],
 active_key:['source_cycle_id','candidate_id','candidate_cohort_id'],active_states:['READY','ACTIVE','OVERDUE','PARTLY_SUBMITTED'],
 existing_history_rewrite:false,native_qualification:'NOT_RUN',
 deferred_triggers:[['weekly_timesheet_submission_requests','weekly_contact_submission_identity'],
  ['weekly_timesheet_submission_request_memberships','weekly_contact_submission_member_identity'],
  ['weekly_candidate_cohorts','weekly_contact_submission_cohort_identity'],
  ['weekly_candidate_outreach_generations','weekly_contact_submission_generation_identity']].map(([table,trigger])=>({table,trigger,
   function:'private.weekly_contact_submission_identity_deferred_v1()',source:'forward/36_submission_identity_and_guard.sqlinc'}))};
writeFileSync(file,JSON.stringify(register,null,2)+'\n');
console.log(JSON.stringify({tables:register.tables.length,function_signatures:register.functions.length,status:register.status}));
