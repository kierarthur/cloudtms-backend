import {execFileSync} from 'node:child_process';
import {writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const base=path.dirname(fileURLToPath(import.meta.url));
const commit='a934ee1d460b444939234e812babccb90dc3ddff';
const input='supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql';
const source=execFileSync('git',['show',`${commit}:${input}`],{encoding:'utf8',maxBuffer:8*1024*1024});
const start=source.indexOf('create or replace function private.weekly_source_query_resolved_decision_matches_v1(');
const end=source.indexOf('$function$;',source.indexOf('as $function$',start))+'$function$;'.length;
if(start<0||end<start)throw new Error('Native query resolved predicate missing');
const original=source.slice(start,end);const before="select coalesce((select latest.state='RESOLVED'";
const after="select private.weekly_source_pay_decision_contact_suppressed_v1(p_source_group_id,p_work_event_id,p_issue)\n    or coalesce((select latest.state='RESOLVED'";
if(original.split(before).length!==2)throw new Error('Native query predicate seam changed');
const changed=original.replace(before,after);
if(changed.replace(after,before)!==original)throw new Error('Native query whole reverse failed');
const output='forward/42_native_contact_reimport_match.sqlinc';
const sql=changed+'\nrevoke all on function private.weekly_source_query_resolved_decision_matches_v1(uuid,uuid,jsonb) from public,anon,authenticated,service_role;\n';
const sha=value=>createHash('sha256').update(value).digest('hex');
const schemaInput='supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql';
const schemaSource=execFileSync('git',['show',`${commit}:${schemaInput}`],{encoding:'utf8',maxBuffer:8*1024*1024});
writeFileSync(path.join(base,output),sql);
writeFileSync(path.join(base,'NATIVE_CONTACT_MATCH_RECEIPT.json'),JSON.stringify({version:'SOURCE_NATIVE_CONTACT_MATCH_RECEIPT_V1',
 input_commit:commit,input,input_sha256:sha(source),original_body_sha256:sha(original),output,output_sha256:sha(sql),edits:[{before,after}],
 existing_constraint_input:{commit,path:schemaInput,sha256:sha(schemaSource),relation:'public.weekly_discrepancy_incidents',
  constraint:'weekly_discrepancy_incidents_resolution_kind_check',added_literal:'SOURCE_PAY_DECISION',
  preserved_literals:['SOURCE_MATCHED','OFFICE_ACCEPTED_SYSTEM_HOURS','CANDIDATE_CORRECTED','MANAGER_CONFIRMED_SYSTEM_HOURS','RECORDED_NOT_WORKED','CORRECT_FINAL_SOURCE']},
 qualification:'SOURCE_REVERSE_ONLY_NATIVE_REQUIRED_NOT_RUN'},null,2)+'\n');
