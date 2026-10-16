import {execFileSync} from 'node:child_process';
import {writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(base,'../../../..');
const commit='90ffcf3f40644280fcdb5b791a62a86613896d6e',inputs=[];
const sha=s=>createHash('sha256').update(s).digest('hex');
function source(file){const text=execFileSync('git',['show',`${commit}:${file}`],{cwd:root,encoding:'utf8',maxBuffer:8e6}).replace(/\r\n/g,'\n');
 inputs.push({path:file,source_commit:commit,sha256_normalized_lf:sha(text)});return text;}
function extract(text,name){const start=text.indexOf(`create or replace function ${name}(`),end=text.indexOf('$function$;',text.indexOf('as $function$',start));
 if(start<0||end<start)throw Error('Complete current owner '+name);return text.slice(start,end+12)+'\n';}
function once(text,old,next){if(text.split(old).length!==2)throw Error('Unique native source anchor '+old.slice(0,130));return text.replace(old,next);}
let outreach=extract(source('supabase/repeatable/13102026_1438_weekly_source_protection_queries/15_import_outreach.sqlinc'),
 'private.weekly_source_import_outreach_v1');
outreach=once(outreach,'  for v_candidate in',`  perform private.weekly_contact_source_submission_lock_v1(v_publication.source_cycle_id,null,'[]',v_publication.id);
  for v_candidate in`);
outreach=once(outreach,'    -- One candidate/cycle request owner. Serialize import/recheck attempts\n    -- before inspecting memberships so concurrent imports cannot duplicate it.',
 '    -- Existing Candidate/cycle serialization follows deployment/sorted scope\n    -- locks; actual request/generation ownership is now cohort-local.');
outreach=once(outreach,'v_result:=private.weekly_source_submission_request_start_v1(',
 'v_result:=private.weekly_contact_source_submission_partition_v1(');
let publication=extract(source('supabase/repeatable/13102026_1755_weekly_source_publication_comparison_family_union_v1.sql'),
 'public.weekly_source_projection_publish_atomic_v1');
publication=once(publication,'  perform private.weekly_source_pay_query_admit_v2();',
 `  -- Current native discovered publication rows supply only lock scope, not
  -- invented CURRENT evidence. Acquire contact deployment/sorted scopes before
  -- existing native financial-family, Source group, cycle or publication locks.
  perform private.weekly_contact_source_submission_lock_v1(v_cycle.id,null,'[]',v_publication.id);
  perform private.weekly_source_pay_query_admit_v2();`);
const output=(outreach+'\n'+publication).trimEnd()+'\n';
writeFileSync(path.join(base,'forward/39_native_submission_callers.sqlinc'),output);
writeFileSync(path.join(base,'SUBMISSION_CALLER_OWNER_INPUTS.json'),JSON.stringify({version:'CONTACT_SUBMISSION_CALLER_OWNER_INPUTS_V1',inputs,
 output:{path:'forward/39_native_submission_callers.sqlinc',sha256:sha(output)},
 public_start_wrapper:'Unchanged native wrapper and public single-request CREATED/UNCHANGED receipt; NOT redefined by this package.',
 qualification:'Finite complete Health source owners; Root must mount against actual current body/policy guards; PostgreSQL/native/concurrency NOT_RUN.'},null,2)+'\n');
console.log(JSON.stringify({complete_owners:2,source_only:true}));
