import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const construction=path.dirname(fileURLToPath(import.meta.url));
const own=path.resolve(construction,'../agency-check-only-protection-lineage-source-20261014');
const root=path.resolve(process.argv[2]||'');
const expected='5db7abee7925fffbeaf7c4e885cce809980fa079';
if(fs.existsSync(own))throw new Error('PROPOSAL_EXISTS');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=(...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...a],{cwd:root,maxBuffer:48*1024*1024});
if(git('rev-parse','HEAD').toString().trim()!==expected)throw new Error('INPUT_PIN');
const out=(rel,b)=>{const p=path.join(own,rel);fs.mkdirSync(path.dirname(p),{recursive:true});fs.writeFileSync(p,b);};
function copyDir(from,to){for(const e of fs.readdirSync(from,{withFileTypes:true})){if(e.isDirectory())copyDir(path.join(from,e.name),to+'/'+e.name);else out(to+'/'+e.name,fs.readFileSync(path.join(from,e.name)));}}
copyDir(path.join(construction,'protection-lineage-construction'),'lineage');
out('native/07_check_only_shift_protection_store.sqlinc',fs.readFileSync(path.join(construction,'native/07_check_only_shift_protection_store.sqlinc')));
const inputs=JSON.parse(fs.readFileSync(path.join(construction,'SOURCE_INPUTS.json')));
const primary=fs.readFileSync(inputs.owner_plan.path);if(sha(primary)!==inputs.owner_plan.raw_sha256)throw new Error('OWNER_PLAN_CHANGED');
for(const rel of ['primary_26_5.md.txt','primary_26_4.md.txt'])out('evidence/'+rel,fs.readFileSync(path.join(construction,'source-evidence',rel)));
const tables=[
['public.weekly_exceptional_pay_target_families','supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql','Full Source/C1 target ownership is canonical Timesheet family authority, not independent check-only Candidate/work_event protection intent.'],
['public.weekly_exceptional_pay_generations','supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql','Complete native target generations/components are actual computed Source/C1 pay positions; they are not mutable original signed-claim protection intent.'],
['public.weekly_exceptional_pending_reconciliation_targets','supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql','Existing target reconciliation binds actual approved native generation/target; it cannot be repurposed as a standalone check-only waiting-shift intent keyed independently of T0.'],
['private.bpay_next_work','supabase/migrations/03102026_1324_banking_pay_next_core.sql','Banking work represents obligations/disposition. Its original Timesheet/booking ownership is retained; storing Office protection intent here would invent Banking authority.'],
['private.bpay_next_work_revision','supabase/migrations/03102026_1324_banking_pay_next_core.sql','Immutable native financial approval revision, with physical Timesheet and computed amount/detail authority. This proposal produces no such approval or money.'],
['public.candidate_submission_workflows','supabase/migrations/07082026_2055_candidate_app_foundation_schema.sql','Actual immutable worker submission is the claim evidence. Office Protect/Amend/Unprotect may not mutate it or relabel it as Office intent.']
];
const closest=[];
for(const [name,rel,reason]of tables){const b=fs.readFileSync(path.join(root,rel));if(!b.equals(git('show',expected+':'+rel)))throw new Error('SAVED_GIT:'+rel);const s=b.toString();const rx=new RegExp('create\\s+table\\s+(?:if\\s+not\\s+exists\\s+)?'+name.replace('.','\\.')+'\\s*\\(','ig'),matches=[...s.matchAll(rx)];if(matches.length!==1)throw new Error('TABLE_UNIQUE:'+name);const at=matches[0].index,tail=s.slice(at),stop=tail.indexOf('\n);');if(stop<0)throw new Error('TABLE_END:'+name);const ddl=tail.slice(0,stop+4);const evidence='evidence/'+name.replace('.','_')+'.table.sql.txt';out(evidence,ddl);closest.push({name,source_path:rel,source_raw_sha256:sha(b),definition_sha256:sha(ddl),source_line:s.slice(0,at).split('\n').length,evidence,reason});}
const reverse=JSON.parse(fs.readFileSync(path.join(own,'lineage/FULL_NATIVE_AND_WORKER_REVERSE.json')));
for(const row of reverse.rows){const b=fs.readFileSync(path.join(root,row.source_path));if(!b.equals(git('show',expected+':'+row.source_path))||sha(b)!==row.source_raw_sha256)throw new Error('CURRENT_OWNER:'+row.source_path);}
out('CLOSEST_STORE_AND_CURRENT_SOURCE_INPUTS.json',JSON.stringify({schema:'AGENCY_CHECK_ONLY_PROTECTION_LINEAGE_SOURCE_INPUTS_V1',source_only:true,input_git_commit:expected,owner_plan:inputs.owner_plan,closest_stores:closest,current_replaced_owners:reverse.rows.map(({name,source_path,source_raw_sha256,before_sha256,after_sha256,forward_file_sha256,forward_path})=>({name,source_path,source_raw_sha256,before_sha256,after_sha256,forward_file_sha256,forward_path:'lineage/'+forward_path})),new_private_table:'private.weekly_source_check_only_shift_protections',new_owner_private_signature:'private.weekly_source_check_only_protection_identity_guard_v1()',new_public_signature_count:0,banking_obligation_write:false,native_protection_actions_present:false,native_nx_ai_6_present:false,native_compiled_contract_sha256:null},null,2)+'\n');
out('prepare-protection-lineage-review.mjs',fs.readFileSync(fileURLToPath(import.meta.url)));
console.log(JSON.stringify({proposal:own,closest_stores:closest.length,replaced_owners:reverse.rows.length}));
