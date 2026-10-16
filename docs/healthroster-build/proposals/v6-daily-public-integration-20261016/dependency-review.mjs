// Exact current I2 + reviewed delivered pack only. No Banking worktree reads.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-install2-14102026';
const pack='C:/tmp/healthroster-handover/HealthRoster_Builder_Pack_16102026/NHSP_AUTHORISATION_AUDIT_15102026_HANDOFF_V6';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const wanted=['private.daily_rate_correction_review_v1','private.daily_rate_correction_accept_v1','private.daily_rate_correction_result_v1','private.bpay_next_daily_rate_correction_publish_v1'];
const relevant=execFileSync('rg',['-l','daily_rate_correction_(review|accept|result|publish)_v1','supabase/repeatable'],{cwd:root,encoding:'utf8',windowsHide:true}).trim().split(/\r?\n/).filter(Boolean);
const declarations=[];
for(const file of relevant){const text=fs.readFileSync(path.join(root,file),'utf8');for(const m of text.matchAll(/create\s+(?:or\s+replace\s+)?function\s+((?:private|public)\.[a-z0-9_]+)\s*\(/gi))if(wanted.includes(m[1]))declarations.push({name:m[1],file});}
const contracts=['Health_CODE_AUDIT.md','BANKING_CODE_AUDIT.md','HISTORY_RECONCILIATION_CONTRACT.md','OWNERSHIP.md',
 'WORKING_PREPARATION/DAILY_RATE_CORRECTION_CONTRACT.md','WORKING_PREPARATION/DAILY_RATE_CORRECTION_CONTRACT.json',
 'WORKING_PREPARATION/MODULE_AND_CLOSURE_TARGETS.json','WORKING_PREPARATION/BATCH_DECISION_INTEGRATION_DELTA_PLAN.json'];
const packInputs=contracts.map(file=>{const text=fs.readFileSync(path.join(pack,file),'utf8');return{file,sha256:sha(text),declaredSymbols:[...new Set(text.match(/(?:private\.)?(?:bpay_next_)?daily_rate_correction_[a-z0-9_]+_v1/g)??[])].sort()};});
if(declarations.length)throw Error('DAILY_APPROVED_DEPENDENCY_BASELINE_CHANGED_REVIEW_ACTUAL_BODY');
const out={scope:'REVIEWED_DELIVERY_PACK_AND_CURRENT_I2_ONLY',readOnly:true,
 searchedCurrentFiles:relevant.map(file=>({file,sha256:sha(fs.readFileSync(path.join(root,file)))})),wantedCurrentDefinitions:wanted,currentDefinitions:declarations,
 packInputs,contractualHealthDelegates:[{signature:'private.daily_rate_correction_review_v1(jsonb)',returns:'jsonb',body:'NOT_PRESENT_CURRENT_I2'},
 {signature:'private.daily_rate_correction_accept_v1(jsonb)',returns:'jsonb',body:'NOT_PRESENT_CURRENT_I2'},
 {signature:'private.daily_rate_correction_result_v1(uuid,uuid)',returns:'jsonb',body:'NOT_PRESENT_CURRENT_I2'}],
 contractualBankContribution:{signature:'private.bpay_next_daily_rate_correction_publish_v1(uuid,uuid)',returns:'jsonb',
  closedResult:['version:DAILY_RATE_PUBLICATION_V1','work_id','revision_id','command_id','agency_sequence','state:REGISTERED','summary_dto'],
  onceOnlyPath:'approval_route_v1 Daily arm -> publish_ordinary_from_financial_v1 -> publish_staged_revision_v1 -> existing POSITION_APPLY',
  currentI2Body:'NOT_PRESENT',native:'NOT_RUN'},
 missingBeforeFreshReady:['Actual native original/current paid/frozen/history basis and canonical SummaryDTO preflight for Daily',
 'Exact non-Contract native preflight/delegate ABI with lock/currentness/role behavior',
 'Complete Health immutable review/CAS/accept/result composers using that genuine basis',
 'Genuine once-only Daily publication producer and narrowly composed ordinary INSERT trigger guard',
 'Atomic revision admission/publication/invoice exclusive preparation/completion proof'],
 rejectedSubstitutes:['Contract cut/stage ABI relabelled Daily','RETIRED resolution_review/resolution_adopt','NoWork guessed as NOT_REQUIRED',
 'Nominal/zero money, fake cut/command/receipt, Ready from transport success','Copied pending Source/internal result cast as public Daily DTO'],
 sourceImplementationBoundary:'Public Health callers/route/sole generated decoder are concrete; fresh Daily economic composition remains HELD',native:'NOT_RUN'};
fs.writeFileSync(path.join(here,'DEPENDENCY_REVIEW.json'),JSON.stringify(out,null,2)+'\n');
console.log(JSON.stringify({status:'APPROVED_DAILY_DEPENDENCIES_TRACED',currentDefinitions:declarations.length,currentRelevantFiles:relevant.length,packContracts:packInputs.length,native:'NOT_RUN'}));
