// Finite offline source binding. No credential, HTTP, connector or database access.
import fs from'node:fs';import path from'node:path';import{fileURLToPath}from'node:url';import{execFileSync}from'node:child_process';import{createHash,webcrypto}from'node:crypto';import vm from'node:vm';
export const here=path.dirname(fileURLToPath(import.meta.url));
export const sha=b=>createHash('sha256').update(b).digest('hex');
export const census=JSON.parse(fs.readFileSync(here+'/CURRENT_SOURCE_CENSUS.json'));
export const recipe=JSON.parse(fs.readFileSync(here+'/../daily-health-genuine-fixture-recipe-20261017/RECIPE.json'));
export const raw=name=>{const row=census.functions.find(f=>f.name===name);if(!row)throw Error('EXACT_FUNCTION_REQUIRED');const b=fs.readFileSync(here+'/'+row.file);if(sha(b)!==row.sha256||b.length!==row.bytes)throw Error('FUNCTION_RAW_BYTES_CHANGED');return b.toString();};
export const offlineFunction=(name,dependencies={})=>new vm.Script('('+raw(name)+')').runInNewContext({fetch:()=>{throw Error('NETWORK_DISABLED_OFFLINE');},console:{log(){},warn(){},error(){}},...dependencies},{timeout:1000});
export function assertCurrentSource(){
 const physical=p=>fs.realpathSync.native(p).replaceAll('\\','/');
 if(physical(census.root)!=='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026'||!physical(process.cwd()).startsWith('D:/Development/GitHub'))throw Error('ACTUAL_D_PHYSICAL_ROOT_REQUIRED');
 const git=(...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',census.root,...args],{maxBuffer:32e6});
 const head=git('rev-parse','HEAD').toString().trim();
 if(physical(git('rev-parse','--show-toplevel').toString().trim())!==physical(census.root)
  ||physical(git('rev-parse','--absolute-git-dir').toString().trim())!=='D:/Development/GitHub/cloudtms-backend/.git/worktrees/healthroster-install2-14102026'
  ||physical(path.resolve(census.root,git('rev-parse','--git-common-dir').toString().trim()))!=='D:/Development/GitHub/cloudtms-backend/.git')throw Error('ROOT_GIT_PHYSICAL_BINDING_REQUIRED');
 for(const row of [...census.inputs,...census.rootDefinitionReceipts]){const b=fs.readFileSync(census.root+'/'+row.file);if(b.length!==row.bytes||sha(b)!==row.sha256||!b.equals(git('show',census.commit+':'+row.file))||!b.equals(git('show',head+':'+row.file)))throw Error('CURRENT_SAVED_COMMITTED_SOURCE_CHANGED:'+row.file);}
 for(const row of census.functions)raw(row.name);
 return {sourceParent:census.commit,currentCheckpoint:head,inputs:census.inputs.length,completeFunctions:census.functions.length,savedCommittedRawEqual:true,native:'NOT_RUN'};
}
export async function deriveFixture(){
 const names=['norm','sha256Hex','makeBookingId','ukLocalToUtcISO','londonDate','weekEndingSunday'];
 const timezone=fs.readFileSync(here+'/reference/worker/timezone.js.txt','utf8');
 const pure=new vm.Script(timezone+'\n'+names.map(raw).join('\n')+'\n({makeBookingId,ukLocalToUtcISO,weekEndingSunday})').runInNewContext({crypto:webcrypto,TextEncoder,Intl},{timeout:1000});
 const {mapCanonicalDailyScheduleToIso}=await import('./reference/current/daily-schedule-authority.js');
 const body=recipe.stages.find(s=>s.id==='S4').body;
 const schedule=mapCanonicalDailyScheduleToIso(body.schedule_json,{workedDateFallback:'',ukLocalToUtcISO:pure.ukLocalToUtcISO});
 if(schedule.error)throw Error(schedule.error);
 const bookingId=await pure.makeBookingId(body.candidate_id,schedule.ymd,body.hospital.toLowerCase(),body.ward.toLowerCase(),body.role.toLowerCase(),body.shift_label.toLowerCase());
 return {version:'DAILY_GENUINE_PURE_WORKER_FIXTURE_DERIVATION_V1',testType:'ACTUAL_PURE_WORKER_SOURCE_NO_NATIVE_OR_FINANCIAL_ORACLE',bookingId,
  schedule,weekEndingDate:pure.weekEndingSunday(schedule.ymd),candidateId:body.candidate_id,clientId:body.client_id,jobTitleNorm:body.role.toLowerCase(),canonicalRole:body.role.toUpperCase(),
  actualTimesheetId:null,actualFinancialId:null,actor:null,vacancy:'NOT_BOUND',native:'NOT_RUN'};
}
