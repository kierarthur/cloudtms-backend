// Source-only composition of the bounded WP-PP changes onto the published TEST.
// No database, network, secret or deployment operation is available here.
import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {inventory, repoRoot} from './cloudtms-db-release-lib.mjs';
import {definerInventory, routineDefinitions} from './bpay-next-final-definers.mjs';
import {scanManagedSql, digest, canonical} from './cloudtms-managed-release-sql.mjs';

const predecessor='07e66fb2b2742fbe634a41582f2ad3e004eed3b9';
const original='e0a02bb5f86b1fb2134e540bbf0ccc52f6feec92';
const proposed='547c74da5f0a05b09297c042bc7f3cc0c0105b85';
const ppFile='supabase/repeatable/13102026_1113_weekly_source_protected_pay_authority_v1.sql';
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:repoRoot,encoding:'utf8',maxBuffer:64000000,windowsHide:true});
const readGit=(commit,file)=>canonical(git(['show',`${commit}:${file}`]));
const definitions=(source,file)=>scanManagedSql(source).statements.flatMap(row=>{
 const sql=source.slice(row.start,row.end), names=routineDefinitions(sql,file);
 if(names.length>1)throw Error('MULTIPLE_DEFINITIONS_IN_STATEMENT');
 return names.map(signature=>({signature,sql,start:row.start,end:row.end}));
});
if(git(['rev-parse','HEAD']).trim()!==predecessor)throw Error('SOURCE_PREDECESSOR_DRIFT');
const out=path.join(repoRoot,'docs/healthroster-build/install1-source');
fs.mkdirSync(out,{recursive:true});
const pp=readGit(proposed,ppFile), selected=definitions(pp,ppFile);
if(selected.length!==16)throw Error('ORIGINAL_PP_DEFINITION_CENSUS_DRIFT');
const current=inventory(), final=definerInventory(current), rows=[];
for(const definition of selected){
 const filename=definition.signature.replace(/[^a-z0-9]+/g,'_')+'.sql';
 const currentRow=final.routines.get(definition.signature)?.at(-1);
 if(definition.signature==='private.weekly_source_pay_actor_allowed_v1(uuid)'){
  if(currentRow)throw Error('NEW_HELPER_ALREADY_IN_SOURCE');
  rows.push({signature:definition.signature,new:true,proposedDefinitionSha256:digest(definition.sql)});
  continue;
 }
 const sourceComments=[...pp.slice(0,definition.start).matchAll(/^-- Current TEST source: (\S+)$/gm)];
 const oldFile=sourceComments.at(-1)?.[1];
 if(!oldFile||!currentRow)throw Error('ROUTINE_SOURCE_MISSING:'+definition.signature);
 const old=definitions(readGit(original,oldFile),oldFile).filter(row=>row.signature===definition.signature);
 const bankSource=readGit(predecessor,currentRow.physical);
 if(canonical(fs.readFileSync(path.join(repoRoot,currentRow.physical),'utf8'))!==bankSource)throw Error('CURRENT_SOURCE_DIRTY');
 const bank=definitions(bankSource,currentRow.physical).filter(row=>row.signature===definition.signature);
 if(old.length!==1||bank.length!==1)throw Error('ROUTINE_SOURCE_AMBIGUOUS');
 const folder=path.join(out,'definitions');fs.mkdirSync(folder,{recursive:true});
 for(const [suffix,sql] of [['original',old[0].sql],['pp',definition.sql],['current',bank[0].sql]])fs.writeFileSync(path.join(folder,filename+'.'+suffix),sql+'\n');
 rows.push({signature:definition.signature,originalPhysical:oldFile,currentFinal:currentRow,
  originalDefinitionSha256:digest(old[0].sql),proposedDefinitionSha256:digest(definition.sql),
  currentDefinitionSha256:digest(bank[0].sql),currentEqualsOriginal:bank[0].sql===old[0].sql,
  completeDefinitionFiles:['original','pp','current'].map(suffix=>'docs/healthroster-build/install1-source/definitions/'+filename+'.'+suffix)});
}
const result={status:'SOURCE_INPUTS_ONLY_NOT_COMPOSED_OR_INSTALLABLE',predecessor,original,proposed,
 inventory:{migrations:current.migrations.length,repeatables:current.repeatables.length},
 originalPpRootSha256:digest(pp),rows,
 dynamicPatchReview:'Source final-definer scan does not replace canonical pg_get_functiondef and dynamic-patch verification. Read every controlling root and exact staged PG17 seal before installation.'};
fs.writeFileSync(path.join(out,'PP_CURRENT_OWNER_COMPOSITION_INPUTS.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({status:result.status,...result.inventory,routines:rows.length,
 changedExistingOwners:rows.filter(row=>!row.new&&!row.currentEqualsOriginal).map(row=>({signature:row.signature,physical:row.currentFinal.physical,root:row.currentFinal.root}))}));

if(process.argv.includes('--compose')){
 const ppNew='supabase/repeatable/14102026_0044_weekly_source_protected_pay_authority_v2.sql';
 const merged=[];
 for(const definition of selected){
  const row=rows.find(row=>row.signature===definition.signature);
  if(row.new){merged.push(definition.sql);continue;}
  const [oldFile,ppInput,currentFile]=row.completeDefinitionFiles.map(file=>path.join(repoRoot,file));
  const sql=git(['merge-file','-p',currentFile,oldFile,ppInput]).trimEnd();
  if(/^(?:<{7}|={7}|>{7})/m.test(sql))throw Error('SQL_COMPOSITION_CONFLICT');
  const signed=definitions(sql,ppNew);
  if(signed.length!==1||signed[0].signature!==row.signature)throw Error('SQL_COMPOSITION_SIGNATURE_DRIFT');
  merged.push(sql);row.composedDefinitionSha256=digest(sql);
 }
 const aclStart=pp.indexOf('alter function private.weekly_source_pay_actor_allowed_v1(uuid) owner to postgres;');
 if(aclStart<0)throw Error('PP_ACL_FINISH_MISSING');
 const newSql='-- WP-PP current TEST composition; all Banking native hooks retained.\n'
  +'-- Predecessor '+predecessor+'; original PP source '+proposed+'.\n'
  +'\\set ON_ERROR_STOP on\nbegin;\n\n'+merged.join('\n\n')+'\n\n'+pp.slice(aclStart);
 const edits=[];
 function stage(file,after){
  const full=path.join(repoRoot,file),before=fs.existsSync(full)?canonical(fs.readFileSync(full,'utf8')):null;
  if(before!==null&&before!==readGit(predecessor,file))throw Error('COMPOSITION_TARGET_NOT_BASE:'+file);
  edits.push({file,before,after});
 }
 stage(ppNew,newSql);
 function hunks(file){
  const diff=git(['diff','--unified=3',original,proposed,'--',file]);
  return diff.split(/(?=^@@ )/m).slice(1).map(chunk=>({header:chunk.slice(0,chunk.indexOf('\n')),
   lines:chunk.slice(chunk.indexOf('\n')+1).split('\n').filter(line=>/^[ +\-]/.test(line))}));
 }
 function applyHunks(file,chosen){
  let source=readGit(predecessor,file);const applied=[];
  for(const hunk of chosen){
   const oldText=hunk.lines.filter(line=>line[0]!=='+' ).map(line=>line.slice(1)).join('\n')+'\n';
   const newText=hunk.lines.filter(line=>line[0]!=='-' ).map(line=>line.slice(1)).join('\n')+'\n';
   const position=source.indexOf(oldText);
   if(position<0||source.indexOf(oldText,position+1)>=0)throw Error('WORKER_HUNK_NOT_UNIQUE:'+file+':'+hunk.header);
   source=source.slice(0,position)+newText+source.slice(position+oldText.length);
   applied.push({header:hunk.header,beforeSha256:digest(oldText),afterSha256:digest(newText)});
  }
  stage(file,source);return applied;
 }
 const indexFile='broker/src/index.js';
 const indexHunks=hunks(indexFile).filter(hunk=>{
  const text=hunk.lines.join('\n');
  return text.includes("+import { isActivePaymentActor }")
   || /@@.*(?:handleTimesheetDetails|handleTimesheetUnauthorise|attachWeeklySourceOfficeTimesheetPresentation|handleTimesheetBulkAuthoriseContext|buildTimesheetLifecyclePatchFromMutation|requireUser|handleTimesheetAuthoriseGeneric)/.test(hunk.header)
   || text.includes('+async function weeklySourceScopedLifecycleActor(')
   || text.includes(' async function handleTimesheetAuthoriseGeneric(');
 });
 // Context-three combines the seventeen context-zero edit regions into fourteen
 // hunks; the Authorise entry's displayed header is the preceding tsfin owner.
 if(indexHunks.length!==14)throw Error('PP_INDEX_HUNK_CENSUS_DRIFT:'+indexHunks.length);
 const routeFile='broker/src/weekly-source/routes.js';
 const routeHunks=hunks(routeFile).filter(hunk=>hunk.lines.some(line=>line.startsWith('+')&&(
  line.includes("isActivePaymentActor")||line.includes('PAY_VIEW_COMMANDS')||line.includes('user.payView')
  ||line.includes('const user = await dependencies.requireUser(env, req, []);'))));
 for(const hunk of routeHunks){
  hunk.lines=hunk.lines.filter(line=>line!=="+import { validateRuleSetValues } from './rule-set-validation.js';");
  if(hunk.lines.includes("+import { isActivePaymentActor } from '../payment-actor-permission.js';")){
   // Banking added independent imports after this exact existing import.
   // Insert only the PP import; do not require/rewrite its adjacent new imports.
   hunk.lines=[" import { bpayBusyEnvelope } from '../bpay-busy.js';",
    "+import { isActivePaymentActor } from '../payment-actor-permission.js';"];
  }
  if(hunk.header.includes('handleCommand(')){
   // Preserve Banking's entire new LATER_CHANGE_DECISION owner below this
   // unchanged request boundary. The PAY allowlist still gates before dispatch.
   const firstAddition=hunk.lines.findIndex(line=>line.startsWith('+'));
   hunk.lines=[...hunk.lines.slice(0,firstAddition),...hunk.lines.filter(line=>line.startsWith('+'))];
  }
 }
 if(routeHunks.length!==6)throw Error('PP_ROUTE_HUNK_CENSUS_DRIFT:'+routeHunks.length);
 const worker={index:applyHunks(indexFile,indexHunks),routes:applyHunks(routeFile,routeHunks)};
 for(const file of ['broker/src/payment-actor-permission.js','tests/payment-actor-permission.test.js','tests/weekly-source/weekly-source-pay-view.test.mjs'])stage(file,readGit(proposed,file));
 stage('tests/weekly-source/weekly-source-pay-authority-sql.test.mjs',readGit(proposed,'tests/weekly-source/weekly-source-pay-authority-sql.test.mjs').replaceAll('13102026_1113_weekly_source_protected_pay_authority_v1.sql',path.basename(ppNew)));
 const verifier='supabase/verification/13102026_1126_weekly_source_protected_pay_authority_verification.sql';
 stage(verifier,readGit(proposed,verifier));
 const releaseFile='supabase/release/current-release.json',release=JSON.parse(readGit(predecessor,releaseFile));
 for(const key of ['verificationFiles','newVerificationFiles']){
  if(!Array.isArray(release[key])||release[key].includes(verifier))throw Error('PP_VERIFIER_REGISTRATION_DRIFT');
  release[key].push(verifier);
 }
 stage(releaseFile,JSON.stringify(release,null,2)+'\n');
 for(const file of ['supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql','supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql']){
  const source=readGit(predecessor,file),anchor="    ('private.weekly_source_local_preauthorisation_write_allowed_v1(uuid,jsonb)'),";
  const verifierAnchor="    'private.weekly_source_local_preauthorisation_write_allowed_v1(uuid,jsonb)',";
  const exact=file.includes('/repeatable/')?anchor:verifierAnchor;
  if(!source.includes(exact)||source.indexOf(exact)!==source.lastIndexOf(exact))throw Error('SERVICE_CONTRACT_ANCHOR_DRIFT:'+file);
  const line=file.includes('/repeatable/')?"    ('private.weekly_source_pay_actor_allowed_v1(uuid)'),\n":"    'private.weekly_source_pay_actor_allowed_v1(uuid)',\n";
  stage(file,source.replace(exact,line+exact));
 }
 const registryFile='docs/bpay-next/final-definer-closures.json',registry=JSON.parse(readGit(predecessor,registryFile));
 for(const row of rows){
  const chain=registry.chains.find(chain=>chain.signature===row.signature);
  if(chain){
   chain.allowedPhysical=[...new Set([...(chain.allowedPhysical??[]),ppNew])];
   chain.predecessors=[...new Set([...(chain.predecessors??[]),...(chain.finalClosures??[]),chain.finalRoot])];
   chain.finalRoot=ppNew;chain.finalPhysical=ppNew;chain.finalClosures=[ppNew];
   chain.owner=(chain.owner??'')+'; WP-PP bounded actor/view successor preserving current native body';
  }else{
   const sources=final.routines.get(row.signature)??[];
   registry.chains.push({signature:row.signature,finalRoot:ppNew,finalPhysical:ppNew,
    allowedPhysical:[...new Set([...sources.map(source=>source.physical),ppNew])],
    predecessors:[...new Set(sources.map(source=>source.root))],finalClosures:[ppNew],
    owner:'WP-PP bounded actor/view successor; no change to native financial/refusal/lock authority',baselineSource:row.new?'new':predecessor});
  }
 }
 stage(registryFile,JSON.stringify(registry,null,2)+'\n');
 // All source checks/merges finish before a runtime file is written.
 for(const edit of edits){const full=path.join(repoRoot,edit.file);fs.mkdirSync(path.dirname(full),{recursive:true});fs.writeFileSync(full,edit.after);}
 const receipt={status:'SOURCE_COMPOSED_NATIVE_AND_RELEASE_GATES_PENDING',predecessor,original,proposed,ppNew,rows,worker,
  edits:edits.map(({file,before,after})=>({file,beforeSha256:before===null?null:digest(before),afterSha256:digest(after)})),
  gate:'Source merge only; no database/contract/full rehearsal/publication/activation/API proof.'};
 fs.writeFileSync(path.join(out,'PP_SOURCE_COMPOSITION_RECEIPT.json'),JSON.stringify(receipt,null,2)+'\n');
 console.log(JSON.stringify({status:receipt.status,files:edits.length,indexHunks:worker.index.length,routeHunks:worker.routes.length,ppNew}));
}
