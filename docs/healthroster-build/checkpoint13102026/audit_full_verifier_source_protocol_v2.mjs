import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';import {fileURLToPath,pathToFileURL} from 'node:url';
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-backend-13102026',out=path.dirname(fileURLToPath(import.meta.url)),commit='ab26a5ff22200be037d215b1100c969d6c0bae73';
const {canonical,digest,scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const {mapExecutableSqlSource}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-db-release-lib.mjs')));
const {neutraliseReviewedVerifierEnvelope,renderFullRehearsalUnits}=await import(pathToFileURL(path.join(root,'scripts/healthroster-full-rehearsal-protocol.mjs')));
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:root,encoding:'utf8',maxBuffer:32000000});
if(git(['ls-remote','origin','refs/heads/codex/healthroster-v2-backend-13102026']).trim().split(/\s+/)[0]!==commit)throw Error('AUDIT_SOURCE_REMOTE_DRIFT');
for (const file of ['scripts/cloudtms-managed-release-sql.mjs','scripts/cloudtms-db-release-lib.mjs','scripts/healthroster-full-rehearsal-protocol.mjs']) if(canonical(git(['show',commit+':'+file]))!==canonical(fs.readFileSync(path.join(root,file),'utf8')))throw Error('AUDIT_MODULE_SOURCE_DRIFT');
process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER='CURRENT_USER';
function closure(file,stack=[]){
 if(stack.includes(file)||file.split('/').includes('..')||! /^(?:supabase\/(?:verification|repeatable|release)\/|tests\/)[A-Za-z0-9_./-]+\.(?:sql|sqlinc)$/.test(file))throw Error('AUDIT_INCLUDE_SCOPE_REFUSED');
 const blob=git(['show',commit+':'+file]),saved=fs.readFileSync(path.join(root,file),'utf8');
 if(canonical(blob)!==canonical(saved))throw Error('AUDIT_SAVED_SOURCE_DRIFT');
 const source=mapExecutableSqlSource(canonical(blob),file),ordered=[{file,gitBlobSha256:digest(blob),canonicalSha256:digest(canonical(blob))}];
 const expanded=source.replace(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))\s*;?\s*$/gm,(_m,a,b,c)=>{
 const child=closure(path.posix.normalize(path.posix.join(path.posix.dirname(file),a??b??c)),[...stack,file]);ordered.push(...child.ordered);return child.expanded;});
 return {expanded,ordered};
}
const release=JSON.parse(git(['show',commit+':supabase/release/current-release.json'])),rows=[];
for(const [index,file]of release.verificationFiles.entries()){
 let inputs=[];try{
 const source=closure(file);inputs=source.ordered;const parsed=scanManagedSql(source.expanded),unit={file,kind:'verifier',sql:source.expanded,sqlHash:digest(source.expanded)};
 const removals=[...parsed.statements.filter(s=>['BEGIN','COMMIT','END','START','ABORT','PREPARE','ROLLBACK'].includes(s.tokens[0])&&!(s.tokens[0]==='ROLLBACK'&&s.tokens[1]==='TO')).map(s=>({start:s.start,command:s.tokens.join(' ')})),
 ...parsed.meta.filter(m=>/^\\set\s+ON_ERROR_(?:STOP|ROLLBACK)\b/i.test(m.text)).map(m=>({start:m.start,command:m.text.trim()}))].sort((a,b)=>a.start-b.start).map(x=>x.command);
 const hasReadonly=removals.includes('BEGIN READ ONLY'); if(hasReadonly && !(file==='supabase/verification/22092026_1052_weekly_source_operation_local_validation_v1.sql' && unit.sqlHash==='d63c766ee4a26eed5038b8c10840686eb007334761d2c770d82901e0352a4669'))throw Error('AUDIT_READ_ONLY_BODY_REVIEW_REQUIRED');
 const neutral=neutraliseReviewedVerifierEnvelope(unit,{sourceSqlHash:unit.sqlHash,removedCommands:removals,readOnlyBodyReviewed:hasReadonly&&file==='supabase/verification/22092026_1052_weekly_source_operation_local_validation_v1.sql'});
 const rendered=renderFullRehearsalUnits([neutral],{outerIsolation:'REPEATABLE READ'});rows.push({index,file,status:'SOURCE_ENVELOPE_RENDERABLE_NOT_SAFETY_OR_EXECUTION_APPROVAL',sourceSqlHash:unit.sqlHash,neutralSqlHash:neutral.sqlHash,envelope:neutral.envelope,isolationRequirements:rendered.isolationRequirements,statements:scanManagedSql(neutral.sql).statements.length,renderedSha256:rendered.sha256,inputs});
 }catch(e){const code=String(e.message).match(/^[A-Z][A-Z0-9_]+/)?.[0]??'AUDIT_FAILURE_DETAILS_WITHHELD';rows.push({index,file,status:'SOURCE_PROTOCOL_REVIEW_REQUIRED',error:code,inputs});}
}
const result={status:'SOURCE_ONLY_FULL_VERIFIER_PROTOCOL_CENSUS_NO_DATABASE_EXECUTION',sourceCommit:commit,outerIsolation:'REPEATABLE READ',isolationScope:'Explicit source packaging mode only; actual complete runner compatibility and effect review still required, including excluded source roots.',protocolSha256:digest(git(['show',commit+':scripts/healthroster-full-rehearsal-protocol.mjs'])),total:rows.length,renderable:rows.filter(r=>r.status.startsWith('SOURCE_ENVELOPE_RENDERABLE')).length,blocked:rows.filter(r=>r.error).length,scope:'Includes excluded verifier SOURCE only, without permission or implication that any may execute. Fresh complete effect/safety reviews and exact post-Banking source/installed plan remain required. This tool reads Git/saved source only; it has no database, secret or release executor.',verifiers:rows};
for(const dir of [out,path.join(root,'docs/healthroster-build/checkpoint13102026')])fs.writeFileSync(path.join(dir,'FULL_VERIFIER_SOURCE_PROTOCOL_CENSUS_V2.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({status:result.status,sourceCommit:commit,total:result.total,renderable:result.renderable,blocked:result.blocked,errors:rows.filter(r=>r.error).map(r=>({file:r.file,error:r.error}))}));
