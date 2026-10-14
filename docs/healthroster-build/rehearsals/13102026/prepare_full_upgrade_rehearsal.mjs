// Historical interrupted checkpoint. Not an executable resume plan.
throw new Error('ARCHIVED_REHEARSAL_PAUSED_REBASE_AND_SEND7_TURN_REQUIRED');
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {spawnSync,execFileSync} from 'node:child_process';
import {inventory,mapExecutableSqlSource} from '../../../../scripts/cloudtms-db-release-lib.mjs';
import {pendingInventory} from '../../../../scripts/cloudtms-managed-release.mjs';
import {canonical,digest,readManagedClosure,scanManagedSql} from '../../../../scripts/cloudtms-managed-release-sql.mjs';
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-backend-13102026',workspace='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub';
const commit='547c74da5f0a05b09297c042bc7f3cc0c0105b85',expectedTest='e0a02bb5f86b1fb2134e540bbf0ccc52f6feec92';
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:root,encoding:'utf8',maxBuffer:64000000});
const save=(name,data)=>fs.writeFileSync(new URL('./'+name,import.meta.url),JSON.stringify(data,null,2)+'\n');
process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER='CURRENT_USER';
function readOnly(sql){
 const secret=JSON.parse(fs.readFileSync(path.join(workspace,'cloudtms-backend/.codex-tmp/miget-postgres-service.secret.json'),'utf8'));
 const url=new URL(secret.connection_details.external.external_connection_url);url.pathname='/cloudtms_test_clone';
 const result=spawnSync('C:/Program Files/PostgreSQL/18/bin/psql.exe',[url.toString(),'-X','-q','-A','-t','-v','ON_ERROR_STOP=on','-v','VERBOSITY=sqlstate'],{cwd:root,encoding:'utf8',input:'begin read only;\n'+sql+'\nrollback;\n',windowsHide:true,maxBuffer:8000000,env:{...process.env,PGCONNECT_TIMEOUT:'20',PGOPTIONS:'-c jit=off -c pg_show_plans.is_enabled=off -c statement_timeout=0 -c application_name=healthroster_rehearsal_preflight'}});
 if(result.status!==0)throw Error('READ_ONLY_PROBE_FAILED '+(result.stderr.match(/(?:ERROR|FATAL):\s*([A-Z0-9]{5})/)?.[1]??'CONNECTION'));return JSON.parse(result.stdout.trim());
}
function exactSource(original,file){const expected=canonical(git(['show',commit+':'+file]));if(original!==expected)throw Error('SOURCE_COMMIT_DRIFT '+file);return mapExecutableSqlSource(original,file);}
function verifierClosure(file,stack=[]){
 if(stack.includes(file)||path.posix.isAbsolute(file)||file.split('/').includes('..')||!/^(?:supabase\/verification\/|tests\/)/.test(file))throw Error('VERIFIER_INCLUDE_SCOPE_INVALID '+file);
 const original=canonical(fs.readFileSync(path.join(root,file),'utf8')),source=exactSource(original,file),ordered=[{path:file,source:original}];
 const expanded=source.replace(/^\s*\\ir\s+(?:'([^']+)'|"([^"]+)"|([^\s;]+))\s*;?\s*$/gm,(_m,a,b,c)=>{const child=verifierClosure(path.posix.normalize(path.posix.join(path.posix.dirname(file),a??b??c)),[...stack,file]);ordered.push(...child.ordered);return child.expanded;});
 return {source,expanded,ordered};
}
function prepareUnit(row,kind){
 let closure;try{closure=kind==='verifier'?verifierClosure(row.path):readManagedClosure(row.path,root,[],exactSource);}catch(error){throw Error(row.path+' '+error.message);}const parsed=scanManagedSql(closure.expanded);
 const remove=[...parsed.meta.filter(x=>/^\\set\s+ON_ERROR_STOP\s+(?:on|off)\s*$/i.test(x.text)),...parsed.statements.filter(x=>(['BEGIN','COMMIT','ROLLBACK','END'].includes(x.tokens[0])&&x.tokens.length===1)||/^BEGIN ISOLATION LEVEL (?:REPEATABLE READ|SERIALIZABLE)$/.test(x.tokens.join(' ')))];
 const escaped=parsed.statements.find(x=>['START','ABORT','PREPARE'].includes(x.tokens[0]));if(escaped)throw Error('OUTER_ESCAPE_REFUSED '+row.path);
 let sql=closure.expanded;for(const x of remove.sort((a,b)=>b.start-a.start))sql=sql.slice(0,x.start)+sql.slice(x.end);
 const stmts=scanManagedSql(sql);return {file:row.path,kind,sourceHash:row.sha256??digest(closure.source),sql,sqlHash:digest(sql),statements:stmts.statements.length,meta:stmts.meta.map(x=>x.text),closures:closure.ordered.map(x=>({file:x.path,sha256:digest(x.source)}))};
}
try{
 const remote=git(['ls-remote','origin','refs/heads/test']).trim().split(/\s+/)[0];if(remote!==expectedTest)throw Error('REMOTE_TEST_CHANGED_REPLAN_REQUIRED');
 const runs=JSON.parse(execFileSync('C:/Program Files/GitHub CLI/gh.exe',['run','list','--repo','kierarthur/cloudtms-backend','--workflow','340102476','--limit','50','--json','databaseId,status,conclusion,headSha'],{encoding:'utf8'}));if(runs.some(x=>x.status!=='completed'))throw Error('PROTECTED_RELEASE_RUNNING_HOLD');
 const before=readOnly("select jsonb_build_object('database',current_database(),'identity_test',exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST'),'module',(select active_owner||':'||owner_epoch::text from private.bpay_next_module_control where id=1),'applying',(select count(*) from private.cloudtms_database_releases where status='APPLYING'),'other_rehearsals',(select count(*) from pg_stat_activity where datname=current_database() and pid<>pg_backend_pid() and application_name like 'healthroster_rehearsal_%'))::text;");
 if(before.database!=='cloudtms_test_clone'||before.identity_test!==true||before.module!=='LEGACY:1'||before.applying!==0||before.other_rehearsals!==0)throw Error('TEST_IDENTITY_MODULE_OR_RELEASE_GUARD_REFUSED');
 const installed=readOnly("select jsonb_build_object('migrations',(select coalesce(jsonb_agg(jsonb_build_object('path',path,'sha256',content_sha256) order by path),'[]'::jsonb) from private.cloudtms_migration_ledger),'repeatables',(select coalesce(jsonb_agg(jsonb_build_object('path',path,'sha256',closure_sha256) order by path),'[]'::jsonb) from private.cloudtms_repeatable_ledger))::text;");
 const current=inventory(),pending=pendingInventory(current,installed),release=JSON.parse(git(['show',commit+':supabase/release/current-release.json']));
 const units=[...pending.migrations.map(x=>prepareUnit(x,'migration')),...pending.repeatables.map(x=>prepareUnit(x,'repeatable'))],verifierUnits=release.verificationFiles.map(file=>prepareUnit({path:file},'verifier'));
 const plan={commit,remoteTest:remote,mode:'UPGRADE',before,currentCounts:{migrations:current.migrations.length,repeatables:current.repeatables.length},pendingCounts:{migrations:pending.migrations.length,repeatables:pending.repeatables.length},units,verifierUnits,protectedWorkflowCheck:runs.map(x=>({id:x.databaseId,status:x.status,conclusion:x.conclusion,headSha:x.headSha}))};
 save('FULL_REHEARSAL_EXACT_SOURCE_PLAN.json',plan);save('FULL_REHEARSAL_PREPARE_IDENTITY.json',before);
 console.log(JSON.stringify({status:'PREPARED_READ_ONLY_NOT_EXECUTED',commit,remoteTest:remote,identity:before.database,module:before.module,pending:plan.pendingCounts,verifiers:verifierUnits.length,unitsWithMeta:units.filter(x=>x.meta.length).map(x=>({file:x.file,meta:x.meta})),verifiersWithMeta:verifierUnits.filter(x=>x.meta.length).map(x=>({file:x.file,meta:x.meta})),sqlBytes:units.reduce((n,x)=>n+Buffer.byteLength(x.sql),0)}));
}catch(error){console.error('REHEARSAL_PREPARE_FAILED '+String(error.message));process.exitCode=1;}

