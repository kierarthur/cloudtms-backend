// Mechanical prerequisites for the one task-owned contract seal, never hosted.
import fs from 'node:fs';import path from 'node:path';import {spawnSync} from 'node:child_process';
import {repoRoot} from './cloudtms-db-release-lib.mjs';import {digest} from './cloudtms-managed-release-sql.mjs';
import {profile,parseArguments,configureLocalProcess,assertProject,inspectOwnResources} from './healthroster-install2-local-seal-profile.mjs';
const candidate=parseArguments(process.argv.slice(2));
const sourceIdentity=assertProject(repoRoot,candidate);
configureLocalProcess(process.env);
if(typeof process.env.POSTGRES_PASSWORD!=='string'||process.env.POSTGRES_PASSWORD.length===0)throw Error('I2_SEAL_LOCAL_ADMIN_PASSWORD_REQUIRED');
const {container,owner,database}=profile,resources=inspectOwnResources();
const output=path.join(repoRoot,profile.output);
if(fs.existsSync(output))throw Error('I2_SEAL_FRESH_OUTPUT_REQUIRED');
function execute(sql,target='postgres'){
 const result=spawnSync('docker',['exec','-i','--env','PGPASSWORD',container,'psql','-h','127.0.0.1','-U','postgres','-d',target,
  '-X','-q','-A','-t','-v','ON_ERROR_STOP=on','-v','VERBOSITY=sqlstate','-v','SHOW_CONTEXT=never'],
 {input:sql,encoding:'utf8',windowsHide:true,maxBuffer:2000000,env:{...process.env,PGPASSWORD:process.env.POSTGRES_PASSWORD}});
 if(result.status!==0)throw Error('OWN_LOCAL_PROVIDER_SQL_FAILED_'+(result.stderr.match(/(?:ERROR|FATAL):\s*([A-Z0-9]{5})/)?.[1]??'CONNECTION'));
 return result.stdout.trim();
}
const sourcePath='tests/fixtures/28082026_1229_banking_modal_local_pg17_prerequisites.sql';
const source=fs.readFileSync(path.join(repoRoot,sourcePath),'utf8');
if(execute(`select (not exists(select 1 from pg_database where datname='${database}')
 and not exists(select 1 from pg_roles where rolname='${owner}'))::text;`)!=='true')
 throw Error('I2_SEAL_FRESH_DATABASE_AND_OWNER_REQUIRED');
{
 const password="'"+process.env.PGPASSWORD.replaceAll("'","''")+"'";
 execute(`create role ${owner} login nosuperuser nobypassrls nocreatedb nocreaterole password ${password};\ncreate database ${database} owner ${owner};\n`);
 execute(source,database);
 execute(`alter role service_role nobypassrls;\ncreate extension if not exists "uuid-ossp" with schema extensions;\ncreate extension if not exists pg_stat_statements with schema extensions;\ngrant usage on schema extensions,auth,vault to ${owner};\ngrant references on auth.users to ${owner};\ngrant select on auth.users,vault.secrets to ${owner};\ngrant select on vault.decrypted_secrets to ${owner};\ngrant execute on all functions in schema auth,vault to ${owner};\n`,database);
}
const scaffold=execute(`select (not exists(select 1 from pg_class c join pg_namespace n on n.oid=c.relnamespace
 where n.nspname in ('public','private') and c.relkind in ('r','p')) and not exists(select 1 from auth.users)
 and not exists(select 1 from vault.secrets) and to_regprocedure('auth.uid()') is not null
 and to_regprocedure('auth.role()') is not null and to_regprocedure('auth.jwt()') is not null
 and to_regprocedure('vault.create_secret(text,text,text,uuid)') is not null
 and has_schema_privilege('${owner}','extensions','USAGE')
 and has_schema_privilege('${owner}','auth','USAGE') and has_schema_privilege('${owner}','vault','USAGE')
 and has_table_privilege('${owner}','vault.decrypted_secrets','SELECT'))::text;`,database);
if(scaffold!=='true')throw Error('OWN_LOCAL_PROVIDER_SCAFFOLD_INCOMPLETE');
const identity=JSON.parse(execute(`select json_build_object('database',current_database(),'major',current_setting('server_version_num')::integer/10000,
 'owner',(select rolname from pg_roles where oid=(select datdba from pg_database where datname=current_database())),
 'ownerSuperuser',(select rolsuper from pg_roles where rolname='${owner}'),'ownerBypass',(select rolbypassrls from pg_roles where rolname='${owner}'),
 'serviceBypass',(select rolbypassrls from pg_roles where rolname='service_role'),
 'ownerAuthUsersSelect',has_table_privilege('${owner}','auth.users','SELECT'),
 'ownerVaultSecretsSelect',has_table_privilege('${owner}','vault.secrets','SELECT'),
 'authUsersEmpty',not exists(select 1 from auth.users),
 'vaultSecretsEmpty',not exists(select 1 from vault.secrets));`,database));
if(identity.database!==database||identity.major!==17||identity.owner!==owner||identity.ownerSuperuser||identity.ownerBypass||identity.serviceBypass
 ||identity.ownerAuthUsersSelect!==true||identity.ownerVaultSecretsSelect!==true
 ||identity.authUsersEmpty!==true||identity.vaultSecretsEmpty!==true)throw Error('OWN_LOCAL_SEAL_PROFILE_REFUSED');
assertProject(repoRoot,candidate);
fs.mkdirSync(output);
fs.mkdirSync(path.join(output,'tmp'));
fs.writeFileSync(path.join(output,'PROVIDER.json'),JSON.stringify({
 status:'LOCAL_MECHANICAL_FRESH_PROVIDER_ONLY',version:profile.version,sourceIdentity,resources,
 sourcePath,sourceSha256:digest(source),identity,applicationRowsCopied:0,hostedConnection:false,
 qualification:'Fresh local provider scaffolding only; no local business test, native/API acceptance or hosted installation.'},null,2)+'\n',{flag:'wx'});
console.log(JSON.stringify({status:'I2_SEAL_FRESH_PROVIDER_READY',sourceCommit:candidate,identity}));
