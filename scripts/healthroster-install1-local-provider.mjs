// Mechanical prerequisites for the one task-owned contract seal, never hosted.
import fs from 'node:fs';import path from 'node:path';import {spawnSync} from 'node:child_process';
import {repoRoot} from './cloudtms-db-release-lib.mjs';import {digest} from './cloudtms-managed-release-sql.mjs';
const container='healthroster-install1-seal-pg17-20261014',owner='cloudtms_hr_install1_local_owner',database='banking_modal_v2_test';
const [mode,...extra]=process.argv.slice(2);
if(extra.length||mode!==undefined&&mode!=='--record-existing-own-prerequisites')throw Error('OWN_LOCAL_PROVIDER_MODE_REFUSED');
const inspection=spawnSync('docker',['inspect',container],{encoding:'utf8',windowsHide:true});
if(inspection.status!==0)throw Error('OWN_LOCAL_SEAL_CONTAINER_MISSING');
const row=JSON.parse(inspection.stdout)[0];
if(row.Config.Labels['codex.task']!=='healthroster-install1-seal-20261014'
 ||row.Config.Image!=='postgres:17.11-bookworm'||row.Name!=='/'+container
 ||row.HostConfig.PortBindings['5432/tcp']?.[0]?.HostIp!=='127.0.0.1'
 ||row.HostConfig.PortBindings['5432/tcp']?.[0]?.HostPort!=='55447')throw Error('OWN_LOCAL_SEAL_CONTAINER_IDENTITY_REFUSED');
function execute(sql,target='postgres'){
 const result=spawnSync('docker',['exec','-i',container,'psql','-h','127.0.0.1','-U','postgres','-d',target,
  '-X','-q','-A','-t','-v','ON_ERROR_STOP=on','-v','VERBOSITY=sqlstate','-v','SHOW_CONTEXT=never'],
 {input:sql,encoding:'utf8',windowsHide:true,maxBuffer:2000000});
 if(result.status!==0)throw Error('OWN_LOCAL_PROVIDER_SQL_FAILED_'+(result.stderr.match(/(?:ERROR|FATAL):\s*([A-Z0-9]{5})/)?.[1]??'CONNECTION'));
 return result.stdout.trim();
}
const sourcePath='tests/fixtures/28082026_1229_banking_modal_local_pg17_prerequisites.sql';
const source=fs.readFileSync(path.join(repoRoot,sourcePath),'utf8');
if(mode===undefined){
 execute(`create role ${owner} login nosuperuser nobypassrls nocreatedb nocreaterole;\ncreate database ${database} owner ${owner};\n`);
 execute(source,database);
 execute(`alter role service_role nobypassrls;\ncreate extension if not exists "uuid-ossp" with schema extensions;\ncreate extension if not exists pg_stat_statements with schema extensions;\ngrant usage on schema extensions,auth,vault to ${owner};\ngrant references on auth.users to ${owner};\ngrant select on vault.decrypted_secrets to ${owner};\ngrant execute on all functions in schema auth,vault to ${owner};\n`,database);
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
 'serviceBypass',(select rolbypassrls from pg_roles where rolname='service_role'));`,database));
if(identity.database!==database||identity.major!==17||identity.owner!==owner||identity.ownerSuperuser||identity.ownerBypass||identity.serviceBypass)throw Error('OWN_LOCAL_SEAL_PROFILE_REFUSED');
fs.writeFileSync(path.join(repoRoot,'docs/healthroster-build/install1-source/LOCAL_PROVIDER_SEAL_PREREQUISITES.json'),JSON.stringify({
 status:'LOCAL_MECHANICAL_PROVIDER_ONLY',container,volume:'healthroster-install1-seal-pg17-data-20261014',port:55447,
 sourcePath,sourceSha256:digest(source),identity,applicationRowsCopied:0,hostedConnection:false,
 qualification:'Provider scaffolding exists only in this disposable mechanical seal; not business/API/native acceptance or hosted installation.'},null,2)+'\n');
console.log(JSON.stringify({status:'OWN_LOCAL_SEAL_PROVIDER_READY',identity}));
