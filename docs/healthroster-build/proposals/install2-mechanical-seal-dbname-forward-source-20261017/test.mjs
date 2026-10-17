import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import {fileURLToPath} from 'node:url';
import {profile,parseArguments,validateTarget,validateContainer,validateVolume,configureLocalProcess,
 dockerImageArgs,dockerVolumeArgs,dockerContainerArgs} from './payload/scripts/healthroster-install2-local-seal-profile.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),candidate='a'.repeat(40),hash='b'.repeat(64);
const read=p=>fs.readFileSync(path.join(here,p),'utf8');
const source=name=>read('payload/scripts/healthroster-install2-local-'+name+'.mjs');
const copy=value=>JSON.parse(JSON.stringify(value));
const imageId='sha256:'+'1'.repeat(64);
function container(){return {Id:'2'.repeat(64),Image:imageId,Name:'/'+profile.container,
 Config:{Image:profile.image,Labels:{'codex.task':profile.task},Cmd:['postgres','-c','jit=off','-c','pg_show_plans.is_enabled=off']},
 HostConfig:{Privileged:false,NetworkMode:'bridge',PortBindings:{'5432/tcp':[{HostIp:'127.0.0.1',HostPort:profile.port}]}},
 Mounts:[{Type:'volume',Name:profile.volume,Destination:'/var/lib/postgresql/data',RW:true}]};}
const resource=validateContainer(container(),imageId);
const sourceIdentity={candidate,branch:profile.branch,physicalRoot:profile.root};
const target=`postgresql://${profile.owner}@127.0.0.1:${profile.port}/${profile.database}`;
const environment=()=>({CLOUDTMS_DATABASE_URL:target,CLOUDTMS_ALLOW_LOCAL:'1',CLOUDTMS_LOGICAL_POSTGRES_OWNER:'CURRENT_USER',
 PGPASSWORD:"local-test-canary-'-$&-`-never-persist",PGSERVICE:'foreign',PGSERVICEFILE:'foreign',PGHOST:'foreign',PGHOSTADDR:'foreign',PGOPTIONS:'foreign'});
test('only NEW with one exact real-candidate argument is accepted',()=>{
 assert.equal(parseArguments(['NEW','--candidate='+candidate]),candidate);
 for(const args of [[],['NEW'],['CHANGED','--candidate='+candidate],['NEW','--replay-from='+candidate],
  ['NEW','--candidate='+candidate,'extra'],['NEW','--candidate='+'A'.repeat(40)],['--record-existing-own-prerequisites']])
  assert.throws(()=>parseArguments(args));
});
test('fixed passwordless local URI and explicit process-only owner controls',()=>{
 const settings={allowLocal:'1',logicalOwner:'CURRENT_USER',passwordPresent:true};
 assert.equal(validateTarget(target,settings).hostname,'127.0.0.1');
 for(const value of [target.replace('127.0.0.1','host.docker.internal'),target.replace('127.0.0.1','localhost'),
  target.replace('127.0.0.1','host.example.test'),target.replace('55448','55447'),target.replace(profile.database,'cloudtms_test_clone'),
  target.replace(profile.owner,'postgres'),target.replace('@',':secret@'),target+'?options=bad',target+'#fragment'])
  assert.throws(()=>validateTarget(value,settings));
 for(const changed of [{allowLocal:'0'},{logicalOwner:'postgres'},{passwordPresent:false}])
  assert.throws(()=>validateTarget(target,{...settings,...changed}));
 const env=environment(),secret=env.PGPASSWORD;configureLocalProcess(env);
 assert.equal(env.PGPASSWORD,secret);assert.equal(env.PGSERVICE,undefined);assert.equal(env.PGSERVICEFILE,undefined);
 assert.equal(env.PGHOSTADDR,'127.0.0.1');assert.equal(env.PGUSER,profile.owner);
 assert.equal(env.PGOPTIONS,'-c jit=off -c pg_show_plans.is_enabled=off');
 assert.equal(env.TEMP,path.join(profile.root,profile.output,'tmp'));assert.equal(env.TMP,env.TEMP);
});
test('closed container/image/loopback/single named volume and no Docker Env projection',()=>{
 const good=container();assert.equal(validateContainer(good,imageId).containerId,good.Id);
 const changes=[x=>x.Id='old',x=>x.Image='sha256:'+'3'.repeat(64),x=>x.Name+='_other',x=>x.Config.Image='postgres:latest',
  x=>x.Config.Labels['codex.task']='other',x=>x.Config.Cmd.push('-c','work_mem=1GB'),x=>x.HostConfig.Privileged=true,
  x=>x.HostConfig.NetworkMode='host',x=>x.HostConfig.PortBindings['5432/tcp'][0].HostIp='0.0.0.0',
  x=>x.HostConfig.PortBindings['5432/tcp'].push({HostIp:'127.0.0.1',HostPort:'55449'}),
  x=>x.Mounts.push(copy(x.Mounts[0])),x=>x.Mounts[0].Type='bind',x=>x.Mounts[0].Name='foreign',
  x=>x.Mounts[0].Destination='/other',x=>x.Mounts[0].RW=false];
 for(const mutate of changes){const row=copy(good);mutate(row);assert.throws(()=>validateContainer(row,imageId));}
 const volume={Name:profile.volume,Driver:'local',Scope:'local',Labels:{'codex.task':profile.task},Options:null};
 assert.equal(validateVolume(volume).name,profile.volume);
 for(const patch of [{Name:'other'},{Driver:'nfs'},{Scope:'global'},{Labels:{}},{Options:{type:'none',device:'C:/old',o:'bind'}}])
  assert.throws(()=>validateVolume({...volume,...patch}));
 for(const args of [dockerImageArgs,dockerVolumeArgs,dockerContainerArgs]){
  assert(!args.some(x=>x==='--env'));assert(!args.join(' ').includes('.Env'));
 }
});
const withoutImports=text=>text.replace(/^import[\s\S]*?;\r?\n/gm,'').replace(/^#!.*\n/,'');
async function runProvider(options={}){
 const calls=[],writes=[],logs=[],dirs=[],env=environment();
 let execution=0;
 const identity={database:profile.database,major:17,owner:profile.owner,ownerSuperuser:false,ownerBypass:false,serviceBypass:false,...options.identity};
 const sandbox={path,process:{argv:['node','provider','NEW','--candidate='+candidate],env,pid:1},
  fs:{existsSync:()=>options.outputExists===true,readFileSync:()=> 'SEALED_PROVIDER_FIXTURE',
   mkdirSync:p=>dirs.push(p),writeFileSync:(p,b,o)=>writes.push({p,b,o})},
  profile,parseArguments,configureLocalProcess,assertProject:()=>sourceIdentity,inspectOwnResources:()=>resource,
  repoRoot:profile.root,digest:()=>hash,console:{log:s=>logs.push(s)},
  spawnSync:(file,args,opts)=>{
   calls.push({file,args,input:opts.input});execution++;
   let stdout='';
   if(opts.input.startsWith('select (not exists(select 1 from pg_database'))stdout=options.collision?'false':'true';
   else if(opts.input.startsWith('select (not exists(select 1 from pg_class'))stdout='true';
   else if(opts.input.startsWith('select json_build_object'))stdout=JSON.stringify(identity);
   if(options.sqlFailure&&execution===2)return {status:1,stdout:'',stderr:'ERROR: XX001 canary-private-body'};
   return {status:0,stdout,stderr:''};
  }};
 let error;try{await new vm.Script('(async()=>{'+withoutImports(source('provider'))+'})()').runInNewContext(sandbox);}
 catch(e){error=e;}
 return {calls,writes,logs,dirs,error,env};
}
test('actual provider owner body creates only fresh namespace and never persists its password',async()=>{
 const r=await runProvider();assert.equal(r.error,undefined);assert.equal(r.writes.length,1);
 assert(r.writes[0].p.endsWith('PROVIDER.json'));assert.equal(r.writes[0].o.flag,'wx');
 assert(r.calls.every(c=>c.file==='docker'&&c.args[0]==='exec'&&!c.args.includes('-h')));
 const create=r.calls.find(c=>c.input.startsWith('create role')).input;
 assert(create.includes('nosuperuser nobypassrls nocreatedb nocreaterole password'));
 assert(create.includes("canary-''-$&-`-never-persist"));
 for(const c of r.calls)assert(!c.args.some(a=>a.includes('canary')));
 assert(!JSON.stringify({writes:r.writes,logs:r.logs}).includes('canary'));
 const record=JSON.parse(r.writes[0].b);assert.equal(record.applicationRowsCopied,0);assert.equal(record.hostedConnection,false);
 assert.deepEqual(record.resources,resource);assert.deepEqual(record.sourceIdentity,sourceIdentity);
 assert.equal(r.dirs.length,2);
});
test('provider refuses existing output/namespace or wrong native local profile, with closed SQLSTATE',async()=>{
 for(const options of [{outputExists:true},{collision:true},{identity:{major:18}},{identity:{ownerSuperuser:true}},
  {identity:{ownerBypass:true}},{identity:{serviceBypass:true}},{sqlFailure:true}]){
  const r=await runProvider(options);assert(r.error);assert.equal(r.writes.length,0);assert.equal(r.logs.length,0);
  assert(!r.error.message.includes('canary'));if(options.outputExists)assert.equal(r.calls.length,0);
  if(options.collision)assert.equal(r.calls.length,1);
 }
});
function fixture(){
 const acl=['supabase/repeatable/24082026_0322_miget_postrestore_rpc_browser_isolation.sql',
  'supabase/repeatable/26082026_0230_general_security_definer_browser_isolation_v2.sql',
  'supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql'];
 const paths=['supabase/repeatable/old.sql','supabase/repeatable/final.sql','supabase/repeatable/reader_a.sql','supabase/repeatable/reader_b.sql',...acl];
 const current={migrations:[{path:'supabase/migrations/BASE.SQL',sha256:hash},{path:'supabase/migrations/NEW.SQL',sha256:hash}],
  repeatables:paths.map(p=>({path:p,sha256:hash,paths:[p]}))};
 return {acl,current,chains:[{predecessors:[paths[0]],finalClosures:[paths[1]]}]};
}
async function runSchema(options={}){
 const {acl,current,chains}=fixture(),events=[],writes=[],logs=[];let inventoryCalls=0,pendingCalls=0,closed=0;
 const validProvider={status:'LOCAL_MECHANICAL_FRESH_PROVIDER_ONLY',version:profile.version,sourceIdentity,resources:resource,
  applicationRowsCopied:0,hostedConnection:false,...options.provider};
 const identity={database:profile.database,major:17,owner:profile.owner,session:profile.owner,superuser:false,bypassrls:false,jit:'off',collector:'off',...options.identity};
 const release={contractPath:'contract',baselineRepeatableLock:'baseline'};
 const ledger={migrations:[current.migrations[0]],repeatables:[]};
 const selectPending={migrations:[current.migrations[1]],repeatables:[current.repeatables[0],current.repeatables[2],current.repeatables[3]]};
 const sandbox={path,process:{argv:['node','schema','NEW','--candidate='+candidate],env:environment(),pid:7},
  fs:{readFileSync:p=>p.endsWith('PROVIDER.json')?JSON.stringify(validProvider):JSON.stringify({chains}),
   existsSync:p=>p.endsWith('SCHEMA.json')?!!options.checkpoint:p.endsWith('final-definer-closures.json'),
   writeFileSync:(p,b,o)=>writes.push({p,b,o}),renameSync:(a,b)=>events.push(['rename',a,b])},
  profile,parseArguments,configureLocalProcess,assertProject:()=>{events.push(['source']);return sourceIdentity;},
  inspectOwnResources:()=>resource,repoRoot:profile.root,verifyIntegrity:()=>current,
  inventory:()=>{inventoryCalls++;return options.drift?{...current,migrations:[]}:current;},
  readJson:p=>p==='supabase/release/current-release.json'?release:p==='baseline'?{repeatables:[]}: {},
  canonicalContractHash:()=>hash,databaseUrl:()=>'',executableSqlFile:x=>x,mapExecutableSqlSource:x=>x,
  shellGitHead:()=>candidate,literal:s=>"'"+s+"'",acquireWriterSql:'ACQUIRE',assertWriterSql:'ASSERT',resetWriterSql:'RESET',
  assertBootstrapPrefix:()=>0,managedReleaseContext:x=>x.context,
  compileManagedUnit:(item,kind)=>{events.push(['compile',item.path,kind]);return {item,kind,closure:{source:item.path}};},
  compileBootstrap:()=>{events.push(['compileBootstrap']);return 'BOOTSTRAP';},
  closeFinalDefinerSelection:x=>{events.push(['closeFinal']);return x;},
  verifyFinalDefiners:x=>events.push(['verifyFinal',x]),
  pendingInventory:()=>{pendingCalls++;return pendingCalls===1?selectPending:options.incomplete?selectPending:{migrations:[],repeatables:[]};},
  unitCheckpointSql:(unit)=>'UNIT:'+unit.item.path,
  repeatableLedgerSql:row=>'LEDGER:'+row.path,
  prepareManagedSourceReaders:async()=>events.push(['prepareReaders']),
  readerReleasePhases:chosen=>({ordinary:chosen.filter(x=>!x.path.includes('reader_')),readers:chosen.filter(x=>x.path.includes('reader_'))}),
  readerActivationSql:(rows,sourceFor,ledgerFor)=>{for(const r of rows){sourceFor(r.path);ledgerFor(r);}events.push(['activateReaders']);return 'READERS';},
  openManagedPsqlSession:()=>{events.push(['open']);return {close:async()=>{closed++;},execute:async sql=>{
   events.push(['sql',sql]);
   if(sql==='ACQUIRE')return options.busy?'f':'t';
   if(sql.includes("'superuser',r.rolsuper"))return JSON.stringify(identity);
   if(sql.includes("to_regclass('private.cloudtms_database_releases')"))return options.present?'true':'false';
   if(sql.startsWith('select (not exists(select 1 from pg_catalog.pg_class relation_row'))return options.notEmpty?'false':'true';
   if(sql.includes("'migrations',(select coalesce"))return JSON.stringify(ledger);
   return '';
  }};},console:{log:s=>logs.push(s)}};
 let error;try{await new vm.Script('(async()=>{'+withoutImports(source('schema'))+'})()').runInNewContext(sandbox);}
 catch(e){error=e;}
 return {error,events,writes,logs,closed,inventoryCalls,acl,current};
}
test('actual complete schema owner keeps canonical uppercase/order/final-owner/reader/last-ACL choreography',async()=>{
 const r=await runSchema();assert.equal(r.error,undefined);assert.equal(r.closed,1);assert.equal(r.inventoryCalls,1);
 const units=r.events.filter(x=>x[0]==='sql'&&x[1].startsWith('UNIT:')).map(x=>x[1].slice(5));
 assert.equal(units[0],'supabase/migrations/NEW.SQL');
 assert(units.includes('supabase/repeatable/final.sql'));assert.deepEqual(units.slice(-3),r.acl);
 const verify=r.events.find(x=>x[0]==='verifyFinal')[1];assert(verify.selectedRoots.includes('supabase/repeatable/final.sql'));
 assert.deepEqual(Array.from(verify.selectedMigrations),['supabase/migrations/NEW.SQL']);
 assert(r.events.findIndex(x=>x[0]==='prepareReaders')<r.events.findIndex(x=>x[0]==='activateReaders'));
 const record=JSON.parse(r.writes.at(-1).b);assert.equal(record.status,'LOCAL_SCHEMA_COMPILED');
 assert.equal(record.context.mode,'NEW');assert.equal(record.fullProtectedVerification,'PENDING');
 assert.equal(record.localAcceptance,'NOT_RUN');assert.equal(record.hostedExecution,false);
 assert.equal(record.context.gitCommit,candidate);assert(!r.writes.some(x=>x.p.includes('install1-source')));
 assert(r.writes.every(x=>x.o.flag==='wx'));assert.equal(r.events.filter(x=>x[0]==='source').length,2);
});
test('schema refuses provider/current-container mismatch and replay before opening any session',async()=>{
 for(const options of [{provider:{status:'other'}},{provider:{version:'other'}},{provider:{sourceIdentity:{...sourceIdentity,candidate:'c'.repeat(40)}}},
  {provider:{resources:{...resource,containerId:'9'.repeat(64)}}},{provider:{applicationRowsCopied:1}},
  {provider:{hostedConnection:true}},{checkpoint:true}]){
  const r=await runSchema(options);assert(r.error);assert.equal(r.closed,0);assert.equal(r.writes.length,0);
  assert(!r.events.some(x=>x[0]==='open'));
 }
});
test('schema native identity/settings/emptiness/writer and final-completeness refusals always close only own session',async()=>{
 for(const options of [{busy:true},{present:true},{notEmpty:true},{identity:{major:18}},{identity:{superuser:true}},
  {identity:{bypassrls:true}},{identity:{session:'postgres'}},{identity:{jit:'on'}},{identity:{collector:'on'}},
  {drift:true},{incomplete:true}]){
  const r=await runSchema(options);assert(r.error);assert.equal(r.closed,1);
  assert(!r.writes.some(x=>JSON.parse(x.b).status==='LOCAL_SCHEMA_COMPILED'));
 }
});
test('canonical loader core and provider scaffold are raw unchanged from complete Install1 source',()=>{
 const old=read('parents/scripts/healthroster-install1-local-schema.mjs'),fresh=source('schema');
 const start='  const selected = new Map(',end='  await execute("notify pgrst,\'reload schema\';");';
 const oldCore=old.slice(old.indexOf(start),old.indexOf(end));
 const newCore=fresh.slice(fresh.indexOf(start),fresh.indexOf(end)).replace('  assertProject(repoRoot,candidate);\n','');
 assert.equal(newCore,oldCore);
 const oldProvider=read('parents/scripts/healthroster-install1-local-provider.mjs'),newProvider=source('provider');
 const scaffoldStart=' execute(source,database);',scaffoldEnd="if(scaffold!=='true')";
 assert.equal(newProvider.slice(newProvider.indexOf(scaffoldStart),newProvider.indexOf(scaffoldEnd)),
  oldProvider.slice(oldProvider.indexOf(scaffoldStart),oldProvider.indexOf(scaffoldEnd)));
 assert(!fresh.includes('previous'));assert(!fresh.includes("mode === 'CHANGED'"));
 for(const text of [fresh,newProvider])assert(!/C:\/Users\/|C:\/tmp|runVerifiers\(|db:apply|runManagedRelease/.test(text));
});
test('actual physical/source predicate refuses stale root/branch/head/index/current and untracked executable sources',()=>{
 const raw=source('seal-profile').replace(/^import[^\n]*\n/gm,'').replace(/^export /gm,'');
 for(const defect of [null,'physical','head','branch','dirty','untracked','git-storage']){
  const calls=[];
  const fakeFs={realpathSync:{native:p=>p.includes('.git')?(defect==='git-storage'?'C:/retained/.git':p):
   defect==='physical'?'C:/retained/project':profile.root}};
  const exec=(file,args)=>{
   calls.push(args);assert.deepEqual(Array.from(args.slice(0,4)),['-c','gc.auto=0','-c','maintenance.auto=false']);
   const key=args.slice(4).join(' ');
   return ({'rev-parse --show-toplevel':profile.root,'rev-parse HEAD':defect==='head'?'c'.repeat(40):candidate,
    'branch --show-current':defect==='branch'?'test':profile.branch,
    'rev-parse --absolute-git-dir':'D:/Development/GitHub/cloudtms-backend/.git/worktrees/i2',
    'rev-parse --git-common-dir':'D:/Development/GitHub/cloudtms-backend/.git',
    'status --porcelain=v1 --untracked-files=no':defect==='dirty'?' M source':'',
    'ls-files --others --exclude-standard -- scripts supabase':defect==='untracked'?'supabase/new.SQL':''})[key]??'';
  };
  const module=new vm.Script(raw+'\n;({assertProject});').runInNewContext({fs:fakeFs,path,execFileSync:exec});
  if(defect)assert.throws(()=>module.assertProject(profile.root,candidate));
  else {assert.equal(module.assertProject(profile.root,candidate).candidate,candidate);assert(calls.length>=7);}
 }
});
