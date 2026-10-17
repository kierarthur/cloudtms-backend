import test from'node:test';import assert from'node:assert/strict';import fs from'node:fs';import path from'node:path';import vm from'node:vm';import{fileURLToPath}from'node:url';
import{profile,parseArguments,configureLocalProcess}from'./payload/scripts/healthroster-install2-local-seal-profile.mjs';
const P=path.dirname(fileURLToPath(import.meta.url)),read=f=>fs.readFileSync(path.join(P,f),'utf8');
const file='scripts/healthroster-install2-local-provider.mjs',source=read('payload/'+file),candidate='a'.repeat(40),owner="OWNER-'-$&-`-CANARY",admin="ADMIN-'-$&-`-CANARY";
async function run(options={}){
 const calls=[],writes=[],logs=[],env={CLOUDTMS_DATABASE_URL:`postgresql://${profile.owner}@127.0.0.1:55448/banking_modal_v2_test`,
  CLOUDTMS_ALLOW_LOCAL:'1',CLOUDTMS_LOGICAL_POSTGRES_OWNER:'CURRENT_USER',PGPASSWORD:owner,POSTGRES_PASSWORD:admin,...options.env};
 let metadata=0;
 const sandbox={fs:{existsSync:()=>false,readFileSync:()=> 'SEALED_EXISTING_PROVIDER_FIXTURE',mkdirSync:()=>{},writeFileSync:(p,b)=>writes.push({p,b})},path,
  process:{argv:['node','provider','NEW','--candidate='+candidate],env},profile,parseArguments,configureLocalProcess,
  assertProject:()=>({candidate}),inspectOwnResources:()=>{metadata++;return{container:profile.container};},repoRoot:profile.root,digest:()=> 'b'.repeat(64),
  console:{log:s=>logs.push(s)},spawnSync:(command,args,config)=>{calls.push({command,args,config});let stdout='';
   if(config.input.startsWith('select (not exists'))stdout='true';
   if(config.input.startsWith('select json_build_object'))stdout=JSON.stringify({database:profile.database,major:17,owner:profile.owner,ownerSuperuser:false,ownerBypass:false,serviceBypass:false});
   if(options.failure)return{status:1,stderr:'ERROR: XX001 '+owner+' '+admin,stdout:''};
   return{status:0,stdout,stderr:''};}};
 let error;try{await new vm.Script('(async()=>{'+source.replace(/^import[\s\S]*?;\r?\n/gm,'')+'})()').runInNewContext(sandbox);}catch(e){error=e;}
 return{calls,writes,logs,error,env,metadata};
}
test('actual provider uses fixed TCP and name-only admin env on every owned Docker child',async()=>{
 const r=await run();assert.equal(r.error,undefined);assert(r.calls.length>3);
 for(const c of r.calls){assert.equal(c.command,'docker');assert.deepEqual(Array.from(c.args.slice(0,8)),['exec','-i','--env','PGPASSWORD',profile.container,'psql','-h','127.0.0.1']);
  assert.equal(c.config.env.PGPASSWORD,admin);assert.equal(c.config.maxBuffer,2000000);assert(!JSON.stringify(c.args).includes('CANARY'));}
 assert.equal(r.env.PGPASSWORD,owner);assert.equal(r.env.POSTGRES_PASSWORD,admin);
 const create=r.calls.find(c=>c.config.input.startsWith('create role')).config.input;
 assert(create.includes(owner.replaceAll("'","''")));assert(!create.includes(admin));
 assert(!JSON.stringify({writes:r.writes,logs:r.logs}).includes('CANARY'));assert.equal(r.writes.length,1);
});
test('missing/empty admin is an early closed refusal before Docker metadata or SQL',async()=>{
 for(const value of [undefined,'']){const r=await run({env:{POSTGRES_PASSWORD:value}});
  assert.equal(r.error.message,'I2_SEAL_LOCAL_ADMIN_PASSWORD_REQUIRED');assert.equal(r.calls.length,0);assert.equal(r.metadata,0);assert.equal(r.writes.length,0);}
});
test('failed actual body still exposes only the original closed SQLSTATE and preserves owner env',async()=>{
 const r=await run({failure:true});assert.equal(r.error.message,'OWN_LOCAL_PROVIDER_SQL_FAILED_XX001');
 assert.equal(r.env.PGPASSWORD,owner);assert.equal(r.writes.length,0);assert.equal(r.logs.length,0);assert(!r.error.message.includes('CANARY'));
});
test('all three literal full-parent inverses and the sole launcher command element are exact',()=>{
 const f=JSON.parse(read('FORWARD.json'));let actual=source;
 for(const h of [...f.hunks].reverse()){assert.equal(actual.split(h.after).length,2);actual=actual.replace(h.after,()=>h.before);}
 assert.equal(actual,read('parents/'+file));
 const old=read('parents/COMMANDS.ps1.txt'),next=read('COMMANDS.ps1.txt'),c=f.commandForward;
 assert.equal(old.split(c.before).length,2);assert.equal(next,old.replace(c.before,()=>c.after));
 assert.equal(next.replace(c.after,()=>c.before),old);
 assert(read('parents/tests/fixtures/28082026_1229_banking_modal_local_pg17_prerequisites.sql').includes('inet_server_addr() IS NULL'));
 assert.equal(read('payload/scripts/healthroster-install2-local-seal-profile.mjs'),read('parents/scripts/healthroster-install2-local-seal-profile.mjs'));
});
