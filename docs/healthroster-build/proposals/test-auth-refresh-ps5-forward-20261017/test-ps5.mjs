// Offline synthetic launcher boundary plus real PS5 public-module/metadata probe only.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {aclChildEnvironment} from './run-auth-refresh.mjs';
import {ROOT,ownedDirectory,check} from './policy.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const shell=path.join(process.env.SystemRoot,'System32/WindowsPowerShell/v1.0/powershell.exe');

test('only the exact ACL child gets a fresh native PS5 module path, with casing aliases removed',()=>{
 const inherited={PSMODULEPATH:'SYNTHETIC_PS7',PSModulePath:'SYNTHETIC_OTHER',Path:'SYNTHETIC_PATH',PUBLIC_CANARY:'unchanged'};
 const before=structuredClone(inherited),actual=aclChildEnvironment(shell,inherited);
 assert.deepEqual(inherited,before);
 assert.deepEqual(actual,{Path:'SYNTHETIC_PATH',PUBLIC_CANARY:'unchanged',PSModulePath:path.join(path.dirname(shell),'Modules')});
 assert.equal(Object.keys(actual).filter(k=>k.toLowerCase()==='psmodulepath').length,1);
});

test('actual native PS5 imports its Security module and reads only this public folder ACL',()=>{
 const output=execFileSync(shell,['-NoProfile','-NonInteractive','-File',path.join(here,'ps5-module-preflight.ps1'),'-PublicDirectory',here],
  {windowsHide:true,env:aclChildEnvironment(shell),stdio:['ignore','pipe','ignore'],timeout:15000,encoding:'utf8'});
 assert.equal(output.trim(),'PS5_SECURITY_IMPORT_PASS');
});

test('actual complete launcher returns a closed owned-path ACL HOLD before copy/spawn/login',async()=>{
 const source=fs.readFileSync(path.join(here,'run-auth-refresh.mjs'),'utf8');
 const section=source.slice(source.indexOf('export function aclChildEnvironment'),source.indexOf('\nif(process.argv[1]'));
 const calls=[];const context=vm.createContext({process:{env:{SystemRoot:process.env.SystemRoot,PSModulePath:'SYNTHETIC_PS7'}}});
 // Only public source admission is stubbed here; the complete actual run function is unchanged.
 const prefix=`import path from 'node:path'; import {ROOT,ownedDirectory,check} from './policy.mjs';
 import {fs,crypto,execFileSync,sourcePreflight,here} from './closed-stubs';\n`;
 const module=new vm.SourceTextModule(prefix+section,{context});
 const stubs={fs:{existsSync:()=>false,mkdirSync:()=>calls.push('mkdir')},crypto:{randomBytes:()=>({toString:()=> 'a'.repeat(32)})},
  execFileSync:(executable,args,options)=>{calls.push('acl');assert.equal(executable,shell);
   assert.equal(options.env.PSModulePath,path.join(path.dirname(shell),'Modules'));assert.equal(options.stdio,'ignore');
   assert.equal(args[0],'-NoProfile');assert.equal(args[1],'-NonInteractive');assert.equal(args[2],'-File');
   throw Error('SYNTHETIC_PRIVATE_ERROR_NOT_FOR_OUTPUT');},sourcePreflight:()=>calls.push('preflight'),here};
 await module.link(async id=>{
  const values=id==='node:path'?{default:path}:id==='./policy.mjs'?{ROOT,ownedDirectory,check}:stubs;
  return new vm.SyntheticModule(Object.keys(values),function(){for(const[k,v]of Object.entries(values))this.setExport(k,v);},{context});
 });
 await module.evaluate();
 const result=await module.namespace.runAuthRefresh(['--authorised-root-auth-refresh','--outside-native-interval']);
 assert.deepEqual(JSON.parse(JSON.stringify(result)),{status:'HOLD',code:'AUTH_PRIVATE_DIRECTORY_SETUP_HELD',phase:'PRIVATE_DIRECTORY_ACL',
  ownedDirectory:ROOT+'/.codex-tmp/test-auth-refresh-20261017-'+'a'.repeat(32),browserStarted:false,credentialLoaderStarted:false});
 assert.deepEqual(calls,['preflight','mkdir','acl']);assert.doesNotMatch(JSON.stringify(result),/SYNTHETIC|PRIVATE_ERROR|password|email/);
});
