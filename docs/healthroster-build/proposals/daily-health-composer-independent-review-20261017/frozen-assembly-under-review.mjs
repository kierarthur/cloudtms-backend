// Reproduce the actual frozen assembler against its exact original registry.
// This is an explicit read-only source VFS, not validation of the current root.
import fs from'node:fs';import path from'node:path';import vm from'node:vm';import assert from'node:assert/strict';import{createHash}from'node:crypto';import{execFileSync}from'node:child_process';import{fileURLToPath,pathToFileURL}from'node:url';
const self=fileURLToPath(import.meta.url),folder=path.dirname(self),own=path.resolve(folder,'../../../..'),report=path.resolve(own,'../healthroster-v2-report-build-13102026'),root=path.resolve(own,'../healthroster-install2-14102026'),rel='docs/healthroster-build/proposals/v6-daily-health-composer-blocks-20261017/assemble.mjs',sourcePath=path.join(report,rel),registry=path.resolve(root,'docs/bpay-next/final-definer-closures.json');
for(const p of[own,report,root])assert(fs.realpathSync(p).replaceAll('\\','/').startsWith('D:/Development/GitHub/'));
const blob=(commit,p)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',commit+':'+p],{cwd:report,maxBuffer:10*1024*1024,windowsHide:true});
const oldRegistry=blob('24c57e6c7af4a8fb07d5e3126ab9ac76e9448560','docs/bpay-next/final-definer-closures.json');assert.equal(createHash('sha256').update(oldRegistry).digest('hex'),'e6ee24f8f541b4e31d28ba625623b7bb7674a54a6dcabd808a5aa8e17b6e3aaa');
const raw=fs.readFileSync(sourcePath);assert.deepEqual(raw,blob('b776cedf6d2483aff44d6e1bada5e0f7b389112d',rel));
const vfs={...fs,readFileSync:(p,options)=>{if(path.resolve(String(p))!==registry)return fs.readFileSync(p,options);return typeof options==='string'||options?.encoding?oldRegistry.toString(typeof options==='string'?options:options.encoding):Buffer.from(oldRegistry);}};
const cli=process.argv[1]&&path.resolve(process.argv[1])===self,localProcess={argv:cli?[process.execPath,sourcePath,...process.argv.slice(2)]:process.argv,exitCode:0};
const context=vm.createContext({fs:vfs,path,assert,createHash,execFileSync,fileURLToPath,SOURCE_URL:pathToFileURL(sourcePath).href,process:localProcess,console,Buffer,__out:{}});
const code=raw.toString().replace(/^import[^\n]*\n/gm,'').replace(/^export\s+/gm,'').replaceAll('import.meta.url',()=>'SOURCE_URL');
vm.runInContext(code+'\n__out={check,sources,validateHealthBlocks,requireOwnerBindings,here};',context,{timeout:10000});
export const{check,sources,validateHealthBlocks,requireOwnerBindings,here}=context.__out;
if(cli)process.exitCode=localProcess.exitCode;
