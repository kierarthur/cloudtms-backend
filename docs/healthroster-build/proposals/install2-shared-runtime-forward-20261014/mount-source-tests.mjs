import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';import {execFileSync,spawnSync} from 'node:child_process';
const dir=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(dir,'../../../..'),mount=path.join(dir,'.merge-inputs','test-mount');
const pins=JSON.parse(fs.readFileSync(path.join(dir,'THREE_WAY_INPUTS.json'))),money='d4b62cb15159d0afb02cfebf8c1e034c68c26550',sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=args=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:own,encoding:null,maxBuffer:64*1024*1024,stdio:['ignore','pipe','pipe']});
const rows=[],queue=pins.files.map(row=>row.file),seen=new Set(),external=new Set();
const proofFile='docs/healthroster-build/proposals/strict-qualification-native-interfaces/RUNTIME_SIDECAR_REVERSE.json';
const moneyRuntime=JSON.parse(git(['show',money+':'+proofFile]));
function read(file){const forward=pins.files.find(row=>row.file===file);if(forward)return {source:'THIS_PROPOSAL_FORWARD',bytes:fs.readFileSync(path.join(dir,forward.output))};
 try{return {source:pins.i2_commit,bytes:git(['show',pins.i2_commit+':'+file])};}catch{
  const proof=moneyRuntime.proofs.find(row=>row.destination===file);
  if(!proof)throw Error('UNMOUNTED_DEPENDENCY '+file);
  const target='docs/healthroster-build/proposals/strict-qualification-native-interfaces/'+proof.output,bytes=git(['show',money+':'+target]);
  if(sha(bytes)!==proof.output_sha256)throw Error('MONEY_RUNTIME_HASH_CHANGED');
  return {source:money,source_file:target,bytes};
 }}
while(queue.length){const file=queue.shift();if(seen.has(file))continue;seen.add(file);const {bytes,source,source_file}=read(file),imports=[];
 const text=bytes.toString();
 const refs=[...text.matchAll(/^\s*(?:import|export)\s+(?:\{[^;]*?\}|[^;\r\n]*?)\s+from\s*['"]([^'"\r\n]+)['"]|^\s*import\s*['"]([^'"\r\n]+)['"]/gm)].map(m=>m[1]??m[2]);
 refs.push(...[...text.matchAll(/\bimport\s*\(\s*['"]([^'"\r\n]+)['"]/g)].map(m=>m[1]));
 for(const ref of refs){if(!ref.startsWith('.')){external.add(ref);continue;}
  const resolved=path.posix.normalize(path.posix.join(path.posix.dirname(file),ref));
  if(resolved.startsWith('../')||path.posix.isAbsolute(resolved))throw Error('IMPORT_ESCAPES_ROOT');imports.push(resolved);queue.push(resolved);
 }
 const target=path.join(mount,file);fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,bytes);
 rows.push({file,source_commit:source,source_file:source_file??file,sha256:sha(bytes),imports:[...new Set(imports)].sort()});
}
fs.mkdirSync(mount,{recursive:true});fs.writeFileSync(path.join(mount,'package.json'),'{"private":true,"type":"module"}\n');
const syntax=[];for(const row of rows){if(!/\.m?js$/.test(row.file))continue;const check=spawnSync(process.execPath,['--check',path.join(mount,row.file)],{encoding:'utf8'});
 if(check.status!==0)throw Error('SYNTAX_REFUSED '+row.file+' '+check.stderr.slice(0,300));syntax.push(row.file);}
fs.writeFileSync(path.join(dir,'RUNTIME_DEPENDENCY_PINS.json'),JSON.stringify({schema_version:'INSTALL2_SHARED_RUNTIME_IMPORT_CLOSURE_V1',
 i2_commit:pins.i2_commit,current_commit:pins.current_commit,broad_commit:pins.broad_commit,money_commit:money,
 files:rows.sort((a,b)=>a.file.localeCompare(b.file)),external_packages:[...external].sort(),syntax_files:syntax.sort(),
 no_packages_installed:true,no_external_call:true,no_native_execution:true},null,2)+'\n');
console.log(JSON.stringify({closure_files:rows.length,syntax_files:syntax.length,external_packages:[...external].sort()}));
