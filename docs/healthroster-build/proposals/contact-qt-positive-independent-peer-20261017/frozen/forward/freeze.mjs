// Own proposal-only freeze; no database connection or root mutation.
import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';import {spawnSync} from 'node:child_process';
const here=path.dirname(fileURLToPath(import.meta.url));
const sha=b=>createHash('sha256').update(b).digest('hex');
const check=(v,e)=>{if(!v)throw Error(e);};
check(fs.realpathSync.native(here).replaceAll('\\','/').startsWith('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026/'),'OWN_PHYSICAL_D_ONLY');
if(process.argv.includes('--verify')){
 const m=JSON.parse(fs.readFileSync(here+'/MANIFEST.json'));for(const r of m.files){const b=fs.readFileSync(here+'/'+r.file);check(b.length===r.bytes&&sha(b)===r.sha256,'FROZEN_BYTES:'+r.file);}
 console.log(JSON.stringify({files:m.files.length,rawExact:true,native:'NOT_RUN'}));
}else{
 check(!fs.existsSync(here+'/MANIFEST.json'),'FROZEN_HISTORY');
 const r=spawnSync(process.execPath,[here+'/verify.mjs'],{cwd:here,env:{...process.env,PYTHONDONTWRITEBYTECODE:'1'},encoding:'utf8',windowsHide:true});
 check(r.status===0,r.stderr||r.stdout);JSON.parse(r.stdout.trim());fs.writeFileSync(here+'/VERIFICATION.json',r.stdout.trim()+'\n');
 const files=[];const visit=(dir,prefix='')=>{for(const e of fs.readdirSync(dir,{withFileTypes:true}).sort((a,b)=>a.name.localeCompare(b.name))){if(e.name==='__pycache__')continue;const file=prefix+e.name;if(e.isDirectory())visit(dir+'/'+e.name,file+'/');else if(file!=='MANIFEST.json'){const b=fs.readFileSync(dir+'/'+e.name);files.push({file,bytes:b.length,sha256:sha(b)});}}};visit(here);
 fs.writeFileSync(here+'/MANIFEST.json',JSON.stringify({version:'CONTACT_QT_NAME_ADMISSION_FORWARD_SOURCE_ONLY_FREEZE_V1',parentCommit:'ddcb87b6f357ffa5064e3a2f8e15946cd0e12938',parentManifestSha256:'1fba7ea3a91147d75ea69df68b0e80be1300fccba1e5e0be4fe17eaf75713204',native:'NOT_RUN',runtimeTargets:[],files},null,2)+'\n');
 console.log(JSON.stringify({files:files.length,manifestSha256:sha(fs.readFileSync(here+'/MANIFEST.json')),native:'NOT_RUN'}));
}
