// Seal only this new public source packet; no runtime or private-path inspection.
import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';import {baseCommit,hash,json} from './recipe.mjs';
const here=fileURLToPath(new URL('./',import.meta.url)),files=[];
function walk(at=''){for(const e of fs.readdirSync(here+at,{withFileTypes:true})){const file=at+e.name;if(file==='MANIFEST.json')continue;if(e.isDirectory())walk(file+'/');else{const b=fs.readFileSync(here+file);files.push({file,bytes:b.length,sha256:hash(b)});}}}
walk();files.sort((a,b)=>a.file.localeCompare(b.file));fs.writeFileSync(path.join(here,'MANIFEST.json'),json({version:'DAILY_GENUINE_R7_SOURCE_RECIPE_FREEZE_V1',baseReportCommit:baseCommit,files}));
console.log(JSON.stringify({status:'PUBLIC_SOURCE_RECIPE_SEALED',files:files.length+1,manifestSha256:hash(fs.readFileSync(path.join(here,'MANIFEST.json')))}));
