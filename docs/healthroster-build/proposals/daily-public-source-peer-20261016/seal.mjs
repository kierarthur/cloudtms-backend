import fs from 'node:fs';import path from 'node:path';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),sha=x=>createHash('sha256').update(x).digest('hex');
function walk(dir){return fs.readdirSync(dir,{withFileTypes:true}).flatMap(entry=>entry.isDirectory()?walk(path.join(dir,entry.name)):[path.join(dir,entry.name)]);}
const files=walk(here).filter(file=>path.basename(file)!=='PEER_MANIFEST.json').map(file=>({file:path.relative(here,file).replaceAll('\\','/'),bytes:fs.statSync(file).size,sha256:sha(fs.readFileSync(file))}));
const manifest={version:'HEALTH_DAILY_PUBLIC_SOURCE_PEER_MANIFEST_V1',reviewedCommit:'6ba4c06ed5843231fab10c16fb6273ea1f640908',files,native:'NOT_RUN',finalPartB:'NOT_RUN'};
fs.writeFileSync(path.join(here,'PEER_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
for(const file of ['REVIEW.json','TEST_RESULT.json','GRAMMAR_INDEPENDENT.json','PEER_MANIFEST.json'])console.log(file+' '+sha(fs.readFileSync(path.join(here,file))));
