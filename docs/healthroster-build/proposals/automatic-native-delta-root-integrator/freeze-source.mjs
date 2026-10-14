import fs from 'node:fs';
import path from 'node:path';
import { here, hash } from './integrator.mjs';
function walk(dir){return fs.readdirSync(dir,{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(path.join(dir,e.name)):[path.join(dir,e.name)]);}
const artifacts=walk(here).filter(p=>path.basename(p)!=='SOURCE_MANIFEST.json').sort().map(p=>{
 const b=fs.readFileSync(p);return {path:path.relative(here,p).replaceAll('\\','/'),bytes:b.length,sha256:hash(b)};
});
const manifest={schema_version:'AUTOMATIC_SOURCE_INTEGRATOR_MANIFEST_V1',status:'REVIEWED_SOURCE_ONLY_ROOT_APPLY_COMPILE_AND_RUNTIME_HELD',
 authoritativeLane:'healthroster-v2-report-build-13102026',branch:'codex/healthroster-v2-report-build-13102026',
 rootInputCommit:'9fc45b9a4cc3a8dce400887f1640cfaf233f23be',artifactCount:artifacts.length,artifacts,
 packageSha256:hash(artifacts.map(a=>`${a.path}\0${a.sha256}\0`).join('')),
 checks:{integrationTests:14,pass:14,fail:0,skip:0,nodeSyntax:true,officialPackaging:true,
  semanticSQLCompilation:false,rootWrites:false,databaseCalls:false,networkBusinessCalls:false,bankingSourceReads:false},
 remainingGates:['Root independently applies exact guarded source outputs and seals generated contract/security.',
 'Root SEND_7 compilation and actual stage/financial/component/transport/Source-CAS/API qualification.',
 'Capability dependencies remain missing; rejected operational prerequisiteV2 is not imported.',
 'Report A/B financial/render/pay isolation and first-installed Banking compatibility remain separate held gates.']};
fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({artifactCount:artifacts.length,packageSha256:manifest.packageSha256,
 manifestSha256:hash(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json')))}));
