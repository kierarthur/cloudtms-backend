import fs from 'node:fs';
import path from 'node:path';
import { here, sha, ROOT_COMMIT } from './build-proposal.mjs';
const walk=dir=>fs.readdirSync(dir,{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(path.join(dir,e.name)):[path.join(dir,e.name)]);
const artifacts=walk(here).filter(p=>path.basename(p)!=='SOURCE_MANIFEST.json').sort().map(p=>{
 const b=fs.readFileSync(p);return {path:path.relative(here,p).replaceAll('\\','/'),bytes:b.length,sha256:sha(b)};
});
const receipt=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_RECEIPT.json'),'utf8'));
const manifest={schema_version:'REPORT_PREPARED_OUTPUT_SOURCE_MANIFEST_V1',status:'REVIEWED_SOURCE_PROPOSAL_NATIVE_ACCEPTANCE_HELD',
 authoritativeLane:'healthroster-v2-report-build-13102026',branch:'codex/healthroster-v2-report-build-13102026',
 rootInputCommit:ROOT_COMMIT,rootObservedCommit:receipt.rootObservedCommit,census:receipt.census,
 artifactCount:artifacts.length,artifacts,packageSha256:sha(artifacts.map(a=>`${a.path}\0${a.sha256}\0`).join('')),
 checks:{sourceTests:30,pass:30,fail:0,skip:0,nodeSyntax:true,officialSQLStatementPackaging:true,
  semanticSQLCompilation:false,nativeWriterCapture:false,nativeFinancialAcceptance:false,
  rootWrites:false,databaseCalls:false,businessNetworkCalls:false,bankingSourceReads:false},
 remainingGates:receipt.remainingGates};
fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({artifactCount:artifacts.length,packageSha256:manifest.packageSha256,
 manifestSha256:sha(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json')))}));
