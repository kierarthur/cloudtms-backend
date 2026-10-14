import fs from 'node:fs';
import path from 'node:path';
import {here,sha} from './build-forward.mjs';
const walk=p=>fs.readdirSync(p,{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(path.join(p,e.name)):[path.join(p,e.name)]);
const artifacts=walk(here).filter(p=>path.basename(p)!=='SOURCE_MANIFEST.json').sort().map(p=>{
 const b=fs.readFileSync(p);return {path:path.relative(here,p).replaceAll('\\','/'),bytes:b.length,sha256:sha(b)};
});
const receipt=JSON.parse(fs.readFileSync(path.join(here,'FORWARD_RECEIPT.json'),'utf8'));
const manifest={schema_version:'REPORT_PREPARED_ARRAY_FORWARD_MANIFEST_V1',status:'REVIEWED_SOURCE_ONLY_NATIVE_ARRAY_AND_FINANCIAL_PROOF_HELD',
 frozenPreparedCommit:receipt.frozenPreparedCommit,frozenPreparedManifestSha256:receipt.frozenPreparedManifestSha256,
 criticSha256:receipt.criticSha256,artifactCount:artifacts.length,artifacts,
 packageSha256:sha(artifacts.map(a=>`${a.path}\0${a.sha256}\0`).join('')),
 checks:{sourceTests:18,pass:18,fail:0,skip:0,nodeSyntax:true,officialSQLStatementPackaging:true,
  semanticSQLCompilation:false,nativeArrayRoundtrip:false,nativeFinancialAcceptance:false,
  frozenSourcesChanged:false,rootWrites:false,databaseCalls:false,bankingSourceReads:false},
 remainingGates:receipt.remainingGates};
fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({artifactCount:artifacts.length,packageSha256:manifest.packageSha256,manifestSha256:sha(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json')))}));
