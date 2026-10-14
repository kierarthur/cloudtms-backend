// Source-only successor; frozen35 and canonical28 remain immutable inputs.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(here,'../../../..'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const prior=path.join(root,'docs/healthroster-build/proposals/install2-shared-runtime-forward-20261014'),canonical=path.join(root,'docs/healthroster-build/proposals/install2-report-canonical-integration-20261014');
const priorManifest=fs.readFileSync(path.join(prior,'REVIEW_MANIFEST.json')),canonicalManifest=fs.readFileSync(path.join(canonical,'REVIEW_MANIFEST.json'));
if(sha(priorManifest)!=='df1431cae0169faf922e209e677d5a09d753742426597c9e24beaa352c1a7a50'||sha(canonicalManifest)!=='6b16dda5df21ea961086c2336ada3e6337f482d244f0137acd1a6142d82eec75')throw Error('FROZEN_INPUT_MANIFEST_CHANGED');
const before=fs.readFileSync(path.join(prior,'build-native-seal.mjs'));
const hunks=[
 {id:'CANONICAL28_PINNED_MAPPER_DEPENDENCY',old:"from './forward/scripts/weekly-source-report-fin-native-mapper.mjs';",new:"from '../install2-report-canonical-integration-20261014/forward/scripts/weekly-source-report-fin-native-mapper.mjs';"},
 {id:'ACTUAL_MANAGED_CANONICAL_CONTRACT_HASH',old:"const signature=r=>r.schema+'.'+r.identity.replace",new:"// Match actual managed canonicalContractHash; raw formatting is separate input provenance.\nconst contractHash=sha(JSON.stringify(contract));\nconst signature=r=>r.schema+'.'+r.identity.replace"},
 {id:'CATALOGUE_CANONICAL_CONTRACT_BINDING',old:"catalogue.contract_sha256!==sha(contractBytes)",new:"catalogue.contract_sha256!==sha(JSON.stringify(contract))"},
 {id:'EXECUTION_CANONICAL_CONTRACT_BINDING',old:"contract_sha256:sha(contractBytes)",new:"contract_sha256:contractHash"},
 {id:'RETAIN_RAW_CONTRACT_PREPARATION_HASH',old:"console.log(JSON.stringify({contract_sha256:seal.contract_sha256,owners:",new:"console.log(JSON.stringify({contract_sha256:seal.contract_sha256,compiled_contract_raw_sha256:sha(contractBytes),owners:"}
];
let after=before.toString('utf8');for(const h of hunks){if(after.split(h.old).length!==2)throw Error('SEAL_CONSTRUCTOR_HUNK_NOT_UNIQUE');after=after.replace(h.old,()=>h.new);}
let reversed=after;for(const h of [...hunks].reverse()){if(reversed.split(h.new).length!==2)throw Error('SEAL_CONSTRUCTOR_REVERSE_NOT_UNIQUE');reversed=reversed.replace(h.new,()=>h.old);}
if(!Buffer.from(reversed).equals(before))throw Error('SEAL_CONSTRUCTOR_COMPLETE_REVERSE_FAILED');
const foundation='fe7b3dfc2946cf334ba88681ae425105f864c42f',file='scripts/cloudtms-db-release-lib.mjs';
const lib=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','show',foundation+':'+file],{cwd:root,maxBuffer:64*1024*1024});
if(!lib.toString('utf8').includes('export function canonicalContractHash(contract) {\n  return sha256(JSON.stringify(contract));\n}'))throw Error('ACTUAL_MANAGED_CANONICAL_OWNER_CHANGED');
fs.writeFileSync(path.join(here,'build-native-seal.mjs'),after);
fs.writeFileSync(path.join(here,'SEAL_CONSTRUCTOR_REVERSE.json'),JSON.stringify({schema_version:'INSTALL2_REPORT_SEAL_CANONICAL_AUTHORITY_SUCCESSOR_REVERSE_V1',input:'docs/healthroster-build/proposals/install2-shared-runtime-forward-20261014/build-native-seal.mjs',input_sha256:sha(before),output:'build-native-seal.mjs',output_sha256:sha(after),hunks,full_reverse:true,prior35_manifest_sha256:sha(priorManifest),canonical28_manifest_sha256:sha(canonicalManifest),canonical_authority_hash_source:{source_commit:foundation,path:file,sha256:sha(lib),expression:'SHA256(JSON.stringify(parsedContract))'},raw_preparation_hash:'compiled_contract_raw_sha256 = SHA256(exact parsed input file bytes); never installed execution contract identity',no_native_execution:true},null,2)+'\n');
console.log(JSON.stringify({constructor_sha256:sha(after),complete_reverse_hunks:hunks.length,prior35_manifest_sha256:sha(priorManifest),canonical28_manifest_sha256:sha(canonicalManifest),no_native_execution:true}));
