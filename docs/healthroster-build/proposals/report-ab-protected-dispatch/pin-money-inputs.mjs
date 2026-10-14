import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';import {execFileSync} from 'node:child_process';
import {sha256} from './weekly-source-protected-release-qualification.mjs';
const root=process.argv[2],expectedCommit=process.argv[3],dir=path.dirname(fileURLToPath(import.meta.url));
if(!root||!/^[0-9a-f]{40}$/.test(expectedCommit??''))throw Error('EXPLICIT_MONEY_SOURCE_PIN_REQUIRED');
const folder='docs/healthroster-build/proposals/strict-qualification-native-interfaces';
const git=args=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,encoding:null,maxBuffer:32*1024*1024});
if(git(['rev-parse','HEAD']).toString().trim()!==expectedCommit)throw Error('MONEY_SOURCE_PIN_CHANGED');
function measured(file){const saved=fs.readFileSync(path.join(root,file)),committed=git(['show',expectedCommit+':'+file]);
 if(!saved.equals(committed))throw Error('MONEY_SOURCE_NOT_COMMITTED '+file);return {path:file,bytes:saved.length,sha256:sha256(saved),saved_git_equal:true};}
const manifest=measured(folder+'/REVIEW_MANIFEST.json'),checks=measured(folder+'/SOURCE_CHECK_RESULT.json');
const data=JSON.parse(fs.readFileSync(path.join(root,manifest.path),'utf8'));
if(data.schema_version!=='STRICT_QUALIFICATION_NATIVE_INTERFACES_FROZEN_SOURCE_REVIEW_V1'||!Array.isArray(data.files))throw Error('MONEY_MANIFEST_INVALID');
const artifacts=data.files.map(row=>{if(!row.file.startsWith(folder+'/')||row.file.includes('..'))throw Error('MONEY_MANIFEST_PATH_INVALID');
 const actual=measured(row.file);if(actual.bytes!==row.bytes||actual.sha256!==row.sha256)throw Error('MONEY_MANIFEST_ARTIFACT_CHANGED '+row.file);return actual;});
fs.writeFileSync(path.join(dir,'MONEY_DEPENDENCY_PINS.json'),JSON.stringify({schema_version:'REPORT_AB_FINAL_MONEY_DEPENDENCY_SOURCE_PINS_V1',
 source_commit:expectedCommit,source_manifest:manifest,source_checks:checks,artifacts,
 status:'SOURCE_ONLY_SAVED_GIT_MANIFEST_EQUAL_ROOT_COMPOSITION_AND_NATIVE_ACCEPTANCE_HELD',
 instruction:'Merge exact reviewed Report binding/loader hunks into these later complete Money owners; never overwrite their OFF/strict/activation/catalogue/recorder arms with an older successor.'},null,2)+'\n');
console.log(JSON.stringify({source_commit:expectedCommit,artifacts:artifacts.length,manifest_sha256:manifest.sha256,checks_sha256:checks.sha256}));
