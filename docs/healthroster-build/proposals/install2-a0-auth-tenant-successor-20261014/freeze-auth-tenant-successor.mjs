import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),sha=b=>crypto.createHash('sha256').update(b).digest('hex');if(fs.existsSync(path.join(here,'REVIEW_MANIFEST.json')))throw Error('Preserve frozen successor');const artifacts=[];
function walk(dir){for(const row of fs.readdirSync(dir,{withFileTypes:true})){const p=path.join(dir,row.name);if(row.isDirectory())walk(p);else{const b=fs.readFileSync(p);
 for(const pattern of [/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,/eyJ[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{20,}/,/gh[pousr]_[A-Za-z0-9]{30,}/,/AKIA[0-9A-Z]{16}/])if(pattern.test(b.toString()))throw Error('Credential material');
 artifacts.push({path:path.relative(here,p).replaceAll('\\','/'),bytes:b.length,sha256:sha(b)});}}}walk(here);artifacts.sort((a,b)=>a.path.localeCompare(b.path));
const checks=fs.readFileSync(path.join(here,'SOURCE_CHECK_RESULT.json'));if(JSON.parse(checks).unit_pass!==26)throw Error('Source checks incomplete');
fs.writeFileSync(path.join(here,'REVIEW_MANIFEST.json'),JSON.stringify({schema_version:'A0_AUTH_TENANT_SOURCE_SUCCESSOR_FREEZE_V1',source_only:true,
 status:'FROZEN_FOR_INDEPENDENT_CRITIC_NATIVE_API_ISOLATION_AND_CONSUMERS_HELD',source_check_sha256:sha(checks),
 actual_config_changes:0,credential_scan:{all_artifacts:artifacts.length,findings:0},artifacts},null,2)+'\n');
console.log(JSON.stringify({artifacts:artifacts.length+1,manifest_sha256:sha(fs.readFileSync(path.join(here,'REVIEW_MANIFEST.json'))),checks_sha256:sha(checks)}));
