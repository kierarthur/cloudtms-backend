import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const target=path.join(here,'REVIEW_MANIFEST.json');
if(fs.existsSync(target))throw new Error('Frozen proposal exists; use a separate successor');
const files=[];
function walk(dir){for(const row of fs.readdirSync(dir,{withFileTypes:true})){const full=path.join(dir,row.name);if(row.isDirectory())walk(full);else{
 const bytes=fs.readFileSync(full),file=path.relative(here,full).replaceAll('\\','/');
 for(const pattern of [/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,/eyJ[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{20,}/,/gh[pousr]_[A-Za-z0-9]{30,}/,/AKIA[0-9A-Z]{16}/])
   if(pattern.test(bytes.toString()))throw new Error('Credential safety refusal in '+file);
 files.push({path:file,bytes:bytes.length,sha256:sha(bytes)});
}}}
walk(here);files.sort((a,b)=>a.path.localeCompare(b.path));
const checks=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_CHECK_RESULT.json')));
if(checks.unit_pass!==20||checks.native_calls!==0||checks.prior_publication_files_unchanged!==137)throw new Error('Source checks incomplete');
fs.writeFileSync(target,JSON.stringify({schema_version:'A0_OFFICE_CURRENT_RUNTIME_WIRING_FREEZE_V1',
 status:'FROZEN_FOR_INDEPENDENT_SOURCE_CRITIC_NATIVE_AND_OFFICE_CONSUMER_ACCEPTANCE_HELD',source_only:true,
 input_commit:checks.input_commit,source_check_sha256:sha(fs.readFileSync(path.join(here,'SOURCE_CHECK_RESULT.json'))),
 credential_scan:{scope:'ALL_PACKET_ARTIFACT_BYTES_INCLUDING_COMPLETE_CURRENT_INDEX',artifacts:files.length,findings:0},
 native_compiled_contract:null,native_calls:0,artifacts:files},null,2)+'\n');
console.log(JSON.stringify({artifacts:files.length+1,manifest_sha256:sha(fs.readFileSync(target)),checks_sha256:sha(fs.readFileSync(path.join(here,'SOURCE_CHECK_RESULT.json')))}));
