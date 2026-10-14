import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const root=path.dirname(fileURLToPath(import.meta.url));
const sha=bytes=>crypto.createHash('sha256').update(bytes).digest('hex');
const manifest=path.join(root,'REVIEW_MANIFEST.json');
if(fs.existsSync(manifest))throw new Error('FROZEN_DESIGN_EXISTS_DO_NOT_REWRITE');
const checks=JSON.parse(fs.readFileSync(path.join(root,'SOURCE_CHECK_RESULT.json'),'utf8'));
if(checks.pass_count!==15||checks.native_calls||checks.database_calls||checks.implementation_complete)throw new Error('DESIGN_SOURCE_CHECKS_INVALID');
const entries=[];
function visit(dir){for(const row of fs.readdirSync(dir,{withFileTypes:true}).sort((a,b)=>a.name.localeCompare(b.name))){
  const full=path.join(dir,row.name);if(row.isDirectory())visit(full);else if(row.isFile()){
    const bytes=fs.readFileSync(full);const relative=path.relative(root,full).replaceAll('\\','/');
    if(relative.endsWith('.sql')||relative.endsWith('.sqlinc'))throw new Error('DESIGN_MUST_NOT_CONTAIN_EXECUTABLE_SQL');
    if(/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|(?:sb_secret_|ghp_|github_pat_)[A-Za-z0-9_]{15,}|eyJ[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/.test(bytes.toString('utf8')))throw new Error(`CREDENTIAL_PATTERN:${relative}`);
    entries.push({path:relative,bytes:bytes.length,sha256:sha(bytes)});
  }
}}
visit(root);
const result={schema:'AGENCY_SINGLE_STORE_DESIGN_REVIEW_MANIFEST_V1',scope:'SOURCE_DESIGN_ONLY_BEFORE_IMPLEMENTATION',
  actual_current_source_git_commit:checks.actual_source_pin,source_checks_sha256:sha(fs.readFileSync(path.join(root,'SOURCE_CHECK_RESULT.json'))),
  artifact_count:entries.length+1,entries,native_seal:null,implementation_complete:false};
fs.writeFileSync(manifest,JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({artifact_count:result.artifact_count,manifest_sha256:sha(fs.readFileSync(manifest)),checks_sha256:result.source_checks_sha256}));
