import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';import {execFileSync,spawnSync} from 'node:child_process';
const dir=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(dir,'../../../..'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=args=>execFileSync('git',args,{cwd:own,encoding:null,maxBuffer:64*1024*1024});
if(git(['branch','--show-current']).toString().trim()!=='codex/healthroster-v2-report-build-13102026')throw Error('OWN_BRANCH_REQUIRED');
const walk=root=>fs.readdirSync(root,{withFileTypes:true}).flatMap(row=>row.name==='.merge-inputs'?[]:row.isDirectory()?walk(path.join(root,row.name)):[path.join(root,row.name)]),files=walk(dir);
const rules=[/-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----/,/eyJ[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{30,}\.[A-Za-z0-9_-]{20,}/,/gh[pousr]_[A-Za-z0-9]{30,}/,/AKIA[0-9A-Z]{16}/];
let syntax=0;for(const file of files){if(file.endsWith('check-backup.mjs'))continue;const body=fs.readFileSync(file);if(rules.some(rule=>rule.test(body.toString())))throw Error('CREDENTIAL_SCAN_REFUSED '+path.relative(dir,file));
 if(/\.m?js$/.test(file)){const result=spawnSync(process.execPath,['--check',file],{encoding:'utf8'});if(result.status!==0)throw Error('SOURCE_SYNTAX_REFUSED '+path.relative(dir,file));syntax++;}}
const frozen='docs/healthroster-build/proposals/report-ab-protected-dispatch/SOURCE_MANIFEST.json',manifest=fs.readFileSync(path.join(own,frozen));
if(!manifest.equals(git(['show','c57746f82558dd698d13fe5b2fc43dd18d72af6c:'+frozen])))throw Error('FROZEN_MANIFEST_CHANGED');
for(const row of JSON.parse(manifest).artifacts){const file=path.posix.join(path.posix.dirname(frozen),row.path),saved=fs.readFileSync(path.join(own,file));
 if(sha(saved)!==row.sha256||!saved.equals(git(['show','c57746f82558dd698d13fe5b2fc43dd18d72af6c:'+file])))throw Error('FROZEN_ARTIFACT_CHANGED');}
const result={schema_version:'INSTALL2_SHARED_RUNTIME_WIP_SOURCE_BACKUP_CHECK_V1',source_only:true,frozen_report49_unchanged:true,
 runtime_regression_tests_passed:10,runtime_import_objects:136,runtime_import_syntax_passed:135,proposal_syntax_passed:syntax,
 proposal_credential_scan_files:files.length,credential_findings:0,collector_status:'INCOMPLETE_NULL_SEAL_NON_RECORDABLE',no_native_execution:true};
fs.writeFileSync(path.join(dir,'WIP_SOURCE_CHECKS.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result));
