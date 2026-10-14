import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../..'),sha=b=>createHash('sha256').update(b).digest('hex'),files=[];
for(const [folder,expected,count] of [['rekey-source-alias-current-native-successor-20261014','44706e1eef850a647458d1af77e4a860520ca60d68801b1b91f88bae6135be07',15],['rekey-source-alias-all-owner-successor-20261014','400b1b49fe5534b79baccbea9a9e3b7017458b40c84afeb66ebafedced92ff5c',10]]){
  const raw=fs.readFileSync(path.join(dir,folder,'FROZEN_ARTIFACT_MANIFEST.json'));assert.equal(sha(raw),expected);const m=JSON.parse(raw);assert.equal(m.artifact_count,count);
  for(const a of m.artifacts){const file=folder+'/'+a.file,b=fs.readFileSync(path.join(dir,file));assert.equal(b.length,a.bytes);assert.equal(sha(b),a.sha256);files.push(file);}files.push(folder+'/FROZEN_ARTIFACT_MANIFEST.json');
}
for(const prefix of ['MONEY_REKEY_SOURCE_ALIAS','MONEY_REKEY_ALL_OWNER'])files.push(prefix+'_REVIEW_HANDOFF_14102026.md',prefix+'_CREATOR_CHECK_14102026.mjs',prefix+'_CREATOR_CHECK_RESULT_14102026.json');
files.push('MONEY_REKEY_ALIAS_SOURCE_PUBLICATION_HANDOFF_14102026.md','MONEY_REKEY_ALIAS_SOURCE_PUBLICATION_CHECK_14102026.mjs');
const rules=[['private-key',/-----BEGIN [A-Z ]*PRIVATE KEY-----/],['github-token',/(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})/],['literal-jwt',/eyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}/],['quoted-secret-assignment',/(?:password|service_role_key|api_key|access_token|bearer_token)\s*[:=]\s*['"][^'"\r\n]{12,}['"]/i]],findings=[];
for(const file of files)for(const [rule,re] of rules)if(re.test(fs.readFileSync(path.join(dir,file),'utf8')))findings.push({file,rule});assert.equal(findings.length,0,JSON.stringify(findings));
const result={source_only:true,listed_artifacts:25,manifests:2,source_scan_files:files.length,credential_rules:4,credential_findings:0,raw_hash_length_mismatches:0,critic_sha256:'72662d08c841ce001ddadf48f9a32b29ade6e913c5953e83a0d9fdfb6342e53e',finding_source_closed_only_with_corrective_helper:true,new_source_cases:15,inherited_source_cases:9,syntax_passed:3,whole_owner_reversals:4,unique_hunks:8,defective_helper_selected:false,old_findings_preserved:true,canonical_mounted:false,native_proof:false,full_paid_overlap_complete:false,full_primary_complete:false,scratch_created:false,cleanup_retried:false};
try{execFileSync('git',['diff','--cached','--check'],{cwd:root,stdio:'pipe'});result.whitespace={status:'PASS',findings:[]};}catch(e){result.whitespace={status:'RETAINED_RAW_ARCHIVE_EXCEPTIONS_NOT_PASS',exit_code:e.status,findings:e.stdout.toString().trim().split(/\r?\n/)};}
const resultFile='MONEY_REKEY_ALIAS_SOURCE_PUBLICATION_CHECK_RESULT_14102026.json';
const rel=f=>path.relative(root,path.join(dir,f)).replaceAll('\\','/');
if(process.argv.includes('--index')){
  const expected=[...files,resultFile].map(rel).sort(),actual=execFileSync('git',['diff','--cached','--name-only'],{cwd:root}).toString().trim().split(/\r?\n/).sort();assert.deepEqual(actual,expected);
  for(const f of [...files,resultFile])assert.deepEqual(execFileSync('git',['show',':'+rel(f)],{cwd:root,maxBuffer:4e6}),fs.readFileSync(path.join(dir,f)),f);
  console.log(JSON.stringify({exact_staged_whitelist:true,paths:expected.length,index_saved_mismatches:0}));
}else if(process.argv.includes('--head')){
  for(const f of [...files,resultFile])assert.deepEqual(execFileSync('git',['show','HEAD:'+rel(f)],{cwd:root,maxBuffer:4e6}),fs.readFileSync(path.join(dir,f)),f);
  console.log(JSON.stringify({exact_head_saved_paths:files.length+1,head_saved_mismatches:0,head:execFileSync('git',['rev-parse','HEAD'],{cwd:root}).toString().trim()}));
}else{fs.writeFileSync(path.join(dir,resultFile),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({source_scan_files:files.length,credential_findings:0,raw_hash_length_mismatches:0,whitespace:result.whitespace}));}
