// Source-only repeatable packaging, root module vectors and tracked-source checks.
import fs from 'node:fs';
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {readManagedClosure,exactAtomicBody,scanManagedSql} from '../../../scripts/cloudtms-managed-release-sql.mjs';
const base='docs/healthroster-build/install2-composition/';
const sha=b=>createHash('sha256').update(b).digest('hex');
const manifest=JSON.parse(fs.readFileSync(base+'REPORT_CURRENT_SOURCE_COMPOSITION_14102026.json'));
for(const row of manifest.files)if(sha(fs.readFileSync(row.file))!==row.sha256)throw Error('REPORT_SOURCE_PIN_MISMATCH '+row.file);
const predecessor=JSON.parse(fs.readFileSync(base+'REPORT_FROZEN_PREDECESSOR_ARCHIVE_14102026.json'));
for(const row of predecessor.files)if(sha(fs.readFileSync(row.file))!==row.sha256)throw Error('REPORT_ARCHIVE_PIN_MISMATCH '+row.file);
const closure=readManagedClosure(manifest.rootFile,process.cwd()),body=exactAtomicBody(closure.expanded);
const env={};for(const name of ['SystemRoot','WINDIR','TEMP','TMP','PATH','PATHEXT','COMSPEC'])if(process.env[name])env[name]=process.env[name];
const steps=[];
let whitespace=null;
const jobs=[
 ['actual-root-unit-vectors',process.execPath,['--test','--test-reporter=tap',base+'report-current-collector.test.mjs',base+'report-current-btrim.test.mjs']],
 ['db:check',process.execPath,['scripts/cloudtms-db-release.mjs','check']],
 ['security:secrets',process.execPath,['scripts/verify-source-secret-safety.mjs']],
 ['staged-whitespace','git',['diff','--cached','--check']]
];
for(const [name,cmd,args]of jobs){
 const result=spawnSync(cmd,args,{env,encoding:'utf8',maxBuffer:20_000_000});
 const out=(result.stdout??'')+(result.stderr??''),lines=out.split(/\r?\n/);
 const step={name,exitCode:result.status,outputSha256:sha(out),summary:name==='actual-root-unit-vectors'?lines.filter(line=>/^# (tests|suites|pass|fail|cancelled|skipped|todo|duration_ms)\b/.test(line)):lines.filter(Boolean).slice(-12),failures:lines.filter(line=>/^not ok \d+ - /.test(line))};
 steps.push(step);console.log(JSON.stringify(step));
 if(name==='staged-whitespace'&&result.status!==0){
  const immutable=new Map([...manifest.files,...predecessor.files].filter(row=>!row.derivative).map(row=>[row.file,row]));
  const findings=[];
  for(const line of lines){
   const found=line.match(/^(.+):(\d+): (trailing whitespace\.|new blank line at EOF\.)$/);
   if(!found)continue;
   const row=immutable.get(found[1]);if(!row)throw Error('UNCLASSIFIED_CURRENT_WHITESPACE '+found[1]);
   findings.push({file:found[1],line:Number(found[2]),finding:found[3],exactFrozenSourceSha256:row.sha256,publication:row.sourceCommit??predecessor.commit});
  }
  if(result.status!==2||!findings.length||lines.some(line=>line&&!line.startsWith('+')&&!/^.+:\d+: (trailing whitespace\.|new blank line at EOF\.)$/.test(line)))throw Error('UNCLASSIFIED_WHITESPACE_CHECK_FAILURE');
  whitespace={status:'EXACT_FROZEN_SOURCE_WHITESPACE_RETAINED_NOT_RAW_WHITESPACE_PASS',findings,newAuthoredUnclassifiedFindings:0,rawExitCode:result.status};
 }
 if(result.status!==0&&name!=='staged-whitespace')break;
}
fs.writeFileSync(base+'REPORT_SOURCE_INTEGRITY_14102026.json',JSON.stringify({sourceOnly:true,status:whitespace?'SOURCE_TESTS_DATABASE_INTEGRITY_AND_CREDENTIALS_PASS_EXACT_FROZEN_WHITESPACE_RETAINED':'SOURCE_CHECKS_PASS',rootFile:manifest.rootFile,canonicalClosureSha256:closure.closureHash,closureFiles:closure.files.size,atomicPackaging:true,statements:scanManagedSql(body).statements.length,bodySha256:sha(body),steps,whitespace,nativeSeal:null,noDatabaseOrNativeInvocation:true},null,2)+'\n');
if(steps.some(step=>step.exitCode!==0&&step.name!=='staged-whitespace'))process.exitCode=1;
