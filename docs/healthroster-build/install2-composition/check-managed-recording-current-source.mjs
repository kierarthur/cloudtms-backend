// Source-only checks. No protected entry, release writer or real Git tree capture is executed.
import fs from 'node:fs';import {spawnSync} from 'node:child_process';import {createHash} from 'node:crypto';
const base='docs/healthroster-build/install2-composition/',sha=b=>createHash('sha256').update(b).digest('hex');
const composition=JSON.parse(fs.readFileSync(base+'MANAGED_RECORDING_SOURCE_COMPOSITION_14102026.json'));
for(const row of [...composition.files,...composition.replacements])if(sha(fs.readFileSync(row.file))!==row.sha256)throw Error('CURRENT_RECORDING_SOURCE_PIN_MISMATCH '+row.file);
const env={};for(const name of ['SystemRoot','WINDIR','TEMP','TMP','PATH','PATHEXT','COMSPEC'])if(process.env[name])env[name]=process.env[name];
const outputs=['scripts/weekly-source-protected-execution-tree.mjs','scripts/cloudtms-managed-release.mjs','scripts/cloudtms-db-release.mjs','scripts/weekly-source-protected-record-evidence.mjs'];
for(const file of outputs){const result=spawnSync(process.execPath,['--check',file],{env,encoding:'utf8'});if(result.status!==0)throw Error('CURRENT_SCRIPT_SYNTAX_FAILED '+file);}
const packet='docs/healthroster-build/proposals/';
const tests=[
 packet+'managed-qualification-setup-mode-successor-20261014/setup-mode-source.test.mjs',
 packet+'protected-record-evidence-current-root-install2-forward/protected-record-entry-source.test.mjs',
 packet+'protected-record-admission-successor-20261014/record-admission-source.test.mjs',
 packet+'p10-same-descriptor-successor-20261014/same-descriptor-source.test.mjs'
];
const steps=[];
for(const [name,cmd,args]of [['pure-reviewed-source-vectors',process.execPath,['--test','--test-reporter=tap',...tests]],['db:check',process.execPath,['scripts/cloudtms-db-release.mjs','check']],['security:secrets',process.execPath,['scripts/verify-source-secret-safety.mjs']],['staged-whitespace','git',['diff','--cached','--check']]]){
 const result=spawnSync(cmd,args,{env,encoding:'utf8',maxBuffer:20_000_000});const out=(result.stdout??'')+(result.stderr??''),lines=out.split(/\r?\n/);
 const step={name,exitCode:result.status,outputSha256:sha(out),summary:name==='pure-reviewed-source-vectors'?lines.filter(line=>/^# (tests|suites|pass|fail|cancelled|skipped|todo|duration_ms)\b/.test(line)):lines.filter(Boolean).slice(-12),failures:lines.filter(line=>/^not ok \d+ - /.test(line))};
 steps.push(step);console.log(JSON.stringify(step));if(result.status!==0)break;
}
const receiptFile=base+'MANAGED_RECORDING_SOURCE_CHECKS_14102026.json';const prior=fs.existsSync(receiptFile)?JSON.parse(fs.readFileSync(receiptFile)):null;
fs.writeFileSync(receiptFile,JSON.stringify({sourceOnly:true,syntaxOutputs:outputs,tests,steps,priorHeldSteps:prior?.priorHeldSteps??prior?.steps?.filter(row=>row.exitCode!==0)??[],noProtectedEntryReleaseRecorderOrLargeTreeCapture:true,nativeGatesRun:false},null,2)+'\n');
if(steps.some(step=>step.exitCode!==0))process.exitCode=1;
