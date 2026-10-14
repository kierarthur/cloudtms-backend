// Execute the protected workflow's source checks from exact Git archive bytes.
// No database configuration, SQL connection, release dispatch or deployment.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync,spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {sourceCheckEnvironment} from './healthroster-source-check-environment.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const base='07e66fb2b2742fbe634a41582f2ad3e004eed3b9';
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{cwd:root,encoding:'utf8',windowsHide:true,maxBuffer:16000000}).trim();
const sha=git(['rev-parse','HEAD']);
if(!/^[a-f0-9]{40}$/.test(sha)||git(['status','--porcelain']))throw Error('ARCHIVE_PREFLIGHT_CLEAN_COMMIT_REQUIRED');
if(git(['ls-remote','origin','refs/heads/codex/healthroster-install1-14102026']).split(/\s/)[0]!==sha)throw Error('ARCHIVE_PREFLIGHT_PUSHED_COMMIT_REQUIRED');
if(git(['merge-base',base,sha])!==base||git(['rev-list','--merges',base+'..'+sha]))throw Error('ARCHIVE_PREFLIGHT_LINEAR_ANCESTRY_REQUIRED');
const temporaryRoot='C:/tmp';
fs.mkdirSync(temporaryRoot,{recursive:true});
const folder=fs.mkdtempSync(path.join(temporaryRoot,'healthroster-install1-preflight-'+sha.slice(0,12)+'-'));
const tar=path.join(folder,'source.tar'),archive=path.join(folder,'source');
fs.mkdirSync(archive);
const receipt={status:'SOURCE_PREFLIGHT_RUNNING',commit:sha,predecessor:base,beganAt:new Date().toISOString(),
 source:'EXACT_COMMITTED_GIT_ARCHIVE',runtime:'Windows Node/npm running the same portable protected-workflow source commands; not a Linux runtime claim',
 linearAncestryPassed:true,steps:[],temporaryFolder:folder,cleanup:null};
const receiptFile=path.join(root,'.codex-tmp/healthroster-install1-archive-preflight.json');
fs.mkdirSync(path.dirname(receiptFile),{recursive:true});
const save=()=>fs.writeFileSync(receiptFile,JSON.stringify(receipt,null,2)+'\n');save();
try{
 git(['archive','--format=tar','--output='+tar,sha]);
 const expanded=spawnSync('tar.exe',['-xf',tar,'-C',archive],{windowsHide:true,encoding:'utf8'});
 if(expanded.status!==0)throw Error('ARCHIVE_EXTRACTION_FAILED');
 const files=execFileSync('git',['ls-files','-z'],{cwd:root,encoding:'utf8'}).split('\0').filter(Boolean);
 const sql=files.filter(file=>/\.(?:sql|sqlinc|inc)$/i.test(file));
 const carriageReturns=sql.filter(file=>fs.readFileSync(path.join(archive,file)).includes(13));
 receipt.sqlFiles=sql.length;receipt.sqlCarriageReturnFiles=carriageReturns;
 if(carriageReturns.length)throw Error('ARCHIVE_SQL_LF_ONLY_REQUIRED');
 const gitDir=git(['rev-parse','--absolute-git-dir']);
 const registry=JSON.parse(fs.readFileSync(path.join(archive,'docs/bpay-next/final-definer-closures.json'),'utf8'));
 const env=sourceCheckEnvironment(process.env,{gitDir,workTree:archive,finalDefinerBase:registry.releaseBaseCommit});
 const npmCli='C:/Program Files/nodejs/node_modules/npm/bin/npm-cli.js';
 const checks=[
  [npmCli,['run','security:toolchain']],
  [npmCli,['run','security:dependencies']],
  [npmCli,['run','security:secrets']],
  [npmCli,['run','db:check']],
  [npmCli,['run','db:contract:coupling']],
  ['--test',['tests/bpay-next-source-gates.test.js','tests/bpay-next-legacy-fence.test.js','tests/bpay-next-final-definers.test.js']],
  ['--test',['tests/database-release-system.test.mjs','tests/automatic-test-release-policy.test.mjs']],
  ['--test',['tests/healthroster-install1-full-rehearsal.test.mjs','tests/healthroster-install1-native-coordinator.test.mjs','tests/healthroster-source-check-environment.test.mjs','tests/healthroster-turn-control.test.mjs','tests/healthroster-rehearsal-turn.test.mjs','tests/healthroster-full-rehearsal-protocol.test.mjs']],
  ['--test',['tests/weekly-source/weekly-source-pay-authority-sql.test.mjs','tests/weekly-source/weekly-source-pay-view.test.mjs','tests/weekly-source/weekly-source-pay-auth-transport.test.mjs','tests/weekly-source/weekly-source-office-presentation-integration.test.mjs']]
 ];
 for(const [checkIndex,[entry,args]] of checks.entries()){const began=Date.now();
  const result=spawnSync(process.execPath,[entry,...args],{cwd:archive,env,windowsHide:true,encoding:'utf8',maxBuffer:8000000});
  const output=result.stdout??'',diagnostic=(result.stderr??'')+output;
  const row={entry:entry===npmCli?'npm':entry,args,exitCode:result.status,elapsedMs:Date.now()-began,
   outputSha256:crypto.createHash('sha256').update(output+(result.stderr??'')).digest('hex'),
   tests:Number(output.match(/(?:#|ℹ) tests (\d+)/)?.[1]??0),pass:Number(output.match(/(?:#|ℹ) pass (\d+)/)?.[1]??0),
   fail:Number(output.match(/(?:#|ℹ) fail (\d+)/)?.[1]??0),
   diagnosticCodes:[...new Set(diagnostic.match(/\bERR_[A-Z0-9_]+\b/g)??[])],
   missingPackages:[...new Set([...diagnostic.matchAll(/Cannot find package ['"]([@a-z0-9._/-]+)['"]/g)].map(match=>match[1]))]};
  receipt.steps.push(row);save();console.log(JSON.stringify(row));
  if(result.status!==0||result.error)throw Error('ARCHIVE_SOURCE_CHECK_FAILED');
  if(checkIndex===0){
   // Exact lockfile install inside the disposable archive. Installer hooks are
   // suppressed for portable source tests; the protected workflow is unchanged.
   const lockFile=path.join(archive,'package-lock.json');
   const hashFile=()=>crypto.createHash('sha256').update(fs.readFileSync(lockFile)).digest('hex');
   const lockBefore=hashFile(),start=Date.now();
   const installed=spawnSync(process.execPath,[npmCli,'ci','--engine-strict','--ignore-scripts','--no-audit','--no-fund'],
    {cwd:archive,env,windowsHide:true,encoding:'utf8',maxBuffer:8000000});
   receipt.dependencyProvisioning={method:'EXACT_ARCHIVED_LOCK_NPM_CI_SOURCE_ONLY',installerHooks:false,
    exitCode:installed.status,elapsedMs:Date.now()-start,packageLockSha256:lockBefore,lockUnchanged:hashFile()===lockBefore,
    outputSha256:crypto.createHash('sha256').update((installed.stdout??'')+(installed.stderr??'')).digest('hex')};
   save();console.log(JSON.stringify(receipt.dependencyProvisioning));
   if(installed.status!==0||installed.error||!receipt.dependencyProvisioning.lockUnchanged)throw Error('ARCHIVE_DEPENDENCY_PROVISIONING_FAILED');
  }
 }
 receipt.status='SOURCE_PREFLIGHT_PASS';
}catch(error){receipt.status='SOURCE_PREFLIGHT_HOLD';receipt.failure=/^[A-Z_]+$/.test(error.message)?error.message:'ARCHIVE_FAILURE_WITHHELD';process.exitCode=1;
}finally{
 // Validate only our freshly created absolute directory; never remove a sibling.
 const resolved=fs.realpathSync(folder),parent=fs.realpathSync(temporaryRoot);
 if(path.dirname(resolved)!==parent||!path.basename(resolved).startsWith('healthroster-install1-preflight-'+sha.slice(0,12)+'-'))throw Error('ARCHIVE_CLEANUP_TARGET_REFUSED');
 const cleanup=spawnSync('powershell.exe',['-NoProfile','-Command','Remove-Item -LiteralPath $env:CLOUDTMS_HR_ARCHIVE_CLEANUP_TARGET -Recurse -Force'],
  {windowsHide:true,encoding:'utf8',env:{...process.env,CLOUDTMS_HR_ARCHIVE_CLEANUP_TARGET:resolved}});
 receipt.cleanup={removed:cleanup.status===0&&!fs.existsSync(folder),deletionRefused:cleanup.status!==0};
 receipt.endedAt=new Date().toISOString();save();
 console.log(JSON.stringify({status:receipt.status,commit:sha,receiptFile,cleanup:receipt.cleanup,failure:receipt.failure??null}));
}
