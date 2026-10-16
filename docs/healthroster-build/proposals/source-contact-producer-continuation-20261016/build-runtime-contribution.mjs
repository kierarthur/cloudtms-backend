import {execFileSync} from 'node:child_process';
import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(base,'../../../..');
const commit='1859790b0596761f405a10c258394a1d47ca304e',file='broker/src/weekly-source/delivery-runtime.mjs';
const sha=s=>createHash('sha256').update(s).digest('hex');
const source=execFileSync('git',['show',`${commit}:${file}`],{cwd:root,encoding:'utf8',maxBuffer:8e6}).replace(/\r\n/g,'\n');
const edits=[{before:`import { weeklySourceDeploymentContext } from './routes.js';`,
 after:`import { weeklySourceDeploymentContext } from './routes.js';\nimport { runWeeklySourceContactRecheck } from './contact-recheck-runtime.mjs';`},
 {before:'  const renders = await renderDueIntents(env, dependencies, limit);',after:`  // This consumes the existing contact tasks after actual native due-step
  // creation. An unavailable item/RPC cannot interrupt unrelated safe targets;
  // every original final START still independently enforces current admission.
  let contactRecheck;
  try {
    contactRecheck = await runWeeklySourceContactRecheck(dependencies, {
      deployment: weeklySourceDeploymentContext(env), workerId, limit,
    });
  } catch {
    contactRecheck = { ok: false, error_code: 'CONTACT_RECHECK_UNAVAILABLE', processed: 0, held: 0, results: [] };
  }
  const renders = await renderDueIntents(env, dependencies, limit);`},
 {before:'    scheduler,\n    renders,',after:'    scheduler,\n    contact_recheck: contactRecheck,\n    renders,'}];
let result=source;
for(const edit of edits){if(result.split(edit.before).length!==2)throw Error('Exact unique runtime contribution anchor '+edit.before);result=result.replace(edit.before,edit.after);}
const recipe={version:'CONTACT_RECHECK_RUNTIME_CONTRIBUTION_V1',source_commit:commit,source_path:file,
 source_sha256_normalized_lf:sha(source),edits,result_sha256_normalized_lf:sha(result),
 leaf:{source_path:'runtime/contact-recheck-runtime.mjs',mount_path:'broker/src/weekly-source/contact-recheck-runtime.mjs',
  sha256:sha(readFileSync(path.join(base,'runtime/contact-recheck-runtime.mjs')))},
 authoritative_manual_components:{source_commits:['1859790b0596761f405a10c258394a1d47ca304e','81d06f820af6670c3783f0a7d825be9fc5c5d767'],
  record:'docs/healthroster-build/proposals/v6-owner-delivery-20261016/MANUAL_ENQUIRY_WORKER_COMPONENT_SOURCE.json',
  request:'broker/src/weekly-source/contact-manual-enquiry-request.mjs',types:'broker/src/weekly-source/contact-manual-enquiry-types.d.ts',
  handler:'broker/src/weekly-source/contact-manual-enquiry-office.mjs',reply_decoder:'broker/src/weekly-source/contact-manual-enquiry-contract.mjs'},
 qualification:'Finite contribution only. Root rechecks current complete owner and applies reviewed edits; no whole stale parent replacement. Unit transport/orchestration only; native/runtime activation NOT_RUN'};
writeFileSync(path.join(base,'RUNTIME_CONTRIBUTION.json'),JSON.stringify(recipe,null,2)+'\n');
console.log(JSON.stringify({edits:edits.length,status:'SOURCE_ONLY_UNMOUNTED'}));
