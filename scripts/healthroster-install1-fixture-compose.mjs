// Retain the original bounded WP-PP fixture changes on current published source.
import fs from 'node:fs';import path from 'node:path';
import {execFileSync,spawnSync} from 'node:child_process';
import {repoRoot} from './cloudtms-db-release-lib.mjs';
import {digest} from './cloudtms-managed-release-sql.mjs';
const old='e0a02bb5f86b1fb2134e540bbf0ccc52f6feec92',pp='547c74da5f0a05b09297c042bc7f3cc0c0105b85';
const files=['tests/weekly-source/weekly-source-routes.test.mjs','tests/weekly-source/weekly-source-settings-routes.test.mjs',
 'tests/weekly-source/weekly-source-report-real-owner.test.mjs','tests/weekly-source/weekly-source-office-presentation-integration.test.mjs',
 'tests/weekly-source/protected-action-orchestrator.test.mjs','supabase/verification/15092026_1534_weekly_source_protected_pay_publisher_v1.sql'];
const git=args=>execFileSync('git',args,{cwd:repoRoot,encoding:'utf8',windowsHide:true,maxBuffer:8000000});
if(git(['status','--porcelain','--',...files]).trim())throw Error('FIXTURE_TARGET_DIRTY');
const patch=git(['diff',old,pp,'--',...files]);
const checked=spawnSync('git',['apply','--check','-'],{cwd:repoRoot,input:patch,encoding:'utf8',windowsHide:true});
if(checked.status!==0)throw Error('CURRENT_FIXTURE_COMPOSITION_REFUSED');
const records=files.map(file=>({file,beforeSha256:digest(fs.readFileSync(path.join(repoRoot,file),'utf8'))}));
const applied=spawnSync('git',['apply','-'],{cwd:repoRoot,input:patch,encoding:'utf8',windowsHide:true});
if(applied.status!==0)throw Error('FIXTURE_COMPOSITION_FAILED');
for(const record of records)record.afterSha256=digest(fs.readFileSync(path.join(repoRoot,record.file),'utf8'));
fs.writeFileSync(path.join(repoRoot,'docs/healthroster-build/install1-source/PP_FIXTURE_COMPOSITION.json'),JSON.stringify({status:'SOURCE_FIXTURES_ONLY',old,pp,patchSha256:digest(patch),records,
 purpose:'Current-row active fixtures; unflagged admin positive and unflagged non-admin refusal. Native publisher assertions and Banking successor tests retained.'},null,2)+'\n');
console.log(JSON.stringify({status:'BOUNDED_PP_FIXTURES_COMPOSED',files:records.length}));
