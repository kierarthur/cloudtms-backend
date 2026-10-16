// Read-only inventory of the reviewed root source; writes only this proposal's receipt.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';
import {pathToFileURL,fileURLToPath} from 'node:url';import {execFileSync} from 'node:child_process';
const root=process.argv.find(x=>x.startsWith('--root='))?.slice(7);if(!root)throw Error('--root=PATH required');
const absolute=path.resolve(root),git=(...a)=>execFileSync('git',a,{cwd:absolute,encoding:'utf8'}).trim();
if(git('status','--porcelain','--','supabase','broker/src/weekly-source','scripts/cloudtms-db-release-lib.mjs'))throw Error('Root selected source is changing; wait for its source publication');
const commit=git('rev-parse','HEAD');
const {inventory}=await import(pathToFileURL(path.join(absolute,'scripts/cloudtms-db-release-lib.mjs')).href);
const {migrations,repeatables}=inventory();const release=JSON.parse(fs.readFileSync(path.join(absolute,'supabase/release/current-release.json'),'utf8'));
const digest=p=>crypto.createHash('sha256').update(fs.readFileSync(path.join(absolute,p)).toString('utf8').replaceAll('\r\n','\n')).digest('hex');
const controls=['supabase/release/current-release.json','supabase/release/current-contract.json','supabase/release/migration-lock.json',
 'supabase/release/baseline-repeatable-lock.json','supabase/release/protected-boundary-lock.json','scripts/cloudtms-db-release-lib.mjs'];
const result={version:'CONTACT_NATIVE_SEED_ROOT_PREREQUISITES_V1',root_commit:commit,qualification:'SOURCE_ONLY_NATIVE_NOT_RUN',
 recipe:'Existing protected NEW/UPGRADE source inventory and order, not a new installer. Root alone stages the genuine current union under its rollback lock. Never execute the old verifier fake audit/CURRENT seed blocks.',
 controls:controls.map(p=>({path:p,sha256:digest(p)})),
 new_database:{bootstrap:release.bootstrapFile,baselines:release.baselineFiles},
 upgrade:{control_plane_migration:release.controlPlaneMigration,legacy_bootstrap:release.legacyUpgradeBootstrapFiles},
 migrations,repeatables,original_verifiers_read:[
 'supabase/verification/15092026_1534_weekly_source_upload_publication_v1.sql',
 'supabase/verification/15092026_1534_weekly_source_projection_build_v1.sql',
 'supabase/verification/15092026_2203_weekly_source_candidate_app_contract_v1.sql',
 'supabase/verification/15092026_1534_weekly_source_query_delivery_v1.sql',
 'supabase/verification/15092026_2312_weekly_source_delivery_targets_v1.sql'].map(p=>({path:p,sha256:digest(p)}))};
if(git('rev-parse','HEAD')!==commit||git('status','--porcelain','--','supabase','broker/src/weekly-source','scripts/cloudtms-db-release-lib.mjs'))throw Error('Root source changed during read');
fs.writeFileSync(path.join(path.dirname(fileURLToPath(import.meta.url)),'ROOT_PREREQUISITES.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({root_commit:commit,migrations:migrations.length,repeatables:repeatables.length,native:'NOT_RUN'}));
