import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const root=path.dirname(fileURLToPath(import.meta.url));
const sha=file=>crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
const forward=path.join(root,'forward');
fs.mkdirSync(path.join(root,'test-runtime'),{recursive:true});
for(const [source,target] of [['forward/global-contact-timing.mjs','global-contact-timing.mjs'],['reference/dependencies/push-timing-office.mjs','push-timing-office.mjs']]){
 fs.copyFileSync(path.join(root,source),path.join(root,'test-runtime',target));
}
const sql=fs.readdirSync(forward).filter(file=>file.endsWith('.sqlinc')).sort();
const functions=[];
for(const file of sql){
 const body=fs.readFileSync(path.join(forward,file),'utf8');
 for(const match of body.matchAll(/create or replace function\s+((?:private|public)\.[a-z0-9_]+)\s*\(([\s\S]*?)\)\s*returns/gi)){
  functions.push({name:match[1],arguments:match[2].replace(/\s+/g,' ').trim(),payload:'forward/'+file});
 }
}
const files=fs.readdirSync(forward).sort().map(file=>({file:'forward/'+file,sha256:sha(path.join(forward,file))}));
files.push({file:'fixtures/native-contact-assertions.sqlinc',sha256:sha(path.join(root,'fixtures/native-contact-assertions.sqlinc'))});
const manifest={version:'V6_CONTACT_TIMING_SOURCE_V1',native:'REQUIRED_NOT_RUN',database_execution:false,
 source_inputs:'SOURCE_INPUTS.json',ordered_sql:sql.map(file=>'forward/'+file),functions,files,
 backend_targets:[{target:'supabase/migrations/<new protected contact timing migration>.sql',append:sql.slice(0,2).map(file=>'forward/'+file),note:'Integrator allocates new migration; do not amend a locked predecessor.'},
 {target:'supabase/repeatable/13102026_1438_weekly_source_protection_queries/06_automatic_actor_settings.sqlinc',replace_only:['public.weekly_source_global_settings_get_v1(jsonb)','public.weekly_source_global_settings_save_atomic_v1(jsonb)'],payload:'forward/030_global_owner.sqlinc'},
 {target:'supabase/repeatable/<new protected contact timing helper>.sql',payload:['forward/010_timing_validation.sqlinc','forward/040_native_timing.sqlinc','forward/070_acl.sqlinc'],note:'Must precede13 invocation; fixed private ACL.'},
 {target:'supabase/repeatable/13102026_1438_weekly_source_protection_queries/13_manager_generation.sqlinc',payload:'forward/050_manager_generation.sqlinc'},
 {target:'supabase/repeatable/13102026_1438_weekly_source_protection_queries/11_manager_due_scope.sqlinc',payload:'forward/060_manager_due_scope.sqlinc'},
 {target:'broker/src/weekly-source/global-contact-timing.mjs',payload:'forward/global-contact-timing.mjs'},
 {target:'broker/src/weekly-source/global-contact-timing.d.ts',payload:'forward/global-contact-timing.d.ts'},
 {target:'broker/src/weekly-source/routes.js',add_import:'forward/routes-import.jsinc',replace_only:['handleSettingsGet','handleSettingsPut'],payload:'forward/routes-owner-functions.jsinc'}],
 office_targets:[{target:'js/weekly-source/settings.js',payload:'forward/settings.js',baseline:'reference/settings.js'}],
 independent_scope:['existing singleton settings/CAS/replay/current admin','canonical old settings preservation','native first delay and per-incident initial purpose split','accepted-receipt follow-up timing'],
 root_composition_dependencies:['Native14 not-finalised dispatch must use same first delay/purpose and avoid second immediate event.',
 'Existing scheduler/render/common START must consume actual accepted ordinal timing and current policy under its final locks. Existing accepted-batch ownership cannot be bypassed by another old tranche.',
 'Current completed-report/declared work/reply/recipient/need/pause/age/hard Client cutoff and budget predicates remain root final admission.',
 'Manual enquiry preview/accept protocol is explicitly outside this slice.'],
 acceptance:'Run source.test.mjs and offline grammar, then genuine TEST rollback + actual Office route/render/scheduler/dispatch fixtures; no source-only native claim.'};
fs.writeFileSync(path.join(root,'SOURCE_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
console.log(JSON.stringify({sql_files:sql.length,functions:functions.length,manifest_sha256:sha(path.join(root,'SOURCE_MANIFEST.json')),native:manifest.native}));
