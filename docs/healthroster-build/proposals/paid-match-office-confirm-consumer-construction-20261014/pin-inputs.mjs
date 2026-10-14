import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import assert from 'node:assert/strict';
import {functionEvents} from '../a0-current-root-install2-forward/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(dir,'../../../..');
const frontend=path.resolve(root,'../../TEST-Frontend');
const sha=b=>createHash('sha256').update(b).digest('hex');
assert.ok(!fs.existsSync(path.join(dir,'FROZEN_ARTIFACT_MANIFEST.json')));
const inputs=[
 ['backend','fa66183f01913e86651e82816f5e88f97e07123c','supabase/repeatable/14102026_0044_weekly_source_protected_pay_authority_v2.sql','input/current_pp2.sql'],
 ['backend','fa66183f01913e86651e82816f5e88f97e07123c','broker/src/weekly-source/routes.js','input/current_routes.js'],
 ['backend','fa66183f01913e86651e82816f5e88f97e07123c','broker/src/weekly-source/upload-publication-owner.mjs','input/current_upload_owner.mjs'],
 ['frontend','2d15d121ac888a2e2a076dc7a2c4be85f9958556','js/weekly-source/workspace-actions.js','before/frontend/js/weekly-source/workspace-actions.js'],
 ['frontend','2d15d121ac888a2e2a076dc7a2c4be85f9958556','js/weekly-source/import-workspace.js','input/current_import_workspace.js'],
 ['backend','fa66183f01913e86651e82816f5e88f97e07123c','supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql','input/source_schema.sql'],
 ['backend','fa66183f01913e86651e82816f5e88f97e07123c','supabase/repeatable/15092026_1534_weekly_source_upload_publication_v1.sql','input/native_upload_publication.sql'],
 ['backend','fa66183f01913e86651e82816f5e88f97e07123c','supabase/repeatable/15092026_1534_weekly_source_query_delivery_v1.sql','input/native_query_delivery.sql'],
 ['backend','973625f17cd51a49803872a819638e10f13bbbff','docs/healthroster-build/proposals/root-effective-body-amendment-census-case-forward-20261014/BODY_AMENDMENT_CENSUS.json','input/body_amendment_census.json'],
];
const pins=[];
for(const [repository,commit,file,out] of inputs){
 const b=execFileSync('git',['show',`${commit}:${file}`],{cwd:repository==='frontend'?frontend:root,maxBuffer:8e6});
 fs.mkdirSync(path.dirname(path.join(dir,out)),{recursive:true});fs.writeFileSync(path.join(dir,out),b);
 pins.push({repository,commit,file,saved:out,bytes:b.length,sha256:sha(b)});
}
const source=fs.readFileSync(path.join(dir,'input/current_pp2.sql'),'utf8');
const event=functionEvents(source).events.find(e=>e.kind==='CREATE'&&e.id==='public.weekly_source_office_workspace_v1(jsonb)');
assert.ok(event);fs.mkdirSync(path.join(dir,'before'),{recursive:true});
fs.writeFileSync(path.join(dir,'before/02_complete_office_workspace.sqlinc'),event.definition);
fs.writeFileSync(path.join(dir,'CURRENT_SOURCE_INPUT_PINS.json'),JSON.stringify({source_only:true,inputs:pins,workspace_signature:event.id,workspace_definition_sha256:sha(event.definition),
 frontend_installed_main_is_distinct:'8ea8c8af0435702d3bf032b3197fe0e0eda64d1c',
 case_complete_source_census:{roots:1484,paths:1745,occurrences:2160,guarded_body_read_sites:22},native_catalogue_proved:false},null,2)+'\n');
console.log(JSON.stringify({inputs:pins.length,workspace_bytes:Buffer.byteLength(event.definition),workspace_sha256:sha(event.definition)}));
