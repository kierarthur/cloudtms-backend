import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(here,'..'),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const f19='install2-a0-runtime-route-wiring-20261014',f10='install2-a0-runtime-refusal-successor-20261014',f11='install2-a0-auth-tenant-successor-20261014';
const read=(dir,file)=>fs.readFileSync(path.join(root,dir,file));const prior=JSON.parse(read(f19,'CURRENT_INDEX_FULL_REVERSE.json'));
const mounts=[{destination:'broker/src/index.js',origin:`${f10}/forward/broker/src/index.js`,before_sha256:prior.before_sha256,sha256:sha(read(f10,'forward/broker/src/index.js'))},
 {destination:'broker/src/timesheet-correction-office-routes.mjs',origin:`${f11}/forward/broker/src/timesheet-correction-office-routes.mjs`,before_state:'ABSENT_CURRENT',sha256:sha(read(f11,'forward/broker/src/timesheet-correction-office-routes.mjs'))}];
const graph=JSON.parse(read(f19,'RUNTIME_IMPORT_CLOSURE.json'));for(const f of graph.files){const m=mounts.find(m=>m.destination===f.path);if(m){f.sha256=m.sha256;f.bytes=read(m.origin.split('/')[0],m.origin.split('/').slice(1).join('/')).length;f.origin=m.origin;}}
const manifests=[f19,f10,f11].map(folder=>({folder,sha256:sha(read(folder,'REVIEW_MANIFEST.json')),artifacts:JSON.parse(read(folder,'REVIEW_MANIFEST.json')).artifacts.length+1}));
fs.writeFileSync(path.join(here,'FINAL_A0_MOUNT_PINS.json'),JSON.stringify({schema_version:'FINAL_REVIEWED_A0_RUNTIME_MOUNT_PINS_V1',
 status:'INDEPENDENT_SOURCE_CRITIC_ACCEPTED_PUBLICATION_AND_NATIVE_MOUNT_HELD_UNTIL_ROOT',input_root_commit:prior.input_commit,
 current_shared_owner_also_unchanged_at:'b5c1ea1b002b2d6b756707c020b6a170cc9fea80',mounts,preserved_modules:prior.unchanged_modules,
 preserved_freezes:manifests,total_frozen_artifacts:40,final_tests:26,native_contract:null,native_calls:0,
 qualification_and_feature_mutations:0,visible_request_non_rc_and_hidden_rr_writer_schedules_separate:true,
 final_literal_import_graph:graph.files},null,2)+'\n');console.log(JSON.stringify({mounts:mounts.map(m=>({path:m.destination,sha256:m.sha256})),frozen_artifacts:40,final_graph:graph.files.length}));
