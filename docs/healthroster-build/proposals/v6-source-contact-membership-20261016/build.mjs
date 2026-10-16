import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const root=path.dirname(fileURLToPath(import.meta.url));const read=f=>fs.readFileSync(path.join(root,f),'utf8');const sha=f=>crypto.createHash('sha256').update(fs.readFileSync(path.join(root,f))).digest('hex');
fs.mkdirSync(path.join(root,'test-runtime'),{recursive:true});
const recipe=JSON.parse(read('NATIVE_PATCH.json'));const composed=new Map();
for(const patch of recipe.patches){let source=composed.get(patch.source)||read(patch.source);if(source.split(patch.before).length!==2)throw Error('Exact current native anchor not unique: '+patch.description);
 composed.set(patch.source,source.replace(patch.before,()=>patch.after));}
for(const [file,source]of composed)fs.writeFileSync(path.join(root,'test-runtime',path.basename(file)),source.split('\n').filter(line=>!line.startsWith('\\')).join('\n'));
const files=fs.readdirSync(path.join(root,'forward')).filter(f=>f.endsWith('.sqlinc')&&f!=='090_acl.sqlinc').sort(),functions=[];
for(const file of files)for(const m of read('forward/'+file).matchAll(/create or replace function\s+((?:private|public)\.[a-z0-9_]+)\s*\(([\s\S]*?)\)\s*returns/gi))
 functions.push({signature:m[1]+'('+m[2].split(',').map(a=>a.trim().split(/\s+/)[1]).join(',')+')',payload:'forward/'+file});
fs.writeFileSync(path.join(root,'forward/090_acl.sqlinc'),functions.map(f=>`alter function ${f.signature} owner to postgres;\nrevoke all on function ${f.signature} from public,anon,authenticated,service_role;`).join('\n')+'\n');
files.push('090_acl.sqlinc');
const manifest={version:'SOURCE_LOGICAL_MEMBERSHIP_OWNER_V1',database_execution:false,native:'REQUIRED_NOT_RUN',schema_changes:false,
 ordered_sql:files.map(f=>'forward/'+f),functions,native_composition:'NATIVE_PATCH.json',
 owner_targets:['private.weekly_contact_source_admission_v1(jsonb)','private.weekly_contact_source_permit_v1(uuid,uuid,jsonb)','private.weekly_contact_source_result_v1(uuid,text,text,jsonb,jsonb)'],
 existing_native_target_result_and_aggregate:'Unchanged complete actual owners in15092026_2311; no sibling target writes or command finalisation from logical acceptance.',
 files:[...files.map(f=>'forward/'+f),'NATIVE_PATCH.json','fixtures/native-source-membership-assertions.sqlinc'].map(file=>({file,sha256:sha(file)})),
 qualifications:['MANAGER_MANUAL_RESEND has no genuine native producer/explicit ordinal witness; refuses instead of inventing max-accepted followup.',
 'Pre-install genuine native attempts retain actual owner/budget; no fabricated captured pause revisions or accepted memberships. New-command refill is refused.',
 'A frozen Source body containing another command\'s already-owned logical slot is OBSOLETE; this owner cannot send an eligible subset with that old body. Genuine generation/render recomposition remains with its native producer.',
 'New Source initial/reminder/manually staged slots use exact current native tranche/sequence writers; no generation/publication allowance renewal.',
 'All catalogue/ACL/RLS, native result replay, dual-device concurrency/currentness, historical-owner and full runtime tests REQUIRED_NOT_RUN.']};
fs.writeFileSync(path.join(root,'SOURCE_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');console.log(JSON.stringify({functions:functions.length,manifest_sha256:sha('SOURCE_MANIFEST.json'),native:manifest.native}));
