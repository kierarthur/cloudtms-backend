// Literal, fully reversed finite reader adaptation. No native work is performed.
import fs from'node:fs';import {fileURLToPath}from'node:url';import path from'node:path';import {sha}from'./build-program.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));if(fs.existsSync(here+'/MANIFEST.json'))throw Error('FROZEN_HISTORY');
const before=fs.readFileSync(here+'/reference/adapter/fixed-metadata-reader.mjs','utf8');let after=before;
const hunks=[
 ["import {source,SCOPE,markerFor} from './input-builder.mjs';","import {source,SCOPE,markerFor} from './input-builder.mjs';\nimport {NATIVE_BOUNDARY_SQL} from './native-boundary.mjs';"],
 ['createFixedMetadataReader({root,turn})','createFixedMetadataReader({root,turn,caseId})'],
 ['const {rehearsal,live}=await source(root);','const {rehearsal,live,pins}=await source(root,caseId);'],
 ["const relations=[...live.backgroundRelations,'public.tms_users'].sort().map(name=>({schema:name.split('.')[0],name:name.split('.')[1],kind:'r'}));","const relations=pins.relationNames.map(name=>({schema:name.split('.')[0],name:name.split('.')[1],kind:'r'}));"],
 ["'healthroster_daily_actor_fixed_metadata'","'healthroster_contact_qt_fixed_metadata'"],
 ["or to_regclass(${literal('private.'+marker)}) is not null","or to_regclass(${literal('private.'+marker)}) is not null\n  or ${NATIVE_BOUNDARY_SQL}"],
 ['// Exact actor closure: six new native users, only the actual self-touch trigger.','// Exact positive Contact/QT closure: twelve complete relation scopes.'],
 ['// Keep the complete original canonical catalogue/ledger and both complete','// Keep the complete original canonical catalogue/ledger and both complete']
].filter(([a,b])=>a!==b);
for(const[a,b]of hunks){if(after.split(a).length!==2)throw Error('EXACT_READER_PARENT_REQUIRED');after=after.replace(a,()=>b);}
let reverse=after;for(const[a,b]of [...hunks].reverse()){if(reverse.split(b).length!==2)throw Error('EXACT_READER_REVERSE_REQUIRED');reverse=reverse.replace(b,()=>a);}if(reverse!==before)throw Error('WHOLE_READER_PARENT_REVERSE_REQUIRED');
fs.writeFileSync(here+'/fixed-metadata-reader.mjs',after);
for(const name of ['control.mjs','timing.mjs'])fs.copyFileSync(here+'/reference/adapter/'+name,here+'/'+name);
fs.writeFileSync(here+'/READER_PARENT_REVERSE.json',JSON.stringify({parent:'Exact root feb36c0c actor R2 fixed reader; original 826e Contact transport retained through reviewed pure/actor successors',beforeSha256:sha(before),afterSha256:sha(after),before,after,hunks:hunks.map(([before,after])=>({before,after})),wholeRawReverse:true,controlAndTimingBytesUnchanged:true,originalCanonicalQualifierUnchanged:true},null,2)+'\n');
