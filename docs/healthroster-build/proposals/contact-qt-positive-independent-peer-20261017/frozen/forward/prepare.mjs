// Source-only successor. Never opens a native connection or shared turn.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import {execFileSync,spawnSync} from 'node:child_process';
const here=path.dirname(fileURLToPath(import.meta.url));
const own='D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026';
const root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const prefix='docs/healthroster-build/proposals/contact-qt-positive-native-proposal-20261017';
const parent='ddcb87b6f357ffa5064e3a2f8e15946cd0e12938';
const sha=b=>createHash('sha256').update(b).digest('hex');
const check=(ok,code)=>{if(!ok)throw Error(code);};
const git=(where,...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',where,...args],{maxBuffer:32e6});
const run=(exe,args)=>{const r=spawnSync(exe,args,{cwd:own,env:{...process.env,PYTHONDONTWRITEBYTECODE:'1'},encoding:'utf8',windowsHide:true});check(r.status===0,r.stderr||r.stdout);return r.stdout+r.stderr;};
check(fs.realpathSync.native(here).replaceAll('\\','/').startsWith(own+'/'),'OWN_PHYSICAL_D_ONLY');
check(!fs.existsSync(here+'/MANIFEST.json')&&!fs.existsSync(here+'/current'),'FROZEN_OR_EXISTING_POSTIMAGE');
const oldManifestRaw=git(own,'show',parent+':'+prefix+'/MANIFEST.json');
check(sha(oldManifestRaw)==='1fba7ea3a91147d75ea69df68b0e80be1300fccba1e5e0be4fe17eaf75713204','EXACT_DDCB_MANIFEST_REQUIRED');
const oldManifest=JSON.parse(oldManifestRaw),originals=new Map();
for(const row of [...oldManifest.files,{file:'MANIFEST.json',bytes:oldManifestRaw.length,sha256:sha(oldManifestRaw)}]){
 const b=git(own,'show',parent+':'+prefix+'/'+row.file);
 check(b.length===row.bytes&&sha(b)===row.sha256&&b.equals(fs.readFileSync(own+'/'+prefix+'/'+row.file)),'EXACT_SAVED_FROZEN_PARENT_REQUIRED:'+row.file);
 originals.set(row.file,b);
 if(row.file==='MANIFEST.json')continue;
 fs.mkdirSync(path.dirname(here+'/current/'+row.file),{recursive:true});fs.writeFileSync(here+'/current/'+row.file,b);
}
const before=`or exists(select 1 from unnest(array['private.weekly_contact_assert_closed_v1(jsonb,text[],text[],text)',
   'private.weekly_contact_revision_v1(jsonb,boolean)','private.candidate_push_timing_policy_validate_v1(jsonb)',
   'private.weekly_contact_deployment_lock_v1(uuid,text)','private.candidate_push_timing_immutable_guard_v1()',
   'public.candidate_push_timing_get_v1(jsonb)','public.candidate_push_timing_save_v1(jsonb)']) n where to_regprocedure(n) is not null)`;
const after=`or exists(select 1 from pg_catalog.pg_proc qt_routine
   join pg_catalog.pg_namespace qt_namespace on qt_namespace.oid=qt_routine.pronamespace
   where (qt_namespace.nspname,qt_routine.proname) in (
    ('private','weekly_contact_assert_closed_v1'),('private','weekly_contact_revision_v1'),
    ('private','candidate_push_timing_policy_validate_v1'),('private','weekly_contact_deployment_lock_v1'),
    ('private','candidate_push_timing_immutable_guard_v1'),('public','candidate_push_timing_get_v1'),
    ('public','candidate_push_timing_save_v1')))`;
const builder=originals.get('build-program.mjs').toString();
check(builder.split(before).length===2,'ONE_EXACT_ADMISSION_HUNK_REQUIRED');
fs.writeFileSync(here+'/current/build-program.mjs',builder.replace(before,()=>after));
run(process.execPath,[here+'/current/prepare-offline.mjs']);
const plans=JSON.parse(fs.readFileSync(here+'/current/PROGRAMMES.json'));
check(plans.CONTACT_CONSTRUCTORS_V1.sqlSha256===JSON.parse(originals.get('PROGRAMMES.json')).CONTACT_CONSTRUCTORS_V1.sqlSha256,'CONSTRUCTOR_UNCHANGED_REQUIRED');
const oldSize='51 statements/39,656 bytes',newSize='51 statements/'+plans.QT_ADMIN_CONTROLS_V1.sqlBytes.toLocaleString('en-GB')+' bytes';
const readme=originals.get('README.md').toString();check(readme.split(oldSize).length===2,'ONE_README_SIZE_HUNK_REQUIRED');
fs.writeFileSync(here+'/current/README.md',readme.replace(oldSize,()=>newSize));
const grammar=run('python',[here+'/current/grammar-and-bindings.py']);
const tests=run(process.execPath,[here+'/current/test.mjs']);
for(const f of fs.readdirSync(here+'/current').filter(x=>x.endsWith('.mjs')))run(process.execPath,['--check',here+'/current/'+f]);
fs.writeFileSync(here+'/current/GRAMMAR_RUN.txt',grammar);fs.writeFileSync(here+'/current/TEST_RESULTS.txt',tests);
const rows=[];for(const row of oldManifest.files){const b=fs.readFileSync(here+'/current/'+row.file);rows.push({file:row.file,bytes:b.length,sha256:sha(b)});}
fs.writeFileSync(here+'/current/MANIFEST.json',JSON.stringify({...oldManifest,version:'CONTACT_QT_POSITIVE_NAME_ADMISSION_FORWARD_FREEZE_V1',parentManifestSha256:sha(oldManifestRaw),files:rows},null,2)+'\n');
const inverses=[];for(const [file,beforeRaw] of originals){const afterRaw=fs.readFileSync(here+'/current/'+file);if(beforeRaw.equals(afterRaw))continue;
 const beforeText=beforeRaw.toString(),afterText=afterRaw.toString();let start=0;while(start<beforeText.length&&start<afterText.length&&beforeText[start]===afterText[start])start++;
 let suffix=0;while(suffix<beforeText.length-start&&suffix<afterText.length-start&&beforeText.at(-suffix-1)===afterText.at(-suffix-1))suffix++;
 const hunk={offset:start,before:beforeText.slice(start,beforeText.length-suffix),after:afterText.slice(start,afterText.length-suffix)};
 check(afterText.slice(0,start)+hunk.before+afterText.slice(start+hunk.after.length)===beforeText,'WHOLE_PARENT_REVERSE_REQUIRED');
 inverses.push({file,beforeRaw:beforeText,afterRaw:afterText,beforeSha256:sha(beforeRaw),afterSha256:sha(afterRaw),hunks:[hunk],wholeParentRawReverse:true});
}
const exact=rows.filter(row=>!inverses.some(r=>r.file===row.file)).map(row=>row.file);
check(inverses.some(r=>r.file==='build-program.mjs')&&exact.includes('source.mjs')&&exact.includes('SETUP_ENTRY.md')&&exact.includes('control.mjs')&&exact.includes('timing.mjs')&&exact.includes('qt-assertions.sql')&&exact.includes('PROGRAMME_PARENT_REVERSES.json'),'BOUNDARY_ORIGINALS_REQUIRED');
fs.writeFileSync(here+'/PARENT_REVERSES.json',JSON.stringify({version:'CONTACT_QT_NAME_ADMISSION_WHOLE_RAW_PARENT_REVERSES_V1',parentCommit:parent,parentPrefix:prefix,parentManifestSha256:sha(oldManifestRaw),admissionHunk:{before,after},changed:inverses,unchanged:exact,originalFrozenFilesPreserved:45},null,2)+'\n');
const rootHead=git(root,'rev-parse','HEAD').toString().trim(),pins=JSON.parse(originals.get('SOURCE_PINS.json'));
for(const row of pins.rootInputs){const saved=fs.readFileSync(root+'/'+row.file);check(saved.length===row.bytes&&sha(saved)===row.sha256&&saved.equals(git(root,'show',rootHead+':'+row.file)),'ROOT_PIN_CHANGED:'+row.file);}
fs.writeFileSync(here+'/CURRENT_ROOT_BINDING.json',JSON.stringify({rootCommit:rootHead,rootPhysical:fs.realpathSync.native(root),sourceParent:pins.rootCommit,rootInputs:pins.rootInputs.map(({file,bytes,sha256})=>({file,bytes,sha256,savedEqualsCommitted:true})),rootEdits:0,native:'NOT_RUN'},null,2)+'\n');
const selection=[...rows,{file:'MANIFEST.json',bytes:fs.statSync(here+'/current/MANIFEST.json').size,sha256:sha(fs.readFileSync(here+'/current/MANIFEST.json'))}].map(row=>({target:prefix+'/'+row.file,payload:'current/'+row.file,sha256:row.sha256,bytes:row.bytes,parentSha256:sha(originals.get(row.file)),changed:inverses.some(r=>r.file===row.file)}));
fs.writeFileSync(here+'/PAYLOAD_SELECTION.json',JSON.stringify({version:'CONTACT_QT_NAME_ADMISSION_EXACT_ADAPTER_SELECTION_V1',parentCommit:parent,parentManifestSha256:sha(oldManifestRaw),targetKind:'ROOT_OWNED_REHEARSAL_ADAPTER_ONLY_NO_RUNTIME_SQL',originalFrozenHistoryUntouched:true,native:'NOT_RUN',files:selection},null,2)+'\n');
console.log(JSON.stringify({parentFiles:45,selected:selection.length,changed:inverses.length,unchanged:exact.length,qtProgrammeSha256:plans.QT_ADMIN_CONTROLS_V1.sqlSha256,qtBytes:plans.QT_ADMIN_CONTROLS_V1.sqlBytes,native:'NOT_RUN'}));
