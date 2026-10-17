// Offline complete-parent/current-source checks. No native transport is imported.
import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';import {execFileSync,spawnSync} from 'node:child_process';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const sha=b=>createHash('sha256').update(b).digest('hex');
const requireThat=(v,e)=>{if(!v)throw Error(e);};
const git=(...a)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',here,...a],{maxBuffer:32e6});
const reverse=JSON.parse(fs.readFileSync(here+'/PARENT_REVERSES.json'));
const selection=JSON.parse(fs.readFileSync(here+'/PAYLOAD_SELECTION.json'));
const manifest=JSON.parse(fs.readFileSync(here+'/current/MANIFEST.json'));
const original=git('show',reverse.parentCommit+':'+reverse.parentPrefix+'/MANIFEST.json');
requireThat(sha(original)===reverse.parentManifestSha256&&selection.files.length===45&&manifest.files.length===44,'EXACT_PARENT_AND_SELECTION_REQUIRED');
let changed=0,exact=0;
for(const row of selection.files){
 const b=fs.readFileSync(here+'/'+row.payload),parent=git('show',reverse.parentCommit+':'+row.target);
 requireThat(b.length===row.bytes&&sha(b)===row.sha256&&sha(parent)===row.parentSha256,'EXACT_CURRENT_AND_PARENT_BYTES_REQUIRED:'+row.target);
 const name=row.target.slice(reverse.parentPrefix.length+1),edit=reverse.changed.find(r=>r.file===name);
 if(edit){
  requireThat(row.changed&&b.toString()===edit.afterRaw&&parent.toString()===edit.beforeRaw,'EXACT_FULL_RAW_REVERSAL_REQUIRED');
  let text=b.toString();for(const h of [...edit.hunks].reverse()){
   requireThat(text.slice(h.offset,h.offset+h.after.length)===h.after,'LITERAL_REVERSE_OFFSET_REQUIRED');
   text=text.slice(0,h.offset)+h.before+text.slice(h.offset+h.after.length);
  }
  requireThat(text===edit.beforeRaw&&sha(text)===edit.beforeSha256,'WHOLE_RAW_PARENT_REVERSE_REQUIRED');changed++;
 }else{requireThat(!row.changed&&b.equals(parent)&&reverse.unchanged.includes(name),'UNCHANGED_PARENT_BYTES_REQUIRED');exact++;}
}
requireThat(changed===6&&exact===39&&reverse.changed.length===6,'ONLY_SIX_DERIVED_POSTIMAGES_REQUIRED');
const oldBuilder=reverse.changed.find(r=>r.file==='build-program.mjs').beforeRaw;
const currentBuilder=fs.readFileSync(here+'/current/build-program.mjs','utf8');
requireThat(oldBuilder.split(reverse.admissionHunk.before).length===2&&oldBuilder.replace(reverse.admissionHunk.before,()=>reverse.admissionHunk.after)===currentBuilder,'ONE_EXACT_ADMISSION_HUNK_ONLY');
const oldPlans=JSON.parse(reverse.changed.find(r=>r.file==='PROGRAMMES.json').beforeRaw),plans=JSON.parse(fs.readFileSync(here+'/current/PROGRAMMES.json'));
requireThat(JSON.stringify(plans.CONTACT_CONSTRUCTORS_V1)===JSON.stringify(oldPlans.CONTACT_CONSTRUCTORS_V1),'COMPLETE_CONSTRUCTOR_CASE_BYTE_EQUAL_REQUIRED');
requireThat(oldPlans.QT_ADMIN_CONTROLS_V1.sql.split(reverse.admissionHunk.before).length===2
 &&oldPlans.QT_ADMIN_CONTROLS_V1.sql.replace(reverse.admissionHunk.before,()=>reverse.admissionHunk.after)===plans.QT_ADMIN_CONTROLS_V1.sql,'QT_SQL_ONE_EXACT_ADMISSION_HUNK_ONLY');
requireThat(JSON.stringify(plans.QT_ADMIN_CONTROLS_V1.transfers)===JSON.stringify(oldPlans.QT_ADMIN_CONTROLS_V1.transfers)
 &&JSON.stringify(plans.QT_ADMIN_CONTROLS_V1.expected)===JSON.stringify(oldPlans.QT_ADMIN_CONTROLS_V1.expected),'ALL_ORIGINAL_TRANSFERS_AND_ASSERTIONS_REQUIRED');
for(const row of manifest.files){const b=fs.readFileSync(here+'/current/'+row.file);requireThat(b.length===row.bytes&&sha(b)===row.sha256,'CURRENT_MANIFEST_REQUIRED');}
const binding=JSON.parse(fs.readFileSync(here+'/CURRENT_ROOT_BINDING.json'));
for(const row of binding.rootInputs){const b=fs.readFileSync(root+'/'+row.file);requireThat(b.length===row.bytes&&sha(b)===row.sha256,'CURRENT_SAVED_ROOT_PIN_REQUIRED:'+row.file);}
const run=(exe,args)=>{const r=spawnSync(exe,args,{cwd:here,env:{...process.env,PYTHONDONTWRITEBYTECODE:'1'},encoding:'utf8',windowsHide:true});requireThat(r.status===0,r.stderr||r.stdout);return r.stdout+r.stderr;};
const ast=run('python',[here+'/admission-ast.py']);
const grammar=run('python',[here+'/current/grammar-and-bindings.py']);
const tests=run(process.execPath,['--test','--test-reporter=tap',here+'/current/test.mjs']);
requireThat(/# tests 16\r?\n# suites 0\r?\n# pass 16\r?\n# fail 0/.test(tests),'ORIGINAL_SIXTEEN_TESTS_REQUIRED');
for(const file of ['prepare.mjs','verify.mjs'])run(process.execPath,['--check',here+'/'+file]);
console.log(JSON.stringify({version:'CONTACT_QT_NAME_ADMISSION_FORWARD_OFFLINE_REVIEW_V1',parentFiles:45,selectedPostimages:45,wholeParentReversals:changed,unchangedRawParents:exact,
 admissionBuilderHunks:1,qtSqlHunks:1,originalTests:16,actualAstVectors:81,mutationRefusals:4,outerGrammarStatements:98,plBodies:21,
 qtProgrammeSha256:plans.QT_ADMIN_CONTROLS_V1.sqlSha256,qtBytes:plans.QT_ADMIN_CONTROLS_V1.sqlBytes,qtStatements:plans.QT_ADMIN_CONTROLS_V1.statements,
 sourceRoot:binding.rootCommit,rootPins:binding.rootInputs.length,ast:JSON.parse(ast.trim()),grammar:JSON.parse(grammar.trim()),native:'NOT_RUN',nativeCompilation:'NOT_PROVED',rootEdits:0}));
