import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(process.argv[2]||'');
const prior=path.resolve(dir,'../install2-no-shifts-undo-native-20261014');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=(...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,maxBuffer:24*1024*1024});
const sourcePin='87b9908370d238d1af4da910b97018d19a3fd033';
if(fs.existsSync(path.join(dir,'REVIEW_MANIFEST.json')))throw new Error('FROZEN_UNDO_STATE_DO_NOT_OVERWRITE');
const beforeManifest=fs.readFileSync(path.join(prior,'REVIEW_MANIFEST.json'));
assert.equal(sha(beforeManifest),'d97bd1909b77353905ec19d8e988bbc9ce1f010e41386aee18a0d05c94db1337');
const manifest=JSON.parse(beforeManifest);for(const row of manifest.entries)assert.equal(sha(fs.readFileSync(path.join(prior,row.path))),row.sha256,row.path);
const {inventory}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-db-release-lib.mjs')).href);
const inv=inventory(),wanted=new Set(['public.weekly_source_correct_final_open_atomic_v1','public.weekly_source_correct_final_review_atomic_v1',
  'public.weekly_source_correct_final_prepare_atomic_v1','public.weekly_source_correct_final_apply_atomic_v1','public.weekly_source_correct_final_cancel_atomic_v1']);
const owners=new Map();
for(const rel of inv.repeatables.flatMap(row=>row.paths)){
  const text=fs.readFileSync(path.join(root,rel),'utf8');
  for(const m of text.matchAll(/\bcreate\s+(?:or\s+replace\s+)?function\s+((?:public|private)\.[a-z_0-9]+)\s*\(/ig)){
    const name=m[1].toLowerCase();if(!wanted.has(name))continue;
    const sub=text.slice(m.index),body=/\bas\s+(\$[a-z_0-9]*\$)/i.exec(sub);assert(body,name);
    const end=sub.indexOf(body[1],body.index+body[0].length);assert(end>=0);
    const finish=sub.indexOf(';',end+body[1].length);assert(finish>=0);
    owners.set(name,{name,path:rel,line:text.slice(0,m.index).split('\n').length,complete:sub.slice(0,finish+1)});
  }
}
assert.equal(owners.size,5);const protocol=[];fs.mkdirSync(path.join(dir,'current-protocol'),{recursive:true});
for(const [name,row]of owners){const bytes=fs.readFileSync(path.join(root,row.path));assert(bytes.equals(git('show',`${sourcePin}:${row.path}`)));
  const evidence=`current-protocol/${name.replace('.','_')}.sql.txt`;
  fs.writeFileSync(path.join(dir,evidence),row.complete+'\n');protocol.push({name,path:row.path,line:row.line,
    source_raw_sha256:sha(bytes),definition_raw_sha256:sha(row.complete),evidence_path:evidence,evidence_sha256:sha(row.complete+'\n'),
    correction_in_progress_literal_present:row.complete.includes('CORRECTION_IN_PROGRESS'),
    cycle_state_field_reference:/\bv_cycle\.state\b/.test(row.complete)});
}
// No actual controlling Correct-Final owner sets/tests CORRECTION_IN_PROGRESS.
// This proves a source limitation; it does not assert installed row absence.
assert(protocol.every(row=>row.correction_in_progress_literal_present===false));
const leaf='supabase/repeatable/14102026_0900_healthroster_no_shifts_undo/01_no_shifts_undo_atomic.sqlinc';
const before=fs.readFileSync(path.join(prior,'forward',leaf));
const hunk={before:"  if v_cycle.state not in('OPEN','FINALISABLE','FINALISED') then\n    raise exception 'WEEKLY_SOURCE_NO_SHIFTS_UNDO_CYCLE_STATE_INVALID' using errcode='55000';\n  end if;\n",after:""};
assert.equal(before.toString().split(hunk.before).length,2);
const after=before.toString().replace(hunk.before,()=>hunk.after);
// Empty replacement is anchored by the complete immutable prefix/suffix.
const start=before.toString().indexOf(hunk.before);assert.equal(after.slice(0,start)+hunk.before+after.slice(start),before.toString());
for(const [folder,bytes]of [['before',before],['forward',Buffer.from(after)]]){const target=path.join(dir,folder,leaf);fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,bytes);}
const result={schema:'NO_SHIFTS_UNDO_STATE_PROTOCOL_SUCCESSOR_V1',source_pin:sourcePin,predecessor_manifest_sha256:sha(beforeManifest),
  changed_leaf:{path:leaf,before_sha256:sha(before),after_sha256:sha(after),hunk,reverse_insert_offset:start,
    reverse_prefix_sha256:sha(after.slice(0,start)),reverse_suffix_sha256:sha(after.slice(start))},
  controlling_current_protocol:protocol,source_code_finding:'No controlling Correct-Final owner sets/tests CORRECTION_IN_PROGRESS; no blanket state exclusion justified by those source owners',
  runtime_route_and_wrapper_unchanged:true,actual_native_state_absence_claim:false,native_calls:false,database_calls:false,native_seal:null};
fs.writeFileSync(path.join(dir,'CURRENT_STATE_OWNER_REVERSE_AND_PINS.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({after_sha256:result.changed_leaf.after_sha256,protocol_owner_count:protocol.length,old44_untouched:true}));
