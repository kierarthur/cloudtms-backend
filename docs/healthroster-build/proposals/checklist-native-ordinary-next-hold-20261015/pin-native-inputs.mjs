// Current literal native ABI/provenance only. No native writer executes.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {execFileSync} from 'node:child_process';import {fileURLToPath} from 'node:url';
import {readCurrentNativeIndex,sha} from '../checklist-native-invoice-20261014/read-current-native-index.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),native=readCurrentNativeIndex(repo);
fs.mkdirSync(path.join(here,'input'),{recursive:true});const selected=[];
const expected={
 'private.bpay_next_workbench_refresh_work_core_v1':'39596ef09ff413e81292f7f0bbea02df551730f343a2687c69ff8ae39b8a2769',
 'private.bpay_next_workbench_work_members_v1':'96afdf304d8679195039817622ea1b08289b3a704e8b20db5878b2dea1723af6',
 'private.bpay_next_publish_ordinary_from_financial_v1':'8aa1452a8038f833613f86a71107c01b30c7064e3496830ab036c3155844dd9b',
 'private.bpay_next_pay_hold_control_trigger_v1':'ea07e47e44c988b90fa8606fc95961c4c2b560d119e21e00cfb14a7f77b106e6'};
for(const name of [...Object.keys(expected),'private.bpay_next_approval_route_v1','private.bpay_next_daily_approval_evidence_v1',
 'private.bpay_next_workbench_accepted_work_v1','private.bpay_next_snooze_at_cut_v1']){
 const records=native.byName.get(name);assert.equal(records?.length,1,name);const r=records[0];if(expected[name])assert.equal(sha(r.definition),expected[name]);
 const file='input/'+name.replaceAll('.','_')+'.sqlinc';fs.writeFileSync(path.join(here,file),r.definition);
 selected.push({id:r.id,file:r.file,source_file_sha256:r.source_file_sha256,definition_sha256:sha(r.definition),saved:file,bytes:Buffer.byteLength(r.definition),native_executed:false});}
const files=['supabase/repeatable/11102026_0426_banking_pay_next_pay_hold_controls_v1.sql','supabase/release/current-contract.json'];
const raw=[];for(const file of files){const b=execFileSync('git',['cat-file','blob',native.sourceCommit+':'+file],{cwd:repo,maxBuffer:32e6});
 const saved='input/'+path.basename(file);fs.writeFileSync(path.join(here,saved),b);raw.push({source_commit:native.sourceCommit,file,saved,bytes:b.length,sha256:sha(b)});}
const kernelCommit='77eb654a72d32d88cd250e63535478495f7f5429',kernelPath='supabase/repeatable/healthroster_wp_u3_source_proposals/wp_u3_financial_native_kernel.forward.sqlinc';
const kernel=execFileSync('git',['cat-file','blob',kernelCommit+':'+kernelPath],{cwd:repo,maxBuffer:4e6});
const kernelPins=JSON.parse(execFileSync('git',['cat-file','blob',kernelCommit+':docs/healthroster-build/proposals/checklist-native-20261014/SOURCE_PINS.json'],{cwd:repo,maxBuffer:4e6}));
assert.equal(sha(kernel),kernelPins.source_pins.find(p=>p.path===kernelPath)?.saved_sha256);
fs.writeFileSync(path.join(here,'input/Native77_shared_financial_kernel.sqlinc'),kernel);
fs.writeFileSync(path.join(here,'CURRENT_NATIVE_INPUT_PINS.json'),JSON.stringify({status:'UNMOUNTED_ORDINARY_NEXT_AVAILABLE_HOLD_LITERAL_ABI_SOURCE_INPUTS',
 native:false,SQL_executed:false,source_commit:native.sourceCommit,census_sha256:native.censusSha256,census:{roots:native.roots,unique_inputs:native.uniqueInputs,occurrences:native.occurrences},
 selected,raw,kernel:{commit:kernelCommit,file:kernelPath,saved:'input/Native77_shared_financial_kernel.sqlinc',bytes:kernel.length,sha256:sha(kernel)},
 unresolved_amendments:native.index.unresolved.length,conditional_compiled_catalogue:false},null,2)+'\n');
console.log(JSON.stringify({source_only:true,selected:selected.length,raw:raw.length,unchanged_kernel:sha(kernel),native:false}));
