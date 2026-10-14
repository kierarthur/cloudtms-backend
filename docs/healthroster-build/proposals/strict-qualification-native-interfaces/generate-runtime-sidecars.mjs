import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),sha=x=>createHash('sha256').update(x).digest('hex');
const modules={
 'final-operational-evidence.mjs':'weekly-source-protected-operational-evidence.mjs',
 'same-installed-prerequisite-evidence.mjs':'weekly-source-protected-prerequisite-evidence.mjs',
 'activation-probe-evidence.mjs':'weekly-source-protected-activation-probe-evidence.mjs',
 'probe-native-evidence.mjs':'weekly-source-protected-probe-native-evidence.mjs',
 'measured-partial-evidence.mjs':'weekly-source-protected-partial-native-evidence.mjs',
 'activation-window-binding.mjs':'weekly-source-activation-window-binding.mjs',
 'off-rule-setup-evidence.mjs':'weekly-source-off-rule-setup-evidence.mjs',
 'post-verified-evidence-controller.mjs':'weekly-source-post-verified-evidence-controller.mjs',
 'off-rule-native-proxy.mjs':'off-rule-native-proxy.mjs',
 'off-rule-transport-rpc.mjs':'off-rule-transport-rpc.mjs'
};
const replacements={
 '../protected-source-capability/release-qualification.mjs':'./weekly-source-protected-release-qualification.mjs',
 '../strict-qualification-git-path-parser-forward/source-equivalence.mjs':'./weekly-source-protected-execution-tree.mjs',
 '../protected-source-qualification-operational-gate/setup-evidence.mjs':'./weekly-source-protected-test-setup-evidence.mjs',
 ...Object.fromEntries(Object.entries(modules).map(([from,to])=>['./'+from,'./'+to]))
};
const proofs=[];
for(const [from,to] of Object.entries(modules)){
 const before=fs.readFileSync(path.join(out,from),'utf8');let after=before;const edits=[];
 for(const [oldName,newName] of Object.entries(replacements))if(oldName!==newName&&after.includes("'"+oldName+"'")){
  const oldValue="'"+oldName+"'",newValue="'"+newName+"'";
  edits.push({oldValue,newValue,count:after.split(oldValue).length-1});after=after.replaceAll(oldValue,newValue);}
 if(!to.startsWith('off-rule-')&&after.includes("'./off-rule-native-proxy.mjs'")){
  const oldValue="'./off-rule-native-proxy.mjs'",newValue="'../broker/src/weekly-source/off-rule-native-proxy.mjs'";
  edits.push({oldValue,newValue,count:after.split(oldValue).length-1});after=after.replaceAll(oldValue,newValue);}
 let reverse=after;for(const edit of [...edits].reverse()){assert.equal(reverse.split(edit.newValue).length-1,edit.count);reverse=reverse.replaceAll(edit.newValue,edit.oldValue);}
 assert.equal(reverse,before);if(from!==to)fs.writeFileSync(path.join(out,to),after);
 const destination=(to.startsWith('off-rule-')?'broker/src/weekly-source/':'scripts/')+to;
 proofs.push({input:from,input_sha256:sha(before),output:to,output_sha256:sha(after),destination,
  import_paths_only:true,reverse_hunks_match_entire_module:true,imports:[...after.matchAll(/(?:from\s+|import\s*)['"]([^'"]+)['"]/g)].map(m=>m[1]),
  edits:edits.map(e=>({before:e.oldValue,after:e.newValue,count:e.count}))});
}
fs.writeFileSync(path.join(out,'RUNTIME_SIDECAR_REVERSE.json'),JSON.stringify({source_only:true,no_runtime_mount:true,proofs,
 external_required_scripts:['weekly-source-protected-release-qualification.mjs','weekly-source-protected-execution-tree.mjs','weekly-source-protected-test-setup-evidence.mjs'],
 native_calculators_changed:false,external_source_equivalence_exception_added:false},null,2)+'\n');
console.log(JSON.stringify({source_only:true,complete_runtime_modules:proofs.length,import_only_reverse_proofs:proofs.length}));
