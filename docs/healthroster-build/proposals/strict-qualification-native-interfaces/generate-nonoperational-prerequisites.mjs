import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),prior=path.resolve(out,'../protected-source-qualification-operational-gate'),forward=path.resolve(out,'../strict-qualification-first-use-inventory-forward'),sha=x=>createHash('sha256').update(x).digest('hex'),proofs=[];
function emit(file,before,changes){let value=before;for(const h of changes){assert.equal(value.split(h.before).length-1,1,h.before);value=value.replace(h.before,()=>h.after);}
 let reverse=value;for(const h of [...changes].reverse()){assert.equal(reverse.split(h.after).length-1,1);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,before);
 fs.writeFileSync(path.join(out,file),value.trimEnd()+'\n');proofs.push({file,input_sha256:sha(before),output_sha256:sha(value),reverse_hunk_matches_entire_input_definition:true});return value;}
const js=fs.readFileSync(path.join(prior,'prerequisite-evidence.mjs'),'utf8');
emit('same-installed-prerequisite-evidence.mjs',js,[
 {before:"import {proveCommittedSources} from './source-equivalence.mjs';",after:"import {proveCommittedSources} from '../strict-qualification-git-path-parser-forward/source-equivalence.mjs';"},
 {before:"export { proveCommittedSources } from './source-equivalence.mjs';",after:"export { proveCommittedSources } from '../strict-qualification-git-path-parser-forward/source-equivalence.mjs';"},
 {before:'value.release_id===context.release_id',after:'(value.release_id===context.release_id&&value.source_commit!==context.git_commit)'},
 {before:'const rows=evidence?.weekly_source_protected_capability_qualifications;',after:'const rows=evidence?.weekly_source_protected_prerequisite_qualifications;'}
]);
const reader=fs.readFileSync(path.join(prior,'02_complete_prerequisite_reader.sqlinc'),'utf8');
emit('NONOPERATIONAL_PREREQUISITE_READER.sqlinc',reader,[{before:"executed.release_id=r.release_id or executed.status<>'VERIFIED'",after:"executed.status<>'VERIFIED'"}]);
const scope=fs.readFileSync(path.join(forward,'TEST_JOURNEY_SCOPE.sqlinc'),'utf8');
emit('10_complete_bulk_test_scope.sqlinc',scope,[{before:"lateral jsonb_array_elements(r.evidence_json->'weekly_source_protected_capability_qualifications') value",
 after:"lateral jsonb_array_elements(r.evidence_json->'weekly_source_protected_prerequisite_qualifications') value"}]);
fs.writeFileSync(path.join(out,'NONOPERATIONAL_PREREQUISITE_REVERSE.json'),JSON.stringify({source_only:true,no_operational_v2_slot:true,
 original_native_gate_census_and_next_prerequisite_retained:true,no_third_install:true,no_runtime_acceptance:true,proofs},null,2)+'\n');
