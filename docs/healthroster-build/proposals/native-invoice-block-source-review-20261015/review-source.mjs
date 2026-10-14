// Independent Git/source review only. Native lane is read-only; no SQL execution.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {execFileSync, spawnSync} from 'node:child_process';
import {fileURLToPath, pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url));
const native=path.resolve(here,'../../../../../healthroster-checklist-native-14102026');
const pin='aea33728bb76c4d2c57a9e82abffa570e743a0f0';
const prefix='docs/healthroster-build/proposals/checklist-native-invoice-block-20261014/';
const sql='supabase/repeatable/healthroster_wp_u3_source_proposals/';
const prior='docs/healthroster-build/proposals/checklist-native-invoice-20261014/';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=(...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:native,maxBuffer:64e6});
const blob=p=>git('cat-file','blob',pin+':'+p);
const saved=p=>fs.readFileSync(path.join(native,p));
const canonical=b=>b.toString().replaceAll('\r\n','\n');
const snapshot=new Map();
function exact(p){const b=blob(p);assert.ok(b.equals(saved(p)),p+' saved/Git mismatch');snapshot.set(p,{path:p,bytes:b.length,sha256:sha(b)});return b;}
const manifestBytes=exact(prefix+'SOURCE_MANIFEST.json');
const manifest=JSON.parse(manifestBytes);assert.equal(manifest.artifacts.length,11);
for(const a of manifest.artifacts){const b=exact(a.path);assert.equal(b.length,a.bytes,a.path);assert.equal(sha(b),a.sha256,a.path);}
const changed=git('diff-tree','--no-commit-id','--name-only','-r',pin).toString().trim().split('\n');
for(const p of changed)exact(p);
const parserPath='docs/healthroster-build/proposals/checklist-native-20261014/financial-native-input-20261014/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const parser=exact(parserPath);assert.doesNotMatch(parser.toString(),/writeFile|fetch\(|execFile|spawn/);
const {functionEvents,sqlTokens}=await import('data:text/javascript;base64,'+parser.toString('base64'));
exact('scripts/cloudtms-managed-release-sql.mjs');
const {scanManagedSql}=await import(pathToFileURL(path.join(native,'scripts/cloudtms-managed-release-sql.mjs')).href);
const pins=JSON.parse(exact(prior+'NATIVE_INVOICE_OWNER_PINS.json'));
const ownerChecks=[];
for(const row of [...pins.selected,...pins.readGraph]){
  const raw=git('cat-file','blob',pins.sourceCommit+':'+row.file);assert.equal(sha(raw),row.source_file_sha256,row.id);
  const event=functionEvents(canonical(raw)).events.find(e=>e.kind==='CREATE'&&e.id===row.id);assert.ok(event,row.id);
  assert.equal(sha(event.definition),row.definition_sha256,row.id+' definition');assert.equal(sha(event.body),row.body_sha256,row.id+' body');
  const d=exact(prior+row.saved_file);assert.equal(sha(d),row.definition_sha256,row.saved_file);
  ownerChecks.push({id:row.id,file:row.file,source_file_sha256:sha(raw),definition_sha256:sha(event.definition),body_sha256:sha(event.body)});
}
const proof=JSON.parse(exact(prefix+'NATIVE_BLOCKER_REVERSE.json'));
const before=exact(proof.before_path).toString();
const forward=exact(sql+'wp_u3_invoice_block_native.forward.sqlinc').toString();
const reverse=exact(sql+'wp_u3_invoice_block_native.reverse.sqlinc').toString();
assert.equal(sha(before),proof.before_sha256);assert.equal(sha(forward),proof.forward_sha256);assert.equal(sha(reverse),proof.reverse_sha256);
const events=functionEvents(forward).events.filter(e=>e.kind==='CREATE');assert.equal(events.length,2);
const writer=events.find(e=>e.name==='public.weekly_source_invoice_admit_atomic_v1');
const core=events.find(e=>e.name==='private.weekly_source_invoice_unreleased_vat_fit_movements_v1');
assert.equal(before.split(proof.hunk.before).length,2);assert.equal(writer.definition.split(proof.hunk.after).length,2);
assert.equal(writer.definition,before.replace(proof.hunk.before,proof.hunk.after));assert.equal(writer.definition.replace(proof.hunk.after,proof.hunk.before),before);
assert.equal(sha(writer.definition),proof.after_sha256);
assert.equal(core.body.trim(),proof.hunk.before.replace('select 1','select m.*').replace('v_manifest.id','p_client_manifest_id')+';');
assert.equal(reverse,before+'\ndrop function private.weekly_source_invoice_unreleased_vat_fit_movements_v1(uuid);\n');
const nativeBefore=git('cat-file','blob',proof.source_commit+':'+proof.source_body);
assert.equal(functionEvents(canonical(nativeBefore)).events.find(e=>e.id===writer.id).definition,before);
const reader=exact(sql+'wp_u3_source_invoice_block.sqlinc').toString();
const readerEvent=functionEvents(reader).events.find(e=>e.kind==='CREATE');
const readerWords=sqlTokens(readerEvent.body).filter(t=>t.kind==='word').map(t=>t.value);
assert.deepEqual(readerWords.filter(w=>['insert','update','delete','truncate','create','lock','notify','call','execute','set_config','round','sum'].includes(w)),[]);
const lexical=[['native_forward',forward],['native_reverse',reverse],['block_items',reader],['single_public_checklist',exact(sql+'wp_u3_native_checklist.sqlinc').toString()]]
  .map(([name,s])=>({name,statements:scanManagedSql(s).statements.length,functions:functionEvents(s).events.filter(e=>e.kind==='CREATE').map(e=>e.id)}));
for(const p of ['broker/src/weekly-source/healthroster-checklist-source.mjs','broker/src/weekly-source/checklist-read.mjs',
  'tests/healthroster-checklist-source-invoice-block.test.mjs','tests/healthroster-checklist-source.test.mjs'])exact(p);
const testFiles=['tests/healthroster-checklist-source-invoice-block.test.mjs','tests/healthroster-checklist-source.test.mjs'];
const result=spawnSync(process.execPath,['--test','--test-reporter=tap',...testFiles],{cwd:native,encoding:'utf8',timeout:60000,maxBuffer:4e6});
assert.equal(result.status,0,result.stdout+result.stderr);
const count=Number(result.stdout.match(/# pass (\d+)/)?.[1]);assert.ok(count>=14);
const syntaxFiles=['broker/src/weekly-source/healthroster-checklist-source.mjs','tests/healthroster-checklist-source-invoice-block.test.mjs',prefix+'build-native-blocker-packet.mjs',prefix+'freeze-source-increment.mjs'];
for(const p of syntaxFiles){const r=spawnSync(process.execPath,['--check',p],{cwd:native,encoding:'utf8',timeout:30000});assert.equal(r.status,0,r.stderr);}
for(const [p,a] of snapshot){assert.equal(sha(saved(p)),a.sha256,p+' changed during review');assert.equal(sha(blob(p)),a.sha256,p+' Git changed');}
const evidence={status:'INDEPENDENT_BOUNDED_SOURCE_ACCEPTANCE_NATIVE_AND_COMPOSITION_HELD',reviewed_commit:pin,
  source_commit:pins.sourceCommit,manifest:{path:prefix+'SOURCE_MANIFEST.json',bytes:manifestBytes.length,sha256:sha(manifestBytes)},
  manifest_artifacts:11,changed_commit_files:changed.length,saved_git_before_after_mismatches:0,
  owner_checks:ownerChecks,selected_native_owners:pins.selected.length,pinned_read_graph_definitions:pins.readGraph.length,
  native_writer_hunks:1,native_writer_complete_reverse:true,lexical_only:lexical,
  inspected_unit_tests:{files:testFiles,passed:count,failed:0,stdout:result.stdout},syntax_checks:syntaxFiles.length,
  files:[...snapshot.values()].sort((a,b)=>a.path.localeCompare(b.path)),new_concrete_P0_P1_findings:[],
  database_calls:false,SQL_executed:false,native_qualified:false,compiled_contract_sha256:null,mount_approved:false,
  pending_decision_known:false,action_proved:false,whole_row_quantity_known:false,
  held:['final whole current native admission composition and catalogue-selected amendments','PostgreSQL compilation/installed body and role ACL proof',
    'real admission error/lock/replay parity and native pure read','real unreleased blocker/release/removal/anchored refresh/retention vectors',
    'true permitted release/Finalise target and decision producer','full native process/financial/API/UI/phone/performance']};
fs.writeFileSync(path.join(here,'INDEPENDENT_SOURCE_CHECKS.json'),JSON.stringify(evidence,null,2)+'\n');
console.log(JSON.stringify({...evidence,owner_checks:ownerChecks.length,files:evidence.files.length,inspected_unit_tests:{files:testFiles,passed:count,failed:0}}));
