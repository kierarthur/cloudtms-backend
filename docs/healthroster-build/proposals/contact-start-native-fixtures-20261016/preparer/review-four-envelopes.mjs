// SOURCE-ONLY proposal for root review. Never connects or installs an adapter.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {execFileSync}from 'node:child_process';import {pathToFileURL,fileURLToPath}from 'node:url';
const root=path.resolve(process.argv.find(x=>x.startsWith('--root='))?.slice(7)??''),here=path.dirname(fileURLToPath(import.meta.url)),out=path.join(here,'four-envelope-review');fs.mkdirSync(out,{recursive:true});
const load=f=>import(pathToFileURL(path.join(root,'scripts',f)).href),{readManagedClosure,scanManagedSql}=await load('cloudtms-managed-release-sql.mjs');
const sha=s=>crypto.createHash('sha256').update(s).digest('hex'),canon=s=>String(s).replaceAll('\r\n','\n'),must=(b,c)=>{if(!b)throw Error(c);};
const pins=[['30072026_1216_invoice_timesheet_refs_ward_selective_numbering.sql','INVOICE_GATE','306489585aec541a38700bb7a9bca96b178fc291a644716a7079d45cf4a79747'],
 ['08092026_1615_invoice_correction_stream_parity_v1.sql','INVOICE_SETTINGS','693c068fac56d4755e8a63664250097a994d3085e9cc32fd8a1f23fb24bfdcbf'],
 ['28092026_1234_stage2_hosted_final_authority_reassert_v1.sql','SOURCE_REASSERT','4b918f87f5c0e5878b1b338b8f0db1e3aa130375439d15734a2f7f41cd277f53'],
 ['13102026_1418_candidate_runtime_reload_authority_v1.sql','CANDIDATE_RUNTIME_RELOAD','eece10432b4080b90cee9a1077336b0accbeb08c08f6d3649d4b4fa754e9de1b']];
const managedSource=canon(fs.readFileSync(path.join(root,'scripts/cloudtms-managed-release.mjs'),'utf8'));
const gateMatch=managedSource.match(/const gate = unit\.exception\.reason === 'INVOICE_GATE' \? `([\s\S]*?)` : '';/);must(gateMatch,'ROOT_NATIVE_GATE_SOURCE_DRIFT');
const gate=gateMatch[1].replaceAll('\\\\','\\');must(!gate.includes('${')&&gate.includes('CLOUDTMS_INVOICE_INSTALL_GATE_MISSING')&&gate.includes('CLOUDTMS_INVOICE_INSTALL_DEFERRED_ACTIVE_WORK'),'ROOT_NATIVE_GATE_UNCLASSIFIED');
const rules={
 INVOICE_GATE:'Retain exact native SELECT current invoice_operation_chunks active DOCUMENT_PLAN/BUILD_MANIFEST or ISSUE_INVOICE/FREEZE; retain original gset/if/else/endif and blocked DO. Append exact unitCheckpointSql gate. True or missing actual variable must fail; never force false. Existing resetWriterSql unsets the variable before the original command.',
 INVOICE_SETTINGS:'Retain actual settings backfill and both ACL DOs unchanged. Backfill reads live settings and writes missing authority snapshots on Weekly TS and processed Daily TS, plus real planned ContractWeek settings refresh. Whole native business writes remain in the one outer rollback and full fresh restoration; not a definition-only unit.',
 SOURCE_REASSERT:'Retain one existing role-presence ACL DO unchanged; its only executed strings revoke anon/authenticated and grant service_role EXECUTE on public.contract_week_delete_planned(uuid,uuid). No business function is invoked by that DO.',
 CANDIDATE_RUNTIME_RELOAD:'Retain both role-presence grants DOs unchanged; only revoke browser and grant existing service_role EXECUTE for the two actual Daily native apply functions. No apply function is called.'};
const rows=[];
for(const [name,reason,pin]of pins){
 const file='supabase/repeatable/'+name,c=readManagedClosure(file,root);must(c.closureHash===pin,'RAW_CLOSURE_CHANGED '+file);
 const parsed=scanManagedSql(c.expanded),removals=[];
 for(const r of parsed.statements.filter(x=>['BEGIN','COMMIT','ROLLBACK','START','END','ABORT','PREPARE'].includes(x.tokens[0]))){must(['BEGIN','COMMIT'].includes(r.tokens.join(' ')),'NON_PACKAGING_TRANSACTION '+file);removals.push({...r,command:r.tokens.join(' '),kind:'EXACT_TOP_LEVEL_PACKAGING'});}
 for(const r of parsed.meta.filter(x=>/^\\set\s+ON_ERROR_STOP\b/i.test(x.text))){must(/^\\set\s+ON_ERROR_STOP\s+on\s*$/i.test(r.text),'ERROR_CONTROL_CHANGED');removals.push({...r,command:r.text,kind:'EXACT_ERROR_STOP_PACKAGING'});}
 const span=(s,r)=>({charStart:r.start,charEnd:r.end,byteStart:Buffer.byteLength(s.slice(0,r.start)),byteEnd:Buffer.byteLength(s.slice(0,r.end)),
  startLine:s.slice(0,r.start).split('\n').length,endLine:s.slice(0,r.end).split('\n').length,command:r.command,kind:r.kind,token:s.slice(r.start,r.end),sha256:sha(s.slice(r.start,r.end))});
 let candidate=c.expanded;for(const r of [...removals].sort((a,b)=>b.start-a.start))candidate=candidate.slice(0,r.start)+candidate.slice(r.end);
 const preserved=parsed.meta.filter(x=>!removals.some(r=>r.start===x.start)).map(x=>({text:x.text,charStart:x.start,sha256:sha(x.text)}));
 const fileHashes=c.ordered.map(x=>({file:x.path,sha256:sha(x.source),bytes:Buffer.byteLength(x.source),physicalPackaging:scanManagedSql(x.source).statements.filter(r=>['BEGIN','COMMIT'].includes(r.tokens[0])).map(r=>span(x.source,{...r,command:r.tokens.join(' '),kind:'PHYSICAL_SOURCE_CONTROL'})),physicalOnErrorStop:scanManagedSql(x.source).meta.filter(r=>/^\\set\s+ON_ERROR_STOP\s+on\s*$/i.test(r.text)).map(r=>span(x.source,{...r,command:r.text,kind:'PHYSICAL_ERROR_STOP'}))}));
 // psql gset ends the preceding SELECT without a semicolon. For locating DO
 // spans only, a same-length in-memory lexer view marks that end. Raw source,
 // preserved command sequence and candidate are never altered by this view.
 const view=c.expanded.replace(/^\\gset[^\r\n]*$/gm,m=>';'+' '.repeat(m.length-1));
 const doCalls=scanManagedSql(view).statements.filter(x=>['DO','CALL'].includes(x.tokens[0])).map(r=>{const sql=c.expanded.slice(r.start,r.end);return{head:r.tokens[0],charStart:r.start,charEnd:r.end,sha256:sha(sql),sql,
  scope:reason==='INVOICE_GATE'?'ORIGINAL_CONDITIONAL_BLOCKED_ASSERTION':'TOP_LEVEL_NATIVE_DO',
  directNativeCallees:[...new Set([...sql.matchAll(/\b(?:perform|from|join)\s+((?:public|private)\.[a-z_][a-z0-9_]*)\s*\(/gi)].map(x=>x[1]))]};});
 const candidateFile=name.replace(/\.sql$/,'.review.psql');const finalCandidate=candidate+(reason==='INVOICE_GATE'?'\n-- Exact existing native unitCheckpointSql post-script gate; review only.\n'+gate:'');
 fs.writeFileSync(path.join(out,candidateFile),finalCandidate);
 rows.push({path:file,reason,rawClosureHash:pin,rawExpandedHash:sha(c.expanded),fileHashes,
  exactProposedRemovals:removals.sort((a,b)=>a.start-b.start).map(r=>span(c.expanded,r)),preservedMetaSequence:preserved,
  doCalls,effectReview:rules[reason],candidate:{file:candidateFile,sha256:sha(finalCandidate),bytes:Buffer.byteLength(finalCandidate)},
  existingNativeGate:reason==='INVOICE_GATE'?{owner:'scripts/cloudtms-managed-release.mjs::unitCheckpointSql',ownerSourceHash:sha(managedSource),literal:gate,sha256:sha(gate),requiredPreUnitReset:'\\unset invoice_presentation_active_work'}:null,
  forbiddenRemovals:['FUNCTION_OR_PROCEDURE_BODY_TRANSACTION','OTHER_SQL','GSET_IF_ELSE_ENDIF','NATIVE_DO','ACTIVE_WORK_QUERY_OR_REFUSAL'],executionQualification:'PROPOSAL_ONLY_ROOT_REVIEW_REQUIRED_NATIVE_NOT_RUN'});
}
const helperFiles=[['supabase/repeatable/03092026_1641_contract_settings_effective_authority_v1.sql',['_contract_settings_refresh_all_planned_v1','_contract_settings_apply_week_snapshot_v1','_contract_settings_effective_core_v1']],
 ['supabase/repeatable/06012026_settings_finwindow_eligibletimesshets_plus_recompute_totals.sqls.sql',['settings_finance_pick']],
 ['supabase/repeatable/26082026_0043_candidate_manager_authoriser_policy_v2.sql',['_candidate_manager_authoriser_effective_v2','_candidate_manager_authoriser_policy_normalize_v2','_candidate_manager_authoriser_email_array_v2','_candidate_manager_authoriser_domain_array_v2']],
 ['supabase/repeatable/07082026_2059_candidate_app_private_helpers_v1.sql',['_candidate_normalize_email','_candidate_normalize_domain_v1']]];
const callees=[];for(const [file,names]of helperFiles){const source=canon(fs.readFileSync(path.join(root,file),'utf8'));for(const r of scanManagedSql(source).statements){const sql=source.slice(r.start,r.end),n=sql.match(/^create\s+(?:or replace\s+)?function\s+((?:public|private)\.([a-z_][a-z0-9_]*))\s*\(/i);if(!n||!names.includes(n[2]))continue;callees.push({name:n[1],file,sourceFileHash:sha(source),statementHash:sha(sql),sql,
 calls:[...new Set([...sql.matchAll(/\b((?:public|private)\.[a-z_][a-z0-9_]*)\s*\(/gi)].map(x=>x[1]).filter(x=>x!==n[1]))],
 effect:n[2]==='_contract_settings_refresh_all_planned_v1'?'Enumerates CW timesheet_id IS NULL in contract/week/id order and calls real apply helper; no Timesheet allocation.':n[2]==='_contract_settings_apply_week_snapshot_v1'?'FOR UPDATE actual CW; refuses linked Timesheet; returns if fingerprint equal; writes submission_mode_snapshot, settings_authority_json/version/fingerprint/resolved_at and updated_at only on still-unlinked CW.':n[2]==='_contract_settings_effective_core_v1'?'Reads Contract/Client/current settings/defaults/finance and constructs settings JSON/hash. Backfill and planned apply always pass p_timesheet_id NULL, so frozen-Timesheet helper branch is not invoked. No DML.':n[2]==='settings_finance_pick'?'STABLE read of effective or earliest settings_finance_windows only; no DML.':'IMMUTABLE pure JSON/text validation/normalization; no table DML.'});}}
must(callees.length===10,'CALLEE_CENSUS_CHANGED');
const calleeSet=new Set(callees.map(x=>x.name)),unreachable='private._timesheet_settings_authority_frozen_v1';for(const c of callees)for(const n of c.calls)must(calleeSet.has(n)||n===unreachable,'UNREVIEWED_CALLEE '+n);
fs.writeFileSync(path.join(out,'CALLEE_EFFECT_AUDIT.json'),JSON.stringify({callees,unreachableBranch:{name:unreachable,reason:'All actual backfill/core and planned-apply call sites pass literal NULL p_timesheet_id; enclosing core IF p_timesheet_id IS NOT NULL is false.'},
 triggerBoundary:'Actual UPDATE triggers are retained. Existing settings/table trigger effects and full fresh business/catalogue/ledger restoration remain part of root full-rehearsal review, never a zero-write waiver.',native:'NOT_RUN'},null,2)+'\n');
const result={version:'CONTACT_FOUR_EXISTING_EXCEPTION_ENVELOPE_PROPOSAL_V1',rootCommit:execFileSync('git',['-C',root,'rev-parse','HEAD'],{encoding:'utf8'}).trim(),status:'SOURCE_REVIEW_PROPOSAL_NOT_EXECUTION_APPROVAL',rows,
 businessEffect:'Native settings snapshot backfill retained; no payment/provider/email drain, no Bank approval or fake publication. Existing actual active-work refusal retained.',databaseCalls:0,newExecutionAdapters:0,sharedWrites:0};
fs.writeFileSync(path.join(out,'ENVELOPE_REVIEW.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({rootCommit:result.rootCommit,units:rows.length,removals:rows.map(x=>({path:x.path,controls:x.exactProposedRemovals.length,preservedMeta:x.preservedMetaSequence.map(x=>x.text),DO:x.doCalls.length})),callees:callees.length,native:'NOT_RUN'}));
