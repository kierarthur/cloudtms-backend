// Source-only preparation for four individually reviewed release closures.
// This module opens no connection and grants no execution/release authority.
import fs from 'node:fs';
import path from 'node:path';
import {canonical,digest,readManagedClosure,scanManagedSql} from './cloudtms-managed-release-sql.mjs';
import {executionPolicy} from './cloudtms-managed-execution-policy.mjs';
import {assertConnectionPreservingException,unitCheckpointSql} from './cloudtms-managed-release.mjs';

export const managedEnvelopeReviewPath='docs/healthroster-build/v6-owner-delivery-20261016/REHEARSAL_MANAGED_ENVELOPE_REVIEW.json';
export const managedCalleeAuditPath='docs/healthroster-build/v6-owner-delivery-20261016/REHEARSAL_MANAGED_CALLEE_AUDIT.json';
const reviewHash='b43101eb42c2ec52a97f9925ff2b066985e44ae2ea1ad109dce2f106ebae39e1';
const calleeHash='8f8fdc98d361b5054e3b6cceb2b9d12097616b0df7ae3293280f75a774dfef28';
const requireThat=(value,code)=>{if(!value)throw Error(code);};
const transactionHeads=new Set(['BEGIN','COMMIT','END','START','ABORT','PREPARE','ROLLBACK']);

function packaging(sql){
 const parsed=scanManagedSql(sql);
 return [...parsed.statements.filter(s=>transactionHeads.has(s.tokens[0]))
   .map(s=>({...s,command:s.tokens.join(' ')})),
  ...parsed.meta.filter(s=>/^\\set\s+ON_ERROR_(?:STOP|ROLLBACK)\b/i.test(s.text))
   .map(s=>({...s,command:s.text.trim()}))].sort((a,b)=>a.start-b.start);
}

function assertSpans(sql,spans){
 let last=0;
 for(const s of spans){
  requireThat(Number.isInteger(s.charStart)&&Number.isInteger(s.charEnd)
   &&s.charStart>=last&&s.charEnd>s.charStart&&s.charEnd<=sql.length
   &&Buffer.byteLength(sql.slice(0,s.charStart))===s.byteStart
   &&Buffer.byteLength(sql.slice(0,s.charEnd))===s.byteEnd
   &&sql.slice(s.charStart,s.charEnd)===s.token&&digest(s.token)===s.sha256
   &&['BEGIN','COMMIT','\\set ON_ERROR_STOP on'].includes(s.command),
   'REHEARSAL_MANAGED_REVIEW_SPAN_DRIFT');
  last=s.charEnd;
 }
}

function removeSpans(sql,spans){
 for(const span of [...spans].reverse())sql=sql.slice(0,span.start)+sql.slice(span.end);
 return sql;
}

function nativeInvoiceGate(row,item,exception,root){
 if(!row.existingNativeGate)return '';
 const gate=row.existingNativeGate;
 requireThat(digest(canonical(fs.readFileSync(path.join(root,'scripts/cloudtms-managed-release.mjs'),'utf8')))
  ===gate.ownerSourceHash,'REHEARSAL_INVOICE_GATE_OWNER_DRIFT');
 // Extract the actual protected-release guard, rather than author another one.
 const boundary='HR_REHEARSAL_NATIVE_GATE_BOUNDARY';
 const checkpoint=unitCheckpointSql({kind:'repeatable',item,exception,script:boundary},
  {releaseId:'00000000-0000-4000-8000-000000000000'});
 const actual=checkpoint.split(boundary+'\n')[1]?.split('\nbegin;\n')[0];
 requireThat(actual===gate.literal&&digest(actual)===gate.sha256,
  'REHEARSAL_INVOICE_NATIVE_GATE_DRIFT');
 return actual;
}

export function compileReviewedManagedEnvelope(item,kind,{root,mapSource=value=>value}={}){
 const reviewBytes=fs.readFileSync(path.join(root,managedEnvelopeReviewPath));
 const calleeBytes=fs.readFileSync(path.join(root,managedCalleeAuditPath));
 requireThat(digest(reviewBytes)===reviewHash&&digest(calleeBytes)===calleeHash,
  'REHEARSAL_MANAGED_EFFECT_REVIEW_DRIFT');
 const review=JSON.parse(reviewBytes),row=review.rows.find(r=>r.path===item.path);
 if(!row)return null;
 requireThat(kind==='repeatable'&&item.sha256===row.rawClosureHash,
  'REHEARSAL_MANAGED_EXACT_SCOPE_REFUSED');
 const callees=JSON.parse(calleeBytes).callees;
 for(const callee of callees){
  const source=canonical(fs.readFileSync(path.join(root,callee.file),'utf8'));
  requireThat(digest(source)===callee.sourceFileHash&&digest(callee.sql)===callee.statementHash
   &&source.includes(callee.sql),'REHEARSAL_MANAGED_CALLEE_SOURCE_DRIFT');
 }
 const raw=readManagedClosure(item.path,root),exception=executionPolicy(raw);
 requireThat(raw.closureHash===row.rawClosureHash&&digest(raw.expanded)===row.rawExpandedHash
  &&exception?.reason===row.reason,'REHEARSAL_MANAGED_RAW_CLOSURE_DRIFT');
 assertConnectionPreservingException(raw,exception);
 requireThat(JSON.stringify(raw.ordered.map(r=>r.path))===JSON.stringify(row.fileHashes.map(r=>r.file)),
  'REHEARSAL_MANAGED_INCLUDE_CENSUS_DRIFT');
 assertSpans(raw.expanded,row.exactProposedRemovals);
 const files=new Map(row.fileHashes.map(r=>[r.file,r])),mappedSpans=[];
 const mapped=readManagedClosure(item.path,root,[],(source,file)=>{
  const pin=files.get(file);
  requireThat(pin&&digest(source)===pin.sha256&&Buffer.byteLength(source)===pin.bytes,
   'REHEARSAL_MANAGED_PHYSICAL_SOURCE_DRIFT');
  const removed=[...pin.physicalPackaging,...pin.physicalOnErrorStop]
   .sort((a,b)=>a.charStart-b.charStart);
  assertSpans(source,removed);
  const original=packaging(source);
  requireThat(JSON.stringify(original.map(s=>[s.start,s.end,s.command]))
   ===JSON.stringify(removed.map(s=>[s.charStart,s.charEnd,s.command])),
   'REHEARSAL_MANAGED_PHYSICAL_CONTROL_DRIFT');
  // Owner mapping requires the COMPLETE original physical child. Its changes
  // can shift offsets: derive fresh mapped spans, never reuse raw offsets.
  const executable=mapSource(source,file),controls=packaging(executable);
  requireThat(JSON.stringify(controls.map(s=>[s.command,digest(executable.slice(s.start,s.end))]))
   ===JSON.stringify(removed.map(s=>[s.command,s.sha256])),
   'REHEARSAL_MANAGED_MAPPED_CONTROL_DRIFT');
  mappedSpans.push({file,sourceSha256:pin.sha256,mappedSourceSha256:digest(executable),
   removals:controls.map(s=>({start:s.start,end:s.end,command:s.command,
    byteStart:Buffer.byteLength(executable.slice(0,s.start)),
    byteEnd:Buffer.byteLength(executable.slice(0,s.end)),sha256:digest(executable.slice(s.start,s.end))}))});
  return removeSpans(executable,controls);
 });
 requireThat(!packaging(mapped.expanded).length,'REHEARSAL_MANAGED_CONTROL_SURVIVED');
 for(const call of row.doCalls){
  requireThat(digest(call.sql)===call.sha256&&raw.expanded.slice(call.charStart,call.charEnd)===call.sql
   &&mapped.expanded.includes(call.sql),'REHEARSAL_MANAGED_NATIVE_DO_CHANGED');
 }
 const preserved=scanManagedSql(mapped.expanded).meta.map(s=>s.text);
 requireThat(JSON.stringify(preserved)===JSON.stringify(row.preservedMetaSequence.map(s=>s.text)),
  'REHEARSAL_MANAGED_CONDITIONAL_SEQUENCE_CHANGED');
 const sql=mapped.expanded+nativeInvoiceGate(row,item,exception,root);
 return {file:item.path,kind,sql,sqlHash:digest(sql),sourceSqlHash:item.sha256,phase:kind,
  sourceFiles:[...new Map([...row.fileHashes.map(r=>({file:r.file,sha256:r.sha256})),
   ...callees.map(r=>({file:r.file,sha256:r.sourceFileHash}))].map(r=>[r.file,r])).values()],
  clientVariables:row.existingNativeGate?['invoice_presentation_active_work']:[],
  isolateClientVariables:!!row.existingNativeGate,
  reviewedManagedEnvelope:{reason:row.reason,reviewSha256:reviewHash,calleeAuditSha256:calleeHash,
   rawExpandedSha256:row.rawExpandedHash,mappedSpans,nativeProof:'NOT_RUN'}};
}
