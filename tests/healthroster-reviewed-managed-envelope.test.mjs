import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {compileReviewedManagedEnvelope,managedEnvelopeReviewPath,managedCalleeAuditPath} from '../scripts/healthroster-reviewed-managed-envelope.mjs';
import {mapExecutableSqlSource} from '../scripts/cloudtms-db-release-lib.mjs';
import {digest,scanManagedSql} from '../scripts/cloudtms-managed-release-sql.mjs';
import {renderFullRehearsalUnits,scanRehearsalSql} from '../scripts/healthroster-full-rehearsal-protocol.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const review=JSON.parse(fs.readFileSync(path.join(root,managedEnvelopeReviewPath)));
const compile=(row,options={})=>compileReviewedManagedEnvelope({path:row.path,sha256:row.rawClosureHash},'repeatable',{root,...options});

for(const row of review.rows)test('exact complete reviewed source: '+row.reason,()=>{
 const unit=compile(row);
 assert.equal(unit.sourceSqlHash,row.rawClosureHash);
 // The first review candidate includes this explanatory comment only.
 const expected=row.reason==='INVOICE_GATE'?null:row.candidate.sha256;
 if(expected)assert.equal(unit.sqlHash,expected);
 for(const call of row.doCalls)assert.ok(unit.sql.includes(call.sql));
 assert.ok(!scanManagedSql(unit.sql).statements.some(s=>['BEGIN','COMMIT','ROLLBACK','START','PREPARE'].includes(s.tokens[0])));
 assert.equal(unit.reviewedManagedEnvelope.nativeProof,'NOT_RUN');
 for(const file of row.fileHashes)assert.ok(unit.sourceFiles.some(s=>s.file===file.file&&s.sha256===file.sha256));
 if(row.reason==='INVOICE_GATE'){
  assert.ok(unit.sql.endsWith(row.existingNativeGate.literal));
  assert.ok(unit.sql.includes("raise exception 'INVOICE_PRESENTATION_SNAPSHOT_ACTIVE_WORK'"));
 }
});

test('actual complete physical-file owner mapping precedes derived removal offsets',()=>{
 const previous=process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;
 process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER='CURRENT_USER';
 try{
  for(const row of review.rows){
   const seen=[];
   const unit=compile(row,{mapSource:(source,file)=>{
    const pin=row.fileHashes.find(r=>r.file===file);
    assert.equal(digest(source),pin.sha256);
    assert.equal(Buffer.byteLength(source),pin.bytes);
    seen.push(file);
    return mapExecutableSqlSource(source,file);
   }});
   assert.deepEqual(seen,row.fileHashes.map(r=>r.file));
   assert.ok(unit.reviewedManagedEnvelope.mappedSpans.some(s=>s.sourceSha256!==s.mappedSourceSha256));
   assert.ok(!/\bowner to postgres\s*;/i.test(unit.sql));
   renderFullRehearsalUnits([unit]);
  }
 }finally{if(previous===undefined)delete process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;else process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER=previous;}
});

test('native invoice client variable reset surrounds actual body and false/missing guards',()=>{
 const unit=compile(review.rows.find(r=>r.reason==='INVOICE_GATE'));
 const rendered=renderFullRehearsalUnits([unit]);
 const reset='\\unset invoice_presentation_active_work';
 assert.ok(rendered.sql.indexOf(reset)<rendered.sql.indexOf('select exists ('));
 assert.ok(rendered.sql.lastIndexOf(reset)>rendered.sql.indexOf('CLOUDTMS_INVOICE_INSTALL_GATE_MISSING'));
 assert.equal(rendered.units[0].clientVariableIsolation,true);
 assert.equal(rendered.units[0].reviewedManagedEnvelope.reason,'INVOICE_GATE');
 assert.equal(rendered.sql.includes('savepoint hr_verifier_'),false);
 const blocked=scanRehearsalSql(unit.sql).statements.find(s=>unit.sql.slice(s.start,s.end).includes("raise exception 'INVOICE_PRESENTATION_SNAPSHOT_ACTIVE_WORK'"));
 assert.equal(blocked.tokens[0],'DO');
 assert.equal(unit.sql.slice(blocked.start,blocked.end),review.rows[0].doCalls[0].sql);
});

test('gset census leaves quoted and dollar-body text untouched',()=>{
 const sql="select '\\gset';\ndo $body$ begin perform '\\gset'; end $body$;\nselect 1 answer\n\\gset prefix_\ndo $next$ begin raise exception 'REAL_NEXT'; end $next$;";
 const parsed=scanRehearsalSql(sql);
 assert.equal(parsed.statements.length,4);
 assert.equal(parsed.statements[3].tokens[0],'DO');
 assert.equal(sql.slice(parsed.statements[2].start,parsed.statements[2].end),"select 1 answer\n\\gset prefix_");
 assert.deepEqual(parsed.meta.map(s=>s.text),['\\gset prefix_']);
});

test('CRLF cannot conceal an outer commit after native gset',()=>{
 const sql='select 1\r\n\\gset\r\ncommit;';
 assert.throws(()=>scanRehearsalSql(sql),/CANONICAL_SQL_REQUIRED/);
 assert.throws(()=>renderFullRehearsalUnits([{file:review.rows[0].path,kind:'repeatable',sql,sqlHash:digest(sql)}]),/CANONICAL_SQL_REQUIRED/);
 const lf=sql.replaceAll('\r\n','\n');
 assert.deepEqual(scanRehearsalSql(lf).statements.map(s=>s.tokens[0]),['SELECT','COMMIT']);
 assert.throws(()=>renderFullRehearsalUnits([{file:review.rows[0].path,kind:'repeatable',sql:lf,sqlHash:digest(lf)}]),/OUTER_TRANSACTION_ESCAPE_REFUSED/);
});

test('unknown units delegate and reviewed identity cannot be widened',()=>{
 assert.equal(compileReviewedManagedEnvelope({path:'supabase/repeatable/unreviewed.sql'},'repeatable',{root}),null);
 const row=review.rows[0];
 assert.throws(()=>compileReviewedManagedEnvelope({path:row.path,sha256:'0'.repeat(64)},'repeatable',{root}),/EXACT_SCOPE/);
 assert.throws(()=>compileReviewedManagedEnvelope({path:row.path,sha256:row.rawClosureHash},'migration',{root}),/EXACT_SCOPE/);
});

test('raw included SQL, review, callee audit and native gate owner drift each fail closed',()=>{
 const fixture=fs.mkdtempSync(path.join(os.tmpdir(),'healthroster-envelope-source-'));
 try{
  const files=new Set([managedEnvelopeReviewPath,managedCalleeAuditPath,'scripts/cloudtms-managed-release.mjs',
   ...review.rows.flatMap(row=>row.fileHashes.map(r=>r.file)),
   ...JSON.parse(fs.readFileSync(path.join(root,managedCalleeAuditPath))).callees.map(r=>r.file)]);
  for(const file of files){fs.mkdirSync(path.dirname(path.join(fixture,file)),{recursive:true});fs.copyFileSync(path.join(root,file),path.join(fixture,file));}
  const row=review.rows[0];
  assert.equal(compile(row,{root:fixture}).sqlHash,compile(row).sqlHash);
  for(const file of [row.fileHashes[1].file,managedEnvelopeReviewPath,managedCalleeAuditPath,'scripts/cloudtms-managed-release.mjs']){
   const target=path.join(fixture,file),before=fs.readFileSync(target);
   fs.appendFileSync(target,'\n-- source drift\n');
   assert.throws(()=>compile(row,{root:fixture}),/DRIFT|MANAGED_REPLAY_POLICY_HASH_MISMATCH/);
   fs.writeFileSync(target,before);
  }
 }finally{fs.rmSync(fixture,{recursive:true,force:true});}
});
