import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {pathToFileURL} from 'node:url';
import {scanManagedSql} from '../scripts/cloudtms-managed-release-sql.mjs';
import {routineDefinitions} from '../scripts/bpay-next-final-definers.mjs';

const root=process.cwd(),base='docs/healthroster-build/proposals/';
const hash=value=>createHash('sha256').update(value).digest('hex');
const read=file=>fs.readFileSync(path.join(root,file),'utf8');
const quoted=JSON.stringify;

// Execute the frozen protocol cases against the complete mounted route and
// decoder. The proposal builder is deliberately not imported or executed.
// Only test location/subject selection and its result-file writer are changed;
// every transformation must reverse to the exact frozen test source.
async function mountedCases(directory,{publicContext=false}={}){
 const file=base+directory+'/test.mjs',original=read(file),edits=[];
 let source=original;
 const replace=(before,after)=>{
  assert.equal(source.split(before).length,2,before);
  source=source.replace(before,()=>after);edits.push({before,after});
 };
 const here=path.join(root,base,directory);
 if(publicContext){
  replace("import{here,root,sha}from'./build.mjs';",
   `import{createHash}from'node:crypto';const here=${quoted(here)},root=${quoted(root)},sha=v=>createHash('sha256').update(v).digest('hex');`);
  replace("const raw=read('kept-pay-context.mjs')",
   "const raw=fs.readFileSync(path.join(root,'broker/src/weekly-source/kept-pay-context.mjs'),'utf8')");
 }else{
  replace("const here=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(here,'../../../../../healthroster-install2-14102026')",
   `const here=${quoted(here)},root=${quoted(root)}`);
 }
 replace("source=read('forward/broker/src/weekly-source/routes.js')",
  "source=fs.readFileSync(path.join(root,'broker/src/weekly-source/routes.js'),'utf8')");
 replace("fs.writeFileSync(path.join(here,'TEST_RESULTS.json'),",
  "globalThis.__healthMountedProtocolResult(path.join(here,'TEST_RESULTS.json'),");
 let reversed=source;for(const h of [...edits].reverse()){
  assert.equal(reversed.split(h.after).length,2);reversed=reversed.replace(h.after,()=>h.before);
 }assert.equal(reversed,original);
 let result;globalThis.__healthMountedProtocolResult=(file,value)=>{
  assert.equal(file,path.join(here,'TEST_RESULTS.json'));assert.equal(result,undefined);
  result=JSON.parse(value);assert.equal(result.failed,0);
 };
 try{await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));}
 finally{delete globalThis.__healthMountedProtocolResult;}
 assert(result);assert.equal(result.native,'NOT_RUN');assert.equal(result.dbConnections,0);
 return {result,originalSha256:hash(original),exactTestReverse:true};
}

test('actual mounted complete route retains all 37 Later protocol and current-owner cases',async()=>{
 const {result}=await mountedCases('source-later-accept-current-successor-20261017');
 assert.equal(result.passed,37);
});

test('actual mounted complete public Keep route and corrected decoder pass all 29 frozen cases',async()=>{
 const {result}=await mountedCases('kept-current-public-read-successor-20261017',{publicContext:true});
 assert.equal(result.passed,29);
});

test('new independent catalogue verifier matches all 21 complete final function headers',()=>{
 const proof=JSON.parse(read('docs/healthroster-build/v6-owner-delivery-20261016/LATER_KEEP_MANDATORY_VERIFIERS_20261017.json'));
 const text=read(proof.newCatalogueVerifier);assert.equal(hash(text),proof.newCatalogueVerifierSha256);
 const expected=[...text.matchAll(/\('((?:private|public)\.[a-z_0-9]+\([^']*\))',(false|true),'([^']+)'\)/g)];
 assert.equal(expected.length,21);assert.equal(expected.filter(row=>row[2]==='true').length,3);
 assert.deepEqual(expected.map(row=>row[1]),[...proof.newPrivate,...proof.newService]);
 const registry=JSON.parse(read('docs/bpay-next/final-definer-closures.json'));
 for(const [,signature,service,searchPath]of expected){
  const owners=registry.chains.filter(row=>row.signature===signature);assert.equal(owners.length,1,signature);
  const owner=owners[0],body=read(owner.finalPhysical),parsed=scanManagedSql(body);
  const statements=parsed.statements.map(s=>parsed.source.slice(s.start,s.end)).filter(s=>routineDefinitions(s,owner.finalPhysical).includes(signature));
  assert.equal(statements.length,1,signature);
  const header=statements[0].split(/\bas\s+\$[a-z_0-9]*\$/i)[0];assert.match(header,/security\s+definer/i);
  assert.equal(/set\s+search_path\s*(?:to\s+|=\s*)([^\n\r]+)/i.exec(header)[1].replace(/[\s']/g,''),searchPath,signature);
  assert.equal(service==='true',signature.startsWith('public.'));
 }
 const release=JSON.parse(read(proof.registration.target));
 for(const key of ['verificationFiles','newVerificationFiles']){
  assert.deepEqual(release[key].slice(0,-1),proof.registration.beforeLists[key]);
  assert.equal(release[key].at(-1),proof.newCatalogueVerifier);
 }
});

test('mounted UTC decoder rejects normalized dates without changing genuine microsecond ACKs',async()=>{
 const file='broker/src/weekly-source/kept-pay-context.mjs';
 const edits=JSON.parse(read(base+'keep-public-read-utc-calendar-forward-20261017/EDITS.json'));
 assert.equal(hash(read(file)),edits.outputSha256);
 const {decodeKeptPayContext}=await import(pathToFileURL(path.join(root,file)));
 const id='ffffffff-ffff-ffff-ffff-ffffffffffff';
 const value={schema_version:'WEEKLY_SOURCE_KEPT_AS_PAID_CONTEXT_READ_V1',...Object.fromEntries(['work_event_id','candidate_id','client_id','contract_id','source_group_id','source_cycle_id','projection_publication_id','upload_id'].map(k=>[k,id])),finalisation_week_ending:'2026-10-18',source_present:false,invoiced:true,paid:null,paid_evidence_state:'UNKNOWN',root_timesheet_ids:[id],credit_lines:[{invoice_line_id:id}],reason:'CREDITED',kept_by_default:true,decision_available:true,decision_needed:true,facts_fingerprint:'a'.repeat(64),decision_applies:false,latest_acknowledgement:null,holds_pay:false,holds_invoice:false,accepted_source_removed:false,disabled_reason:null};
 const ack=timestamp=>({...value,latest_acknowledgement:{audit_id:id,decision_id:id,actor_user_id:id,actor_display:null,acknowledged_at_utc:timestamp,acknowledgement_sequence:'9007199254740993'}});
 const request={schema_version:'WEEKLY_SOURCE_KEEP_AS_PAID_CONTEXT_REQUEST_V1',work_event_id:id};
 for(const t of ['2026-02-31T10:00:00Z','2026-04-31T10:00:00Z','2025-02-29T10:00:00Z','2026-10-17T24:00:00Z'])assert.throws(()=>decodeKeptPayContext(ack(t),request));
 const timestamp='2026-10-17T01:12:33.123456+00:00';
 assert.equal(decodeKeptPayContext(ack(timestamp),request).latest_acknowledgement.acknowledged_at_utc,timestamp);
});
