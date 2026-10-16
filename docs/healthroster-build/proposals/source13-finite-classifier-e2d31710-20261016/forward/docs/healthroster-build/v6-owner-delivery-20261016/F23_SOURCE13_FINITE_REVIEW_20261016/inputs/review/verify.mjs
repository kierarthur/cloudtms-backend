import fs from'node:fs';import path from'node:path';import{createHash}from'node:crypto';import{fileURLToPath,pathToFileURL}from'node:url';import assert from'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];if(!root)throw Error('ROOT_REQUIRED');const sha=x=>createHash('sha256').update(x).digest('hex');
const map=JSON.parse(fs.readFileSync(path.join(here,'EXTENSION_MAP.json'))),review=JSON.parse(fs.readFileSync(path.join(here,'REVIEW.json')));
const{specialConstructViolations,executableParts}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-source-gates.mjs')));
const{scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs'))),{routineDefinitions}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-final-definers.mjs')));
let checks=0;const checked=f=>{f();checks++;};
for(const input of map.artifacts)checked(()=>assert.equal(sha(fs.readFileSync(path.join(here,input.path))),input.sha256));
for(const pin of map.filePins)checked(()=>assert.equal(sha(fs.readFileSync(path.join(root,pin.path))),pin.sha256));
for(const pin of review.sourceInputs)checked(()=>assert.equal(sha(fs.readFileSync(path.join(root,pin.path))),pin.sha256));
for(const pin of map.completeProducerPins){const source=fs.readFileSync(path.join(root,pin.file),'utf8').replace(/^\\(?:set|ir)\b.*$/mg,'');const parts=executableParts(source,pin.file);checked(()=>assert.equal(parts.filter(p=>p.signature===pin.signature&&sha(p.text.trim())===pin.bodySha256).length,1));if(pin.completeStatementSha256)checked(()=>assert.equal(scanManagedSql(source).statements.filter(s=>routineDefinitions(source.slice(s.start,s.end),pin.file).includes(pin.signature)&&sha(source.slice(s.start,s.end).trim())===pin.completeStatementSha256).length,1));}
for(const p of map.points){const text=fs.readFileSync(path.join(root,p.file),'utf8').replace(/^\\(?:set|ir)\b.*$/mg,'');const rows=specialConstructViolations(text,p.file).filter(r=>r.key===p.key);checked(()=>{assert.equal(rows.length,p.wholeSourceOccurrenceCount);assert.equal(p.newDiagnosticCount,p.reviewIds.length);assert.equal(sha(p.key),p.keySha256);for(const id of p.reviewIds){const proof=review.rows.find(r=>r.id===id);assert.equal(proof.key,p.key);assert.equal(proof.proof.verdict,p.reviewVerdict);assert.equal(proof.counterpart.baselineCovered,false);}});}
checked(()=>assert.equal(map.points.reduce((n,p)=>n+p.newDiagnosticCount,0),26));checked(()=>assert.equal(map.points.length,23));
checked(()=>assert.equal(review.rows.filter(r=>r.counterpart.baselineCovered).length,12));
checked(()=>assert.deepEqual(map.requiredSqlCorrections,[]));checked(()=>assert.deepEqual(map.bankingSchemaOrAbiAssumptions,[]));
checked(()=>assert.equal(review.rows.find(r=>r.id===24).proof.cardinalityBound.includes('no declared unique key'),true));
checked(()=>assert.equal(review.rows.find(r=>r.id===38).proof.scannerArtifact.innerPlpgsqlAst.strict,true));
const result={qualification:'EXACT_PIN_AND_FINITE_MULTIPLICITY_VERIFICATION_ONLY',extensionMapSha256:sha(fs.readFileSync(path.join(here,'EXTENSION_MAP.json'))),passed:checks,failed:0,native:'NOT_RUN'};fs.writeFileSync(path.join(here,'VERIFY_RESULTS.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result,null,2));
