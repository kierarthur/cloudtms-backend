// Exact data-free dynamic source census. No database or external action.
import fs from 'node:fs';import path from 'node:path';import {fileURLToPath,pathToFileURL} from 'node:url';
import {spawnSync} from 'node:child_process';import {createHash} from 'node:crypto';import assert from 'node:assert/strict';
const dir=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(dir,'../../../..'),root=path.resolve(own,'../healthroster-install2-14102026');
const commit='c1ebb691feb6a11324060e0c28210e2bd735624a',sha=x=>createHash('sha256').update(x).digest('hex');
const censusFile=path.join(root,'.codex-tmp/f23-current-source-review-20261016/CENSUS.json'),censusBytes=fs.readFileSync(censusFile),census=JSON.parse(censusBytes);
assert.equal(sha(censusBytes),'509162ee4b70e223473b17e065ec6d3ac84c8c9cf8e7fef300ce5fcbe17346a6');assert.equal(census.sourceCommit,commit);
const oldBytes=fs.readFileSync(path.resolve(own,'../healthroster-v2-money-build-13102026/.codex-tmp/f23-uuid-review/dynamic/REVIEW.json'));
assert.equal(sha(oldBytes),'ec6085b5746ef5542c38c1cb4624b04e86040ba66aef3a82e45f82b0d2de631a');const old=JSON.parse(oldBytes);
const {specialConstructViolations,executableParts}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-source-gates.mjs')));
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
// The root may have added independent review hooks since c1ebb. Require the
// actual detector/extractor/scanner definitions used here to be exact c1ebb.
const analysisToolSourcePins=[];
for(const [file,functions]of[['scripts/bpay-next-source-gates.mjs',[specialConstructViolations,executableParts]],['scripts/cloudtms-managed-release-sql.mjs',[scanManagedSql]]]){
 const r=spawnSync('git',['show',`${commit}:${file}`],{cwd:root,maxBuffer:5e6});assert.equal(r.status,0);const source=r.stdout.toString();
 for(const fn of functions)assert(source.includes(fn.toString()),`Analysis function drift ${fn.name}`);
 analysisToolSourcePins.push({file,frozenFileSha256:sha(source),usedDefinitions:functions.map(fn=>({name:fn.name,sha256:sha(fn.toString()),exactInFrozenSource:true}))});
}
const frozenBase='a6f7423eb71e6e7cd173e705c2290e5408a1d077',frozenFile='supabase/repeatable/04092026_1952_candidate_expense_history_anchor_recovery_v1.sql';
const frozenResult=spawnSync('git',['show',`${frozenBase}:${frozenFile}`],{cwd:root,maxBuffer:1024*1024});assert.equal(frozenResult.status,0);
const inheritedKeys=new Set(specialConstructViolations(frozenResult.stdout.toString(),frozenFile).filter(x=>x.hazard==='DYNAMIC_SQL_UNCLASSIFIED').map(x=>x.key));
const grouped=new Map(),prefix='BPAY_NEXT_SQL_DYNAMIC_SQL_UNCLASSIFIED: ';
for(const diagnostic of census.diagnostics.filter(x=>x.startsWith(prefix))){const m=diagnostic.slice(prefix.length).match(/^(.*?) \((.*)\)$/);assert(m);const key=JSON.stringify([m[1],m[2]]);grouped.set(key,(grouped.get(key)??0)+1);}
const files=new Map(),bodies=new Map(),groups=[];
for(const [key,count]of grouped){const [file,signature]=JSON.parse(key);let source=files.get(file)?.source;
 if(source===undefined){const r=spawnSync('git',['show',`${commit}:${file}`],{cwd:root,maxBuffer:20*1024*1024});assert.equal(r.status,0);source=r.stdout.toString();assert.equal(fs.readFileSync(path.join(root,file),'utf8').replaceAll('\r\n','\n'),source);files.set(file,{file,sha256:sha(source),source});}
 const allPoints=specialConstructViolations(source,file).filter(x=>x.hazard==='DYNAMIC_SQL_UNCLASSIFIED'&&JSON.parse(x.key)[0]===signature);
 const inherited=allPoints.filter(x=>inheritedKeys.has(x.key)),points=allPoints.filter(x=>!inheritedKeys.has(x.key));
 const parts=executableParts(source,file).filter(x=>x.signature===signature);assert.equal(points.length,count,`${file} ${signature}`);
 const row={file,signature,fileSha256:sha(source),diagnostics:count,rawDynamicKeys:allPoints.length,inheritedFrozenKeys:inherited.map(x=>({key:x.key,keySha256:sha(x.key),frozenBase,frozenFile,frozenFileSha256:sha(frozenResult.stdout)})),points:[]};
 for(const point of points){const statement=JSON.parse(point.key)[2],matching=parts.filter(x=>x.text.includes(statement));assert.equal(matching.length,1);const body=matching[0].text.trim(),bodyHash=sha(body);
  const oldPoint=old.rows.flatMap(r=>r.points.map(p=>({file:r.file,signature:r.signature,...p}))).find(x=>x.file===file&&x.signature===signature&&x.statement_sha256===sha(statement));
  const counterpart=old.rows.flatMap(r=>r.points.map(p=>({file:r.file,signature:r.signature,...p}))).filter(x=>x.statement_sha256===sha(statement));
  const bodyFile=`bodies/${String(bodies.size+1).padStart(2,'0')}-${bodyHash.slice(0,12)}.sqlinc`;
  const bodyKey=file+'\0'+bodyHash;if(!bodies.has(bodyKey)){
   const full=scanManagedSql(source).statements.map(x=>source.slice(x.start,x.end)).filter(x=>executableParts(x,file).some(p=>p.signature===signature&&sha(p.text.trim())===bodyHash));assert.equal(full.length,1);
   const producerFile=bodyFile.replace('bodies/','producers/');bodies.set(bodyKey,{file,signature,sha256:bodyHash,bodyFile,producerFile,producerSha256:sha(full[0]),source:body});
   fs.mkdirSync(path.join(dir,'bodies'),{recursive:true});fs.mkdirSync(path.join(dir,'producers'),{recursive:true});fs.writeFileSync(path.join(dir,bodyFile),body);fs.writeFileSync(path.join(dir,producerFile),full[0]);
  }
  row.points.push({statementSha256:sha(statement),keySha256:sha(point.key),key:point.key,statement,bodySha256:bodyHash,bodyFile:bodies.get(bodyKey).bodyFile,
   executeTokens:scanManagedSql(statement).statements.flatMap(x=>x.tokens).filter(x=>x==='EXECUTE').length,
   priorExactPoint:!!oldPoint,priorExactBody:!!oldPoint&&oldPoint.enclosing_producer_body_sha256===bodyHash,
   counterpart:counterpart.map(x=>({file:x.file,signature:x.signature,statementSha256:x.statement_sha256,bodySha256:x.enclosing_producer_body_sha256}))});
 }
 groups.push(row);
}
const points=groups.flatMap(x=>x.points),result={version:'HEALTH_F23_CURRENT_DYNAMIC_EXTRACTION_V1',sourceCommit:commit,censusSha256:sha(censusBytes),priorReviewSha256:sha(oldBytes),counts:{diagnostics:points.length,fileSignatureGroups:groups.length,files:files.size,enclosingBodies:bodies.size,priorExactPoints:points.filter(x=>x.priorExactPoint).length,priorExactBodies:points.filter(x=>x.priorExactBody).length,newPoints:points.filter(x=>!x.priorExactPoint).length},
 files:[...files.values()].map(({source,...x})=>x),bodies:[...bodies.values()].map(({source,...x})=>x),groups,analysisToolSourcePins,native:'NOT_RUN',databaseCalls:0};
fs.writeFileSync(path.join(dir,'EXTRACTION.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result.counts));
for(const g of groups.filter(g=>g.points.some(x=>!x.priorExactPoint||!x.priorExactBody)))console.log(JSON.stringify({file:g.file,signature:g.signature,points:g.points.map(x=>({statementSha256:x.statementSha256,bodySha256:x.bodySha256,bodyFile:x.bodyFile,priorExactPoint:x.priorExactPoint,priorExactBody:x.priorExactBody,counterpart:x.counterpart}))}));
