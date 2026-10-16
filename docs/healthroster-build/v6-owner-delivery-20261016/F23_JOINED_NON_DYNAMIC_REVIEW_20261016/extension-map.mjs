import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const root=process.argv[2],here=path.dirname(fileURLToPath(import.meta.url));
if(!root)throw Error('ROOT_REQUIRED');
const commit='c1ebb691feb6a11324060e0c28210e2bd735624a',sha=x=>createHash('sha256').update(x).digest('hex');
const frozen=file=>execFileSync('git',['show',`${commit}:${file}`],{cwd:root,maxBuffer:30_000_000}).toString('utf8');
const read=file=>JSON.parse(fs.readFileSync(path.join(here,file)));
const D=read('DISCOVERY.json'),R=read('REVIEW.json'),P=read('PROVENANCE.json'),C=read('CORRECTION_RECEIPT.json');
const {specialConstructViolations,executableParts}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-source-gates.mjs')));
const proofRows=[...R.tupleRows,...R.aggregateRows,...R.uuidRows];
if(proofRows.length!==37||R.jsonRows.length!==1)throw Error('EXPECTED_FINITE_REVIEW_COUNTS');
const files=D.filePins.map(f=>{const text=frozen(f.file);if(sha(text)!==f.sourceSha256)throw Error('SOURCE_DRIFT');let corrected=text;if(f.file===C.target){if(text.split(C.exactReplacement.before).length!==2)throw Error('CORRECTION_COUNT');corrected=text.replace(C.exactReplacement.before,C.exactReplacement.after);if(sha(corrected)!==C.target_after_exact_replacement_sha256)throw Error('AFTER_CORRECTION_DRIFT');}return {path:f.file,originalSha256:sha(text),afterRequiredCorrectionSha256:sha(corrected),correctionRequired:f.file===C.target,sourceBytes:f.sourceBytes};});
const points=new Map();
for(const r of proofRows){const id=r.file+'\0'+r.key;const p=points.get(id)??{file:r.file,signature:r.signature,hazard:r.hazard,key:r.key,keySha256:r.keySha256,statementSha256:r.statementSha256,completeFunctionSha256:r.routineSha256,bodySha256:r.bodySha256,newDiagnosticCount:0,reviewIds:[],reviewVerdict:r.proof.verdict};p.newDiagnosticCount++;p.reviewIds.push(r.id);points.set(id,p);}
for(const point of points.values()){
 const full=specialConstructViolations(frozen(point.file),point.file).filter(r=>r.key===point.key).length;
 if(full<point.newDiagnosticCount)throw Error('POINT_COUNT_DRIFT');point.wholeSourceOccurrenceCount=full;point.exactFrozenBaselineConsumedCount=full-point.newDiagnosticCount;
 const body=executableParts(frozen(point.file),point.file).filter(r=>r.signature===point.signature);if(body.length!==1||sha(body[0].text.trim())!==point.bodySha256)throw Error('BODY_DRIFT');
 if(!P.functions.some(f=>f.signature===point.signature&&f.routineSha256===point.completeFunctionSha256&&f.producer.completeFunctionByteEqual))throw Error('PROVENANCE_MISSING');
}
if(points.size!==37)throw Error('EXPECTED_37_UNIQUE_POINTS');
const report={version:'F23_JOINED_NON_DYNAMIC_FINITE_EXTENSION_MAP_V1',qualification:'SOURCE_REVIEWED_EXTENSION_INPUT_NOT_ALREADY_ADMITTED',sourceCommit:commit,artifacts:['DISCOVERY.json','REVIEW.json','PROVENANCE.json','CORRECTION_RECEIPT.json'].map(p=>({path:p,sha256:sha(fs.readFileSync(path.join(here,p)))})),filePins:files,completeFunctionPins:D.functions,points:[...points.values()].sort((a,b)=>a.reviewIds[0]-b.reviewIds[0]),excluded:[{reviewId:23,hazard:'JSON_CONCAT',reason:'Apply exact complete-function grouping correction first; no JSON allowance proposed.'}],integrationRules:['Keep every generic source fence unchanged.','Require exact file, complete function, body, key, statement and occurrence count pins; any change needs fresh finite review.','Original file pins identify review provenance; afterRequiredCorrectionSha256 pins the exact intended corrected joined file.','Only the enumerated37points are eligible; do not admit all statements in the named functions.','Baseline-consumed occurrences remain governed by the existing exact frozen baseline, not this extension.','Types/cardinality only: source ownership, native currentness, permissions and business behavior need genuine native acceptance.'],native:'NOT_RUN'};
fs.writeFileSync(path.join(here,'EXTENSION_MAP.json'),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify({points:points.size,newDiagnostics:report.points.reduce((n,p)=>n+p.newDiagnosticCount,0),sha256:sha(fs.readFileSync(path.join(here,'EXTENSION_MAP.json')))},null,2));
