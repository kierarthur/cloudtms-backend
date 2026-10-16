import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];
if(!root)throw Error('ROOT_REQUIRED');
const sha=x=>createHash('sha256').update(x).digest('hex');
const git=args=>execFileSync('git',args,{cwd:root,maxBuffer:128*1024*1024});
const raw=JSON.parse(fs.readFileSync(path.join(here,'RAW_CENSUS.json')));
const base='a6f7423eb71e6e7cd173e705c2290e5408a1d077';
const names=[...new Set(raw.rows.map(r=>r.signature).filter(s=>!s.startsWith('outer:')).map(s=>s.slice(0,s.indexOf('('))))];
const {specialConstructViolations,executableParts}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-source-gates.mjs')));
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const {routineDefinitions}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-final-definers.mjs')));
const files=git(['grep','-i','-l','-F',...names.flatMap(n=>['-e',n]),base,'--','supabase/migrations/*.sql','supabase/repeatable/*.sql','supabase/verification/*.sql']).toString('utf8').trim().split(/\r?\n/).filter(Boolean).map(x=>x.slice(base.length+1));
const baseline=new Map(),oldFunctions=[];
for(const file of files){
 const source=git(['show',`${base}:${file}`]).toString('utf8'),counts=new Map();
 for(const h of specialConstructViolations(source,file)){if(raw.rows.some(r=>r.key===h.key))counts.set(h.key,(counts.get(h.key)??0)+1);}
 for(const[key,count]of counts){const prior=baseline.get(key)??{maxPerFile:0,files:[]};prior.maxPerFile=Math.max(prior.maxPerFile,count);prior.files.push({file,sourceSha256:sha(source),count});baseline.set(key,prior);}
 for(const row of scanManagedSql(source).statements){const sql=source.slice(row.start,row.end).trim();for(const signature of routineDefinitions(sql,file))if(raw.rows.some(r=>r.signature===signature)){const part=executableParts(sql,file).find(x=>x.signature===signature);oldFunctions.push({file,sourceSha256:sha(source),signature,completeStatementSha256:sha(sql),bodySha256:part?sha(part.text.trim()):null});}}
}
const reviews=[
 'docs/healthroster-build/v6-owner-delivery-20261016/ROWTYPE_TUPLE_TYPE_REVIEW.json',
 'docs/healthroster-build/v6-owner-delivery-20261016/AGGREGATE_TYPE_CLASSIFICATION.json',
 'docs/healthroster-build/v6-owner-delivery-20261016/UUID_CARDINALITY_CLASSIFICATION.json',
 'docs/healthroster-build/v6-owner-delivery-20261016/F23_JOINED_NON_DYNAMIC_REVIEW_20261016/EXTENSION_MAP.json',
];
const reviewed=[];
for(const file of reviews){if(!fs.existsSync(path.join(root,file)))continue;const bytes=fs.readFileSync(path.join(root,file)),data=JSON.parse(bytes);const walk=(o,at)=>{if(!o||typeof o!=='object')return;if(o.keySha256||o.statementSha256)reviewed.push({file,reviewSha256:sha(bytes),jsonPath:at,keySha256:o.keySha256??null,statementSha256:o.statementSha256??null,signature:o.signature??null});for(const[k,v]of Object.entries(o))if(v&&typeof v==='object')walk(v,at+'.'+k);};walk(data,'$');}
const consumed=new Map();
const rows=raw.rows.map(r=>{const perFileKey=r.file+'\0'+r.key,n=(consumed.get(perFileKey)??0)+1;consumed.set(perFileKey,n);const b=baseline.get(r.key);return{id:r.id,keySha256:r.keySha256,baselineMaxPerFile:b?.maxPerFile??0,baselineOccurrenceConsumed:n,baselineCovered:n<=(b?.maxPerFile??0),baselineMatches:b?.files??[],oldReviewedExactKeys:reviewed.filter(x=>x.keySha256===r.keySha256),oldReviewedExactStatements:reviewed.filter(x=>x.statementSha256===r.statementSha256),oldCompleteFunctions:oldFunctions.filter(x=>x.signature===r.signature).map(x=>({...x,completeStatementEqual:x.completeStatementSha256===r.completeStatementSha256,bodyEqual:x.bodySha256===r.bodySha256}))};});
const out={version:'SOURCE13_EXACT_COUNTERPART_COMPARISON_V1',qualification:'PROVENANCE_AND_BASELINE_COUNTS_ONLY_NO_NEW_BEHAVIORAL_APPROVAL',base,baselineSearch:'Case-insensitive fixed-name Git grep selects every .sql file in the exact gate baseline directories that can contain a target signature; each selected complete file is scanned with the unchanged gate. Outer keys contain the new physical path and cannot equal an old path.',baselineFilesScanned:files.length,baselineFiles:files,counts:{raw:rows.length,baselineCovered:rows.filter(r=>r.baselineCovered).length,newFinitePoints:rows.filter(r=>!r.baselineCovered).length,exactOldReviewedKeys:rows.filter(r=>r.oldReviewedExactKeys.length).length,completeOldFunctionEqual:rows.filter(r=>r.oldCompleteFunctions.some(f=>f.completeStatementEqual)).length},rows,native:'NOT_RUN'};
fs.writeFileSync(path.join(here,'COUNTERPARTS.json'),JSON.stringify(out,null,2)+'\n');
console.log(JSON.stringify({counts:out.counts,files:files.length,sha256:sha(fs.readFileSync(path.join(here,'COUNTERPARTS.json')))},null,2));
