// Read-only root source discovery. Writes evidence only in this isolated proposal.
import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {pathToFileURL,fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';
const root=process.argv[2];
if(!root)throw Error('ROOT_REQUIRED');
const here=path.dirname(fileURLToPath(import.meta.url));
const sha=s=>createHash('sha256').update(s).digest('hex');
const censusPath='.codex-tmp/f23-current-source-review-20261016/CENSUS.json';
const censusBytes=fs.readFileSync(path.join(root,censusPath));
const census=JSON.parse(censusBytes);
const diagnostics=Object.values(census).find(v=>Array.isArray(v)&&v.some(x=>typeof x==='string'&&x.startsWith('BPAY_NEXT_SQL_AGGREGATE_TYPE_UNCLASSIFIED:')));
const selected=diagnostics.filter(x=>x.startsWith('BPAY_NEXT_SQL_AGGREGATE_TYPE_UNCLASSIFIED:'));
const expected=new Map();
for(const d of selected){const m=d.match(/^BPAY_NEXT_SQL_AGGREGATE_TYPE_UNCLASSIFIED: (.*?) \((.*)\)$/);if(!m)throw Error(d);const k=JSON.stringify([m[1],m[2]]);expected.set(k,(expected.get(k)??0)+1);}
const {specialConstructViolations}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-source-gates.mjs')));
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const {routineDefinitions}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-final-definers.mjs')));
const files=[...new Set([...expected.keys()].map(k=>JSON.parse(k)[0]))];
const rows=[],countChanges=[];
for(const file of files){
 const bytes=fs.readFileSync(path.join(root,file)),source=bytes.toString('utf8').replaceAll('\r\n','\n');
 const base=spawnSync('git',['-c','maintenance.auto=false','-c','gc.auto=0','show','a6f7423eb71e6e7cd173e705c2290e5408a1d077:'+file],{cwd:root,encoding:'utf8'});
 const retained=new Map();
 if(base.status===0)for(const r of specialConstructViolations(base.stdout,file))retained.set(r.key,(retained.get(r.key)??0)+1);
 const actual=specialConstructViolations(source,file).filter(x=>x.hazard==='AGGREGATE_TYPE_UNCLASSIFIED').filter(r=>{const n=retained.get(r.key)??0;if(n){retained.set(r.key,n-1);return false;}return true;});
 const seen=new Map();
 for(const row of actual){const [signature,hazard,statement]=JSON.parse(row.key),k=JSON.stringify([file,signature]);if(!expected.has(k))continue;seen.set(k,(seen.get(k)??0)+1);
 const definition=scanManagedSql(source).statements.map(s=>source.slice(s.start,s.end).trim()).find(s=>signature.startsWith('outer:')?s.includes(statement):routineDefinitions(s,file).includes(signature));
 if(!definition)throw Error('DEFINITION_MISSING '+signature);
 rows.push({id:rows.length+1,file,signature,hazard,sourceSha256:sha(source),sourceBytesSha256:sha(bytes),routineStatementSha256:sha(definition),statementSha256:sha(statement),keySha256:sha(row.key),key:row.key,statement});}
 for(const [k,n]of expected)if(JSON.parse(k)[0]===file&&(seen.get(k)??0)!==n)countChanges.push({file,signature:JSON.parse(k)[1],census:n,current:seen.get(k)??0});
}
const out={version:'HEALTHROSTER_AGGREGATE_SOURCE_DISCOVERY_V1',qualification:'READ_ONLY_SOURCE_REVIEW_ONLY',native:'NOT_RUN',censusPath,censusSha256:sha(censusBytes),censusSourceCommit:census.sourceCommit,censusCount:selected.length,count:rows.length,countChanges,rows};
fs.writeFileSync(path.join(here,'DISCOVERY.json'),JSON.stringify(out,null,2)+'\n');
fs.writeFileSync(path.join(here,'STATEMENTS.txt'),rows.map(r=>`[${r.id}] ${r.file}\n${r.signature}\n${r.statement}\n`).join('\n'));
console.log(JSON.stringify({count:rows.length,files:files.length,sha256:sha(JSON.stringify(out,null,2)+'\n')}));
