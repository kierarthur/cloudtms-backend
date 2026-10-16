import fs from 'node:fs';
import path from 'node:path';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath,pathToFileURL} from 'node:url';
const root=process.argv[2];if(!root)throw Error('ROOT_REQUIRED');
const here=path.dirname(fileURLToPath(import.meta.url));
const commit='c1ebb691feb6a11324060e0c28210e2bd735624a';
const sha=x=>createHash('sha256').update(x).digest('hex');
const frozen=file=>execFileSync('git',['show',`${commit}:${file}`],{cwd:root,maxBuffer:20_000_000}).toString('utf8');
const {specialConstructViolations,executableParts}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-source-gates.mjs')));
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const {routineDefinitions}=await import(pathToFileURL(path.join(root,'scripts/bpay-next-final-definers.mjs')));
const censusPath='.codex-tmp/f23-current-source-review-20261016/CENSUS.json';
const censusBytes=fs.readFileSync(path.join(root,censusPath)),census=JSON.parse(censusBytes);
if(census.sourceCommit!==commit||census.diagnosticSha256!=='79bb8909d774f93551b3f53b6afa3d09176a9eb4d264cd9337e44e82649ca8d1')throw Error('CENSUS_CHANGED');
const selected=census.diagnostics.filter(x=>!x.startsWith('BPAY_NEXT_SQL_DYNAMIC_SQL_UNCLASSIFIED:'));
if(selected.length!==38)throw Error('EXPECTED_38');
const expected=new Map();
for(const d of selected){const m=d.match(/^BPAY_NEXT_SQL_([^:]+(?::[^:]+:[^:]+)?): (.*?) \((.*)\)$/);if(!m)throw Error(d);const key=JSON.stringify([m[2],m[3],m[1]]);expected.set(key,(expected.get(key)??0)+1);}
const files=[...new Set([...expected.keys()].map(x=>JSON.parse(x)[0]))];
const frozenBase='a6f7423eb71e6e7cd173e705c2290e5408a1d077';
const names=[...new Set([...expected.keys()].map(x=>JSON.parse(x)[1].split('(')[0]))];
const namePattern=names.map(x=>x.replace(/[.*+?^${}()|[\]\\]/g,'\\$&')).join('|');
const oldNames=execFileSync('git',['grep','-l','-i','-E',namePattern,frozenBase,'--','supabase/migrations','supabase/repeatable','supabase/verification'],{cwd:root,encoding:'utf8'}).trim().split(/\r?\n/).map(x=>x.slice(frozenBase.length+1)).filter(x=>x.endsWith('.sql'));
const baseline=new Map();
for(const file of oldNames){const source=execFileSync('git',['show',`${frozenBase}:${file}`],{cwd:root,maxBuffer:20_000_000}).toString('utf8'),counts=new Map();for(const row of specialConstructViolations(source,file))counts.set(row.key,(counts.get(row.key)??0)+1);for(const [key,count]of counts)baseline.set(key,Math.max(baseline.get(key)??0,count));}
const prior={rowtype:JSON.parse(frozen('docs/healthroster-build/v6-owner-delivery-20261016/ROWTYPE_TUPLE_TYPE_REVIEW.json')),aggregate:JSON.parse(frozen('docs/healthroster-build/v6-owner-delivery-20261016/F23_AGGREGATE_SOURCE_REVIEW_20261016/AGGREGATE_REVIEW.json')),uuid:JSON.parse(frozen('docs/healthroster-build/v6-owner-delivery-20261016/F23_UUID_CARDINALITY_REVIEW.json'))};
const rowMatches=key=>prior.rowtype.files.flatMap(f=>f.rows.filter(r=>r.keySha256===key).map(r=>({file:f.path,sourceSha256:f.sha256,...r})));
const aggMatches=key=>prior.aggregate.rows.filter(r=>r.keySha256===key);
const uuidMatches=key=>prior.uuid.rows.flatMap(r=>r.violations.filter(v=>v.key_sha256===key).map(v=>({file:r.file,sourceSha256:r.frozen_file_sha256,bodySha256:r.frozen_body_sha256,qualification:r.qualification,evidence:r.evidence,...v})));
const rows=[],functions=[],filePins=[],seen=new Map();
for(const file of files){
 const source=frozen(file),saved=fs.readFileSync(path.join(root,file),'utf8');if(saved!==source)throw Error('SAVED_SOURCE_DRIFT '+file);
 const parts=executableParts(source,file),statements=scanManagedSql(source).statements;
 filePins.push({file,sourceSha256:sha(source),sourceBytes:Buffer.byteLength(source),savedDiskEqualsFrozen:true});
 const defs=new Map();
 for(const statement of statements){const text=source.slice(statement.start,statement.end).trim();for(const signature of routineDefinitions(text,file))defs.set(signature,{text,line:source.slice(0,statement.start).split('\n').length});}
 const used=new Set();
 const remaining=new Map(baseline);
 for(const row of specialConstructViolations(source,file).filter(r=>r.hazard!=='DYNAMIC_SQL_UNCLASSIFIED')){
  const priorCount=remaining.get(row.key)??0;if(priorCount){remaining.set(row.key,priorCount-1);continue;}
  const [signature,hazard,statement]=JSON.parse(row.key),expectedKey=JSON.stringify([file,signature,hazard]);if(!expected.has(expectedKey))continue;
  seen.set(expectedKey,(seen.get(expectedKey)??0)+1);const part=parts.filter(p=>p.signature===signature);if(part.length!==1)throw Error('PART_COUNT '+signature);
  const definition=defs.get(signature);if(!definition)throw Error('DEFINITION_MISSING '+signature);
  if(!used.has(signature)){functions.push({file,signature,line:definition.line,routineSha256:sha(definition.text),bodySha256:sha(part[0].text.trim()),completeSql:definition.text});used.add(signature);}
  const keySha256=sha(row.key);rows.push({id:rows.length+1,file,signature,hazard,key:row.key,keySha256,statement,statementSha256:sha(statement),sourceSha256:sha(source),routineSha256:sha(definition.text),bodySha256:sha(part[0].text.trim()),functionLine:definition.line,priorExactKeyMatches:hazard.startsWith('ROWTYPE_SELECT:')?rowMatches(keySha256):hazard==='AGGREGATE_TYPE_UNCLASSIFIED'?aggMatches(keySha256):hazard==='UUID_CARDINALITY_UNCLASSIFIED'?uuidMatches(keySha256):[]});
 }
}
for(const [key,count]of expected)if(seen.get(key)!==count)throw Error('COUNT_CHANGED '+key+' '+count+' '+seen.get(key));
if(rows.length!==38)throw Error('DISCOVERY_NOT_38');
const output={version:'JOINED_INVOICE_CANDIDATE_NON_DYNAMIC_DISCOVERY_V1',qualification:'SOURCE_REVIEW_INPUT_NOT_GATE_ADMISSION',sourceCommit:commit,censusPath,censusSha256:sha(censusBytes),diagnosticSha256:census.diagnosticSha256,frozenBaseline:{commit:frozenBase,matchedLegacySqlFiles:oldNames.length,rule:'Consume only exact same signature/hazard/statement baseline counts; no new allowance'},filePins,counts:Object.fromEntries([...new Set(rows.map(r=>r.hazard.split(':')[0]))].map(k=>[k,rows.filter(r=>r.hazard.split(':')[0]===k).length])),functions: functions.map(({completeSql,...r})=>r),rows,native:'NOT_RUN'};
fs.writeFileSync(path.join(here,'DISCOVERY.json'),JSON.stringify(output,null,2)+'\n');
const tmp=path.join(here,'../../../../.codex-tmp/f23-joined-nondynamic');fs.mkdirSync(tmp,{recursive:true});
for(const [n,f]of functions.entries())fs.writeFileSync(path.join(tmp,`${String(n+1).padStart(2,'0')}.sqlinc`),f.completeSql+'\n');
fs.writeFileSync(path.join(tmp,'FUNCTIONS.json'),JSON.stringify(functions.map(({completeSql,...r},n)=>({number:n+1,...r})),null,2)+'\n');
console.log(JSON.stringify({rows:rows.length,functions:functions.length,counts:output.counts,matched:rows.filter(r=>r.priorExactKeyMatches.length).length,discoverySha256:sha(fs.readFileSync(path.join(here,'DISCOVERY.json')))},null,2));
