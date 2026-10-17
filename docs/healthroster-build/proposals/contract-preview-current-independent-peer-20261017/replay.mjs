// Independent replay of frozen author bytes with operational path changes only.
import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {execFileSync,spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {here,repo,get,sha,write} from './capture.mjs';
const source=path.join(here,'reference'),run=path.join(here,'replay'),i2=path.resolve(repo,'../healthroster-install2-14102026');
fs.mkdirSync(run,{recursive:true});
const adapt=(file,edits)=>{
 const raw=fs.readFileSync(path.join(source,file));let text=raw.toString('utf8');
 const rows=[];
 for(const [before,after] of edits){assert.equal(text.split(before).length-1,1,file+': '+before);text=text.replace(before,()=>after);rows.push({before,after,count:1});}
 fs.writeFileSync(path.join(run,file),text);
 return {file,inputSha256:sha(raw),operationalOutputSha256:sha(Buffer.from(text)),edits:rows};
};
const py=x=>JSON.stringify(x.replaceAll('\\','/'));
const common=[['HERE = Path(__file__).resolve().parent',`HERE = Path(${py(source)})`],['OWN = HERE.parents[3]',`OWN = Path(${py(repo)})`], ["ROOT = OWN.parent / 'healthroster-install2-14102026'",`ROOT = Path(${py(i2)})`]];
const edits=[adapt('assemble.py',common),adapt('offline-checks.py',[...common,
 ["(HERE / name).write_bytes(",`(Path(${py(run)}) / name).write_bytes(`],
 ["spec_from_file_location('health_contract_assembly', HERE / 'assemble.py')",`spec_from_file_location('health_contract_assembly', Path(${py(run)}) / 'assemble.py')`],
 ["str(HERE / 'assemble.py')",`str(Path(${py(run)}) / 'assemble.py')`],
 ["str(HERE / 'protocol-vectors.mjs')",`str(Path(${py(run)}) / 'protocol-vectors.mjs')`],
 ["'tests/contract-correction-v3.test.mjs'], cwd=ROOT",`'tests/contract-correction-v3.test.mjs'], cwd=Path(${py(path.join(run,'current'))})`]
 ]),adapt('protocol-vectors.mjs',[["const here=path.dirname(fileURLToPath(import.meta.url));",`const here=${JSON.stringify(run)};`],["const root=path.resolve(here,'../../../../../healthroster-install2-14102026');",`const root=${JSON.stringify(path.join(run,'current'))};`]])];
// Actual test and imported product module: exact 84738 Git bytes, own-WT only.
const current=[];
for(const p of ['tests/contract-correction-v3.test.mjs','broker/src/contract-correction-v3.mjs']){
 const raw=get('84738fe4057d9d693ccd723eb1835106f1091f57',p),target=path.join(run,'current',p);
 fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,raw);current.push({path:p,sha256:sha(raw),bytes:raw.length});
}
write('REPLAY_OPERATIONAL_EDITS.json',{scope:'PEER_HARNESS_PATHS_AND_RESULT_DESTINATION_ONLY',edits,currentProductInputs:current,productEdits:0,referenceWrites:0});
const result=spawnSync('python',[path.join(run,'offline-checks.py')],{cwd:repo,encoding:'utf8',windowsHide:true,maxBuffer:64000000});
fs.writeFileSync(path.join(run,'stdout.txt'),result.stdout??'');fs.writeFileSync(path.join(run,'stderr.txt'),result.stderr??'');
assert.equal(result.status,0,result.stderr);
console.log(result.stdout.trim());
const capture=JSON.parse(fs.readFileSync(path.join(here,'CAPTURE.json')));
for(const row of capture.files)assert.equal(sha(fs.readFileSync(path.join(source,row.file))),row.sha256,row.file);
console.log('Frozen reference '+capture.files.length+' files remain exact; root writes 0; native NOT_RUN.');
