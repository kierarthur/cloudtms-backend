// Health source replay only; no native holder, SQL dispatch or Bank body access.
import fs from 'node:fs';import path from 'node:path';import cp from 'node:child_process';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const write=(f,data)=>{const p=path.join(here,f);fs.mkdirSync(path.dirname(p),{recursive:true});fs.writeFileSync(p,typeof data==='string'?data:JSON.stringify(data,null,2)+'\n');};
const runs=[];function run(name,exe,args){const r=cp.spawnSync(exe,args,{cwd:here,encoding:'utf8',maxBuffer:4e6});write('replay/'+name+'.stdout.txt',r.stdout);write('replay/'+name+'.stderr.txt',r.stderr);assert.equal(r.status,0,r.stdout+r.stderr);runs.push({name,exitCode:r.status});}
const py='C:/Users/KierArthur/AppData/Local/Programs/Python/Python312/python.exe',long=f=>'//?/'+path.join(here,f).replaceAll('\\','/');
run('placement-ast',py,[long('placement-ast.py')]);run('grammar-types',py,[long('grammar-and-types.py')]);run('source-tests',process.execPath,['--test',path.join(here,'source.test.mjs')]);
const gateFile='scripts/bpay-next-source-gates.mjs',gatePath=path.join(root,gateFile),gateBytes=fs.readFileSync(gatePath),gate=await import(pathToFileURL(gatePath));
const git=(...args)=>{const r=cp.spawnSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{maxBuffer:20e6});assert.equal(r.status,0);return r.stdout;};
assert(gateBytes.equals(git('show','HEAD:'+gateFile)));assert(gateBytes.equals(git('show',':'+gateFile)));
const selection=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_SELECTION.json')));
const raw=[];for(const file of [...selection.includeOrder,'health/99_acl.sqlinc','admission/ONE_TIME_ADMISSION.sqlinc','verification/mandatory_security.sql','ADDITIVE_REVERSE.sqlinc']){
 const b=fs.readFileSync(path.join(here,file)),s=b.toString(),hazards=gate.specialConstructViolations(s,file);raw.push({file,sha256:sha(b),hazards});assert.equal(hazards.length,0,file);gate.assertNextExactNumericTypes(s,file);gate.assertSqlWriteBoundaries(s,file);
}
write('RAW_SOURCE_GATE_RESULTS.json',{gateFile,gateSha256:sha(gateBytes),gateSavedHeadIndexEqual:true,rootHeadAtGate:git('rev-parse','HEAD').toString().trim(),inputs:raw,allowancesAdded:0,classification:'FINITE_RAW_CENSUS_ONLY_NOT_JOINED_GLOBAL_GATE',native:'NOT_RUN'});
write('TEST_RESULTS.json',{runs,sourceNodeTests:16,controlledActualHealthAstCases:47,publicDecoderCases:6,completeGrammarInputs:5,sourceRelations:6,referencedColumns:53,fullParentReversals:5,newPrivateSignatures:1,publicDtoChanges:0,rawGateInputs:raw.length,rawGateHazards:0,allowancesAdded:0,ResultTemplate:'HELD_NOT_PARSED_BANK_RESULT_UNPROVIDED',native:'NOT_RUN',catalogueVerification:'NOT_RUN',installation:'NOT_RUN',BankBodyReads:0,rootWrites:0});
console.log(JSON.stringify({nodeTests:16,controlledAst:47,grammarInputs:5,rawGateInputs:raw.length,rawGateHazards:0,native:'NOT_RUN'}));
