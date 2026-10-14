import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath,pathToFileURL} from 'node:url';import {execFileSync} from 'node:child_process';
const dir=path.dirname(fileURLToPath(import.meta.url)),root=process.argv[2];if(!root)throw Error('READ_ONLY_ROOT_REQUIRED');
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),pins=JSON.parse(fs.readFileSync(path.join(dir,'CONTROLLING_INPUTS.json'),'utf8'));
const git=args=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,encoding:null,maxBuffer:16*1024*1024});
const prior='ab26a5ff22200be037d215b1100c969d6c0bae73';
const inputForward=pins.inputs.map(row=>{const saved=fs.readFileSync(path.join(root,row.path)),before=git(['show',prior+':'+row.path]),after=git(['show',pins.root_commit+':'+row.path]);
 if(sha(saved)!==row.sha256||!saved.equals(after)||!before.equals(after))throw Error('CONTROLLING_INPUT_PIN_FORWARD_CHANGED '+row.path);
 return {path:row.path,prior_git_sha256:sha(before),current_git_sha256:sha(after),saved_sha256:sha(saved),equal:true};});
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const lexical=fs.readdirSync(dir).filter(file=>file.endsWith('.sqlinc')).map(file=>{
 const parsed=scanManagedSql(fs.readFileSync(path.join(dir,file),'utf8'));
 return {file,statements:parsed.statements.length,meta:parsed.meta.length,status:'LEXICAL_ONLY_NO_PG_COMPILE'};});
fs.writeFileSync(path.join(dir,'METADATA_PIN_FORWARD_AND_LEXICAL_RECEIPT.json'),JSON.stringify({schema_version:'REPORT_AB_SOURCE_PIN_FORWARD_AND_LEXICAL_V1',
 prior_root_commit:prior,current_root_commit:pins.root_commit,inputs:inputForward,
 changed_tracked_paths:git(['diff','--name-only',prior,pins.root_commit]).toString('utf8').trim().split('\n').filter(Boolean),lexical,
 native_compile:'NOT_RUN',native_ab_financial:'NOT_RUN',off_prerequisite:'FINAL_SOURCE_PINS_SEPARATELY_MEASURED_ROOT_COMPOSITION_NATIVE_ACCEPTANCE_HELD',
 statement:'All 29 controlling saved/native/proposal inputs remain byte-identical across this input pin-forward; the complete tracked execution tree differs and requires genuine same-Install2 execution proof. Paired current native owners have separate complete reverse proofs.'},null,2)+'\n');
console.log(JSON.stringify({exact_inputs:inputForward.length,lexical_files:lexical.length,root_commit:pins.root_commit}));
