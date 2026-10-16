import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';

// One closed decoder owner: generate the Office UMD from the actual Worker
// contract, without introducing another independently maintained field list.
export function officeContactContract(source){
 const scalars=source.slice(source.indexOf('const UUID ='),source.indexOf('export function isContactPauseCommand'));
 const decoder=source.slice(source.indexOf('function audience('),source.indexOf('export async function handleContactPauseOfficeCommand'));
 assert(scalars.startsWith('const UUID =')&&scalars.includes('const unavailable ='));
 assert(decoder.startsWith('function audience(')&&decoder.includes('export function decodeContactPauseReply'));
 const hash=createHash('sha256').update(source).digest('hex');
 return `// Generated from Health contact-pause-office.mjs SHA256 ${hash}.\n`+
  `(function(root){'use strict';\n${scalars}\n${decoder.replace('export function decodeContactPauseReply','function decodeContactPauseReply')}\n`+
  `const api=Object.freeze({decode:decodeContactPauseReply});\n`+
  `if(typeof module==='object'&&module.exports)module.exports=api;\nroot.CloudTMSContactPauseContractV1=api;\n`+
  `})(typeof window==='object'?window:globalThis);\n`;
}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const args=process.argv.slice(2),write=args.includes('--write');
 assert(args.every(x=>x==='--write'||x.startsWith('--office-root=')),'unknown composer option');
 const officeRoot=args.find(x=>x.startsWith('--office-root='))?.slice('--office-root='.length);
 assert(officeRoot&&path.isAbsolute(officeRoot),'exact Office checkout required');
 const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
 const source=fs.readFileSync(path.join(root,'broker/src/weekly-source/contact-pause-office.mjs'),'utf8');
 const result=officeContactContract(source),target=path.join(officeRoot,'js/weekly-source/contact-pause-contract.js');
 if(write)fs.writeFileSync(target,result);else assert.equal(fs.readFileSync(target,'utf8'),result,'regenerate exact current decoder');
 console.log(JSON.stringify({target,sha256:createHash('sha256').update(result).digest('hex'),write,source_only:true}));
}
