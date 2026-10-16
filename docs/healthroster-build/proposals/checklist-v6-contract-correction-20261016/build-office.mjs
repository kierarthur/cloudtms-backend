import {readFileSync,writeFileSync,mkdirSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
const root=path.dirname(fileURLToPath(import.meta.url)),source=readFileSync(path.join(root,'forward/contracts/contract-correction-v3.mjs'),'utf8');
const names=[...source.matchAll(/export\s+(?:const|function)\s+(\w+)/g)].map(m=>m[1]);
const sha=createHash('sha256').update(source).digest('hex');
const output=`// Generated from sole Health closed DTO source SHA256 ${sha}. Do not edit.\n(function(root){'use strict';\n${source.replace(/export\s+(?=(?:const|function)\b)/g,'')}\nroot.CloudTMSCorrectionContractV3=Object.freeze({${names.join(',')}});\n})(typeof window!=='undefined'?window:globalThis);\n`;
const target=path.join(root,'forward/office/js/correction-contract-v3.js');
if(process.argv.includes('--check')){if(readFileSync(target,'utf8')!==output)throw Error('Generated Office DTO differs from canonical source');}
else{mkdirSync(path.dirname(target),{recursive:true});writeFileSync(target,output);}
console.log('Office closed DTO matches '+sha);
