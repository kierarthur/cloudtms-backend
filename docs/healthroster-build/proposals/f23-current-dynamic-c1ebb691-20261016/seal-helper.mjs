import fs from'node:fs';import path from'node:path';import{createHash}from'node:crypto';import{fileURLToPath}from'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url)),hash=x=>createHash('sha256').update(x).digest('hex');
const source=fs.readFileSync(path.join(dir,'helper.template.mjs'),'utf8');if(source.split('__APPROVAL_SHA256__').length!==2)throw Error('Unique approval pin required');
fs.writeFileSync(path.join(dir,'healthroster-dynamic-source-review.mjs'),source.replace('__APPROVAL_SHA256__',hash(fs.readFileSync(path.join(dir,'CLASSIFICATION.json')))));
