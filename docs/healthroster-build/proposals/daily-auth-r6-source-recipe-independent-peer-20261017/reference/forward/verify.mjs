// Public frozen recipe and payloads only. Does not call any runtime/source preflight.
import fs from 'node:fs';import {operator,hash} from './recipe.mjs';
const here=new URL('./',import.meta.url),manifest=JSON.parse(fs.readFileSync(new URL('MANIFEST.json',here)));
for(const r of manifest.files){const b=fs.readFileSync(new URL(r.file,here));if(b.length!==r.bytes||hash(b)!==r.sha256)throw Error('R6_FROZEN_RECIPE_CHANGED');}
const op=operator();for(const t of op.targets)if(!fs.readFileSync(new URL('payload/operator/'+t.file,here)).equals(Buffer.from(t.text)))throw Error('R6_OPERATOR_PAYLOAD_CHANGED');
console.log(JSON.stringify({status:'PASS',files:manifest.files.length+1,operatorTargets:3,holderTargets:2,coordinatorTargets:2,privateStateAccess:'NONE',native:'NOT_RUN'}));
