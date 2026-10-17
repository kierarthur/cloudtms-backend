// No source preflight or actual runtime, private path, state, browser, IPC or native access.
import fs from 'node:fs';import {diagnostic,hash} from './recipe.mjs';
const here=new URL('./',import.meta.url),manifest=JSON.parse(fs.readFileSync(new URL('MANIFEST.json',here)));
for(const r of manifest.files){const b=fs.readFileSync(new URL(r.file,here));if(b.length!==r.bytes||hash(b)!==r.sha256)throw Error('R7_FROZEN_SOURCE_CHANGED');}
for(const r of JSON.parse(fs.readFileSync(new URL('SOURCE_PINS.json',here)))){const b=fs.readFileSync(new URL(r.capture,here));if(b.length!==r.bytes||hash(b)!==r.sha256)throw Error('R7_PARENT_CAPTURE_CHANGED');}
const d=diagnostic();if(!fs.readFileSync(new URL('payload/network.mjs',here)).equals(Buffer.from(d.text)))throw Error('R7_DIAGNOSTIC_PAYLOAD_CHANGED');
console.log(JSON.stringify({status:'PASS',files:manifest.files.length+1,parents:10,diagnosticHunks:1,futureR7:'NOT_PROVIDED',native:'NOT_RUN',privateStateAccess:'NONE'}));
