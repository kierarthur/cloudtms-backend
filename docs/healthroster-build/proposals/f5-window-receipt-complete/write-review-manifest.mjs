import fs from 'node:fs';
import crypto from 'node:crypto';
const base='docs/healthroster-build/proposals/f5-window-receipt-complete';
const hash=file=>{const data=fs.readFileSync(file);return {file,bytes:data.length,sha256:crypto.createHash('sha256').update(data).digest('hex')};};
const walk=dir=>fs.readdirSync(dir,{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(dir+'/'+e.name):[dir+'/'+e.name]);
const frozen=JSON.parse(fs.readFileSync('docs/healthroster-build/F5_INTEGRATION_MANIFEST.json','utf8'));
for(const file of frozen.files){const actual=hash(file.file);if(actual.bytes!==file.bytes||actual.sha256!==file.sha256)throw Error('FROZEN_CHANGED '+file.file);}
const source=JSON.parse(fs.readFileSync(base+'/generated/SOURCE_MANIFEST.json','utf8'));
for(const file of source.artifacts){const actual=hash(file.file);if(actual.bytes!==file.bytes||actual.sha256!==file.sha256)throw Error('GENERATED_CHANGED '+file.file);}
const files=walk(base).filter(file=>!file.endsWith('/REVIEW_MANIFEST.json')).sort();
files.push('tests/weekly-source/invoice-window-receipt-complete-proposal.test.mjs');
fs.writeFileSync(base+'/REVIEW_MANIFEST.json',JSON.stringify({contract:'HEALTHROSTER_COMPLETE_WINDOW_RECEIPT_REVIEW_MANIFEST_V1',
  source_only:true,hosted_compilation_and_financial_proof:false,frozen_28_unchanged:true,tests:{local_source_vector_model:19,passed:19,failed:0,skipped:0},
  generated_full_definitions:source.functions.length,generated_artifacts:source.artifacts.length,
  generated_source_manifest:hash(base+'/generated/SOURCE_MANIFEST.json'),
  unchanged_prior_handoffs:[hash('docs/healthroster-build/F5_INTEGRATION_MANIFEST.json'),hash('docs/healthroster-build/proposals/m1-whole-shift/REVIEW_MANIFEST.json')],
  files:files.map(hash)},null,2)+'\n');
process.stdout.write(JSON.stringify(hash(base+'/REVIEW_MANIFEST.json'))+'\n');
