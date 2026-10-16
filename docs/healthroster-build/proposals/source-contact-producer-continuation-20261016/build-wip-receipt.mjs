import {readFileSync,writeFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),sha=value=>createHash('sha256').update(value).digest('hex');
const checkpoint=JSON.parse(readFileSync(path.join(base,'READ_SOURCE_CHECKPOINT.json'),'utf8'));
const frozen={'runtime/contact-manual-enquiry.mjs':'cbec35660eab50592d3749e31a7acf7be385cdc8cb6b644aa1b72df97087db51',
 'runtime/contact-manual-enquiry.d.ts':'d946b2a2ec0b68afc0941e7bd822cff313f860c505938d089690bf09dcc7c61e'};
for(const[file,hash]of Object.entries(frozen))if(sha(readFileSync(path.join(base,file)))!==hash)throw Error('Frozen7161 byte drift '+file);
const sourceFiles=['READ_SOURCE_CHECKPOINT.json','REGISTER_DELTA_WIP.json','NATIVE_OWNER_INPUTS.json','MANAGER_ACTION_OWNER_INPUTS.json',
 'SCHEDULER_NOTIFICATION_OWNER_INPUTS.json','RUNTIME_CONTRIBUTION.json','SUBMISSION_PAUSE_OWNER_INPUTS.json','SUBMISSION_READER_OWNER_INPUTS.json',
 'SUBMISSION_CALLER_OWNER_INPUTS.json','SUBMISSION_CONSUMER_INVENTORY.json','SUBMISSION_COHORT_DELTA_PROPOSAL.md','CANDIDATE_CONTACT_ENTRY_INPUTS.json',
 'EXACT_NATIVE_LOCK_OWNER_INPUTS.json','OFFLINE_GRAMMAR_INPUTS.json','OFFLINE_GRAMMAR_RESULT.json'];
writeFileSync(path.join(base,'WIP_SUCCESSOR_RECEIPT.json'),JSON.stringify({version:'CONTACT_NATIVE_CONTINUATION_WIP_SUCCESSOR_RECEIPT_V1',
 status:'NOT_FOR_MOUNT_NOT_REVIEWED_NATIVE_REQUIRED_NOT_RUN',prior_source_checkpoint:'b33021b5019bff30e2300f202bbe5f191a68efca',
 source_files:sourceFiles.map(file=>({path:file,sha256:sha(readFileSync(path.join(base,file)))})),frozen_7161:frozen,
 source_only_tests:{pass:33,native:false,scope:'Request/shared parser, strict service reply and actual finite current host orchestration with mocked RPC/senders'},
 sql_scan:{statements:checkpoint.sql_statement_count,PLpgSQL:'OPAQUE_NOT_PARSED',fixtures_excluded:true},
 offline_grammar:JSON.parse(readFileSync(path.join(base,'OFFLINE_GRAMMAR_RESULT.json'),'utf8')),
 database_actions:'NONE',work_continues:true},null,2)+'\n');
console.log(JSON.stringify({receipt_sha256:sha(readFileSync(path.join(base,'WIP_SUCCESSOR_RECEIPT.json'))),status:'NOT_FOR_MOUNT'}));
