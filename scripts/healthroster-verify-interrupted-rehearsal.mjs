import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';
import {agencyChildEnvironment,withSharedRehearsalTurn} from './healthroster-rehearsal-turn.mjs';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const dir=path.join(root,'docs/healthroster-build/rehearsals/13102026');
const prior=JSON.parse(fs.readFileSync(path.join(dir,'FULL_REHEARSAL_PRE_MUTATION_VACANCY.json')));
if(!/^hr_rehearsal_[a-f0-9_]+$/.test(prior.marker))throw Error('INTERRUPTED_MARKER_INVALID');
// Read the reviewed constant only; the archived harness remains non-executable.
const historical=fs.readFileSync(path.join(dir,'run_full_upgrade_rehearsal.mjs'),'utf8');
const literal=historical.match(/^const fingerprintSQL=("[^\n]*");$/m)?.[1];
if(!literal)throw Error('REVIEWED_FINGERPRINT_CONSTANT_MISSING');
const fingerprint=JSON.parse(literal);
const progressPath=path.join(root,'docs/healthroster-build/PROGRESS.md');
const receiptPath=path.join(dir,'INTERRUPTED_REHEARSAL_FRESH_VERIFICATION.json');
const result=await withSharedRehearsalTurn({task:'Interrupted-rehearsal fresh absence check (read-only, no install)',progressPath,async work(){
 const started=new Date().toISOString();
 const child=spawnSync('C:/Program Files/PostgreSQL/18/bin/psql.exe',['-X','-q','-A','-t','-v','ON_ERROR_STOP=on','-v','VERBOSITY=sqlstate'],{windowsHide:true,env:agencyChildEnvironment('healthroster_pause_fresh'),encoding:'utf8',maxBuffer:1000000,input:'begin read only;\n'+fingerprint+"\nselect (to_regclass('private."+prior.marker+"') is null)::text;\nrollback;\n"});
 if(child.status!==0){const receipt={status:'FRESH_PROOF_UNAVAILABLE',started,finished:new Date().toISOString(),sqlstate:child.stderr?.match(/(?:ERROR|FATAL):\s*([A-Z0-9]{5})/)?.[1]??null,connectedFresh:false,markerAbsent:null,restorationProved:false};fs.writeFileSync(receiptPath,JSON.stringify(receipt,null,2)+'\n');return receipt;}
 const lines=child.stdout.trim().split(/\r?\n/);let after;try{after=JSON.parse(lines[0]);}catch{throw Error('FRESH_FINGERPRINT_ENVELOPE_INVALID');}
 // The separate turn session is intentional coordination, not another rehearsal.
 // Exclude only this ephemeral concurrency count from restoration comparison.
 const keys=Object.keys(prior.before).filter(key=>key!=='other_rehearsals');
 const mismatches=keys.filter(key=>JSON.stringify(prior.before[key])!==JSON.stringify(after[key]));
 const markerAbsent=lines[1]==='true';
 const proved=markerAbsent&&after.state0?.physical_database==='cloudtms_test_clone'&&after.identity_test===true&&after.module==='LEGACY:1'&&mismatches.length===0;
 const receipt={status:proved?'INTERRUPTED_TRANSACTION_ABSENCE_AND_RESTORATION_PROVED':'FRESH_COMPARISON_REQUIRES_RECONCILIATION',started,finished:new Date().toISOString(),connectedFresh:true,marker:prior.marker,markerAbsent,physicalDatabase:after.state0?.physical_database,restorationProved:proved,mismatches,before:prior.before,after,explicitOriginalRollbackMarkerObserved:false,fullRehearsalResumed:false,comparisonScope:'All original ledger/catalogue/business count keys except transient other_rehearsals coordination count; original receipt preserved unchanged.'};
 fs.writeFileSync(receiptPath,JSON.stringify(receipt,null,2)+'\n');return receipt;
}});
console.log(JSON.stringify({status:result.status,markerAbsent:result.markerAbsent,restorationProved:result.restorationProved,mismatches:result.mismatches,fullRehearsalResumed:false}));
