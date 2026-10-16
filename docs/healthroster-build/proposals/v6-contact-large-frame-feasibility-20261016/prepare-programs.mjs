// Offline exact SQL derivation only; fake turn cannot open a connection.
import fs from 'node:fs';
import crypto from 'node:crypto';
import {createFixedMetadataReader} from './fixed-metadata-reader.mjs';
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-install2-14102026';
const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:16384,backendPid:42,turnId:'hr_turn_'+'a'.repeat(32),key:[20261013,1]};
const turn={identity,signal:new AbortController().signal,assertHeld(){throw Error('OFFLINE_ONLY');},runOwnedChild(){throw Error('OFFLINE_ONLY');}};
const plan=JSON.parse(fs.readFileSync(new URL('../v6-contact-six-lock-contention-20261016/PLAN.json',import.meta.url),'utf8'));
const rows=[];
for(const c of plan.cases){const reader=await createFixedMetadataReader({root,turn,caseId:c.id});rows.push({caseId:c.id,...reader.inspectSource(),descriptor:reader.sourceDescriptor});}
const bytes=JSON.stringify({scope:'OFFLINE_SQL_SOURCE_ONLY_NOT_NATIVE_INPUT',syntheticTurnIdentity:identity,programs:rows},null,2)+'\n';
fs.writeFileSync(new URL('./OFFLINE_PROGRAMS.json',import.meta.url),bytes);
console.log(JSON.stringify({status:'SIX_FIXED_SOURCE_PROGRAMS_DERIVED_OFFLINE',sha256:crypto.createHash('sha256').update(bytes).digest('hex')}));
