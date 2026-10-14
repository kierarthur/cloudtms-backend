// Freeze only this lane's reviewed source construction. No database/route/release.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(here,'../../../..');
const digest=p=>crypto.createHash('sha256').update(fs.readFileSync(path.join(root,p))).digest('hex');
const doc='docs/healthroster-build/proposals/checklist-native-20261014/',sql='supabase/repeatable/healthroster_wp_u3_source_proposals/';
const walk=d=>fs.readdirSync(path.join(root,d),{withFileTypes:true}).flatMap(e=>e.isDirectory()?walk(d+'/'+e.name):[d+'/'+e.name]);
const selected=[...walk(doc+'financial-native-input-20261014'),doc+'build-financial-kernel-packet.mjs',doc+'update-financial-source-pins.mjs',
  doc+'SOURCE_QUERY_MONEY_HANDOFF.md',sql+'wp_u3_financial_native_kernel.forward.sqlinc',sql+'wp_u3_financial_native_kernel.reverse.sqlinc',
  sql+'wp_u3_source_shift_query_money.sqlinc','tests/healthroster-checklist-source-query-money.test.mjs',
  'supabase/verification/support/wp_u3_source_query_money_vectors.sqlinc','broker/src/weekly-source/healthroster-checklist-source.mjs',sql+'wp_u3_native_checklist.sqlinc'];
const p=path.join(here,'SOURCE_PINS.json'),pins=JSON.parse(fs.readFileSync(p));
pins.source_pins=[...new Set([...pins.source_pins.map(p=>p.path),...selected])].map(path=>({path,saved_sha256:digest(path)}));
pins.predecessor_source_increment='0e373d6c62286aa94718dd6698b256e611c04246';
pins.status='SOURCE_ONLY_FINITE_SOURCE_SHIFT_QUERY_MONEY_AND_SINGLE_NATIVE_KERNEL_UNMOUNTED_ALL_NATIVE_GATES_HELD';
pins.source_tests={passed:112,failed:0,native_SQL_executed:false,database_connection:false,deployment:false};
pins.r254_reminder_checkpoint.source_review='INDEPENDENTLY_SOURCE_CLOSED_AT_0E_104_OF_104';
pins.r254_reminder_checkpoint.critic_sha256='2ecfba7b2f4711e30d0da1cf6dcb93454445496467fd2a1bbf6d87303be6a8b9';
pins.d10_frame_source_review.successor_disposition='INDEPENDENTLY_SOURCE_CLOSED_AT_EXACT_9E_87_OF_87';
pins.d10_frame_source_review.critic_sha256='76d1ef4cae5a7470ff3b3566b2a1a34921911da4b891bd5554ccbdd231d4abde';
pins.source_comparison_note.reported_86_of_87_run='WITHDRAWN_MOVING_DISK_OBSERVATION_EXACT_9E_GIT_87_OF_87_SOURCE_CLOSED';
pins.financial_checkpoint={status:'SOURCE_BUILT_NOT_INDEPENDENTLY_REVIEWED_NOT_MOUNTED_NOT_NATIVE_QUALIFIED',
  handoff:doc+'SOURCE_QUERY_MONEY_HANDOFF.md',input_pins:doc+'financial-native-input-20261014/NATIVE_KERNEL_INPUT_PINS.json',
  input_pins_sha256:digest(doc+'financial-native-input-20261014/NATIVE_KERNEL_INPUT_PINS.json'),
  source_authority:'SOURCE_QUERY_EX_VAT',grain:'NATIVE_SOURCE_WORK_SEGMENT',invoice_hold_known:false,pending_decision_known:false,
  whole_row_quantity_known:false,ordinary_quantity_known:false,native_executed:false,canonical_Banking_edited:false};
pins.held_gates=pins.held_gates.filter(g=>g!=='SOURCE_5804_HISTORICAL_CALLER_FILTER_CHECKING_AND_R254_SUCCESSOR_INDEPENDENT_REVIEW');
pins.held_gates=[...new Set([...pins.held_gates,'FINITE_SOURCE_QUERY_MONEY_SHARED_NATIVE_OWNER_SOURCE_REVIEW',
  'SOURCE_QUERY_MONEY_INITIAL_BUNDLE_TREATMENT_NUMERIC_NATIVE_CASES','EXTRACTED_NATIVE_TRUE_MODE_PARITY_AND_FALSE_MODE_GET_LOCK_ABSENCE',
  'COMPLETE_NATIVE_NEXT_DIRTYING_HOOK_AND_INSTALLED_BODY_CATALOGUE_PROOF','ORDINARY_WHOLE_TS_ROW_MONEY_INVOICE_HOLD_AND_PENDING_DECISION_PRODUCERS'])];
const manifest={status:'UNMOUNTED_SOURCE_ONLY_NOT_NATIVE_QUALIFIED',predecessor:pins.predecessor_source_increment,source_checks:112,
  artifacts:selected.map(file=>({path:file,bytes:fs.statSync(path.join(root,file)).size,sha256:digest(file)}))};
fs.writeFileSync(path.join(here,'SOURCE_QUERY_MONEY_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
pins.source_pins=pins.source_pins.filter(p=>p.path!==doc+'SOURCE_QUERY_MONEY_MANIFEST.json');
pins.source_pins.push({path:doc+'SOURCE_QUERY_MONEY_MANIFEST.json',saved_sha256:digest(doc+'SOURCE_QUERY_MONEY_MANIFEST.json')});
fs.writeFileSync(p,JSON.stringify(pins,null,2)+'\n');
console.log(`Pinned ${pins.source_pins.length} complete source artifacts and ${manifest.artifacts.length} financial artifacts; native qualification false.`);
