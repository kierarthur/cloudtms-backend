// Saved source packaging only. No database, native action or release.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(here,'../../../..');
const doc='docs/healthroster-build/proposals/checklist-native-invoice-block-20261014/',sql='supabase/repeatable/healthroster_wp_u3_source_proposals/';
const digest=p=>crypto.createHash('sha256').update(fs.readFileSync(path.join(root,p))).digest('hex');
const files=[doc+'build-native-blocker-packet.mjs',doc+'freeze-source-increment.mjs',doc+'NATIVE_BLOCKER_REVERSE.json',doc+'HANDOFF.md',
  sql+'wp_u3_invoice_block_native.forward.sqlinc',sql+'wp_u3_invoice_block_native.reverse.sqlinc',sql+'wp_u3_source_invoice_block.sqlinc',
  sql+'wp_u3_native_checklist.sqlinc','broker/src/weekly-source/healthroster-checklist-source.mjs','tests/healthroster-checklist-source-invoice-block.test.mjs',
  'docs/healthroster-build/proposals/checklist-native-invoice-20261014/NATIVE_INVOICE_OWNER_PINS.json'];
const manifest={status:'UNMOUNTED_FINITE_POSITIVE_SOURCE_INVOICE_BLOCK_SOURCE_CONSTRUCTION',sourceTests:126,failed:0,
  history:{acceptedSourceShiftMoney:'77eb654a72d32d88cd250e63535478495f7f5429',criticSha256:'e7f840a1ab4ae409c75d8f1a47024b3270c4cb29a3c95dc6057c216cb2fc218c',
    invoiceWIPCheckpoint:'02714dc6265a8636e9eb5f4bf02d2d56a51dec96',invoiceWIPIsNotMounted:true},
  native:false,SQL_executed:false,writer_ACL_changed:false,canonical_owner_edited:false,pending_decision_known:false,action_proved:false,
  financialBasis:'NATIVE_UNRELEASED_VAT_FIT_INVOICE_ADMISSION_BLOCK_NO_PAY_QUANTITY',
  artifacts:files.map(file=>({path:file,bytes:fs.statSync(path.join(root,file)).size,sha256:digest(file)}))};
fs.writeFileSync(path.join(here,'SOURCE_MANIFEST.json'),JSON.stringify(manifest,null,2)+'\n');
const pinsFile=path.join(root,'docs/healthroster-build/proposals/checklist-native-20261014/SOURCE_PINS.json'),pins=JSON.parse(fs.readFileSync(pinsFile));
pins.source_pins=[...new Set([...pins.source_pins.map(p=>p.path),...files,doc+'SOURCE_MANIFEST.json'])].map(path=>({path,saved_sha256:digest(path)}));
pins.status=manifest.status;pins.source_tests={passed:126,failed:0,native_SQL_executed:false,database_connection:false,deployment:false};
pins.financial_checkpoint.status='FINITE_SOURCE_SHIFT_QUERY_MONEY_INDEPENDENTLY_ACCEPTED_AT_SOURCE_77EB_NATIVE_GATES_HELD';
pins.financial_checkpoint.accepted_commit='77eb654a72d32d88cd250e63535478495f7f5429';
pins.financial_checkpoint.critic_sha256=manifest.history.criticSha256;
pins.invoice_block_checkpoint={manifest:doc+'SOURCE_MANIFEST.json',manifest_sha256:digest(doc+'SOURCE_MANIFEST.json'),
  authority:'SOURCE_INVOICE_BLOCK',grain:'REAL_CURRENT_SOURCE_CLIENT_MANIFEST_MOVEMENT_UPLOAD_ROW',
  source_review:'PENDING_INDEPENDENT_REVIEW',native:false,pending_decision_known:false,action_proved:false,whole_row_quantity_known:false};
pins.held_gates=pins.held_gates.filter(g=>g!=='FINITE_SOURCE_QUERY_MONEY_SHARED_NATIVE_OWNER_SOURCE_REVIEW');
pins.held_gates=[...new Set([...pins.held_gates,'SOURCE_INVOICE_BLOCK_SHARED_NATIVE_GUARD_SOURCE_REVIEW',
  'SOURCE_INVOICE_BLOCK_NATIVE_ADMISSION_PARITY_PURE_GET_RETENTION_AND_RELEASE_CASES',
  'SOURCE_INVOICE_BLOCK_PENDING_OFFICE_DECISION_AND_GENUINE_ACTION_TARGET_PRODUCER'])];
fs.writeFileSync(pinsFile,JSON.stringify(pins,null,2)+'\n');
console.log(JSON.stringify({source_pins:pins.source_pins.length,invoice_artifacts:manifest.artifacts.length,native:false}));
