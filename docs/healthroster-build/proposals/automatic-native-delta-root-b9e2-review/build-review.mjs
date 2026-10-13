// Guarded adjacent integration proposal generation, never a root edit.
import fs from 'node:fs';
import path from 'node:path';
import { inventory, hash, assertRoot, here, lexical } from './read-source.mjs';
const root=path.resolve(process.argv[2]), lane=path.resolve(here,'../../../..');
const base='b9e2cbfcdf061a4e8fe07044e658c2daf043a362';
const observedHead=assertRoot(root,base), inv=await inventory(root);
const read=p=>fs.readFileSync(path.join(root,p),'utf8').replaceAll('\r\n','\n');
const fact=(p,r=root)=>{const b=fs.readFileSync(path.join(r,p));return {path:p,bytes:b.length,sha256:hash(b)};};
const write=(p,v)=>fs.writeFileSync(path.join(here,p),typeof v==='string'?v:JSON.stringify(v,null,2)+'\n');
const census=JSON.parse(fs.readFileSync(path.join(here,'SOURCE_CENSUS.json')));
if(census.sourceCommit!==base||census.nativePinComparison.some(p=>!p.unchanged))throw Error('CENSUS_BASE_INVALID');
const orig='docs/healthroster-build/proposals/automatic-native-delta/';
const forward='docs/healthroster-build/proposals/automatic-native-delta-forward-review/';
const manifest=JSON.parse(fs.readFileSync(path.join(lane,orig+'SOURCE_MANIFEST.json')));
for(const packet of [orig,forward,'docs/healthroster-build/proposals/report-native-pay-fin-delegate/',
 'docs/healthroster-build/proposals/rehearsal-entitlement-fixture-forward/']) {
  const m=JSON.parse(fs.readFileSync(path.join(lane,packet+'SOURCE_MANIFEST.json')));
  for(const f of m.files??m.artifacts) {
    const p=f.path.startsWith('docs/')||f.path.startsWith('tests/')?f.path:packet+f.path;
    if(fact(p,lane).sha256!==f.sha256)throw Error('FROZEN_ARTIFACT_DRIFT:'+p);
  }
}
const privateSignatures=manifest.newPrivateSignatures;
const contracts=['supabase/repeatable/15092026_1534_weekly_source_acl_contract_v1.sql',
 'supabase/repeatable/13102026_1436_weekly_source_invoice_money_acl_inventory_v1.sql'];
const verifier='supabase/verification/15092026_1534_weekly_source_acl_contract_v1.sql';
function definition(text,name) {
 const parsed=inv.scanManagedSql(text);
 const candidates=parsed.statements.map(s=>parsed.source.slice(s.start,s.end)).filter(s=>s.startsWith('create or replace function '+name+'('));
 if(candidates.length!==1)throw Error('ONE_COMPLETE_OWNER_REQUIRED:'+name);
 return candidates[0];
}
const contractName='private._weekly_source_acl_private_helper_contract_v1';
const existingSignatures=text=>[...text.matchAll(/'((?:private|public)\.[^']+\([^']*\))'/g)].map(m=>m[1]);
const initial=existingSignatures(definition(read(contracts[1]),contractName));
if(initial.length!==221||new Set(initial).size!==221||privateSignatures.some(s=>initial.includes(s)))throw Error('PRIVATE_BASE_CENSUS_CHANGED');
if(definition(read(contracts[0]),contractName)!==definition(read(contracts[1]),contractName))throw Error('CENTRAL_INVENTORIES_DIVERGE');
const addValues=privateSignatures.map(s=>`    ('${s}'),`).join('\n')+'\n';
function unionContract(text) {
 const old=definition(text,contractName), anchor='  values\n';
 if(old.split(anchor).length!==2)throw Error('PRIVATE_VALUES_ANCHOR_CHANGED');
 return text.replace(old,old.replace(anchor,anchor+addValues));
}
const sourceACL=read(contracts[0]), sourceRegistry=read(contracts[1]);
const finalACL=unionContract(sourceACL), finalRegistry=unionContract(sourceRegistry);
// The official packaging parser removes only these original BEGIN/COMMIT and
// ON_ERROR_STOP controls, so included support cannot commit the outer release.
write('12_complete_private_registry_union.sqlinc',inv.exactAtomicBody(finalRegistry).trimEnd()+'\n');
write('13_complete_final_acl_union.sqlinc',inv.exactAtomicBody(finalACL).trimEnd()+'\n');
const verifierSource=read(verifier), verifierAnchor='  v_expected_private_helpers text[]:=array[\n';
if(verifierSource.split(verifierAnchor).length!==2)throw Error('INDEPENDENT_PRIVATE_ARRAY_ANCHOR_CHANGED');
write('14_complete_acl_verifier_union.sqlinc',verifierSource.replace(verifierAnchor,
 verifierAnchor+privateSignatures.map(s=>`    '${s}',`).join('\n')+'\n'));
const explicit=privateSignatures.map(s=>`alter function ${s} owner to postgres;\nrevoke all on function ${s} from public,anon,authenticated,service_role;`).join('\n')+'\n';
write('15_final_automatic_private_acl.sqlinc',explicit);
// Packaging recipe: root copies exact frozen bytes into this supabase support
// directory. Retained Stage20/Source27 are deliberately not copied over later
// capability owners. Their existing call sites are checked by body hash below.
write('16_proposed_root.sqlinc',`-- Proposed destination: supabase/repeatable/13102026_2205_weekly_source_automatic_native_delta_v1.sql\n`
 +`-- Root must rebase chronological naming if a later controlling owner exists.\n\\set ON_ERROR_STOP on\nbegin;\n`
 +['01_staged_source_documents.sqlinc','02_staged_publication_context.sqlinc','03_approved_detail_evidence.sqlinc',
 '04_native_receipt_successors.sqlinc','05_staged_prior_wait_receipt.sqlinc','06_expense_evidence.sqlinc',
 '07_staged_entitlement_components.sqlinc','08_complete_automatic_reconcile_delta_forward.sqlinc',
 '09_complete_local_successor.sqlinc','12_complete_private_registry_union.sqlinc','13_complete_final_acl_union.sqlinc',
 '15_final_automatic_private_acl.sqlinc'].map(p=>`\\ir 13102026_2205_weekly_source_automatic_native_delta/${p}\n`).join('')
 +`notify pgrst, 'reload schema';\ncommit;\n`);
const preserved=[
 'supabase/repeatable/13102026_2110_healthroster_report_native_fin_worker_v1.sql',
 ...inv.readManagedClosure('supabase/repeatable/13102026_2110_healthroster_report_native_fin_worker_v1.sql',root).ordered.map(p=>p.path),
 'supabase/repeatable/13102026_1544_weekly_source_self_bill_report_scope_v1.sql',
 'supabase/repeatable/13102026_1561_weekly_source_magnit_contract_price_v1.sql',
 'supabase/repeatable/13102026_1127_healthroster_correction_requests/13102026_1127_approval.sqlinc',
 'broker/src/weekly-source/upload-staging-adapter.mjs','broker/src/weekly-source/upload-publication-owner.mjs',
 'broker/src/weekly-source/acacium-report-normalise.js','broker/src/weekly-source/report-upload-provenance.mjs',
 'supabase/release/current-release.json','scripts/cloudtms-managed-release-sql.mjs','scripts/cloudtms-db-release-lib.mjs'];
const actualPreserved=[...new Set(preserved)];
// Also pin actual complete closures, including corrected projection pages whose
// filenames differ from historical package aliases.
for(const p of ['supabase/repeatable/13102026_1544_weekly_source_self_bill_report_scope_v1.sql',
 'supabase/repeatable/13102026_1561_weekly_source_magnit_contract_price_v1.sql']) {
 for(const leaf of inv.readManagedClosure(p,root).ordered)if(!actualPreserved.includes(leaf.path))actualPreserved.push(leaf.path);
}
const preservedFacts=actualPreserved.map(p=>fact(p));
const allOwners=[...inv.functions.values()];
const keyOwners=allOwners.filter(f=>/weekly_source_entitlement_component_id_v1|weekly_source_entitlement_components_v1|weekly_source_inventory_approval_basis_v2|weekly_source_initial_additional_components_v2|weekly_source_local_publication_context_v2|weekly_source_protected_component_manifest_v1|weekly_exceptional_pay_complete_local_v1|weekly_exceptional_pay_stage_c1_request_v1|weekly_source_automatic_reconcile_certificate_v1|weekly_source_automatic_reconcile_atomic_v1|timesheet_correction_native_original_reversal_v1|timesheet_correction_authorise_atomic_v1|tsfin_write_current_snapshot_single_bounded|tsfin_write_snapshots_and_complete/.test(f.name))
 .map(({sql,body,...f})=>f);
const sourceInputs=[...new Map([...census.nativePinComparison.map(p=>p.current),...preservedFacts,
 ...contracts.map(p=>fact(p)),fact(verifier),
 ...keyOwners.map(f=>fact(f.path))].map(f=>[f.path,f])).values()];
write('CONTROLLING_INPUTS.json',{schema_version:'AUTOMATIC_B9E2_REVIEW_INPUTS_V1',sourceCommit:base,observedHead,
 unrelatedRootChangeExcluded:'scripts/healthroster-rehearsal-turn-cli.mjs is root-owned SEND_7 instrumentation, not imported/read/executed as a controlling input. Every selected source input remains exactly hash-guarded.',
 frozenPackets:[orig,forward,'docs/healthroster-build/proposals/report-native-pay-fin-delegate/',
 'docs/healthroster-build/proposals/rehearsal-entitlement-fixture-forward/'].map(p=>fact(p+'SOURCE_MANIFEST.json',lane)),
 sourceInputs,keyOwners,preservedFacts});
const oldReviews=['docs/healthroster-build/rehearsals/13102026/VERIFIER_SAFETY_0_55.json',
 'docs/healthroster-build/rehearsals/13102026/VERIFIER_SAFETY_56_111.json',
 'docs/healthroster-build/rehearsals/13102026/VERIFIER_SAFETY_112_167.json',
 'docs/healthroster-build/rehearsals/13102026/FULL_REHEARSAL_EXECUTION_MANIFEST.json'];
const scopes=[
 {fixturePrefix:'b7000000-0000-4000-8000-',category:'ROLLBACK_CONTAINED_FIXTURES',
  fences:['Fixture actor/Client/Contract/week/Timesheet/batch/transfer/advance/invoice identities use b7000000... constants or closed fixture world lookup.',
   'Included key capture derives quoted primary-key attributes from pg_index/pg_attribute, watches the seven explicit regclass inputs, and rejects UPDATE/DELETE of noninserted captured keys and every TRUNCATE.',
   'drain_workbench_jobs performs direct UPDATE only on job IDs in pg_temp.ws_verify_keys for public.banking_pay_workbench_jobs; it never calls a queue processor.',
   'force_withdrawal_failure is deliberately unconditional. Its temporary trigger is attached to Source root authorisations only around the single withdrawal for b700...0312 and dropped immediately; it is not falsely described as an ID-checking function.',
   'Named head/receipt/component constraints are deferred only during construction, forced IMMEDIATE for each finished fixture, then restored DEFERRED; no SET CONSTRAINTS ALL.',
   'Schema-seed entitlement receipt/head fixture facts are not accepted financial/manager/transport proof. Real lifecycle calls and exact rollback/refusal/security assertions remain intact.'],
  exceptions:['settings_defaults id=1 is an explicit shared singleton rollback fixture; its settings-authority trigger may touch existing Contract settings/outbox intent. This is not claimed to be prefix isolated. Captured-job guards can therefore expose a fixture incompatibility rather than silently changing existing jobs.',
   'Synthetic financial and ISSUED invoice rows deliberately exercise refusal readers/guards; setting those row fields does not invoke payment/provider/settlement/send owners.']},
 {fixturePrefix:'c7000000-0000-4000-8000-',category:'ROLLBACK_CONTAINED_FIXTURES',
  fences:['Fixture world and effects loop 2..12 derive Candidate/root/Contract/batch/item/transfer/operation UUIDs solely from c7000000... constants.',
   'drain_workbench_jobs changes QUEUED/RUNNING rows only where dedupe_key contains the full c7000000-0000-4000-8000- prefix. It does not claim, execute or submit those jobs.',
   'assert_refused executes only the closed literal/format statements supplied by this verifier against fixture roots; no externally supplied SQL.',
   'publish_head constructs the complete SCHEMA seed bundle/component/receipt relation with exact named deferred guards flushed IMMEDIATE after the finished unit.',
   'Seeded provider-attempt, settlement/remittance/transfer scope and PAYMENT_EXECUTE values are forensic refusal inputs; no native provider/payment/settlement executor is called.'],
  exceptions:['settings_defaults id=1 is shared transactional settings setup, not a fixture-prefix key. Native settings changes/dirty-enqueue intent remain SQL in the rolled-back frame.',
   'Direct dedupe-prefix drain is not proof that a genuine native Workbench job would finish. First-use positive financial/HEAD acceptance remains held.']},
 {fixturePrefix:'17092026-0800-4000-8000-',category:'ROLLBACK_CONTAINED_FIXTURES_WITH_SOURCE_SCHEMA_CORRUPTION_NEGATIVES',
  fences:['Only real Workbench seam invocations are economic_occurrence_page and timesheet_input_fingerprint readers over the synthetic obsolete build/unit/Timesheet world.',
   'walk/walk_n use only fixture build/unit/Timesheet selectors and bounded limits; expected-failure SQL arguments are closed literal/format fixture inputs.',
   'Section6 temporarily removes two Source-head unique indexes, three Source immutable/root-identity triggers, three Source-head NOT NULL requirements, two Source-head self-FKs and only CHECKs on the fixed Source-head regclass that mention publication_receipt_digest or scope_change_tx_token.',
   'Dynamic ALTER uses a fixed Source-head table and %I catalogue constraint name, never a supplied relation or executable business callback.',
   'Corruption UPDATEs use exact fixture head IDs. Final DELETE of components is restricted to heads of v_ts; final head DELETE is root_timesheet_id=v_ts. No global head purge.',
   'No Banking definition or guard is replaced/disabled; the ordinary no-head byte equality, complete/zero/ambiguous/foreign identity, paging and fingerprint assertions remain.'],
  exceptions:['A settings_finance_windows row for the fixture date is inserted only if no existing covering window exists; no existing window is overwritten.',
   'Source schema-corruption DDL is eligible only inside an outer transaction with guaranteed rollback and fresh catalogue restoration, never as an install verifier executed outside that frame.']},
 {fixturePrefix:null,category:'CATALOGUE_AND_MISSING_AUTHORITY_READ_ONLY',
  fences:['The three signatures are inspected via to_regprocedure/catalogue ACL/owner/security/search_path/default checks; FIN is never invoked.',
   'The only executed Source calls are pay_authority(NULL) and compact pay_witness(NULL). Detailed authority SELECT binding WHERE id=NULL cannot match; NOT FOUND returns NULL before other reads/helpers. Compact witness delegates and returns NULL.',
   'No DML, fixtures, stage, pay orchestration, report FIN, A/B selection, release fabrication or feature activation.'],
  exceptions:['Null refusal and catalogue security do not qualify native ordinary/protected Report pay readiness, actual A/B renderer admission or API availability.']}
];
const safety=census.verifiers.map((v,i)=>({...fact(v.path),closureHash:v.closureHash,
 decision:'INCLUDE_ONLY_WITH_PINNED_OUTER_ROLLBACK_CONTRACT',...scopes[i],
 actualNativeTopLevelCalls:v.direct.filter(n=>allOwners.some(f=>f.name===n)),
 expectedFailureSqlAndDynamicStatements:v.dynamic,
 nestedSourceOwnerCount:v.conservativeCalleeClosure.owners.length,
 nestedBodiesAndTriggers:'SOURCE_CENSUS.json conservativeCalleeClosure owners/edges includes all executable schema-qualified and recognised unqualified native references, literal EXECUTE targets and DML trigger candidates; all source bytes/hash-bound.',
 requiresGenuineApplyingRelease:false,containsActualQueueProcessorCall:false,
 invokesPaymentProviderSettlementNetworkOrEmailSend:false,executed:false}));
write('VERIFIER_SAFETY_REREVIEW.json',{schema_version:'HEALTHROSTER_CHANGED_VERIFIER_SOURCE_SAFETY_V1',sourceCommit:base,
 status:'ADJACENT_SOURCE_REVIEW_NOT_NATIVE_EXECUTION',oldReviewInputs:oldReviews.map(p=>fact(p)),
 oldCountsPreserved:{verifiers:168,eligible:152,excluded:16},oldExclusionsChanged:false,verifiers:safety,
 method:'Full verifier/include bytes plus complete controlling native function bodies and conservative DML trigger traversal. Actual dynamic EXECUTE and pg_temp closed expected-failure SQL are reviewed separately from definition/signature string inspection. No keyword-only safety inference.',
 dynamicNativeReview:['Current Workbench dirty/summary trigger dynamic SQL is fixed temporary context SELECT/UPDATE/DELETE, owner/schema/transaction-bound; it records/enqueues SQL intent, never claims or drains a queue.',
 'Approval basis/candidate-head dynamic reads target fixed retained chosen-detail relations after exact relation-existence guards.',
 'extensions.digest is the pgcrypto cryptographic primitive, not a network/business executor; unresolved relation constructors are excluded from routine calls.'],
 runtimePrerequisites:['Root acquires SEND_7 turn lock before fingerprints; no protected release/phone test overlap.',
 'Actual TEST identity, LEGACY:1 and exact commit/closure verification before invoking native fixtures.',
 'One outer rollback transaction, per-verifier savepoint isolation and no nested COMMIT/ROLLBACK escapes; preserve each original psql conditional semantics.',
 'psql variable state is independent of DB savepoint rollback. Preserve/restore caller variable definedness/value per unit; do not silently force a global outer-mode setting.',
 'Fresh post-rollback marker absence and exact ledgers/catalogue fingerprints; newly installed Bank native owners require re-review/isolated rerun, not assumption from this source census.'],
 databaseCalls:false,networkCalls:false,secretsRead:false});
write('INTEGRATION_RECEIPT_PROPOSAL.json',{schema_version:'AUTOMATIC_B9E2_GUARDED_INTEGRATION_RECEIPT_V1',
 status:'REVIEWED_SOURCE_PROPOSAL_NOT_INTEGRATED_COMPILED_OR_QUALIFIED',sourceCommit:base,observedHead,
 frozenPacketChecks:'All Automatic23, StableKey7, Report53 and Fixture16 manifest artifacts rehashed unchanged.',
 privateSignatures,newPublicSignatures:[],newServiceSignatures:[],newTables:[],newIndexes:[],
 changedExistingOwners:manifest.changedExistingOwners,
 unchangedCallSiteOwners:census.nativeFinalOwners.filter(o=>/stage_c1_request|automatic_reconcile_certificate|automatic_reconcile_atomic/.test(o.name)),
 counts:{baselineTables:105,baselinePrivate:221,baselineService:142,automaticOnlyTables:105,automaticOnlyPrivate:232,automaticOnlyService:142},
 centralACL:{guardedInputs:contracts.map(p=>fact(p)),verifierInput:fact(verifier),
  completeSuccessors:['12_complete_private_registry_union.sqlinc','13_complete_final_acl_union.sqlinc','14_complete_acl_verifier_union.sqlinc','15_final_automatic_private_acl.sqlinc'],
  supportPackaging:'Official exactAtomicBody removes original BEGIN/COMMIT/ON_ERROR_STOP from complete12/13; trailing packaging-only blank lines are removed. Full native definitions and all remaining SQL statement bytes are retained.',
  warning:'232 applies only to this exact b9e2 union. Integrating Money capability changes the current union: merge actual latest private/service signatures first and regenerate both complete inventories and independent expected arrays. Never overwrite a later capability/Bank owner with these guarded copies.'},
 closure:{proposedRoot:'supabase/repeatable/13102026_2205_weekly_source_automatic_native_delta_v1.sql',
  proposedSupport:'supabase/repeatable/13102026_2205_weekly_source_automatic_native_delta/',
  recipe:'16_proposed_root.sqlinc',sourceOrder:'01-07 frozen helpers, forward08, frozen complete09 Local,12 complete registry,13 complete ACL,15 final private ACL. Existing Stage20 and Source27 are hash-checked but not redefined.',
  verificationRegistration:'Root replaces the complete guarded existing mandatory ACL verifier with14, and records a dedicated native automatic admission/security verifier in both NEW and UPGRADE. Runtime qualification journeys are separate, never inserted as a fake release receipt.',
  generatedContract:'Root must seal actual full contract after complete later-owner union; source package cannot supply generated OIDs/fingerprints.'},
 nativeI1Identity:{owner:'private.weekly_source_entitlement_component_id_v1(text,text,text,text)',
  domain:'WEEKLY_SOURCE_ENTITLEMENT_COMPONENT_V1',tuple:['component_kind','economic_key_type','economic_key_value','component_member_identity'],
  worked:'Actual native composer uses SEGMENT / weekly-source-event:<actual event UUID> / exact proved event member. segment_stable_key remains separately compared and derives from native weekly_source.calculation_fingerprint.',
  transport:'C1 carrier component UUID, TS_DAY date and ordinal/source-key are independently verified transport identity and never used as I1 entitlement ID.'},
 capability:{missingAtBase:['private.weekly_source_bulk_source_cas_v1(uuid,uuid[])','private.weekly_source_bulk_producer_ready_v1(uuid)','private.weekly_source_automatic_reconcile_producer_ready_v1(uuid)'],
  owner:'Money protected-source-capability plus exact native-financial-fixtures schema-forward, root integrated only after current owner review.',
  rule:'AUTO requires both strict actual qualified BULK and AUTO readiness, dated enabled CLIENT_SELF_BILL/CLEAR_AUTOMATICALLY assignment and actual matched VERIFIED release/native execution evidence. No function-presence/GUC/env/constant-ready substitute.',
  ownStageCAS:'All39 Source fact categories remain. No own-stage normalization or waiver. Dynamic/triggers/current identity materialisation and missing-event preparation require genuine C03 before/after equality.'},
 A0:{owner:'private.timesheet_correction_native_original_reversal_v1(jsonb,uuid)',
  conflict:'No signature or owner overlap. The actual current CN atomic approval stores canonical A0 with unchanged bounded writer, reads actual stored output, then derives R0 sign clone in that same transaction. Automatic positive target components do not call this reversal helper or reuse its negative quantities.',
  preserved:'CN complete approval, recursive original clone, single bounded writer and canonical batch writer bytes are pinned unchanged. No current TSFIN fallback is added to prior approved automatic authority.'},
 ReportM1:{preservedFacts,FINDispatcherChanged:false,ABQualified:false,operationalVariantSelected:false,
  rule:'Real repaired Report/M1 projection successors and actual Report staging/publication/provenance Workers remain untouched. Accepted report/final-export/payment publication authority continues through current four-facts/WAIT/read owners; report metadata or report hours cannot become Pay.'},
 runtimeGates:['Post-Bank final native owner/hash/security review and NEW/UPGRADE compile under SEND_7.',
 'Actual stage K->K+1 before PREPARING, retained old WAIT accepted receipt with unique transport, complete native I1 identity/provenance, explicit stable-key drift refusal.',
 'Complete component quantity/pay/charge/rates/detail/raw-additional/expense policy evidence deltas each zero; compensating totals refuse.',
 'C03 all39 Source categories unchanged across actual own-stage/native triggers, foreign/stale/partial/ambiguous evidence refuses.',
 'Actual Local published result, exact replay, native first-authorise/pending-freeze behavior, rollback on returned failure/exception, unchanged ordinary Local/C1/NEXT regressions.',
 'Actual release-qualified capability, API/security/generated contract, Report A/B pay/render isolation remain separate held root gates.'],
 databaseCalls:false,networkCalls:false,rootFilesEdited:false,frozenFilesEdited:false});
console.log(JSON.stringify({safety:safety.map(v=>({path:v.path,decision:v.decision})),privateCount:232,
 guardedInputCount:sourceInputs.length,newPrivateCount:privateSignatures.length}));
