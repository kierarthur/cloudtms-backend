// Pure scope/evidence checks. No source caller, connection, transaction or gate.
import assert from 'node:assert/strict';import fs from 'node:fs';import path from 'node:path';import {createHash} from 'node:crypto';
import {finiteMatrix} from '../SOURCE_JOINED_FINANCIAL_CALLBACKS_V4_20261007/joined-acceptance-matrix.mjs';
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const sha=x=>createHash('sha256').update(x).digest('hex').toUpperCase();
export const GUARD_DELTA=Object.freeze({
  "schema_version": "SOURCE_CAPTURE_GENERATED_DISPATCH_FINANCIAL_MATERIAL_DELTA_V1",
  "path": "supabase/repeatable/04102026_1519_banking_pay_next_work_collection_v1.sql",
  "signature": "private.bpay_next_work_collection_capture_guard_v1()",
  "preimageFile": "SOURCE_COMPARISON_CASE_OWNER_V1_20261007/candidate/supabase/repeatable/04102026_1519_banking_pay_next_work_collection_v1.sql",
  "preimageRawSha256": "231D1B86F36118378C2FE463934EAFE40DE0FEBD597218A108664798F304AF0D",
  "candidateFile": "SOURCE_CAPTURE_DISPATCH_GUARD_V1_20261007/candidate/supabase/repeatable/04102026_1519_banking_pay_next_work_collection_v1.sql",
  "candidateRawSha256": "35067DC1297E10EC7D19F1A9E642AAB4D2FDA6FE555CD1523E22CA3E021B395F",
  "candidateClosureSha256": "E1FCF3FECC1B0A3A965729FB7A3A0A73EB5331B96612640DBD7E3550130F8CE9",
  "beforeUnitBodySha256": "1C3CE4D0D3DFB5F38F5079BF9CC9DCDBD48E0FC47F67E04A258A283380BCDCF6",
  "originalOverlaySha256": "6CF8093E9D651306CE284A729D4ACF7D902BF9D0B4CD9E122A84E5DD9AAD2530",
  "effectiveOverlaySha256": "B2A9A14155A537F8B1D8C99E718285A8A2F5CB320F798799B0836B750D8CFD66",
  "excludedJsonKeys": [
    "managed_capture_completed",
    "legacy_collection_work_id",
    "legacy_collection_component_key",
    "comparison_approval_state"
  ],
  "spans": [
    {
      "label": "COMPARISON_ROW_JSON_ONLY",
      "before": "(to_jsonb(new)-'managed_capture_completed') is distinct from (to_jsonb(old)-'managed_capture_completed')",
      "after": "(to_jsonb(new)-array['managed_capture_completed','legacy_collection_work_id','legacy_collection_component_key','comparison_approval_state']) is distinct from (to_jsonb(old)-array['managed_capture_completed','legacy_collection_work_id','legacy_collection_component_key','comparison_approval_state'])"
    },
    {
      "label": "ORDINARY_ROW_JSON_ONLY",
      "before": "(pg_catalog.to_jsonb(new)-'managed_capture_completed')\n         is distinct from (pg_catalog.to_jsonb(old)-'managed_capture_completed')",
      "after": "(pg_catalog.to_jsonb(new)-array['managed_capture_completed','legacy_collection_work_id','legacy_collection_component_key','comparison_approval_state'])\n         is distinct from (pg_catalog.to_jsonb(old)-array['managed_capture_completed','legacy_collection_work_id','legacy_collection_component_key','comparison_approval_state'])"
    },
    {
      "label": "RUN_POSITION_RELATION_DISPATCH",
      "before": "  if tg_table_name='bpay_next_run_position' and tg_op in ('INSERT','UPDATE') and new.source_comparison_receipt_id is not null\n    and (new.work_collection_id is not null or new.managed_capture_completed or\n      (new.approved_line_id is null and new.selected_residual_ex_vat=0)) then",
      "after": "  if tg_table_name='bpay_next_run_position' and tg_op in ('INSERT','UPDATE') then\n    if new.source_comparison_receipt_id is not null and (new.work_collection_id is not null or new.managed_capture_completed or\n      (new.approved_line_id is null and new.selected_residual_ex_vat=0)) then"
    },
    {
      "label": "CLOSE_RUN_POSITION_DISPATCH",
      "before": "    return new;\n  end if;\n  if tg_table_name='bpay_next_case_period' then",
      "after": "    return new;\n  end if; end if;\n  if tg_table_name='bpay_next_case_period' then"
    }
  ],
  "beforeDeclaration": {
    "name": "private.bpay_next_work_collection_capture_guard_v1",
    "signature": "private.bpay_next_work_collection_capture_guard_v1()",
    "argumentDeclaration": "",
    "root": "supabase/repeatable/04102026_1519_banking_pay_next_work_collection_v1.sql",
    "line": 72,
    "declarationSha256": "E6DDE20754F023A54533A3D347E3835BE73B3FB46499A99D9F13DE02416B0053",
    "bodySha256": "84EA2DA249DCC919C92C26671ADC262F3E0B1BDDCC6FCB74477CC45FF1CDD392",
    "bodyMd5": "a627e16206c7c7961ffb24d0419e28ae",
    "securityDefiner": false,
    "volatility": "v",
    "parallel": "u",
    "strict": false,
    "language": "plpgsql",
    "config": [
      "search_path=pg_catalog, private"
    ]
  },
  "afterDeclaration": {
    "name": "private.bpay_next_work_collection_capture_guard_v1",
    "signature": "private.bpay_next_work_collection_capture_guard_v1()",
    "argumentDeclaration": "",
    "root": "supabase/repeatable/04102026_1519_banking_pay_next_work_collection_v1.sql",
    "line": 72,
    "declarationSha256": "B24AFFAD1385599A13CE1FEDE3CFAC8B4E3606A77FA3D79A3936369034A63786",
    "bodySha256": "E77266D4464DF6742FE2351F707CF429776C79166D8B14B71D58C5B5C853CCF9",
    "bodyMd5": "ac5aecc6cb10e2c497aadb6f7ce1d61a",
    "securityDefiner": false,
    "volatility": "v",
    "parallel": "u",
    "strict": false,
    "language": "plpgsql",
    "config": [
      "search_path=pg_catalog, private"
    ]
  },
  "historicalGuardProof": "V11_UNCORRECTED_GUARD_PREFIX_ONLY",
  "correctedGuardFirstUse": "NOT_INFERRED",
  "generatedPredecessorFile": "SOURCE_CAPTURE_GENERATED_GUARD_V1_20261007/candidate/supabase/repeatable/04102026_1519_banking_pay_next_work_collection_v1.sql",
  "generatedPredecessorRawSha256": "5BE2D339B3A62921A846B7442339C16D93960ACDB54F9304899B37D2C582B66D"
});
export const FINANCIAL_SCOPE=Object.freeze({schema_version:'SOURCE_MOVE_JOINED_CURRENT_DISPOSITION_SCOPE_V1',
 negativeCapacity:'CURRENT_ONLY',acceptedVectors:12,directions:4,acceptedResiduals:[-3,0,3],
 specs:finiteMatrix.map(x=>x.key),syntheticFixtures:true,localCsvSimulatedPayments:true,
 providerCalls:0,prefixOnly:false,fullMatrix:false,heldCaseAndRepeatedTails:false,
 hostedProof:false,permanentInstall:false,factDeltaQualification:'OPEN',fullCatalogueAcceptance:false});
// Exact genuine V11 PREFIX prerequisite, not a financial-matrix pass.
// New financial dispatch remains root-gated; no CLI/env supplies this fact.
export const FIRST_USE_PREREQUISITE=Object.freeze({status:'QUALIFIED',
 file:'SOURCE_MOVE_JOINED_FIRST_USE_V11_20261007/driver/ACTUAL_FIRST_USE.json',sha256:'86DD0481D6CA49A72392C489D746C55DF76FAE79B430AEE7AEE5D8403C1F93F9',
 requiredDisposition:'BOUNDED_PREFIX_FIRST_USE_PASS',requiredSpec:'PAYE_PAYE_0',
 admissionSupplierQualification:'GENUINE_V11_PREFIX_PASS_ONLY'});
const minor=x=>{const s=String(x);assert.match(s,/^-?\d+(?:\.\d{1,2})?$/,'COMPILE_FINANCIAL_PENNY_RESULT');
 const negative=s.startsWith('-'),p=(negative?s.slice(1):s).split('.'),n=Number(p[0])*100+Number((p[1]??'').padEnd(2,'0'));
 assert(Number.isSafeInteger(n));return negative?-n:n;};
const nonnegative=x=>{const n=minor(x);assert(n>=0,'COMPILE_FINANCIAL_NONNEGATIVE_RESULT');return n;};
const ids=x=>{assert.match(x,UUID,'COMPILE_FINANCIAL_REAL_RESULT_ID');};
export function assertActualResult(result){
 assert.equal(result?.kind,'SOURCE_JOINED_ACTUAL_ACCEPTANCE_AND_DISPOSITION_SCOPE_V2','COMPILE_FINANCIAL_ACTUAL_RESULT_KIND');
 assert.equal(result.allDirectionsAccepted,true);assert.equal(result.fullFinancialMatrix,false);assert.equal(result.heldCaseAndRepeatedTails,false);
 assert.equal(result.hostedProof,false);assert.equal(result.providerCalls,0);assert.equal(result.permanentInstall,false);
 const a=result.acceptance;assert.equal(a?.status,'FINITE_ACTUAL_OWNER_ACCEPTANCE_MATRIX_COMPLETED');
 assert.equal(a.scope,'ALL_FOUR_DIRECTIONS_THREE_ACCEPTED_RESIDUALS');assert.equal(a.acceptedVectors,12);
 assert.equal(a.downstreamDispositionAward,false);assert.equal(a.fullFinancialMatrix,false);assert.equal(a.heldCaseAndRepeatedTails,false);
 assert.equal(a.hosted_route_proof,false);assert.equal(a.provider_calls,0);assert.equal(a.matrix.length,12);
 const expected=finiteMatrix.map(x=>x.key).sort();assert.deepEqual(a.matrix.map(x=>x.key).sort(),expected,'COMPILE_FINANCIAL_EXACT_12_ACCEPTED_SPECS');
 assert.equal(new Set(a.matrix.map(x=>x.key)).size,12);assert.equal(new Set(a.matrix.map(x=>x.witness_id)).size,12);
 for(const row of a.matrix){ids(row.witness_id);ids(row.canonical_work_id);}
 const d=result.downstream;assert.equal(d?.scope,'NO_HELD_CURRENT_OR_GENUINE_FUTURE_EARNING_DISPOSITION');
 assert.equal(d.negativeCapacity,'CURRENT_ONLY','COMPILE_FINANCIAL_CURRENT_ONLY');assert.equal(d.receipts.length,12);
 assert.deepEqual(d.receipts.map(x=>x.spec).sort(),expected,'COMPILE_FINANCIAL_EXACT_12_DISPOSITIONS');
 assert.equal(new Set(d.receipts.map(x=>x.spec)).size,12);let transfers=0;
 for(const row of d.receipts){const spec=finiteMatrix.find(x=>x.key===row.spec);
  assert.equal(row.originalPaymentsUnchanged,true,'COMPILE_FINANCIAL_ORIGINAL_ASSERTIONS_REQUIRED');
  assert.equal(row.fullFinancialMatrix,false);assert.equal(row.providerCalls,0);assert.notEqual(row.futureGenuineSource,true);
  assert(row.futurePayment==null,'COMPILE_FINANCIAL_NO_FUTURE_SCOPE');minor(row.sourceResidual);minor(row.ownBResidual);
  assert(Array.isArray(row.allCases)&&Array.isArray(row.openExposures)&&Array.isArray(row.allocationResults));
  assert.equal(new Set(row.allCases.map(x=>x.case_component_id)).size,row.allCases.length,'COMPILE_FINANCIAL_DISJOINT_CASE_RESULTS');
  for(const c of row.allCases){assert(['SOURCE','OWN_B'].includes(c.kind));ids(c.source_component_id);ids(c.case_id);ids(c.case_component_id);
   assert.equal(c.source_pay_channel,spec.to);assert.equal(c.target_pay_channel,spec.to);nonnegative(c.sourceCaseOutstandingExVat);nonnegative(c.remainingTargetDebtExVat);}
  assert.deepEqual(row.openExposures,row.allCases.filter(x=>minor(x.sourceCaseOutstandingExVat)>0||minor(x.remainingTargetDebtExVat)>0),
   'COMPILE_FINANCIAL_COMPLETE_OPEN_EXPOSURES');
  if(row.actualFinancialExecution===false){assert(['UNFUNDED_NEGATIVE_EXPOSURES','ZERO_DIFFERENCE_NO_NEW_TRANSFER'].includes(row.kind));
   assert.equal(row.exactReplay,null,'COMPILE_FINANCIAL_NO_FAKE_NO_TRANSFER_REPLAY');assert.equal(row.execution,'NO_REAL_EARNINGS');
   assert.equal(row.allocationResults.length,0);assert(row.transfer_id==null);assert.equal(minor(row.paid),0);assert.equal(minor(row.recoveredSourceExVat),0);
   assert.equal(minor(row.deductedTargetExVat),0);assert.equal(minor(row.bankCash),0);assert(minor(row.sourceResidual)<=0&&minor(row.ownBResidual)<=0);
   if(row.kind==='UNFUNDED_NEGATIVE_EXPOSURES'){assert(row.allCases.length>0);assert.deepEqual(row.openExposures,row.allCases);}
   else{assert.equal(row.allCases.length,0,'COMPILE_FINANCIAL_NO_FAKE_ZERO_WITH_CASE');assert.equal(minor(row.sourceResidual),0);assert.equal(minor(row.ownBResidual),0);}
  }else{assert.equal(row.actualFinancialExecution,true);assert(['ACTUAL_GROSS_CASE_RECOVERY','ACTUAL_POSITIVE_DIFFERENCE_SETTLEMENT'].includes(row.kind));
   for(const f of ['run_id','run_worker_id','transfer_id','outcome_command_id'])ids(row[f]);transfers++;assert.equal(row.exactReplay,true);
   assert.equal(row.allocationResults.length,row.allCases.length,'COMPILE_FINANCIAL_COMPLETE_ALLOCATION_RESULTS');
   assert.deepEqual(row.allocationResults.map(x=>x.case_component_id).sort(),row.allCases.map(x=>x.case_component_id).sort());
   let source=0,target=0,inclusive=0;for(const x of row.allocationResults){
    for(const f of ['instruction_id','result_id','case_id','case_component_id','comparison_receipt_id','collection_id'])ids(x[f]);
    const c=row.allCases.find(y=>y.case_component_id===x.case_component_id);assert.equal(x.case_id,c.case_id);assert.equal(x.stage,'GROSS_DEDUCT');assert.equal(x.pass,'DRAFT');
    assert.equal(x.source_channel,c.source_pay_channel);assert.equal(x.target_channel,spec.to);source+=nonnegative(x.source);target+=nonnegative(x.target);
    assert.equal(minor(x.vat),0);assert.equal(minor(x.inclusive),minor(x.target));inclusive+=nonnegative(x.inclusive);}
   assert.equal(nonnegative(row.recoveredSourceExVat),source);assert.equal(nonnegative(row.deductedTargetExVat),target);
   assert(nonnegative(row.paid)>0);assert.equal(nonnegative(row.bankCash),minor(row.paid)-inclusive,'COMPILE_FINANCIAL_SAVED_RESULT_CASH_CONSERVATION');
   assert(minor(row.sourceResidual)<=0&&minor(row.ownBResidual)<=0,'COMPILE_FINANCIAL_POSITIVE_RESIDUAL_NOT_SETTLED');
   if(minor(row.bankCash)===0)assert.equal(row.execution,'INTERNAL_ZERO');else assert.equal(typeof row.execution,'string');
  }
 }
 assert.equal(result.actualDispositionVectors,transfers);assert.deepEqual(result.openExposures,
  d.receipts.flatMap(x=>x.openExposures.map(e=>({spec:x.spec,...e}))),'COMPILE_FINANCIAL_ALL_VECTOR_EXPOSURES');
 // No result transformation or synthesized PASS is returned.
}
function qualifyHistoricalPrerequisitePlan(p){
 const d=GUARD_DELTA,h=p.historicalMaterial;
 assert(h&&typeof h==='object','COMPILE_FINANCIAL_HISTORICAL_V11_REQUIRED');
 assert.deepEqual(p.guardAmendment,d,'COMPILE_FINANCIAL_EXACT_GUARD_DELTA');
 assert.equal(p.firstUseMaterialOverlaySha256,d.effectiveOverlaySha256,'COMPILE_FINANCIAL_CORRECTED_OVERLAY');
 assert.equal(h.firstUseMaterialOverlaySha256,d.originalOverlaySha256,'COMPILE_FINANCIAL_HISTORICAL_OVERLAY');
 const before=h.plan.functions.filter(x=>x.signature===d.signature),after=p.plan.functions.filter(x=>x.signature===d.signature);
 assert.equal(before.length,1);assert.equal(after.length,1);
 assert.deepEqual(before[0],d.beforeDeclaration,'COMPILE_FINANCIAL_HISTORICAL_GUARD_DECLARATION');
 assert.deepEqual(after[0],d.afterDeclaration,'COMPILE_FINANCIAL_CURRENT_GUARD_DECLARATION');
 assert.deepEqual({...p.plan,functions:p.plan.functions.map(x=>x.signature===d.signature?d.beforeDeclaration:x)},h.plan,
  'COMPILE_FINANCIAL_OTHER185_AND_DECLARATIONS_EXACT');
 return h;
}
export function qualifyFirstUsePrerequisite(p,E){
 const historical=qualifyHistoricalPrerequisitePlan(p);
 assert.equal(FIRST_USE_PREREQUISITE.status,'QUALIFIED','COMPILE_FINANCIAL_FIRST_USE_HELD');
 assert.match(FIRST_USE_PREREQUISITE.file??'',/^[A-Za-z0-9_./-]+\.json$/);assert.match(FIRST_USE_PREREQUISITE.sha256??'',/^[A-F0-9]{64}$/);
 const bytes=fs.readFileSync(path.join(E,FIRST_USE_PREREQUISITE.file));assert.equal(sha(bytes),FIRST_USE_PREREQUISITE.sha256);
 const actual=JSON.parse(bytes),r=actual.result,b=actual.readback;
 assert.equal(p.plan.unitCount,53);assert.equal(p.plan.functions.length,186);assert.equal(p.plan.createCount,199);
 assert.equal(r.kind,'SOURCE_MOVE_JOINED_FIRST_USE_V1');assert.equal(r.heldCaseAndRepeatedTails,false);
 assert.equal(r.permanentInstall,false);assert.equal(r.externalEffects,false);
 assert.equal(r.managedLedgerWrite,false);assert.equal(r.localCsvSimulatedPaymentsCompleted,true);
 assert.equal(r.prefixResult.genuineOriginalPayments,2);assert.equal(r.prefixResult.completeComparedComponents,2);
 assert.equal(r.disposition,FIRST_USE_PREREQUISITE.requiredDisposition);assert.equal(r.compilationOnly,false);assert.equal(r.prefixOnly,true);
 assert.equal(r.compiledUnits,p.plan.unitCount);assert.equal(r.expectedUnits,p.plan.unitCount);assert.equal(r.routineCount,p.plan.functions.length);
 assert.equal(r.overlaySha256,historical.firstUseMaterialOverlaySha256);assert.equal(r.rollbackSent,true);assert.equal(r.freshFullEquality,true);
 for(const f of ['primaryFailure','cleanupFailure','restorationFailure'])assert.equal(r[f],null);
 assert.equal(r.fingerprints.before,r.fingerprints.fresh);assert.equal(r.prefixResult.spec,FIRST_USE_PREREQUISITE.requiredSpec);
 assert.equal(r.prefixResult.exactReplay,true);assert.equal(r.providerCalls,0);assert.equal(r.fullMatrix,false);assert.equal(r.finalGatesComplete,true);
 assert.deepEqual(b.declarationPlan,historical.plan);assert.equal(b.firstUse.finalFullCatalogueEqual,true);assert.equal(b.firstUse.finalFullInventoryEqual,true);
 assert.equal(b.firstUse.finalFullSchemaEqual,true);assert.equal(b.firstUse.finalFullLedgerEqual,true);
 return {qualification:'GENUINE_V11_PREFIX_ONLY_HISTORICAL_GUARD',correctedGuardFirstUse:'NOT_INFERRED',currentPlanDelta:'EXACT_ONE_CAPTURE_GUARD_DECLARATION_ONLY',sha256:FIRST_USE_PREREQUISITE.sha256,disposition:r.disposition,compiledUnits:r.compiledUnits,routineCount:r.routineCount,
  rollbackSent:true,freshFullEquality:true,fullCatalogueAcceptance:false};
}
