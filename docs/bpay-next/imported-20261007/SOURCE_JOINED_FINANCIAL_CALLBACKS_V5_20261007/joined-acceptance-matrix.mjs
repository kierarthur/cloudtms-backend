// Finite actual-owner fixture composition. Import/default execution has no I/O.
// Root alone supplies its approved isolated rollback RPC/query/financial owners.
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {assertUnboundPoint} from '../SOURCE_MOVE_JOINED_FIRST_USE_V11_20261007/fixture-readiness.mjs';
const sha=x=>createHash('sha256').update(x).digest('hex');
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const csv=x=>/[",\r\n]/.test(String(x))?'"'+String(x).replaceAll('"','""')+'"':String(x);
const clone=x=>JSON.parse(JSON.stringify(x));

// Actual raw HealthRoster layout B, not a staged row/economic/financial patch.
// One-hour is the default. A two-hour initial bootstrap may precede the genuine
// later one-hour Final/APP where initial authority is TSFIN rather than HEAD.
export function rawWorkedCsv({requestId,staff,workDate,bootstrap=false}){
  assert.ok(requestId&&staff);assert.match(workDate,/^\d{4}-\d{2}-\d{2}$/);
  const headers=['Request Id','Status','Staff','Date','Start','End','Actual Start','Actual End',
    'Actual Break','Actual Hours','Timesheet Finalised By'];
  const row=[requestId,'Timesheet Finalised',staff,workDate,'09:00',bootstrap?'11:00':'10:00',
    '09:00',bootstrap?'11:00':'10:00',0,bootstrap?'2:00':'1:00','Joined TEST Manager'];
  const bytes=new TextEncoder().encode(headers.map(csv).join(',')+'\r\n'+row.map(csv).join(',')+'\r\n');
  return {bytes,fileName:requestId+'.csv',byteCount:bytes.byteLength,sha256:sha(bytes),
    profile:'HEALTHROSTER_WEEKLY_EXPLICIT_ACTUAL_V1',expectedMinutes:bootstrap?120:60};
}

// Same saved producer chain as factual produceSourceBatch, with raw artifact
// parameter instead of its fixed 8-hour template. No post-parser overrides.
export async function produceParsedFinal({p,b,artifact,owners,env,dataRpc,qualifyProducedRoot,first=false}){
  assert.equal(typeof dataRpc,'function');assert.equal(typeof qualifyProducedRoot,'function');
  assert.equal(b.rows.length,1,'FINITE_ONE_ACTUAL_REPORT_ROW');
  const actor={id:p.actor},upload=owners.m300Upload({dataRpc});
  const body={source_group_id:b.group,source_cycle_id:b.cycle,client_id:b.client,
    profile_id:artifact.profile,original_filename:artifact.fileName,import_use:'CHECKING'};
  const preview=await upload.previewUpload({body,bytes:artifact.bytes,actor,parseWeeklySourceFile:owners.m300Parse});
  assert.equal(preview.parsed.ok,true);assert.equal(preview.parsed.rows.length,1);
  assert.equal(preview.parsed.rows[0].actual.totalMinutes,artifact.expectedMinutes);
  assert.equal(preview.parsed.rows[0].actual.breakMinutes,0);
  Object.assign(body,preview.accept_context);
  const accepted=await upload.acceptUpload({body,bytes:artifact.bytes,actor,parseWeeklySourceFile:owners.m300Parse});
  assert.equal(accepted.status,'CURRENT','ACTUAL_UPLOAD_OWNER_ACCEPT');
  const context=await dataRpc('weekly_source_upload_context_v1',{p_request:{operation:'BUILD_PROJECTION',
    actor_user_id:p.actor,source_group_id:b.group,source_cycle_id:body.source_cycle_id,
    report_scope_id:body.report_scope_id??null,client_id:b.client,upload_id:accepted.upload_id}});
  assert.equal(context.rows.length,1);const row=context.rows[0],fact=b.rows[0];
  assert.equal(row.candidate_matches.length,1);assert.equal(row.contracts.length,1);
  assert.equal(row.candidate_matches[0].candidate_id,fact.candidate);
  assert.equal(row.contracts[0].contract_id,fact.contract);
  const prepared=await dataRpc('weekly_source_import_prepare_atomic_v1',{p_request:{actor_user_id:p.actor,
    upload_id:accepted.upload_id,projection_publication_id:accepted.publication_id,
    expected_authority_scope_version:accepted.authority_scope_version,expected_row_manifest_hash:context.row_manifest_hash}});
  assert.equal(prepared.status,'PREPARED');
  let builds=0;
  const final=await owners.m300Finalise({actor,env,ctx:{},request:{actor_user_id:p.actor,
    authority_scope_kind:'CYCLE',source_cycle_id:body.source_cycle_id,report_scope_id:null,
    upload_id:accepted.upload_id,projection_publication_id:accepted.publication_id,
    expected_authority_scope_version:accepted.authority_scope_version,expected_row_manifest_hash:context.row_manifest_hash,
    expected_comparison_manifest_hash:context.comparison_manifest_hash,expected_issue_set_hash:context.issue_set_hash},
    dependencies:{dataRpc,buildOrdinaryServiceSnapshot:async input=>{builds++;
      return owners.weeklySourceScenarioJourneyInternals.buildWeeklyCorrectFinalServiceSnapshot(env,input);}}});
  assert.equal(final.status,'FINALISED');assert.equal(builds,1,'GENUINE_SERVICE_ECONOMIC_ROW_OWNER');
  const tasks=final.ordinary_pay_projection.tasks;assert.equal(tasks.length,1);
  assert.equal(tasks[0].state,first?'PREPARED_FOR_AUTHORISATION':'PROPOSED');
  const root=tasks[0].root_timesheet_id,scope=await qualifyProducedRoot(root);
  assert.equal(scope.candidate_id,fact.candidate);assert.equal(scope.contract_id,fact.contract);
  assert.equal(scope.week_ending_date,fact.week);assert.equal(scope.is_current,true);
  if(first){
    const authorised=await dataRpc('weekly_source_first_authorise_v1',{p_timesheet_id:root,p_expected_timesheet_id:root,
      p_expected_row_signature:null,p_actor_user_id:p.actor});assert.equal(authorised.ok,true);
  }
  assert.ok(UUID.test(final.source_finalisation.final_revision_id),'ACTUAL_FINALISATION_REVISION');
  return {root_timesheet_id:root,final_revision_id:final.source_finalisation.final_revision_id,
    artifact_sha256:artifact.sha256,input_minutes:artifact.expectedMinutes,final,scope};
}

export const finiteMatrix=Object.freeze(['PAYE','UMBRELLA'].flatMap(from=>['PAYE','UMBRELLA'].flatMap(to=>
  [-3,0,3].map(difference=>({key:`${from}_${to}_${difference}`,from,to,difference,
    originalPaid:20,selectedPaid:from===to?20:17,destinationRate:(from===to?20:17)+difference,
    selectedMode:from===to?'NATIVE_IDENTITY':'MANUAL_REPLACEMENT_RATE',manualRate:from===to?null:17,
    ownB:true})))));

function assertSourcePoint(point,expected){
  assert.ok(UUID.test(point.canonical_work_id)&&UUID.test(point.source_component_id));
  assert.equal(point.source_book_state,'READY');assert.equal(point.binding_control_state,'APPLIED');
  assert.equal(point.binding_worker_state,'DONE');assert.equal(point.receipt_qualification_state,'READY');
  for(const [key,value]of Object.entries(expected))assert.equal(Number(point[key]),value,key);
}

// One finite complete family, not a framework or an auto-dispatched proof. Every
// operation callback must be the existing actual owner identified in readiness.
// A callback failure/refusal ends this run; there is no fake completion fallback.
export async function runFiniteJoinedAcceptanceMatrix({createActualPair,settleFirstPayment,joinedOwner,readPoint,
  makeActualMoveRequest,explicitReviewedSelections,driveActualAcceptance,onAcceptedVector=null}){
  for(const f of [createActualPair,settleFirstPayment,readPoint,makeActualMoveRequest,explicitReviewedSelections,
    driveActualAcceptance])assert.equal(typeof f,'function','REAL_OWNER_BINDING_REQUIRED');
  assert.equal(typeof joinedOwner?.preview,'function');assert.equal(typeof joinedOwner?.accept,'function');
  assert(onAcceptedVector===null||typeof onAcceptedVector==='function','EXACT_OPTIONAL_ACCEPTED_VECTOR_HOOK');
  const completed=[];
  for(const spec of finiteMatrix){
    const pair=await createActualPair(clone(spec));
    assert.ok(UUID.test(pair.actor.id)&&UUID.test(pair.sourceRoot)&&UUID.test(pair.destinationRoot));
    const before=await readPoint(pair,'SOURCE');
    assertUnboundPoint(before);
    const original=await settleFirstPayment(pair,spec.from,20);
    assert.ok(UUID.test(original.effect_id)&&UUID.test(original.approved_line_id),'REAL_FIRST_PAYMENT_NOT_SEED');
    const paid=await readPoint(pair,'SOURCE');
    assert.equal(paid.canonical_work_id,before.canonical_work_id);
    assert.equal(Number(paid[spec.from==='PAYE'?'native_realised_paye':'native_realised_umbrella']),20);
    const baseRequest=await makeActualMoveRequest(pair,spec);
    const financialSelections=await explicitReviewedSelections(pair,spec,baseRequest);
    assert.equal(financialSelections.find(x=>x.component_id===paid.source_component_id).selected_mode,spec.selectedMode);
    const reviewed=await joinedOwner.preview({actor:pair.actor,baseRequest,financialSelections});
    assert.equal(reviewed.joined_review.financial_context.schema_version,'BPAY_NEXT_SOURCE_FINANCIAL_CONTEXT_V3');
    const scope=reviewed.joined_review.financial_context.financial_scope;
    assert.equal(scope.moved_component_ids.length,1);assert.equal(scope.own_b_component_ids.length,1);
    assert.equal(scope.comparison_component_ids.length,2,'REAL_COMPLETE_A_PLUS_OWN_B');
    const args={actor:pair.actor,reviewed,idempotencyKey:'joined-native:'+spec.key,reason:'Finite genuine move family '+spec.key};
    const accepted=await joinedOwner.accept(args);assert.ok(UUID.test(accepted.witness_id));
    await driveActualAcceptance(pair,accepted);
    const after=await readPoint(pair,'SOURCE');
    assert.equal(after.canonical_work_id,before.canonical_work_id,'DIRECT_FLAT_ORIGINAL');
    assertSourcePoint(after,{comparable_paid_ex_vat:spec.selectedPaid,comparable_held_ex_vat:0,
      approved_source_ex_vat:spec.destinationRate,selected_residual_ex_vat:spec.difference});
    const own=await readPoint(pair,'OWN_B');
    const ownPlan=reviewed.joined_review.financial_plan.components.find(x=>x.source_component_id===own.source_component_id)?.plan;
    assert(ownPlan,'REAL_OWN_B_SAVED_PLAN');
    assertSourcePoint(own,{approved_source_ex_vat:spec.destinationRate,
      comparable_paid_ex_vat:Number(ownPlan.comparable_paid_ex_vat),comparable_held_ex_vat:Number(ownPlan.comparable_held_ex_vat),
      selected_residual_ex_vat:Number(ownPlan.selected_residual_ex_vat)});
    assert.equal(own.target_pay_channel,spec.to,'OWN_B_NOT_OLD_CHANNEL_RELABELED');
    const snapshot=clone(after),replay=await joinedOwner.accept(args);
    assert.equal(replay.witness_id,accepted.witness_id);assert.deepEqual(await readPoint(pair,'SOURCE'),snapshot);
    completed.push({key:spec.key,witness_id:accepted.witness_id,canonical_work_id:after.canonical_work_id});
    if(onAcceptedVector)await onAcceptedVector({pair,spec,reviewed,accepted});
  }
  return {status:'FINITE_ACTUAL_OWNER_ACCEPTANCE_MATRIX_COMPLETED',matrix:completed,
    scope:'ALL_FOUR_DIRECTIONS_THREE_ACCEPTED_RESIDUALS',acceptedVectors:12,
    downstreamDispositionAward:false,heldCaseAndRepeatedTails:false,fullFinancialMatrix:false,
    hosted_route_proof:false,provider_calls:0};
}
