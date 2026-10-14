// Finite internal consumer of actual native begin/publish receipts. The existing
// public review owner independently rederives every pointer/hash/session CAS.
export async function reviewWeeklyCorrectFinalPaidMatchRecheck(dependencies, request, begun, published) {
  const frame = plainObject(begun?.correction_review_identity,
    'WEEKLY_SOURCE_CORRECTION_REBUILD_IDENTITY_MISMATCH', 'Saved correction scope', 502);
  const fields = ['schema_version','correction_session_id','source_cycle_id',
    'expected_current_final_revision_id','expected_final_manifest_hash'];
  if (!sameJson(Object.keys(frame).sort(), fields.sort())
    || frame.schema_version !== 'WEEKLY_SOURCE_PAID_MATCH_CORRECTION_RECHECK_CONTEXT_V1'
    || begun.purpose !== 'FINAL_SOURCE_CORRECTION'
    || frame.correction_session_id !== request.correction_session_id
    || begun.correction_session_id !== frame.correction_session_id
    || begun.upload_id !== request.upload_id || published.upload_id !== request.upload_id
    || published.status !== 'CORRECTION_READY' || published.ok !== true
    || published.publication_id !== begun.publication_id
    || published.authority_scope_version !== request.expected_authority_scope_version) {
    fail('WEEKLY_SOURCE_CORRECTION_REBUILD_IDENTITY_MISMATCH', 'The saved correction scope changed.', 409);
  }
  const actorUserId = uuid(request.actor_user_id,'WEEKLY_SOURCE_ACTOR_REQUIRED','Office user');
  const identity = Object.freeze({
    sourceCycleId: uuid(frame.source_cycle_id,'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID','Source cycle'),
    scopeKind: 'CYCLE', reportScopeId: null,
    correctionSessionId: uuid(frame.correction_session_id,'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID','Correction session'),
    expectedCurrentFinalRevisionId: uuid(frame.expected_current_final_revision_id,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID','Current final source'),
    expectedFinalManifestHash: sha256(frame.expected_final_manifest_hash,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID','Current final-source proof'),
    replacementUploadId: uuid(published.upload_id,'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID','Replacement upload'),
    replacementProjectionPublicationId: uuid(published.publication_id,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID','Replacement comparison'),
    expectedAuthorityScopeVersion: integer(published.authority_scope_version,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID','Authority version',502,1),
    expectedRowManifestHash: sha256(request.expected_row_manifest_hash,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID','Replacement row proof'),
    expectedComparisonManifestHash: sha256(published.comparison_manifest_hash,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID','Replacement comparison proof'),
    expectedIssueSetHash: sha256(published.issue_set_hash,
      'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID','Replacement issue proof'),
  });
  const version=integer(published.ready_session_version,
    'WEEKLY_SOURCE_CORRECTION_VERSION_INVALID','Saved correction version',502,1);
  const reviewKey=uuid(request.request_id,'WEEKLY_SOURCE_CORRECTION_REQUEST_INVALID','Saved recheck reference')+':review';
  const reviewed=validateReviewed(await rpc(dependencies,'weekly_source_correct_final_review_atomic_v1',
    reviewRequest(actorUserId,identity,version,reviewKey)),identity,version);
  const context=correctionContext(identity,version,reviewKey,reviewed);
  return Object.freeze({ok:true,status:reviewed.status,correction_session_id:identity.correctionSessionId,
    changes:reviewed.office_preview.changes,blockers:reviewed.office_preview.blockers,
    confirmation_text:reviewed.office_preview.confirmation_text,
    paid_match_questions:reviewed.office_preview.paid_match_questions ?? Object.freeze([]),
    review_context:context,apply_context:reviewed.status==='READY_FOR_CONFIRMATION'?context:null});
}
