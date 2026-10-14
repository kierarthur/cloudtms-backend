function normaliseCorrectFinalPaidQuestions(value) {
  if (value === undefined) return Object.freeze([]);
  if (!Array.isArray(value) || value.length>10000) {
    fail('WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID','Paid-shift questions are invalid.',502);
  }
  const seen=new Set();
  return Object.freeze(value.map(raw=>{
    const row=plainObject(raw,'WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID','Paid-shift question',502);
    if (!sameJson(Object.keys(row).sort(),['candidate','client','shift','system_hours','paid_match_question'].sort()))
      fail('WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID','Paid-shift question is invalid.',502);
    const q=plainObject(row.paid_match_question,'WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID','Paid-shift question',502);
    const keys=['schema_version','state','reason_code','upload_row_id','upload_id','projection_publication_id',
      'candidate_id','client_id','contract_id','expected_authority_scope_version','expected_row_manifest_hash',
      'expected_paid_overlap_inputs_sha256','choices'];
    if (!sameJson(Object.keys(q).sort(),keys.sort()) || q.schema_version!=='WEEKLY_SOURCE_PAID_MATCH_QUESTION_V1'
      || q.state!=='READY' || q.reason_code!==null || !Array.isArray(q.choices) || !q.choices.length || q.choices.length>100
      || typeof q.expected_authority_scope_version!=='number' || !Number.isSafeInteger(q.expected_authority_scope_version)
      || q.expected_authority_scope_version<1) fail('WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID','Paid-shift question is invalid.',502);
    for (const key of ['upload_row_id','upload_id','projection_publication_id','candidate_id','client_id','contract_id']) {
      if (typeof q[key]!=='string' || !UUID_PATTERN.test(q[key]))
        fail('WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID','Paid-shift identity is invalid.',502);
    }
    for (const key of ['expected_row_manifest_hash','expected_paid_overlap_inputs_sha256']) {
      if (typeof q[key]!=='string' || !SHA256_PATTERN.test(q[key]))
        fail('WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID','Paid-shift proof is invalid.',502);
    }
    if (seen.has(q.upload_row_id)) fail('WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID','Paid-shift row is duplicated.',502);
    seen.add(q.upload_row_id);
    const targets=new Set();
    const choices=q.choices.map(item=>{
      if (!item || typeof item!=='object' || Array.isArray(item)
        || !sameJson(Object.keys(item).sort(),['work_event_id','work_date','start','end','break_minutes','contract_id','same_allowed'].sort())
        || typeof item.work_event_id!=='string' || !UUID_PATTERN.test(item.work_event_id)
        || typeof item.contract_id!=='string' || !UUID_PATTERN.test(item.contract_id)
        || typeof item.work_date!=='string' || !/^\d{4}-\d{2}-\d{2}$/.test(item.work_date)
        || ![item.start,item.end].every(v=>typeof v==='string'&&/^(?:[01]\d|2[0-3]):[0-5]\d$/.test(v))
        || (item.break_minutes!==null&&(!Number.isSafeInteger(item.break_minutes)||item.break_minutes<0))
        || typeof item.same_allowed!=='boolean' || item.same_allowed!==(item.contract_id===q.contract_id)
        || targets.has(item.work_event_id)) fail('WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID','Paid-shift target is invalid.',502);
      targets.add(item.work_event_id);return Object.freeze({...item});
    });
    return Object.freeze({candidate:text(row.candidate,'WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID','Candidate',502),
      client:text(row.client,'WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID','Client',502),
      shift:text(row.shift,'WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID','Shift',502),
      system_hours:text(row.system_hours,'WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID','Imported hours',502),
      paid_match_question:Object.freeze({...q,choices:Object.freeze(choices)})});
  }));
}
