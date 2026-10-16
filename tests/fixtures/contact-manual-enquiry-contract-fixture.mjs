const id=n=>'00000000-0000-0000-0000-'+String(n).padStart(12,'0');
const hex=n=>String(n).repeat(64),time='2026-10-16T09:00:00.123456Z';
const copy=v=>JSON.parse(JSON.stringify(v));
function fixture(){
 const request={version:'CONTACT_MANUAL_ENQUIRY_V1',request_id:id(1),audience:'MANAGER',reason:'The original manager email was missed.',
  scopes:[{candidate_id:id(2),client_id:id(3),selected_issue_ids:[id(4),id(5)],expected_scope_revision:'9007199254740993',expected_checking_revision:'7'}]};
 const cutoff={version:'CONTACT_CUTOFF_CLASSIFICATION_V1',client_id:id(3),contract_id:id(6),work_date:'2026-10-09',source_group_id:id(7),settings_authority_sha256:hex(1),
  hard_restriction:{present:false,reason:null,resolution_action:null,source:'NO_STRUCTURED_CONTACT_PROHIBITION_IN_REVIEWED_CURRENT_CLIENT_CONTRACT_SETTINGS'},
  report_cutoff:{classification:'REPORT_ONLY',weekday:4,local_time:'09:00:00',cutoff_at_utc:time},
  routine:{in_scope:false,reason:'MAXIMUM_ROUTINE_WORK_AGE',manual_override_permitted:true,recent_completed_weeks:4,max_work_age_days:28,policy_revision:'9007199254740995'}};
 const member={issue_id:id(4),eligible:true,exclusion_reason:null,resolution_action:null,same_purpose_reason:'A useful deliberate follow-up.',
  last_accepted_send:{accepted_at_utc:time,send_ordinal:2},next_routine_step:null,cooldown_until_utc:null,cooldown_remaining_seconds:0,cutoff};
 const scope={candidate_id:id(2),client_id:id(3),scope_revision:'9007199254740993',checking_revision:'7',permission:{allowed:true,reason:null},
  members:[member,{...copy(member),issue_id:id(5),eligible:false,exclusion_reason:'WORK_EVIDENCE_REQUIRED'}]};
 const row={clientId:id(3),clientName:'Fixture client',candidateId:id(2),displayName:'Fixture worker',issueId:id(4),workDate:'2026-10-09',
  sourceStartInstant:'2026-10-09T09:00:00',start:'09:00',end:'17:00',breakMinutes:30,systemStart:'09:00',systemEnd:'16:00',systemBreakMinutes:30,
  systemAbsent:false,issueFamily:'HOURS_DIFFER',candidateRequested:true};
 const group={group_key:hex(2),source_cycle_id:id(8),recipient_route_id:id(9),candidate_generation_id:null,audience:'MANAGER',channel:'EMAIL',
  recipient:{candidate_id:null,manager_address:'fixture@agency.example'},selected_issue_ids:[id(4)],canonical_content_sha256:hex(3),
  preview:{kind:'MANAGER_QUERY_MODEL_V1',policy_version:'1.8.0',renderer_version:'1.4.0',structure_version:'1.0.0',rows:[row],plain_body:null,attachment_count:0}};
 const result={ok:true,version:'CONTACT_MANUAL_ENQUIRY_REVIEW_V1',request_id:id(1),review_id:id(10),review_digest:hex(4),audience:'MANAGER',
  reason:request.reason,can_accept:true,scope_results:[scope],groups:[group],expires_at_utc:time};
 const accept={...request,review_id:result.review_id,review_digest:result.review_digest};
 const saved={ok:true,version:'CONTACT_MANUAL_ENQUIRY_RECEIPT_V1',request_id:id(1),review_id:id(10),review_digest:hex(4),audience:'MANAGER',reason:request.reason,
  accepted_at_utc:time,scope_results:copy(result.scope_results),groups:[{group_key:hex(2),message_intent_id:id(11),members:[{issue_id:id(4),logical_contact_key:hex(5),send_ordinal:3,contact_kind:'EXPLICIT_MANUAL'}]}],provider_submission_started:false};
 return {request,result,accept,saved};
}

export {id,hex,time,copy,fixture};
