/** Closed source-owned manual DTO. Revisions are canonical bigint decimal strings;
 * hash strings lowercase hex64; UUIDs canonical lowercase; UTC exactly6fractionalZ.
 * All eligibility/recipient/clocks/model facts are server-authored read results. */
export type UUID=string;
export type Hash=string;
export type Revision=string;
export type UTC=string;
export type Audience='WORKER'|'MANAGER';
export type ResolutionAction=null|{kind:'OPEN_SOURCE_REVIEW';target_type:'SOURCE_ROW';target_id:UUID;
 url:string;caption:'Review shift';enabled:boolean;disabled_reason:null|string};
export type CutoffClassification={version:'CONTACT_CUTOFF_CLASSIFICATION_V1';client_id:UUID;contract_id:UUID;work_date:string;
 source_group_id:UUID;settings_authority_sha256:Hash;
 hard_restriction:{present:false;reason:null;resolution_action:null;
  source:'NO_STRUCTURED_CONTACT_PROHIBITION_IN_REVIEWED_CURRENT_CLIENT_CONTRACT_SETTINGS'};
 report_cutoff:{classification:'REPORT_ONLY';weekday:0|1|2|3|4|5|6;local_time:string;cutoff_at_utc:UTC|null};
 routine:{in_scope:boolean;reason:null|'WEEK_NOT_COMPLETED'|'MAXIMUM_ROUTINE_WORK_AGE'|'OUTSIDE_RECENT_COMPLETED_WEEKS';
  manual_override_permitted:true;recent_completed_weeks:number;max_work_age_days:number;policy_revision:Revision}};
export type ManagerRow={clientId:UUID;clientName:string;candidateId:UUID;displayName:string;issueId:UUID;workDate:string;
 sourceStartInstant:string;start:string;end:string;breakMinutes:number;systemStart:string|null;systemEnd:string|null;
 systemBreakMinutes:number|null;systemAbsent:boolean;issueFamily:'HOURS_DIFFER'|'NHSP_ABSENT'|'SOURCE_ABSENT'|'HEALTHROSTER_NOT_FINALISED';
 candidateRequested:boolean};
export type PreviewModel={kind:'MANAGER_QUERY_MODEL_V1';policy_version:'1.8.0';renderer_version:'1.4.0';structure_version:'1.0.0';
 rows:ManagerRow[];plain_body:null;attachment_count:0}|{kind:'WORKER_QUERY_MODEL_V1';policy_version:'weekly-source-candidate-push-v1';
 renderer_version:'1.0.0';structure_version:null;rows:[];
 plain_body:'Open MyTMS to check your Timesheet hours.'|'Open MyTMS to submit your Timesheet hours.';attachment_count:0};
export type MemberReview={issue_id:UUID;eligible:boolean;exclusion_reason:null|string;resolution_action:ResolutionAction;
 same_purpose_reason:string;last_accepted_send:null|{accepted_at_utc:UTC;send_ordinal:number};
 next_routine_step:null|{sequence:number;mode:'MANUAL'|'AUTOMATIC';due_at_utc:UTC};cooldown_until_utc:UTC|null;
 cooldown_remaining_seconds:number;
 cutoff:CutoffClassification};
export type ScopeReview={candidate_id:UUID;client_id:UUID;scope_revision:Revision;checking_revision:Revision;
 permission:{allowed:true;reason:null};members:MemberReview[]};
export type ManualGroup={group_key:Hash;source_cycle_id:UUID;
 selected_issue_ids:UUID[];canonical_content_sha256:Hash;preview:PreviewModel}&(
 {audience:'MANAGER';channel:'EMAIL';recipient_route_id:UUID|null;candidate_generation_id:null;
  recipient:{candidate_id:null;manager_address:string}}|
 {audience:'WORKER';channel:'PUSH';recipient_route_id:null;candidate_generation_id:UUID;
  recipient:{candidate_id:UUID;manager_address:null}});
export type ManualReview={ok:true;version:'CONTACT_MANUAL_ENQUIRY_REVIEW_V1';request_id:UUID;review_id:UUID|null;review_digest:Hash|null;
 audience:Audience;reason:string;can_accept:boolean;scope_results:ScopeReview[];groups:ManualGroup[];expires_at_utc:UTC|null};
export type ManualReceipt={ok:true;version:'CONTACT_MANUAL_ENQUIRY_RECEIPT_V1';request_id:UUID;review_id:UUID;review_digest:Hash;
 audience:Audience;reason:string;accepted_at_utc:UTC;scope_results:ScopeReview[];
 groups:{group_key:Hash;message_intent_id:UUID;members:{issue_id:UUID;logical_contact_key:Hash;send_ordinal:number;contact_kind:'EXPLICIT_MANUAL'}[]}[];
 provider_submission_started:false};
/** Distinct property on existing bounded combined row; never extends the original
 * seven-key CONTACT_CONTROLS_ROW_V1. Invoke only under that read's current actual
 * Office/contact actor permission; otherwise omit/redact in its existing owner. */
export type ManualEnquiryRow={version:'CONTACT_MANUAL_ENQUIRY_ROW_V1';candidate_id:UUID;client_id:UUID;
 scope_revision:Revision;checking_revision:Revision;checking_facts_sha256:Hash;available_issue_ids:UUID[];
 actions:{operation:'CONTACT_MANUAL_ENQUIRY_PREVIEW';audience:Audience;label:'New enquiry';enabled:boolean;
  disabled_reason:null|'NO_OPEN_SOURCE_ISSUES'}[]};

/** Limits: 1..100 requested unique scopes, 1..500 total unique selected issues.
 * Scope/member outputs cover every requested tuple, without truncation.
 * Groups partition exactly the eligible issue IDs; each eligible issue occurs once.
 * Groups empty is legal => can_accept=false/review_id,digest,expires allnull.
 * Partial exclusions are legal: can_accept=true iff >=1 eligible group and all
 * expected scope/checking revisions matched. ACCEPT enqueues only those exact
 * reviewed groups and retains every excluded member in the immutable receipt.
 * Any changed eligibility/recipient/report/reply/pause/current facts requires a
 * fresh review (40001), never silent subset shrinking at ACCEPT.
 * Display names1..500codepoints; reason1..1000, address1..320, exclusion/same-purpose
 * reason1..500. HH:MM for row clocks; break0..10080integer; workDate valid YYYY-MM-DD;
 * sourceStartInstant valid native local ISO timestamp (no fabricated timezone).
 * report local_time HH:MM:SS[.1..6digits]; weekday0..6; recent weeks1..52/age1..366;
 * next sequence1..10; send ordinal1..2147483647. Hashes are opaque new domains:
 * canonical_content_sha256 seals exact recipient+selected model+template versions,
 * excluding future secret URL; review_digest seals the accepted request+manifest.
 * Neither is the later genuine WEEKLY_MESSAGE_RENDER_V1 provider-body hash.
 * For this contract resolution_action uses only the exact shared OPEN_SOURCE_REVIEW
 * variant above (or null), validated by canonical shared validateAction; no new action.
 * Manager address presentation requires current authenticated Admin/contact scope.
 * Exact replay returns identical stored ManualReceipt with no replay/time mutation.
 * cooldown_remaining_seconds is integer0..2147483647, sampled once from actual
 * server current clock after locks:0 for null/elapsed cooldown, otherwise ceiling
 * positive remaining UTC seconds. Saved review/receipt keep that At-review value.
 * ACCEPT rechecks absolute cooldown/current eligibility with fresh server time;
 * it never rewrites replay/display seconds or makes browser clock authoritative.
 */
