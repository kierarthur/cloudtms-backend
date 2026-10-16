import {readFileSync,writeFileSync,readdirSync}from'node:fs';
import{createHash}from'node:crypto';
import path from'node:path';
import{fileURLToPath}from'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),sha=value=>createHash('sha256').update(value).digest('hex');
const pin=file=>({path:file,sha256:sha(readFileSync(path.join(base,file)))});
const register=JSON.parse(readFileSync(path.join(base,'REGISTER_DELTA_WIP.json'),'utf8'));
const recipe={version:'CONTACT_COMMISSIONED_SOURCE_COMPLETION_RECIPE_V1',status:'NOT_FOR_MOUNT_NOT_REVIEWED_NATIVE_REQUIRED_NOT_RUN',
 authoritative_worktree:'C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-money-build-13102026',
 branch:'codex/healthroster-v2-money-build-13102026',source_base:'90ffcf3f40644280fcdb5b791a62a86613896d6e',
 untouched_frozen_sources:{financial:'d863577918ce7d439206296e91ad78de298e98b2',manual_request_types:'7161b1732fb682f78f9387e8ed9c9fe89c4e59f5'},
 assembly:{one_time:readdirSync(path.join(base,'one-time')).filter(file=>file.endsWith('.sqlinc')).sort().map(file=>pin('one-time/'+file)),
  repeatable:readdirSync(path.join(base,'forward')).filter(file=>file.endsWith('.sqlinc')).sort().map(file=>pin('forward/'+file)),
  order:'Root-owned exact current native schema and existing contact permits/query/membership/timing authorities; reviewed one-time journal/check/cohort deltas; these stable ordered native includes; final exact ACL. No stale whole-parent replacement and no Banking source overwrite.'},
 new_metadata_tables:register.tables,existing_table_identity_delta:register.existing_table_identity_delta,
 new_and_successor_signatures:register.functions.map(({signature,source,schema,security_definer,fixed_search_path,public:publicExecute,anon,authenticated,service_role_execute})=>({
  signature,source,schema,security_definer,fixed_search_path,public:publicExecute,anon,authenticated,service_role_execute})),
 root_only_mount_deltas:['Reconcile all nine exact metadata table record classes, creation-time FORCE RLS/policies/revokes and immutable/truncate guards with current central contract/literal sets/counts.',
  'Reconcile existing request indexes/constraint and affected-row deferred guards; preserve exact old mixed request IDs/generations/hash/start/reminder/deadline and fail unexplained overlaps.',
  'Reconcile signature exposure/private/final-definer metadata and exact current contract inventory for every listed owner; no browser EXECUTE grants.',
  'Consume existing root canonical manual request/types/strict reply/one-RPC Office handler; mount read contribution and routes/Office through sole root current owners.',
  'Apply only RUNTIME_CONTRIBUTION finite edits around actual current delivery runtime; continue original render, original sender, final START/provider-result owners and release pipeline.'],
 canonical_runtime:{source_commits:['1859790b0596761f405a10c258394a1d47ca304e','81d06f820af6670c3783f0a7d825be9fc5c5d767'],
  files:['broker/src/weekly-source/contact-manual-enquiry-request.mjs','broker/src/weekly-source/contact-manual-enquiry-types.d.ts',
   'broker/src/weekly-source/contact-manual-enquiry-office.mjs','broker/src/weekly-source/contact-manual-enquiry-contract.mjs','broker/src/contract-correction-v3.mjs'],
  duplicate_implementation:false,finite_contribution:pin('RUNTIME_CONTRIBUTION.json')},
 readonly_projection:{signature:'private.weekly_contact_manual_enquiry_read_v1(uuid,text,uuid,uuid)',
  version:'CONTACT_MANUAL_ENQUIRY_ROW_V1',mutations:false,complete_open_issue_bound:1000,request_selected_issue_bound:500,
  pause_row_contract:'Original seven keys unchanged; separate typed row property is a root combined-read contribution.'},
 reminder_refusal:{source:pin('forward/40_candidate_contact_entry_owners.sqlinc'),sqlstate:'55000',message:'CONTACT_PAUSED',
  detail_keys:['version','candidate_id','client_id','audience','next_allowed_utc','next_allowed_local'],
  version:'CONTACT_PAUSE_REFUSAL_V1',audience:'WORKER',utc:'Exact same native expiry, UTC6',local:'YYYY-MM-DD HH:mm Europe/London from existing server London formatter',
  root_http:'Existing narrow exact function/state/message/detail validation -> original failure envelope HTTP409; no new success receipt.'},
 authored_business_completion:['Genuine current Source subset, exact pre-render rows/counts/links/recipient/content and original logical allowance capture.',
  'Closed manual PREVIEW/ACCEPT, unchanged immutable replay, actual server current checking witness and current per-member eligibility/policy/history.',
  'Configured due MANUAL through existing Send manager now; explicit New enquiry does not renew routine vector.',
  'Grouped selected AUTOMATIC per real cycle/recipient with own per-member step/predecessor/ordinal and existing500rows/100Candidates/100Clients native body bounds.',
  'Existing common recheck task/cursor/native sender and actual future wake; exact one-observation selection/complete phase exhaustion; uncertainty stays with original owner.',
  'Bounded original scheduler selectors and complete sorted actual transaction footprint before any business row lock, with exact ID/currentness revalidation.',
  'Per-client first activation, preserved legacy mixed request reuse, unchanged public one-request success and atomic partial/separate-owner refusal; genuine private Import/Bulk aggregation.'],
 remaining_implementation_review:['Same selected group mixing INITIAL and due MANUAL followup currently explicitly refuses; truthful single-body native composition is still being traced.',
  'Review source/current native grouping/throttle/recomposition and retained alias/accepted-history/notification lineage under actual PostgreSQL and concurrency.'],
 qualification:{source_runtime_checks:37,offline_pg17:'OFFLINE_GRAMMAR_RESULT.json; complete SQL/PLpgSQL/SQL-language/DO grammar and declared fields only',
  native_fixtures:'Authored pg_temp assertion owners require genuine current source/actor and accepted history inputs; not executed or qualified.',
  native_compile_execution_acl_concurrency_api:'NOT_RUN',database_actions:'NONE',mount_install_release:'NOT_RUN'},work_continues:true};
writeFileSync(path.join(base,'SOURCE_COMPLETION_RECIPE.json'),JSON.stringify(recipe,null,2)+'\n');
console.log(JSON.stringify({source_recipe_sha256:sha(readFileSync(path.join(base,'SOURCE_COMPLETION_RECIPE.json'))),status:recipe.status}));
