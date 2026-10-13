import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
const root=path.resolve(process.argv[2]);
const local=f=>new URL(f,import.meta.url);
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const source=JSON.parse(fs.readFileSync(local('VERIFIER_SOURCE_CENSUS_0_55.json')));
const owners=JSON.parse(fs.readFileSync(local('VERIFIER_OWNER_SOURCE_CENSUS.json'))).owners;
const ownerMap=new Map(owners.map(o=>[o.name,o]));
const releaseBytes=fs.readFileSync(path.join(root,'supabase/release/current-release.json'));
const release=JSON.parse(releaseBytes);
const catalogue=new Set([12,13,14,15,17,20,22,23,30,33,38,40,42,48,51,52]);
const read=new Set([4,8,9,16,29]);
const reasons=[
 'Fixture generation publication and missing-choice reconciliation mutate only Candidate Daily database authority/receipts and projection outbox rows. No Sheet executor or queue claim/drain is invoked.',
 'Fixture legacy availability writes canonical preferences, command receipts and projection outbox rows only. The legacy owner contains no external executor.',
 'Session-owned notification page, visibility and SNAPSHOT/DELETE/UNDO/MARK_READ owners read or mutate database inbox/operation rows. push_state is fixture data; no push dispatch or claim owner is invoked.',
 'Expense policy/route checks and PAPER preparation create fixture workflow/carrier/document records and an explicitly held mail outbox item. Dynamic PAPER calls resolve to enqueue owners; neither sends mail nor drains queues.',
 'Catalogue ACL assertions plus empty-facts contact-policy result and browser-role denial. The policy computes eligibility; it does not dispatch contact messages.',
 'Synthetic NHSP Client setup calls get/save settings owners only. Included capture guards reject mutation of pre-existing watched rows; full fingerprints are dynamic read-only SELECTs.',
 'Synthetic released-review rows are read through workspace/scopes/review owners. No finalisation or financial mutation RPC is called. Includes install only rollback-local capture/fingerprint helpers.',
 'Emergency-roster reader/publisher and fixture-specific DNA effect receipt claim/completion are DB-only. Complete uses UNKNOWN with no provider reference; the external executor is never called.',
 'Catalogue ownership/ACL/immutability assertions and deliberately invalid/browser-role accepted-publication record/verify calls. Both fail at admission before business mutation.',
 'Catalogue checks and two deliberately invalid protected complete_local calls (browser role and empty service request). No valid completion/publication/payment scope is supplied.',
 'Fixture PAPER preparation, QR proof/component/pack owners and explicit fixture DOCUMENT_PLAN BUILD_MANIFEST advancement are database-only. The planner queues SOURCE_RENDER input work; no global document drain, renderer, mail sender or FINAL_ISSUE call runs.',
 'Sealed physical-rate projection and finance-effect JSON normalisation operate on disposable private build facts and temporary vectors. Synchroniser/Draft/payment names appear in inspected definition strings only, not invoked calls.',
 'Catalogue relation/view/RPC/sequence/default-ACL hashes and MFA execution-grant assertions only; no application routine is executed.',
 'Catalogue security, owner, argument/result and ACL checks for the two compatibility routines only; neither is executed.',
 'Catalogue named table/view/Candidate RPC inventory and MFA ACL assertions only.',
 'Catalogue Candidate/MyTMS table, view and function grant assertions only. Delete function names are regprocedure literals, not calls.',
 'Catalogue/security/settings consistency plus manager email settings GET. Templates are read; no enqueue, claim, sender or retirement mutation is called.',
 'Catalogue private helper schema/function ACL only. Email claim helper is an identity literal, not executed.',
 'Pure authoriser-policy/email-admission normalisers plus fixture Client/Contract policy CAS saves and replay/negative checks. No message dispatch follows policy save.',
 'Fixture membership-ready activation and exact replay use DB-only authority transition, entitlement and sync-state owners. No service target activation or external Sheet executor is called.',
 'Reads configured Candidate system actor and asserts stored role/activity/pay-flag posture; no application call or mutation.',
 'Rollback-local summary cursor/revision upsert and temporary trigger fixtures; rpc_changes_ping reads change cursors. Trigger helpers only maintain database revision rows.',
 'Reads the timesheet-page definition and ACL, then tests required expression strings. No timesheet-page call or mutation.',
 'Reads workflow/replacement/card definitions and searches literal predicates. No workflow transition, resubmission or approval executes.',
 'Fixture Rota generation and legacy preference owners clear only matching fixture days and enqueue their exact projection rows. No projection claim, executor or drain.',
 'Fixture availability writes, Rota reads, freshness reads and completion of one explicitly selected fixture projection receipt. Completion acknowledges a supplied receipt; it never sends to Sheets or globally claims work.',
 'Fixture sync-state refresh and generation publication; refresh aggregates database projection state and does not claim/drain it.',
 'Fixture Google Rota remove/attach and generation publication mutate database identity/entitlement/history and enqueue ordinary derived rechecks. Pending/uncertain external receipts are fixture rows; no normal drain or external executor is invoked.',
 'Fixture Daily workflow/approval/document receipt/canonical TS/TSFIN/delete/reject/authorise owners are DB-only. Financial snapshot processing/authorisation is not a payment execution; derived outbox/jobs can be enqueued but no worker/global drain is called.',
 'Private Daily break-context reads and invalid/missing-Client assertions; no application rows are written.',
 'Projection lease recovery function is read with pg_get_functiondef and tested as text/ACL. No claim call executes.',
 'EXCLUDE: invokes candidate_daily_projection_claim_v1 twice with a global target and no Candidate filter, recovering/claiming up to 100 genuine hosted queue rows. Fixture-only completion does not confine the global claim. Root explicitly confirmed exclusion under the no-queue-drain rule.',
 'Catalogue contract/security checks and missing-session read/selection calls. Native modal context rejects the nonexistent session before any selection or financial mutation.',
 'Catalogue definition SHA/ACL checks only. Draft, provider-adjacent, authorisation, QR enqueue and worker_drain identities are strings, never CALL/PERFORM/EXECUTE targets here.',
 'Exact fixture-session replaced-queue replay/terminalisation creates only target refresh instructions. No worker drain or Draft/payment/provider/settlement owner is invoked.',
 'Fixture session/candidate queue replay and repair replace refresh instructions through canonical enqueue. Scope is the fixture target session; no work is drained or financially executed.',
 'Fixture-specific discarded-session blocker repair and serial-state read affect only fixture jobs/attempts/build states. No source build execution, Draft or payment operation.',
 'Fixture version-one refresh enqueues one canonical Candidate source-build job and tests replay. No source worker drain or financial execution.',
 'Catalogue reconciliation definition/refresh-call count/security checks only; reconciliation is not invoked.',
 'Fixture-only reconciliation batches classify linked/unlinked identities and update DB sync/batch receipts. No outbox claim/drain/external executor.',
 'Catalogue identity-probe definition/security/ACL assertions only.',
 'Fixture-only identity probe invokes reconciliation with probe_only; it records batch replay authority while asserting no availability/projection/sync/command mutation.',
 'Workflow function definition and ACL inspection only. Expense resubmission is text, not executed.',
 'Fixture expense carrier resolver and CREATE workflow only, with independent route/anchor checks. The final section resolves reuse of a reserved carrier; it does not invoke actual finalisation, payment or mail sending.',
 'Fixture dependency closure and pay_sync_overpayments_from_preview use an owner-scoped zero-Timesheet build. The explicit AUTHORITATIVE_EMPTY path returns before finance-case DML, then only seals empty build state. No Draft/provider/payment/settlement call.',
 'Fixture session/Candidate scoped invalid source-build claim is expected to raise before valid attempt/build admission, inside a caught subtransaction. Repair/enqueue targets that fixture session only. No global claim, worker drain or financial execution.',
 'Fixture action-list/summary readers and sort negatives only. Preview/payment-label rows are synthetic presentation data, not payments.',
 'Fixture action-list/summary/eligible-member readers over a synthetic unresolved pay-advance case. No recovery/payment/settlement execution owner is called.',
 'Timesheet-page definition/ACL inspection only. Workflow state names and content predicates are strings.',
 'Rollback-contained information-directory settings CAS update/replay and Candidate read. No email or network call; map_query is stored display data.',
 'Fixture emergency-window readers and DB roster publication only. No external effect claim or executor in this verifier.',
 'Catalogue proof validator security/volatility/search_path/ACL and definition string checks only; validator is not invoked.',
 'Catalogue expense-apply definition/ACL/owner checks only; apply routine is not invoked.',
 'Fixture PAPER transitions, held QR/mail enqueue, manifest promotion and Office finalisation/replay modify database canonical TS/expense/evidence authority only. Dynamic SQL creates the fixture actor. No email sender, renderer/global drain, payment/provider/settlement execution.',
 'Fixture-known DIRTY_APPLY job processing/repair and deferred constraint finalisation affect database Workbench refresh authority only. Dynamic EXECUTE statements are SET CONSTRAINTS. worker_drain/Draft/pay owner names elsewhere are expected catalogue identities, not executed.',
 'Synthetic Daily/Weekly overlap admission and authorisation table triggers plus JSON hash helper. No payment/provider, settlement, communications or queue-drain owner is called.'
];
if(reasons.length!==56||source.length!==56)throw Error('exact 56 review rows required');
const rows=source.map(x=>{
 if(release.verificationFiles[x.index]!==x.file)throw Error('release order changed');
 for(const d of [x,...x.includes])if(sha(fs.readFileSync(path.join(root,d.file)))!==d.sha256)throw Error('reviewed verifier changed: '+d.file);
 const calls=x.calls.filter(n=>ownerMap.has(n)||n.startsWith('pg_temp.')).map(name=>({name,execution:name.startsWith('pg_temp.')?'TEMPORARY_FIXTURE_HELPER':'APPLICATION_CALL',...(ownerMap.has(name)?{source_file:ownerMap.get(name).file,source_file_sha256:ownerMap.get(name).source_file_sha256,line:ownerMap.get(name).line}:{})}));
 const dynamic=x.index===3||x.index===10||x.index===53?[
  {name:'public.timesheet_qr_send_enqueue_v1',execution:'DYNAMIC_PAPER_BRANCH',effect:'DB held outbox/document enqueue only'},
  ...(x.index===3?[{name:'public.candidate_targetless_expense_paper_pack_enqueue_v1',execution:'DYNAMIC_TARGETLESS_PAPER_BRANCH',effect:'DB held outbox only'}]:[]),
  ...(x.index===53?[{name:'FIXTURE_TMS_USER_INSERT',execution:'DYNAMIC_SQL',effect:'Creates only synthetic fixture actor, not an application executor'}]:[])
 ]:x.index===54?[{name:'SET CONSTRAINTS trg_pay_workbench_scope_change_finalize_v1',execution:'DYNAMIC_SQL',effect:'Transaction-local deferred constraint timing; not queue drain'}]:[];
 return {index:x.index,file:x.file,bytes:x.bytes,sha256:x.sha256,full_body_read:true,includes:x.includes.map(d=>({file:d.file,bytes:d.bytes,sha256:d.sha256,full_body_read:true})),decision:x.index===31?'EXCLUDE':catalogue.has(x.index)?'SAFE_CATALOGUE':read.has(x.index)?'SAFE_READ_OR_GUARD_PROBE':'SAFE_TRANSACTIONAL_FIXTURE',reason:reasons[x.index],calls,direct_or_nested_dynamic_calls:dynamic,genuine_applying_release_required:false};
});
const dependencies=[...new Set(rows.flatMap(x=>x.calls.map(c=>c.source_file).filter(Boolean)))].map(file=>({file,sha256:sha(fs.readFileSync(path.join(root,file)))}));
const out={contract:'HEALTHROSTER_VERIFIER_SAFETY_REVIEW_0_55_V1',scope:'current-release.json verificationFiles indices 0 through 55 inclusive',source_root_head:'547c74da5f0a05b09297c042bc7f3cc0c0105b85',saved_disk_authoritative:true,release_file:'supabase/release/current-release.json',release_file_sha256:sha(releaseBytes),review_date:'2026-10-13',full_body_read_count:56,unique_nested_support_body_read_count:4,reviewed_verifier_bytes:rows.reduce((n,x)=>n+x.bytes,0),safe_indices:rows.filter(x=>x.decision!=='EXCLUDE').map(x=>x.index),exclude_indices:[31],actual_database_calls_performed:false,verifiers_executed:false,feature_or_financial_acceptance_claimed:false,rehearsal_conditions:['Root alone executes under the authorized agency TEST outer rollback; preserved native triggers remain enabled.','Exclude index31, with no substituted global claim or queue drain.','Transaction wrappers, psql includes/display directives must be translated by the existing reviewed rehearsal packager; classification does not authorize durable commits.','Outbox/job enqueue rows remain uncommitted and disappear on rollback; no external executor, network, payment, settlement, email sender or worker drain is added.','Historical synthetic manager/signature/render/document fixture evidence qualifies only its historical DB contract assertions; it is not accepted as new native HealthRoster document/financial proof.','Application callee source has been followed for reached effects, including the dynamic PAPER enqueue, explicit empty sync, missing-session and invalid-authority refusal boundaries. Catalogue routine-definition and ACL identity strings are not execution.'],branch_review:[{boundary:'Candidate Daily generation/availability/reconciliation',effect:'DB canonical authority, receipts and outbox rows only; no external Sheet execution.',files:['supabase/repeatable/28082026_1628_candidate_google_rota_reenrolment_v1.sql','supabase/repeatable/12102026_2110_candidate_daily_legacy_two_way_availability_v1.sql','supabase/repeatable/13102026_0029_candidate_daily_missing_legacy_choice_reconciliation_v1.sql']},{boundary:'Emergency effect receipt',effect:'Exact fixture receipt create/replay/UNKNOWN completion; external execution payload is returned, never executed by SQL.',files:['supabase/repeatable/23082026_1820_candidate_daily_specialist_v1.sql']},{boundary:'PAPER and document planning',effect:'Dynamic call targets enqueue DB held mail/document records. BUILD_MANIFEST advances only fixture chunks, producing queued render work; no renderer/queue drain.',files:['supabase/repeatable/10102026_1347_import_expense_qr_approval_v1.sql','supabase/repeatable/07082026_2120_candidate_workflow_transition_atomic_v1.sql','supabase/repeatable/30082026_1232_candidate_qr_document_revision_order_v1.sql','supabase/repeatable/06092026_0540_candidate_targetless_expense_paper_pack_v1.sql','supabase/repeatable/13102026_1143_healthroster_scoped_credit/13102026_1143_planner.sqlinc']},{boundary:'Empty-scope Workbench reconciliation',effect:'Exact empty scope returns before finance-case writes, then updates only empty build/canonical-stage attestation.',files:['supabase/repeatable/04082026_1210_pay_sync_overpayments_from_preview.sql','supabase/repeatable/07082026_1015_pay_sync_overpayments_from_workbench_workspace_v1.sql','supabase/repeatable/29082026_1153_banking_pay_empty_scope_reconciliation_v1.sql']},{boundary:'Fixture-scoped invalid source claim',effect:'Both session and Candidate parameters are fixture identities; expected invalid-authority raise is inside a rollback subtransaction. No actual attempt execution.',files:['supabase/repeatable/26092026_0203_banking_pay_stage2_workbench_draft_v1.sql']},{boundary:'Missing-session modal mutations',effect:'Modal context requires a real session before selection writes; all tested session UUIDs are absent.',files:['supabase/repeatable/28082026_1159_banking_pay_modal_structure_v2.sql']},{boundary:'All stored SQL external primitive cross-check',effect:'Baseline/repeatable external primitive search found only denied-command regexp literals/comments, no executable HTTP/dblink/COPY PROGRAM sender on reached paths. This supplements, not substitutes, full verifier and reached-callee review.',files:['supabase/baseline/22082026_1503_cloudtms_test_routines_05.sql']}].map(b=>({...b,files:b.files.map(file=>({file,sha256:sha(fs.readFileSync(path.join(root,file)))}))})),callee_source_file_fingerprints:dependencies,verifiers:rows};
fs.writeFileSync(local('VERIFIER_SAFETY_0_55.json'),JSON.stringify(out,null,2)+'\n');
console.log(JSON.stringify({safe:out.safe_indices.length,exclude:out.exclude_indices,full_read:out.full_body_read_count,source_bytes:out.reviewed_verifier_bytes,sha256:sha(fs.readFileSync(local('VERIFIER_SAFETY_0_55.json')))}));
