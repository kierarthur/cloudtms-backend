# SP-04 routing investigation

12 October 2026. Independent source-only investigation; root owns product changes. No SQL/RPC, tests, fixtures, hosted requests or screen execution were performed.

## Native boundaries

The full approved card is `05_SOURCE_AND_PROTECTED_PAY.md:188–218`; the local checklist is `IMPLEMENTATION_CHECKLIST.md:4083–4099`. D01/D02/D05 reuse installed owners. D03 is the common Worker router; D04 is the native per-row result presentation. Withdrawal is neither an authorised zero nor a recovery and must retain frozen Draft, financial retention and paid history.

`17092026_0600_weekly_source_first_authorisation_v1.sql:3447–3501` defines service-only `public.weekly_source_first_authorisation_withdraw_request_v1(jsonb)`. Its only server request keys are `actor_user_id`, `timesheet_id`, `expected_timesheet_id`, `expected_row_signature`; it delegates the original four-argument owner. Worker injects actor. Missing trusted original signature must refuse, rather than use `render_signature` or obtain a replacement.

The final owner is `04102026_0556_banking_pay_next_source_factual_legacy_boundary_v1.sql:2126`, `public.weekly_source_first_authorisation_withdraw_v1(uuid,uuid,text,uuid)`. Its original recorded-receipt lookup precedes fresh Source admission, current context and business locks. `0600:1910–2063` recorded lookup checks the latest withdrawn authorisation generation, original signature and expected physical identity; live reauthorisation deliberately invalidates that old generation's replay. It is owner-only, explicitly revoked from service_role at `0600:3539–3540`; do not invent a public receipt RPC.

`0600:2555–2610` availability(uuid) is advisory and cannot establish original-request receipt absence because it receives no signature. A NOT_MANAGED availability result alone never permits ordinary fallback after a possible successful withdrawal. Only the exact original request owner's definite `WEEKLY_SOURCE_UNAUTHORISE_NOT_MANAGED_ROOT` permits fallback for a possibly managed root. Other native refusals remain contained.

Fresh request admission uses `05102026_0044_bpay_next_source_pay_query_admission_v2.sql:10–24`; contention is exact SQLSTATE 55P03 / `WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY`. The root design deliberately accepts that extra admission/refusal-audit path for ordinary NEXT Weekly rows. Do not weaken installed native owners merely to avoid this tradeoff.

Final `10102026_1332_banking_pay_next_withdrawal_projection_v1.sql:390–430` locks Candidate then Work, requires actual SOURCE/current approved ownership and refuses exact `BPAY_NEXT_SOURCE_WITHDRAW_FINANCIAL_DEPENDENCY` when native financial_effect exists. It changes approval/current pointer and native projection, preserving paid effects, positions, holds and frozen history. The router does not infer paid status or release anything.

## Actual caller insertion

`broker/src/index.js:75995` common Unauthorise authenticates admin, parses original expected physical identity/signature and serves actual single, selected bulk and Contract Week calls. Source routing belongs before the ordinary `guardCurrentTimesheetWrite` at 76445 and correction-pair mutation: otherwise current withdrawn state can prevent native receipt replay. Gold dispatcher 94095/94155/94186 retains trusted input and calls this handler; operation items must retain original identity, finite Source result/refusal/replay facts and unknown state. The older helper at 96198 is not proven active.

FE `main.js:276537` fresh preflight currently substitutes a signature when missing/untrusted. The NEXT slice must refuse instead. Selected bulk must remove `render_signature` fallback. Preserve qualified native Simple Source capture controls and existing ordinary/LEGACY behaviour outside the NEXT slice. Exact W9 stale reasons are `ROW_SIGNATURE_MISMATCH`, `EXPECTED_TIMESHEET_MISMATCH`, `EXPECTED_ROW_SIGNATURE_BLANK`; INTEGRITY as a whole is not a stale alias.

## Essential design qualifications

1. After an original request has been dispatched, an unavailable/malformed advisory probe on retry cannot turn its unknown outcome into a definite refusal. Retain exact original identity/signature and UNKNOWN; keep original owner receipt reconciliation reachable. Probe-before-write certainty applies only to a first attempt.
2. Universal Source request dispatch would refuse genuine contractless Daily before NOT_MANAGED: withdrawal context `0600:436–463` requires a nonnull Contract Candidate. `17092026_0200_weekly_source_rotation_authority_v1.sql:456–586` resolves native family identity but supplies no exemption from this Contract requirement. The smallest routing support is a positively authoritative native non-Weekly/Daily classification outside the Source request path; browser ownership hints or general context failure cannot prove absence. All possibly Source-managed Weekly/unknown rows still use receipt-first original request. No new SQL owner is indicated.
3. Once the mutation owner request is sent, timeout, transport failure or malformed acknowledgement is UNKNOWN. Only a validated native result or an exact audited PostgreSQL rollback refusal establishes a definite result; generic HTTP/auth failures do not prove rollback. Refusal-copy mapping must retain native nature/reason and use plain text, not raw technical codes.

## Source hashes

At initial investigation: root design `615f1da75ed366434b85fe03fa05e1646a949aa19b4cd77246b047ea664f3a59`; root investigation `f9e23dbb3f0e67e9b96e1cb52020b5417bc230a0202291abb567ee49fee41d47`; request/availability/recorded0600 `09fbe853b875393170ea2f538eddd41a0e4be41ab65cf5cb54cb663eae2ada71`; final withdraw0556 `64ef7e6d04e1b652af722cb9dd050310fcd8e1cd249f59b9359428a310b70933`; current index `77673593d09145179b9855ffab5563569b8408e7bede806f1691f576642caa19`; weekly-source routes `ec1a819336daeb552791af5170433d3925c9ff48e7d8c422dd3b0d92d14e6805`.

SOURCE investigation only. Product source review, A4 and joined acceptance remain pending; no completion claim.

## Correction-pair atomic lane

The original `runCorrectionPairLifecycleIfApplicable` (`broker/src/index.js:94022–94056` at this read) calls read-only `timesheet_correction_pair_lifecycle_preview_v1([original item], action, actor, 100)`, then, only after original confirmation/readiness, calls `timesheet_unauthorise_bulk_atomic` once with the original selected item. Preserve those original signature/pair-fingerprint operands and native expansion; never replace them with freshly signed preview members.

Native preview `02082026_2014_timesheet_correction_pair_lifecycle_preview_v1.sql:94–128` exposes groups[].members derived from the actual transition's pair_rows. This is the qualification set, together with the original selected ID, deduplicated and bounded by the native100 envelope. Do not infer members from a browser list or guessed parent UUID. Native atomic bulk `21072026_1235_33_timesheet_unauthorise_bulk_atomic.sql:64–68` independently expands its original input, refuses >100, and at90–124 captures and locks canonical family identities in its original order. Its final native managed-root guards are still the business authority.

**Safe narrow support:** obtain the native pair preview before dispatching any Source withdrawal mutation. In an actual pair lane, use read-only availability for every qualified native member. Only exact NOT_MANAGED / PERMANENT / NO_LIVE_ROOT_AUTHORISATION allows ordinary membership; managed, unavailable, integrity, malformed or uncertain probes refuse the whole atomic lane before mutation. Then call the original atomic owner once. Do not withdraw a selected member first and subsequently discover a pair, nor withdraw expanded members individually.

**Daily limitation:** native correction classifier `21072026_1235_00_import_correction_policy_helpers.sql:69–94` qualifies canonical correction markers/policy envelope without requiring WEEKLY or contract_id. Therefore the pair preview is not itself a Source-absence theorem. Strict NOT_MANAGED-only admission safely blocks contractless Daily pairs when availability returns CANDIDATE_UNRESOLVED; do not claim their ordinary behaviour is preserved. If the root elects to preserve genuine current contractless Daily members, the existing native Office presentation DAILY fact plus exact physical contract_id NULL is a separate explicit current-domain qualification, not a historical-authorisation census. Do not call the mutation owner merely to qualify a pair member. Any existing unknown **Source withdrawal** retry must remain in its original receipt-first lane, rather than be reclassified as an ordinary pair action. Unknown ordinary atomic-pair outcomes keep the original native pair lifecycle handling.

No new SQL owner, grant, receipt table or protocol is needed for the read-only pair qualification. Any final relaxation must be qualified in the design and reviewed against the actual source; this note does not certify an unwritten relaxation.

## Final native pair proof and routing correction

This section supersedes the earlier recommendation to move pair preview ahead of the original Source withdrawal request. Native `21072026_1235_30_timesheet_unauthorise_atomic.sql:36–44` returns `IMPORT_CORRECTION_UNIT_REQUIRES_BULK_LIFECYCLE` before lifecycle mutation; its179–198 correction-kind guard also refuses pairs. Final0556:2546–2558 invokes that native single owner and raises exact55000 `WEEKLY_SOURCE_UNAUTHORISE_OWNER_REFUSED` for its false acknowledgement. The exception rolls back the earlier Source withdrawal changes in the same transaction. An actual successful original Source receipt must therefore remain reachable before any fresh pair preview. No installed SQL amendment is needed.

Fresh common helper `broker/src/index.js:94011–94057` performs native preview and original confirmation/readiness, then for UNAUTHORISE only runs the read-only membership preflight before its original bulk atomic call. It qualifies the original selected ID and every native expanded member, deduplicated within100, without replacing atomic input/signature/fingerprint. Its other actual caller at168570 is AUTHORISE and retains its original path. Selected-bulk uses the same preflight at94668. Unknown/unavailable/managed expanded membership blocks the ordinary pair lane; original single managed Source requests retain receipt-first reconciliation and native transactional pair refusal.

The final Daily exception is receipt-first: only exact native CANDIDATE_UNRESOLVED at original canonical identity, plus the existing service Office DAILY presentation and exact physical contract_id NULL, permits the current ordinary Daily domain. It is not a historical absence assertion. Arbitrary failure never qualifies ordinary routing; no owner-only history SELECT or new grant is used.

Final scoped design and router SOURCE review passes. Current hashes: design `c5105c44a77e72b4aa6bdf0f2679c780a839ea90cda7f9c7491ac814f3de09b4`; router `74c63f39beedffd7d8c7fb6a249f6441fffa98130efd202292d537852bfbeb52`; index `f1121d3ba4fa70fd246de85a99617cee98581b231e940c4cc68c994c26400cf7`; native single owner `53e9af7ae8209f834508cb688c7b5d2449a9f7aedbe22db4906dafceb94c8a45`; final0556 unchanged `64ef7e6d04e1b652af722cb9dd050310fcd8e1cd249f59b9359428a310b70933`. UI/A4 and joined acceptance remain separately unverified by this source investigation.
