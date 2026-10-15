# Native finance acceptance specification

15 October 2026. **AUTHORED / NOT_RUN.** This file and its JSON companion define tests to implement in the existing build, not executed money tests. No database calls, further rollback, payment/provider/email or release was performed for V5. Static source observations, local designs, successful install and native lifecycle acceptance are distinct.

Use the actual authenticated Worker route, actual native ordered command/job and normal queue/minute continuation, exact stored Work/revision/Positions/Case/holds/effects/transfers, and real UI adoption. A substitute illustrative response/adapter is insufficient. Do not claim an unsupported-state refusal completes a required positive subtype; complete physical transfer and its concrete financial qualification/resume path.

For all successful moves: immutable original artifacts/evidence/approvals retained; same candidate/client/date scope; one direct canonical root, one current placement, each actual cash/hold/recovery artifact attributed once; existing Source invoice IDs/lines/movements/report/credit membership/net/VAT/gross/documents unchanged; future Source invoice amount/allocation unchanged; B not extra invoice candidate; no unsolicited signature/contact/payment. A and B links both discover original frozen history. Each suite includes unrelated candidate/client/same-day-shift controls.

## Deterministic earnings vectors

N=new approved comparable entitlement; C=genuine retained realised source disposition after actual recovered effects; H=active original **work-pay** reservations; W=forgiveness separately. These are comparable earnings/source values, not invented bank cash or PAYE NET shares. Existing recovery reservation is a separate protected Case capacity; it is not H or recovered C. Positive available=max(N−C−H,0); actual debt=max(C−N−W,0); work-hold overhang=max(H−max(N−C,0),0). Existing protected recovery suppresses further collectable amount until native settle/release, without becoming recovered money.

| Vector | N | C | H | W | Available | Debt | Overhang | Required result |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| M01 | 120.00 | 0.00 | 0.00 | 0.00 | 120.00 | 0.00 | 0.00 | Unpaid100→120: total120 due;20 is approval change only |
| M02 | 80.00 | 0.00 | 0.00 | 0.00 | 80.00 | 0.00 | 0.00 | Unpaid100→80:80 due; no recovery |
| M03 | 120.00 | 100.00 | 0.00 | 0.00 | 20.00 | 0.00 | 0.00 | Paid100→120:20 extra |
| M04 | 100.00 | 100.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | No new payment or move cash row |
| M05 | 80.00 | 100.00 | 0.00 | 0.00 | 0.00 | 20.00 | 0.00 | One20 overpayment; not gross100+new80 |
| M06 | 120.00 | 60.00 | 40.00 | 0.00 | 20.00 | 0.00 | 0.00 | Original40 reserved;20 extra |
| M07 | 80.00 | 60.00 | 40.00 | 0.00 | 0.00 | 0.00 | 20.00 | Overhang20 is not realised debt |
| M08 | 80.00 | 100.00 | 0.00 | 0.00 | 0.00 | 20.00 | 0.00 | M07 native40 settles: now debt20 |
| M09 | 80.00 | 60.00 | 0.00 | 0.00 | 20.00 | 0.00 | 0.00 | M07 exact unissued40 safely released:20 due |
| M10 | 90.00 | 100.00 | 0.00 | 0.00 | 0.00 | 10.00 | 0.00 | Earlier20 recovery reserved: no extra collection while20 protected |
| M11 | 90.00 | 80.00 | 0.00 | 0.00 | 10.00 | 0.00 | 0.00 | M10 actual20 recovery settles:10 top-up |
| M12 | 90.00 | 100.00 | 0.00 | 0.00 | 0.00 | 10.00 | 0.00 | M10 recovery reservation safely released: debt10 |
| M13 | 150.00 | 160.00 | 0.00 | 0.00 | 0.00 | 10.00 | 0.00 | Approval160→140→150, nothing recovered: debt10 only |
| M14 | 150.00 | 140.00 | 0.00 | 0.00 | 10.00 | 0.00 | 0.00 | M13 actual20 recovery then10 genuine top-up |
| M15 | 80.00 | 100.00 | 0.00 | 20.00 | 0.00 | 0.00 | 0.00 | Forgiven20 distinct from received recovery |

The JSON companion **enumerates** all 480 vector cases (15 vectors × eight typed origin fixtures × four channel directions) plus every catalogue row below, with status NOT_RUN and concrete fixture/expected values. The count describes authored cases, never executed tests. Directions are PAYE→PAYE, PAYE→Umbrella, Umbrella→PAYE and Umbrella→Umbrella. Base vector fixtures use native approved ERNI0/VAT0 calibration so their comparable-value scalar is the table value; no PAYE NET/cash is inferred. Non-zero conversion/manual/rounding/VAT must additionally pass FIN-CHANNEL-02..06.

| Origin fixture ID | Exact applicable native component and history |
|---|---|
| ORD_MANAGER | Ordinary genuine manager-signed/Office-authorised WORK. |
| ORD_WORKER | Ordinary worker-only with genuine Office approval; no invented manager signature. |
| LEGACY | When C=0 use actual legacy current approved TSFIN with proved no realised payment; when C>0 use exact qualified original frozen paid component. Active H and prior recovery/forgiveness exist only as their genuine native artifacts. A frozen-paid-only origin is never labelled unpaid. Missing-basis legacy uses pending tests, not these qualified vectors. |
| SOURCE_INITIAL | Real Source initial authorised TSFIN with no HEAD; its original/native money witnesses are genuine. |
| SOURCE_HEAD | Actual common Source HEAD/receipt/component binding. |
| SOURCE_DIRECT | Genuine current Source Direct approval and separately qualified original effects; no fabricated HEAD. |
| DAILY | Booked Daily **hours WORK only**, including null-Contract original where admitted; no Additional/expense injection. |
| EXPENSE_MILEAGE | Genuine existing expense-only **mileage** claim with fixed actual miles and reviewed applicable A/B pay rates sufficient to produce the table's N (100 miles at £1.20 gives120, £0.80 gives80). Real old paid/held mileage slices use the same native claim. NON_TAXABLE, no ERNI conversion; the four directions exercise actual pay-channel routing, not invented taxable conversion. A fixed reimbursement is never repriced to manufacture a vector. |

The fixed-reimbursement rule is covered separately by FIN-EXP-01/02/06/08 and FIN-CHANNEL-05: identical authorised claim retains its actual amount and is credited once. Applicable native pending/no-Work/ambiguous-history populations must pass their explicit positive physical-and-resume cases. No origin is silently skipped, replaced with a mock or marked passed by an unsupported refusal. If a fixture cannot be admitted through its stated actual owner, correct the fixture/spec with reviewed evidence; record a failure or not-run, never invent economic authority.

## Native test catalogue

Every row is REQUIRED and NOT_RUN. IDs are stable prefixes referenced by BANKING_CHANGE_MAP; builders add exact native implementation test paths/results per row.

| ID | Journey | Exact assertions | Existing cards |
|---|---|---|---|
| FIN-TYPE-01 | Ordinary manager-signed move | Actual native apply succeeds; original hours/breaks/signatures/document hashes retained; no new signature | CM-20;WB-01 |
| FIN-TYPE-02 | Approved worker-only ordinary | Preserve worker and actual Office approval; no invented manager evidence | CM-20;HR-07 |
| FIN-TYPE-03 | Pending ordinary/Source/worker-only | Physical B current; actual pending state retained; no payable approval; later genuine approval resumes same operation | CM-20;SP-08 |
| FIN-TYPE-04 | Source initial/CommonHEAD/Direct | Positive typed origin per real authority; no forged HEAD or historic-to-current eligibility | SP-08;SP-09;SP-37 |
| FIN-TYPE-05 | Daily booked/null Contract | Real booking/date/PHONE proof and lifecycle retained; hours only; additional/expense injection refused | DY-04;WB-04 |
| FIN-TYPE-06 | Genuine expense-only carrier | Claim dates/evidence/approval once; no fabricated hours; automatic B carrier if required | CM-20;WB-03 |
| FIN-TYPE-07 | Paid legacy with/without qualified evidence | Sealed actual origin qualifies or explicit pending+resume; paid flag neverC0 | CM-20;HR-05 |
| FIN-TYPE-08 | Occupied B | Different real work stays separate/unchanged; both exact canonical families once; repeat same move saved result | CM-01;CM-20 |
| FIN-SOURCE-01 | Existing Source invoices draft/issued/paid/credited | Before/after full invoice identity/membership/money/document hash equal; no unissue/remove/credit/reinvoice | SP-26;CM-20 |
| FIN-SOURCE-02 | Uninvoiced Source | Later invoice same accepted Source amount/allocation exactly once; A archive not loss; B no ordinary duplicate | SP-26;CM-20 |
| FIN-SOURCE-03 | Source originally CONTRACT_CALCULATION preset | Accepted Source amount not repriced from B now or later; pay can change independently | SP-26;HR-02 |
| FIN-SOURCE-04 | Source100miles A20p pay/30p charge, B25p pay/40p charge | Paid20 gives5 extra reimbursement; Source charge stays30, no10 client adjustment | WB-03;CM-20 |
| FIN-SOURCE-05 | Genuine later Source revised report | Only actual report correction changes its own Source authority; placement mapped once; move never invents report | SP-10;SP-26 |
| FIN-SOURCE-06 | Ordinary actual Contract-priced invoice | Separate correct ordinary owner/line manifest; badge/platform alone cannot select branch | CM-20 |
| FIN-EXP-01 | Paid100work+10expense toB120work | Entitlement130; settled10 claim credited once; only20 further earnings | CM-20;WB-03 |
| FIN-EXP-02 | Same example all unpaid |130 total due, not20 and not140; inclusive TSFIN components counted once | CM-20;WB-03 |
| FIN-EXP-03 | Mileage B0/NULL/missing |0 stays0; NULL uses dated global pay; no oldA/Candidate fallback; all-missing precise remedy | CM-20 |
| FIN-EXP-04 | Ordinary mileage charge hierarchy | Bnon-null including0→actual Clientnon-null including0→datedglobal; no Clientpay field invented | CM-20 |
| FIN-EXP-05 | Additional independent codes and physical buckets | Exact codes/quantities/B rates; same-normalised-code conflict refuses; no blending or proportional paid basis | CM-20;WB-29 |
| FIN-EXP-06 | Workflow+TSFIN sync after move | Current claim remainsB; immutable original workflow/manager/submission; no A resurrection/new claim | CM-20 |
| FIN-EXP-07 | Auto separate B carrier | Exact real claim manifest/date; unrelated claims unaffected; no extra wizard or altered approval | CM-20 |
| FIN-EXP-08 | Frozen expense/mileage later outcome | Original captured amount/rates unchanged; true settle/release updates canonical once | CM-15;CM-20 |
| FIN-CHANNEL-01 | All4 directions ERNI0/VAT0 | All money vectors exact comparable basis; native shared quote only; Candidate channel unchanged | CM-04;CM-19 |
| FIN-CHANNEL-02 | PAYE multiWork recorded NET | Gross never cash; exact whole accepted NET; no perTimesheet/Client split | PS-11;DR-05 |
| FIN-CHANNEL-03 | Umbrella100ex+20VAT minus30ex+6VAT recovery |84 cash stored bridge/member sum; one recovery not Case+effect double | PS-01;CM-13a |
| FIN-CHANNEL-04 | Manual Umbrella20→PAYE17 | Native accepted manual choice wins suggestion; retained paid/held comparability; no second residual repricing | CM-11;CM-19 |
| FIN-CHANNEL-05 | Reimbursements across channels |NON_TAXABLE/no ERNI conversion; proper cash bridge and once-only gross | CM-20;DR-38 |
| FIN-CHANNEL-06 | Nonzero ERNI/VAT, multiple old paid+held slices | Existing approved CM reserve oracle quote(N)−quote(C)−qualified reserve, not independently rounded quote(H); penny differences explicit | CM-03;CM-04 |
| FIN-CUT-01 | Before-click accepted move queued | Earlier Candidate job completes first; complete moved approval included | F-25;CM-07 |
| FIN-CUT-02 | After-click PREPARING/READY | CapturedIDs/rates/channel/payee/values/holds byte-identical; new difference next run | F-25;CM-15 |
| FIN-CUT-03 | After-click SourceFinal/protection/noPay/rate/Snooze | Each existing accepted capture unchanged; current next-run truth updated independently | F-25;SP-21 |
| FIN-CUT-04 | Two Drafts select A/B aliases | One canonical residual reserved; stale/conflict no extra hold/payment | DR-14;CM-09 |
| FIN-RACE-01 | Concurrent A→B and A→C | One current operation; loser REVIEW_CHANGED; no physical/financial half | CM-07 |
| FIN-RACE-02 | A→B→C→A | Direct original root constant, one current placement; actual old effects once | CM-09;CM-18 |
| FIN-RACE-03 | Price/policy/evidence/invoice drift at accept | Exact stale digest refusal before effect; completed replay saved old receipt not today's pricing | CM-06;CM-07 |
| FIN-RACE-04 | Failure each accept/apply page and lost reply | Transaction rollback or persisted complete receipt; exact lease/cursor resume; samekey one result | CM-07;WB-06 |
| FIN-QUEUE-01 | Actual queue/minute native move | Correct enroller/commandkind/epoch/lease/phase/cursor; one terminal complete projection, not adapter mock | WB-06;F-09 |
| FIN-QUEUE-02 | Lease expiry/owner epoch | No stale write; accepted receipt preserved; exact permitted resume, no duplicate job | F-17;WB-06 |
| FIN-QUEUE-03 | Large complete manifest |≤100members per page, complete census; late-page failure Processing/nonselectable entire affected group | CM-07;WB-44 |
| FIN-HOLD-01 | M07 original40 settles | Actual native effect first; debt20 only after, no earlier recovery | CM-14;CM-15 |
| FIN-HOLD-02 | M07 original40 safely released | Exact permitted cancel/discard then20 due; hold RELEASED once | CM-15 |
| FIN-HOLD-03 | Issued/unknown attempted release | Native refusal protects original; concrete Record outcome path; no pretend zero | CM-15;EX-22 |
| FIN-HOLD-04 |72h expiry/native Case reprepare | Only actual released artifact forwards; original frozen rate/context retained; Source unchanged | CM-15 |
| FIN-REC-01 | Paid100→80 plus eligiblepay30 |20 recovery leaves10 comparable earnings; recovered only real posting | CM-12;FC-15 |
| FIN-REC-02 |10/0 headroom, otherCandidate/channel | Partial10 balance10; zeroBlocked once; no cross partition borrowing | CM-12 |
| FIN-REC-03 | Held20 recovery then80→90 | No additional collection while protected; settle20 then10 topup; release then10 debt | FC-16;HR-05 |
| FIN-REC-04 | RECOVERED Case event+effect | Exact event/instruction/hold/effect provenance; one recovery/source disposition | PS-04;HR-05 |
| FIN-REC-05 | Forgiveness then approval | W separate; no recovery/cash/return from writeoff; correct outstanding once | FC-11;HR-05 |
| FIN-PEND-01 | UNKNOWN original transfer | Physicalcomplete; exact original retained; only new difference pending; real Open payment→Record bank outcome | EX-22;CM-15 |
| FIN-PEND-02 | Old component basis missing | Review earlier payment evidence workflow; no C0; qualifying evidence new certificate not fake original effect | CM-10;CM-18 |
| FIN-PEND-03 | Bank-only/NET-only ambiguous multi-component evidence |EVIDENCE_INSUFFICIENT precise missing gross/tax/component list; no proportional gross/NET | PS-11;HR-05 |
| FIN-PEND-04 | New genuine evidence+same operation resume | Newcut/version CAS; no second physical move; immutable old payment; replay/conflict | CM-10 |
| FIN-PEND-05 | One uncertain scope and unrelated Ready work | Business pending job DONE quarantines affected difference; other qualified same-Candidate work can continue. Technical unfinished/failed job blocks later same-Candidate PREPARE until exact retry; other Candidates unaffected | WB-19;HR-05 |
| FIN-NOPAY-01 | Unsigned unpaid/unreserved one/several Source shifts |KEEP_SOURCE_INVOICE/no-client-query saved; actual selector/capture exact denied quantity absent | FPD-01..05 |
| FIN-NOPAY-02 | Independent signed known-unpaid | Separate SIGNED_FINANCIAL adjudication reason/evidence; signature retained; no widening unsigned FPD | SFD-01..06 |
| FIN-NOPAY-03 | Paid/reserved/protected/frozen | Separate RETAINED_FINANCIAL branch; future exclusion+native true difference; original commitments unchanged | HR-05;SFD-01..06 |
| FIN-NOPAY-04 | Unknown money/noPay | Invoice+contact receipt saved independently; financialpending exact Open payment action; no false0 | EX-22;SP-26 |
| FIN-NOPAY-05 |200approval160supported40denied | Actual160 can capture normally;40excluded, no automatic200 freeze or whole160 block | FPD;SFD |
| FIN-NOPAY-06 | Reimport/alias/move/reapproval after noPay | Same decision persists; no restored40/campaign; new evidence deliberate linked reopen | FPD;SFD |
| FIN-NOPAY-07 | Capture/correction race and stale decision | Beforecut inclusion vs aftercut next-run only; denied decision neither lost nor duplicate | F-25;FPD;SFD |
| FIN-NOPAY-08 | Other same-day shift/manual query | Only selected reviewed issue resolved; unrelated query/work/contact unaffected | FPD;SP-15 |
| FIN-IMPORT-01 | Successful native same durable shift reimport | Actual classifier/review/apply succeeds mappedB once; no NEWA/Bduplicate; Source invoice invariant | SP-01;CM-20 |
| FIN-IMPORT-02 | ChangedRequest overlap and alias rekey | Durable exact source match; no guessed identities; real additional work separate | CM-20 |
| FIN-HISTORY-01 | A/B/carrier/original payment navigation | Same direct indexed operation; frozen original client/rate/document plus separate currentB link | CM-18;EX-42 |
| FIN-HISTORY-02 | Current Source unavailable, original qualified | Original cash visible; no current approval from history; unavailable not0 | SP-37 |
| FIN-HISTORY-03 | Withdraw/archive/cleanup | Original cash/evidence reachable by actual UUID/book+mapping; no live label/rate fallback | EX-44;PS-07 |
| FIN-PS-01 | Paid100→120 before actual topup | Originalcash100 only; linked move/case; no120full or0move row | PS-03;CM-18 |
| FIN-PS-02 |160→140→150 none recovered then20recovery+10topup | Original160 until actual effects; case10; cash contributions only genuine completion once | HR-08 |
| FIN-PS-03 | Mileage20 then5 and fixed10expense | Frozen original20 and later genuine5; fixed10 once; no clientcharge cash | PS-06;CM-20 |
| FIN-PS-04 | Source invoice linkage | Reference/document links separate, no invoice Amount/totals/bridge contribution | PS-05;CM-18 |
| FIN-PS-05 |200paid200returned200reissued | Paid400 Returned−200 Netpaid200; original payroll/recovery not reposted; dates/parents exact | PS-10 |
| FIN-PS-06 | MultiTimesheet/Client payment | Whole transfer once; frozenClient-any filter wholecash once; no NET split | PS-11;PS-14 |
| FIN-PS-07 | Client/rate/name changes later | Immutable original labels; current link separate; unknown originalClient explicit | PS-05;PS-06 |
| FIN-PS-08 | List/detail/CSV/print all pages | Complete stable-ID membership and server whole-filter totals; no half export or loaded-page sums | PS-15;PSP-81 |
| FIN-PS-09 | TenPSP choices/advice/privacy | Exact defaults/readonly scope/admin everypage; no bank details; real advice reason | AUTH-PS;S1 |
| FIN-UI-01 | Same-open-modal adoption | Aactive removed Bcurrent once; counters/totals/actions one revision without reopen | WB-19;CM-17 |
| FIN-UI-02 | Bcurrent finance Processing | Named exact next action; no falsefinancialDone or giant technical banner; old frozen detail unchanged | CM-17;DR-19 |
| FIN-AUTH-01 | Unauthenticated/inactive/forged actor/browser SQL | Forbidden private/table/RPC writes and guessed disclosure; Worker actor only; current72 operation triples and original protected63 boundary retained, no new Candidate move operation | F-10;HR-06 |
| FIN-AUTH-02 | Foreign Candidate/client/dates/manual amount | Typed refusal before effect; legitimate manualrate choices only; no new signatures | CM-16;CM-11 |
| FIN-GUARD-01 | New FK/check/RLS/finalclosure | Complete exact tuple or allNULL; duplicate artifact/binding refused; current final body; old frozen hashes equal | CM-21;F-23 |
| FIN-GUARD-02 | Samekey changedbody/forgedmanifest/GUC | Persistent operation certificate/current actor+transaction required; no GUC-only privilege | CM-07 |
| FIN-POST-01 | Replayed hold/effect/Case callbacks | Original artifacts unchanged, derived current once; no repeated payment/source disposition | CM-14;PS-04 |
| FIN-POST-02 | Return/reissue after moved source | Payroll/source/recovery retained; cash obligation separate actual chain | CM-14;PS-10 |
| FIN-ABI-01 | Preview persists REVIEWED metadata | Same preallocated operation/request IDs; manifest/cut/review durable; no B/Work/claim/invoice/command/hold/contact created | CM-02;CM-16 |
| FIN-ABI-02 | Physical family root versus Banking Work | family_root_id is genuine physical UUID; pending has NULL actual Work IDs/no binding; later real approval binds once | CM-02;CM-20 |
| FIN-ABI-03 | Qualified queued command is not applied | Physical receipt COMPLETE and cut QUALIFIED may coexist QUEUED/PROCESSING; APPLIED only native final barrier; no false paid status | CM-07;WB-06 |
| FIN-ABI-04 | Versioned cut and continuation | Two cut_no values allowed same operation; wrong-operation pointer rejected; one initial and max one continuation per accepted cut; original seal unchanged | CM-02;CM-07 |
| FIN-NOPAY-09 | Released worker denial before final backing/Work | Authentic Source-row subject saved; NO_FINAL_REPORT_YET has no amount/invoice/Work; selected enquiry stopped; future pay exclusion retained | FPD;SP-26 |
| FIN-NOPAY-10 | Later final report inclusion or absence | Real accepted Source resolution binds prior decision once; actual included source charge235 stays235; excluded/absent source work produces no invoice or fabricated charge | FPD;SP-26 |
| FIN-NOPAY-11 | Mixed multishift branches/native missing basis | Per-shift unsigned/signed/paid/frozen/unknown results explicit; invoice/no-client outcomes saved where valid; exact financial pending suppression; no false all-success | FPD;SFD;HR-05 |
| FIN-NOPAY-12 | Pay-only decision and independent expense | Selected denied wage quantity suppressed; unselected genuine authorised reimbursement retained once; no broad Timesheet hold blocking invoice | FPD;SFD;WB-03 |
| FIN-NOPAY-13 | Native dependency before/after savepoint | Typed known gap preserves invoice/contact plus disposition; arbitrary SQL/permission/stale error rolls attempted transaction back; no survived partial financial write | FPD;SFD;WB-06 |
| FIN-NOPAY-14 | Decision command queue and immutable continuation | Actual SOURCE_PAY_DECISION_APPLY enrollment/claim/queue/minute/lease/page/final barrier; no phantom Work in FUTURE_SOURCE_ONLY; one linked continuation per fresh cut | FPD;SFD;F-09 |
| FIN-NOPAY-15 | Stale Source alias or overlapping exclusion | Exact accepted subject mapping or typed SOURCE_RESOLUTION_REQUIRED; no names/date guessed match, duplicate exclusion, source contact restart or duplicate payment | FPD;SFD;SP-10 |
| FIN-NOPAY-16 | Multishift/multiworker same-client decision | Sorted Candidate locks, exact per-worker publication membership UNIQUE(decision,cut,candidate), atomic decision receipt and individual actual application results; no money/permission bleed | FPD;SFD;HR-06 |
| FIN-NOPAY-17 | Pending worker denial with active protected pay | Genuine scoped Unprotect clears active invoice blocker and produces its native N0 difference once; historic protection/cash/holds preserved; actual debt/commitment review reachable. Two interleaved same-decision PREVIEW assemblies retain exact distinct cut/quote/review identities; cross-assembly/review refs reject. A NULL-review metadata cut can quote/READY but cannot stage/publish/advance accepted cut; atomic seal creates one matching triangle. Source/cash/hold/aggregate/approval/placement/policy changes at each boundary require fresh review, without patching old certificates. Exact begin/quote/READY/seal/accept timeout replay preserves IDs; altered-body reuse conflicts; unsealed expiry/cancel grants no finance authority. | HR-04;FPD;SFD |
| FIN-FROZEN-01 | Valid original confirmed Draft executes after A→B | Actual native original CSV issue/execution succeeds if normal original eligibility passes; old move FKs NULL/frozen hashes equal; B live rates/channel never read to revalue | CM-15;F-25;EX-02 |
| FIN-FROZEN-02 | Original A hold settles after B/current Source binding | Exact old runline/hold/effect accepted from frozen original proof; no new binding FK backfill; one derived B credit and one genuine Case | CM-14;SP-37 |
| FIN-FROZEN-03 | Original unissued Draft cancel/expiry after move | Native release succeeds once when eligible, preserves frozen rate/treatment; ORIGIN task current mismatch cannot strand expiry; issued/unknown refusal intact | CM-15;DR-05 |
| FIN-FROZEN-04 | Original Case recovery resolves/settles after move | Captured instruction/Casehold/event preserved; actual recovery once, current difference refreshed once; no duplicate principal or changed old cash | FC-16;CM-14 |
| FIN-FROZEN-05 | Original return/reissue/cancel after move | Exact transfer/return_cash chain valid; cash-only accounting; original payroll/source effect unchanged and old/current history reachable | PS-10;EX-35 |



## Historical evidence and completion record

The corrected earlier rollback receipt completed its bounded transaction but the business path returned `IMPORT_REVIEW_REQUIRED` and `IMPORT_REVIEW_RECONCILIATION_SOURCE_INVALID`; review_created=false, B Timesheet count0, A unchanged, cleanup verified. It created no B and retained A unchanged; it neither demonstrated duplicate cash nor proved future risk absent, because the intended successful full apply was not reached. Preserve that history without claiming it closes FIN-IMPORT-01 or any money case. No further DB trial is authorised by this audit.

Each future native result records test ID, exact source+installed definition/asset versions, actual private-origin/cut/command/job IDs in protected local evidence, bounded before/after monetary scalars and artifact hashes, native result, actual UI result, unrelated controls and failure/retry result. Do not publish sensitive raw payment payload. Build/review/install/runtime boxes remain separate. One genuine joined receipt may cover several explicit assertions if each is mapped; no duplicate proof programmes merely for tick counts.

## Exact earlier-payment evidence acceptance

Normative protocol: HISTORY_RECONCILIATION_CONTRACT.md and Health §6.5 / Banking §5.1 final-report pay choices. These are authored native acceptance cases, all NOT_RUN.

| Case | Journey | Required assertions |
|---|---|---|
| FIN-HIST-01 | Closed reference and action schemas | Every kind binds its exact native key/digest; reject extra keys, free amounts, uploaded document IDs, foreign worker/member and incompatible report evidence. |
| FIN-HIST-02 | Reviewed original basis and native changes | Preview changes metadata only; outcome/hash/cut/actor changes force fresh review; accept binds only its reviewed references and derived basis. |
| FIN-HIST-03 | Recheck and continuation replay | Distinct PREVIEW/ACCEPT/RECHECK keys; identical replay same receipt; changed body conflict; recheck never publishes; accepted new cut at most one continuation. |
| FIN-HIST-04 | Genuine legacy component proof | Read sealed original certificate/component and actual payment chain; never revive old engine or fabricate original Banking-next revision; reject corrupt/incomplete basis. |
| FIN-HIST-05 | Missing gross and unrelated ready work | Bank NET cash alone remains precise evidence review; old cash/invoice/Draft unchanged, review-only job completes its barrier and other qualified work proceeds. |
| FIN-HIST-06 | Source financial versus report evidence | Fresh Source preview binds exact history references and digest separately from report facts; accept/resume cannot add unseen basis or substitute a move ID. |
| FIN-HIST-07 | Legacy first genuine Banking-next Work | No prior NEXT Work; qualified legacy paid100 and actual B120 yields20, canonical first real B and originalWorkNULL; once-only legacy credit through replay and later moves. |
| FIN-POSITIVE-01 | Office chooses final Source for pay | Genuine final Source only; exact actor/review and decision enum; query resolution does not pay or authorise; normal named Source/Timesheet/Banking steps, original signed facts/invoice unchanged. |
| FIN-POSITIVE-02 | Explicit superseding decision either direction | Exact per-member prior disposition ID/version and current cut CAS, mixed prior decisions explicit, append-only new version; stale/replay/import cannot restore pay or create new Work. |
| FIN-POSITIVE-03 | Positive pay after prior money and recovery | Paid/held/recovered/forgiven once under native comparison and candidate order;100to120 yields20 comparable extra; unknown review honest; pendingSource cannot become payable. |
| FIN-HIST-08 | Legacy frozen history without V8 certificate | Actual item/snapshot/breakdown/transfer native graph qualifies original gross independently; V8 not required, crossbatch/worker/key/missingtax refused; legacy engine never restarted. |
| FIN-HIST-09 | Legacy cancelled unpaid versus pending commitment | True native terminal no-money and released obligation qualifies C0H0; missing transfer requires full absence proof; pending preserves actualH, unknown never0; archived/void flag cannot erase cash. |
| FIN-HIST-10 | Legacy versus NEXT payment target | Legacy public.pay_bank_transfers UUID never enters NEXT transfer modal; original Timesheet and exact bounded read-only evidence review open under actual permissions; no retired payment control or fake target. |
| FIN-POSITIVE-04 | Partial component supersession of one Source-row aggregate | One row per original Source subject, grouped CAS once, full adjudicated successor members copy unselected member-decision and original qualification_cut_id/member FK unchanged; neverselected native siblings stay native. Current rates still native; no sibling pay/contact reset. |
| FIN-HIST-11 | Original basis versus actual realised and reserved | Original basis200, realised100 before recovery, workHold20, actualRecovery10, currentN120 => retainedC90 and available10. No duplicate recovery deduction, no C200 or H0 from empty NEXT arrays; prefinancial Source-only gets no fake history root. |


## Acceptance-owned native composition — additional FIN-NOPAY-17 assertions

Normative SOURCE_NATIVE_ACCEPTANCE_CONTRACT.md; these extend the existing grouped case, not executed tests or additional case counts. All remain NOT_RUN.

1. Strict pre-cut passes before native generation/control/Work/local TSFIN changes. Original cut/hash/artifacts stay immutable. Exact same-transaction application/adoption and canonical command/member link prove the owned transition; unrelated witness changes reject and roll back.
2. Protected moved A→B retains original A frozen holds/cash/evidence. Actual native B transition and canonical difference occur once; original Draft remains usable through its owner.
3. Two protected members of one Candidate produce two genuine native adoptions and one canonical Source command, preserving old NEXT UNIQUE command receipt and old Local publication PK. No second negative or full offer. Verify all logical-member financial component links.
4. Actual Local unauthorised TSFIN, complete committed head and real pending-freeze bundle retain their distinct valid FKs/result/authority and actual writer. Withdrawal of protection is not a claim of applied financial money. Verify exact14/11/7/17-key normal adapters and command-free internal DTO. Source-owned incomplete/dependency status never falls through normal Worker retry to standalone publish.
5. Wrong application/request/review/member/actor/PID/xid, cross-owned native receipt, extra own mutation, and independent invoice/cash/hold/policy/aggregate/unselected-member change reject. A permitted native dependency rolls its entire member subtransaction back with no applied IDs; genuine native pending-freeze is kept separately. Query/contact retirement follows stage/publication. Exact lost-result replay reads original request/application/command, never re-applies.

6. Atomic post-confirmation native run birth and retained RUN_PREPARED stub: Worker loss before context/calculation or registration, prepare timeout, altered body/actor/member, expiry/cleanup, consumed replay and attempted ordinary native fallback cannot create an orphan standalone financial authority. Positive/no-active-protection refuses run preparation. Later genuine native WITHDRAW/REPLACE/SET_DELTA/context/factual selectors recognise the exact composed current generation through the preserved route owner. Normal native paths keep original contracts. All NOT_RUN.

7. Genuine Local pending-freeze → native RELEASED → current read/later action retains original immutable cut/application/adoption/pending-step hashes, and validates the exact15-field derived release proof/current saved-status overlay. Immediate Source-owned trigger is fenced before first Banking stage even when adoption does not yet exist; deferred proof uses the retained committed Source run/pending linkage. Complete SINGLE_ROOT scope only; mixed/two-root masquerade refuses before staging. No standalone POSITION_APPLY or fake old-publication binder; canonical stage/seal then exact Source binder. Newly qualified money uses fresh reviewed continuation, respects current superseding decision and never restores denied pay. COMPLETE Local without canonical Banking mapping remains published=false with genuine saved result and usable Source continuation. All NOT_RUN.
