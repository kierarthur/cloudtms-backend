# WB-08 maintained projection design

Sequence 49. Card02:320-386, all checklist rows and R2 §1/§5 read before code.
Exact objects are workbench_item, workbench_candidate_summary, workbench_revision,
refresh_many(uuid[],uuid[]), apply_delta, set_bank_warning(uuid,text), the named
Candidate-name trigger, and card-owned refresh_work/refresh_case. The card also
names one refresh after the actual Source disposition seed's position update.
No Worker or Office deliverable; no other Source/Pay Query producer changes.

An item is one exact component or one genuine component/segment attribution.
Its unique non-null identity is group_key/component_key/member_key. WORK groups
use the durable work UUID; CASE groups use the actual Case UUID. Segment keys
are upstream keys retained by WB36; removed/moved credit retains its old parent.
The item stores owner FKs, revision/date/source and target channels, nominal and
approved source gross, realised/held/residual, included/excluded positive gross,
recovery difference, quantities/unit, genuine snooze identity/version/wake,
query/hold/blocker fields, progress/error and financial/projection versions.
Missing or inconsistent evidence has NULL money and a blocking reason, never
an invented zero. Zero approval remains an explicit certified factual state.

Complete group presentation is separately stored with one identity, section
membership/counts and exact financial dimensions. A group may have READY and
WAITING portions simultaneously; each financial child contributes once. Group
counts at each summary key use distinct group membership; the ALL-section row
counts that same group once across its portions. Child/segment counts remain
separate. Selected count means actual accepted server selection membership;
eligible count is separate. Without such membership selected is not applicable,
never guessed from Ready defaults. Actor baselines are WB22, not global metadata.

Natural summary scopes are real Client or ALL, target PAYE/UMBRELLA or ALL, and
READY/ACTION/BLOCKED/WAITING or ALL. ALL has an explicit discriminator and NULL
Client identity, never a fake Client. Case rows without an actual retained Client
appear once at Client-ALL. Candidate summary and necessary agency scope-summary
rollups store payable/selected/snoozed/held/queried/blocked contribution counts,
ready/waiting payout gross, excluded/paid/reserved/recovery gross and deduction
balances separately. No background NET/headroom, channel conversion or mixing
outstanding debt with ready payout gross. Candidate membership transitions update
agency counts rather than adding one Candidate per component.

Only refresh_work and refresh_case call apply_delta. Refresh locks existing
Candidate control, then exact work/Case, Candidate metadata and projection
revision in consistent order. It snapshots at most20 natural aggregate rows
before replacing its exact indexed group, snapshots the same after, and applies
one subtract/add delta per natural key. Typed delta records/arrays are internal,
bounded by rows and bytes; no whole-record JSON payload. A repeated refresh
replaces the same contribution and has zero financial delta. Group replacement,
rollups and revision increment commit atomically. No Candidate/history sweep.
refresh_many accepts at most100 distinct total work/Case IDs and a16KiB identity
envelope, prelocks sorted Candidate/control/owner/metadata identities before any
singleton revision lock. Larger fanout belongs to WB44 continuation, not first500.

The explicit classifier precedence is: missing/damaged/applied-incomplete facts;
processing/current capture; named blocking controls; action needs; future week;
exact valid snooze; ready. Native current/applied pointers and exact position
conservation precede any positive offer. Accepted Create has an exact live run
link and WAITING/IN_PREPARATION without re-reading later selected inputs. Segment
gross comes from current immutable approved children and exact indexed original
shares; aggregate C/H must reconcile each actual position before any child can
be offered. Snooze excludes only positive remainder; negative recovery never
vanishes. Charge-only revision changes preserve a valid worker-pay basis.

Reason vocabulary and display: READY 'Ready to pay'; MISSING_APPROVAL 'Awaiting
authorisation'; DATA_UNAVAILABLE 'Required pay data is missing'; APPLY_PENDING
'Pay facts are being updated'; SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE 'Original
shift credit needs review'; IN_PREPARATION 'Included in Draft preparation';
FUTURE_WEEK 'Not payable until {eligible_from_date}'; SNOOZED 'Snoozed';
SNOOZE_SEGMENT_STALE 'Saved shift snooze no longer matches'; PAY_QUERY_OPEN
'Pay Query open'; PAY_QUERY_UNAVAILABLE 'Pay Query evidence unavailable';
PAY_ON_HOLD 'Pay on hold'; APPROVAL_WITHDRAWN 'Approval withdrawn';
AWAITING_REAUTHORISATION 'Paid — awaiting re-authorisation';
ORIGIN_UNAUTHORISED_WAIT 'Waiting for re-authorisation — recovery paused';
RATE_RESOLUTION_REQUIRED/GROSS_RESOLUTION_REQUIRED 'Pay channel value required'.
Bank readiness/change is a warning only, never a section reason or selectability
gate. Current bank-safety execution remains with EX06/EX07.

Later master branches remain their exact owners: WB02 withdrawal/WAIT, WB07
qualified Source query detail, WB40 live pay hold, WB41 recovery WAIT, WB29 real
resolution requirements. They merge the WB08 winner and cannot override higher
priority. WB36 snooze is consumed now; its tables/commands are not rebuilt.
Future-week eligibility uses the retained actual work week and UK current week;
WB37 promotes due rows. No external Source producer/protocol is introduced.

Candidate metadata stores current sort name/reference and bank warning with a
metadata version and explicit pending-continuation flag. The named NEXT-gated
Candidate-name trigger and set_bank_warning update only this metadata and dirty
barrier, never earnings. WB44 supplies the actual bounded current-row job/seed
continuation and clears the flag only after complete adoption. Revision has
dirty/pending counts and an adoption version; readers cannot offer dirty work as
Ready. This is supporting state for the required projection, not another writer
framework or a substitute for WB44's complete finite census.

New private tables/types/functions retain owner-only RLS/ACL; all new entry
functions refuse unless active_owner=NEXT, and named triggers return early under
LEGACY. Exact source/segment scope and native money owners remain unchanged.
Required checks only: local PG17 load, db:check, node syntax if JS changed,
seal/coupling and existing source gates. No fixtures/first business-use claims.

Native detail refinements: summary accumulators retain known exact money plus unknown_amount_count; a reader must return unavailable totals whenever this count is nonzero, never a fabricated zero. Projection does not compute PAYE NET/headroom. Exact segment keys reconcile native source AND target C/H before use. Original paid/active share sums cannot be assumed absent-zero. Current snooze uses current command clock sequence and UK business date, while Create remains frozen. WB07 alone connects its existing stable query gate; WB08 stores the typed handoff, no producer or volatile admission. Accepted Waiting is proved by run_command/PREPARE and sealed actual choices before workers exist, and applies only admitted components/segments, never the whole Candidate. Native work future eligibility is week_ending_date minus6 after the UK business date; DR01/WB37 retain admission/promotion authority. Case PAUSED and native resolution_state REVIEW remain explicit CASE_PAUSED/CASE_REVIEW_REQUIRED states; no fake channel-change need or background allocation. Metadata pending-continuation makes adoption incomplete until WB44 closes the exact fanout.

Exact effective-section refinement: identity also includes portion_kind AVAILABLE/RESERVED/RECOVERY/ZERO/EVIDENCE. A component or genuine segment may have an eligible positive remainder AND an ACTIVE reserved portion; they are separate disjoint contributions, not one misleading Ready/Waiting reason. The base AVAILABLE/RECOVERY/ZERO/EVIDENCE row carries A/C once; a RESERVED row carries H once and no duplicated A/C. Snooze moves only the positive AVAILABLE portion. Work recovery is informational origin gross; a real Case recovery remains the sole selectable deduction owner and its outstanding balance is a separate dimension, never double-added to WORK recovery gross. Case component available/active-reserved portions likewise remain distinct, with one complete Case group membership. Future WB07/WB40 branches target only the qualified positive portion; metadata warnings alter neither section nor sums.

## Checked design precision (DP01-DP04)

DP01: stable item identity is `(group_key, component_key, member_kind,
member_key, portion_kind)`. member_kind is WORK_COMPONENT/WORK_SEGMENT/
CASE_COMPONENT; member_key is the actual component key, stable segment key or
Case-component UUID text, respectively. No arbitrary sentinel collides with an
upstream key. A WORK row has actual work/Candidate ownership and revision/line/
segment FKs where present; a CASE row has actual Case/component/Candidate FKs
and no fabricated work/Contract/Client. Null owner combinations are constrained
by kind. A removed original segment may have no current approved_segment FK
only with genuine old-origin attribution and native position reconciliation.

DP02: sample UK business date once from server transaction time. Normal native
Sunday week evidence must have isodow=7; current Sunday is business_date +
(7-isodow(business_date)). Future means week_ending_date exceeds that Sunday;
eligible_from_date is week_ending_date-6. Invalid nonstandard evidence is
DATA_UNAVAILABLE unless an actual later native owner explicitly qualifies it.
This replaces the earlier abbreviated week predicate; WB37 owns promotion.

DP03: accepted membership requires sealed run selection, genuine run_command
and PREPARE command, exact selected Candidate and SEALED work_choice; an OPEN
selection is excluded. Run PREPARING/REVIEW/DRAFT/EXECUTING and an absent worker
or worker PREPARING/REVIEW/READY/DRAFT/CANCELLING/ISSUED retains the exact offer.
Worker CANCELLED/COMPLETE or run CANCELLED/COMPLETE ends this membership;
any genuine surviving ACTIVE holds remain separately RESERVED/nonselectable.
Job DONE and command COMPLETE do not release the offer. FAILED command on an
otherwise live accepted run stays visibly interrupted/nonselectable. Retain its
actual module epoch as provenance, never reinterpret an epoch change as release.

For a genuine segment, accepted membership is the exact stable segment in the
accepted choice's selected_segment with INCLUDED disposition. For an indivisible
component, ALL requires the component in the actual expected-revision approved
inventory; SUBSET also requires selected_component. Its accepted-cut snooze must
be absent; an excluded offer is not a selected wage. New sibling identities after
click are next-Draft facts; they do not acquire a Candidate-wide preparation flag.
The accepted offer's old revision/amount is never refreshed or rewritten here.
The live next-offer projection retains current revision and exact original money.

DP04 finite reason/section/rank table (smaller wins):

| Reason | Section | Rank | Current or named successor |
| --- | --- | ---: | --- |
| DATA_UNAVAILABLE | BLOCKED | 10 | WB08 native missing/damaged evidence |
| SEGMENT_CREDIT_ATTRIBUTION_UNAVAILABLE | BLOCKED | 12 | WB08 exact parent mismatch |
| APPROVAL_WITHDRAWN | BLOCKED | 20 | WB02 withdrawal with no realised pay |
| MISSING_APPROVAL | BLOCKED | 21 | WB08 no current authorisation |
| APPLY_PENDING | BLOCKED | 30 | WB08 incomplete native applied pointer |
| PAY_ON_HOLD | BLOCKED | 40 | WB40 current physical financial owner |
| IN_PREPARATION | WAITING | 50 | WB08 exact accepted scope |
| PAY_QUERY_UNAVAILABLE | BLOCKED | 60 | WB07 qualified positive Source WORK |
| PAY_QUERY_OPEN | ACTION | 61 | WB07 qualified positive Source WORK |
| AWAITING_REAUTHORISATION | ACTION | 70 | WB02 real paid withdrawal |
| ORIGIN_UNAUTHORISED_WAIT | ACTION | 71 | WB41 exact recovery origin |
| RATE_RESOLUTION_REQUIRED | ACTION | 80 | WB29 genuine EX08 decision need |
| GROSS_RESOLUTION_REQUIRED | ACTION | 81 | WB29 genuine EX08 decision need |
| CASE_REVIEW_REQUIRED | ACTION | 82 | WB08 native component REVIEW |
| RECOVERY_MANAGED | ACTION | 83 | WB08 informational original work gross |
| CASE_PAUSED | WAITING | 90 | WB08 actual Case PAUSED |
| FUTURE_WEEK | WAITING | 100 | WB08 actual later work week/due date |
| SNOOZED | WAITING | 110 | WB08 genuine current matching WB36 control |
| RESERVED | WAITING | 111 | WB08 disjoint native ACTIVE reservation |
| READY | READY | 200 | Complete eligible native contribution |

SNOOZE_SEGMENT_STALE has no section/rank: it is warning metadata only and
excludes nothing. Preserve every actual secondary control in separate typed
columns. Future classifier owners add only their branch without overriding a
smaller current rank. Qualified query means actual SOURCE/PROTECTED positive
WORK/PROTECTED_WORK portion, not ordinary/reimbursement/Additional/zero/negative
or reserved money. WB07 makes the existing STABLE gate call and traps only that
read's failure, never producer SQL or a VOLATILE admission.

Bank metadata code vocabulary: NULL clears the warning, BANK_DETAILS_MISSING reports the actual missing native normalized hash, and BANK_DETAILS_CHANGED reports a genuine saved bank identity change. This owner asserts no provider verification or execution readiness. The named name trigger listens only to effective display/first/last name and TMS reference changes. WB44 later connects genuine bank-change events/continuation. The revision singleton begins UNSEEDED with zero dirty/pending counts; only WB44's complete current-scope seed can adopt CURRENT. Existing refreshes can maintain rows during UNSEEDED but cannot claim read availability. No source-only check enables the projection.

DP05 count/zero precision: total native payment segments at a key are DISTINCT
(group_key,component_key,member_kind,member_key), excluding portion_kind; section
ALL counts AVAILABLE+RESERVED for the same segment once. Per-category counts
are exact disjoint contribution counts; no hours/miles/NET unit mixture. A ZERO
eligible work contribution requires a genuine current certified-zero approval
with zero native realised/reserved gross. Fully settled A-C-H=0 is absent from
Ready; a fully reserved current contribution remains RESERVED/Waiting only,
carrying its A/C factual fields once if no other base portion remains. No paid
history/context filler becomes a new tickable zero wage. A real repayment with
positive remaining obligation stays Ready without computing headroom; its later
estimate may recover zero. Fully recovered/written-off Case capacity is not
invented as an outstanding Ready repayment. Exact ACTIVE Case reservations
remain separately represented; no guessed stage/quantity or amount allocation.

DP06 implementation closure: a certified, complete empty approval has a genuine
group certified_zero flag and a nonselectable GROUP_EVIDENCE row with NULL
financial-child money. It contributes zero native segments and no fake tickable
wage. Only complete certified group evidence is excluded from the missing-money
count; future week remains Waiting with its real Monday. Case write-off is a
separate typed field and is carried once alongside A/C/H. Each known native
member's whole portion set must reconcile before apply_delta; no per-row false
conservation assumption. Source removed components retain their actual binding
classification line; a present current line's NULL tax/date stays NULL.

Final apply-position hook remains at WB-06's explicit1430 master-chain turn.
WB-02 adds the actual origin-WAIT transition refresh after this item. WB-44 owns
real seed/metadata continuation; D12/whole ITEM remain open until that join.
