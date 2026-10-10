# WB-04 ordered Daily source investigation

Read-only checkpoint, 10 October 2026. Read the full WB-04 card (`02_WORKBENCH.md:185–210`), ITEM and D01–D09 checklist rows, actual final-definer registry, approval/Daily/route editor chains and DY-02/03/04/05/06/08 cards. No SQL, application, tests, gate, fixture, remote or hosted operation was invoked. This is source verification and a handoff; **WB-04 ITEM and D07 remain OPEN**, and every remaining delivery requirement remains required.

## Present source and confirmed incomplete seams

| Named object | Current source and exact evidence | Disposition |
|---|---|---|
| Daily evidence `private.bpay_next_daily_approval_evidence_v1(uuid)` | Registry final is `07102026_0018_banking_pay_next_initial_tsfin_daily_evidence_v1.sql:229–359`. It validates the actual current authorised Daily TF/physical version, booking, Candidate/Client, hours-only AGGREGATE, physical worked/break tuple and captured policy/rate references. Contract identity is checked only when a real Contract exists (`281–287`). Canonical TSFIN context must identify the exact physical/financial/Candidate/Client tuple (`290–296`); work date derives from the actual worked start in Europe/London (`298–300`). Both PAYE and UMBRELLA rates use the actual dated resolver and exact retained selected authority (`304–323`). | Genuine supporting reader exists. This does not close contractless publication, immutable Client/settings capture or complete canonical evidence. DY-02 then DY-08 are its named editors. |
| All five available Standard buckets | Daily reader constructs DAY/NIGHT/SAT/SUN/BH from both resolved channels (`343–350`), but `351–352` filters out a bucket when all three available rates are NULL. Current publisher `1009:207–215` supplies absent STANDARD rows through `bpay_next_contract_rate_rows_v1('{}', Contract labels, Contract Additional rates, mileage rates)`. | **Confirmed DY-02 gap:** genuine Daily evidence must retain all five canonical rows including all-NULL missing rows. Important precision: the publisher passes `{}` for standard rates, so those fallback rows have NULL PAYE/UMBRELLA/charge; it does not substitute Contract standard money. The canonical row nevertheless comes from the Contract-reader fallback instead of complete Daily resolver evidence. Do not claim that this already completes DY-02. |
| Approval route | `03102026_1450_banking_pay_next_producer_route_v1.sql:33–45` joins the physical Timesheet to an actual Contract before returning any route. Expense carrier, imported-child qualification and Source-family branches remain correct WB-03 reuse. | Contractless Daily cannot reach a genuine route. Only DY-04 follows WB-03 in the master route chain; no WB-04 out-of-order rewrite. |
| Ordinary/Daily publisher | Registry final is `10102026_1009_banking_pay_next_ordinary_approval_rate_freeze_v1.sql`. The strict Contract lookup (`85–86`), accepted route allowlist (`88`), Candidate/Client checks against Contract (`94–95`) and labels via Contract (`104/113`) still require a genuine Contract. The Daily reader is called at `206`, after those checks. | Current contracted Daily supporting path is present; genuine contractless path is blocked. `v_resolved_daily_rates` mentioning DAILY_HOURS (`203`) does not admit that route through `88`. DY-04 must merge the current full publisher after WB-36/WB-02, preserving WB-01 available/used rates and all intervening changes. |
| Core work/revision | `03102026_1324_banking_pay_next_core.sql:111–129` requires `bpay_next_work.contract_id NOT NULL` (`114`), real physical UUID/booking and existing work kinds. Revision (`132–176`) retains physical UUID/version, actual TF identity, policy ID and frozen labels; it does not contain the DY-03 `origin_kind`, `origin_work_date` or `daily_approval_evidence_json` additions. | DY-03 owns the supporting schema. Permit NULL Contract only for the finite genuine DAILY/ORDINARY/date shape; retain real physical identity, Candidate and booking. A fake Contract or new financial work kind would violate the card. Schema presence by itself does not prove publication. |
| Immutable guards | Actual registry finals for revision, child and work-pointer guards remain `03102026_1342_banking_pay_next_guards_v1.sql`. Full revision `to_jsonb` seal-only comparison (`31–37`) already freezes future revision columns. Children are insert-only and lock their real revision (`96–117`). Work pointer compares identity tuples with `IS DISTINCT FROM` (`134–139`), including Contract; the actual work insert guard checks original physical booking (`179–182`). | **No Contract JOIN exists in these current 1342 guards.** Do not invent one or blanket-replace them. DY-03/DY-04 must freeze any genuine new work-origin fields as well as the existing identity; retain the existing NULL-safe Contract tuple, physical booking and seal/counter predicates. |
| Actual Client/settings evidence | Reader returns actual TF `client_id`, policy snapshot, retained rate references, resolved channels and work date (`353–358`), but publisher currently freezes Client display through the actual Contract; the complete Daily evidence return is not stored in a revision evidence column. | DY-03 schema and DY-04/05 capture must retain genuine approved TF/booking Client and settings/physical evidence. No later live Client inference or reconstruction of a Contract. |
| Hours-only integrity | Current Daily reader refuses nonempty Additional units/pay, expense/mileage/travel/accommodation/other pay and nonzero mileage units in its approval qualification. Current publisher thus cannot use that genuine Daily branch to approve ancillary lines. | Keep this finite refusal. Dedicated `BPAY_NEXT_DAILY_HOURS_ONLY_VIOLATION` handling is DY-08's named work. Available unused Contract Additional/mileage schedule rows are expressly preserved by DY-02 (`06:906`); OR-39 forbids approved financial lines, not those retained unused available rows. Do not remove them prematurely. |

The current Daily evidence return and current supported contracted publication are useful source, not proof that a real contractless Daily reaches READY or that a particular hosted catalogue contains this capability. Parent local schema compilation is separate evidence; no fresh hosted read was made by this investigation.

## Exact ordered handoff

Master `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:67/69/70` is binding:

- Approval: **WB-01 → WB-36 → WB-02 → DY-04 → DY-06 → DY-10 → DR-38**.
- Daily evidence: **DY-02 → DY-08**.
- Route: **WB-03 → DY-04**.

| Future named owner | Finite retained requirement |
|---|---|
| DY-02 (`06:901–925`, sequence 88) | Adopt the genuine Daily evidence helper changes from the specified contractless artefact, emit all five available Standard rows including NULL missing rows, and keep actual dated override/default PAYE/UMBRELLA/charge authority matched to the approved TF. Preserve Contract Additional/mileage/labels and Weekly branches. The contracted-only overlay does not complete the contractless schema/path. |
| DY-03 (`06:929–952`) | Apply the actual specified contractless schema: finite immutable work origin/date and revision Daily evidence, real NULL-Contract DAILY/ORDINARY shape, no new financial ledger/work kind or invented Client/Contract. Check the actual guards rather than assuming a Contract join. |
| DY-04 (`06:954–980`, sequence 90) | Design first as its card requires. Compose the newest full route/publisher/guard definitions with genuine Contractless Daily processed-financial/booking authority, actual Candidate/Client/date/settings evidence and retained current authorisation/version. Follow Process PENDING_AUTH → Office Authorise/actual auto-finalisation; RECEIVED is not final. No Source HEAD/binding, Source join rewrite, fake Contract or ordinary fallback. Merge all three specified publisher spans into the current shared owner without losing prior approval editor changes. |
| DY-05 (`06:982–1006`, sequence 92) | Retain genuine WORK:DAY AGGREGATE money, clocks/breaks/buckets/certified zero, actual physical UUID/version, immutable Client/settings/display evidence and actual available/used rates. Frozen readers use captured revision; inspect any real NULL-Contract reader join. Combined PAYE/UMBRELLA preparation must seal 2/2 and resume both. Office label is "No Contract (Daily)". |
| DY-06 (`06:1008+`, sequence 93) | Preserve the named withdrawal/rotation identity and existing approval-only retention meaning; Manual change is refused only when real Draft capture/payment/invoice retention exists. Sticky approval retention is not by itself a payment. No WB-04 early rotate change. |
| DY-08 (sequence 95) | Existing hours-only integrity gets its exact typed dedicated refusal and named Worker/Office propagation. No ancillary Daily feature and no premature unqualified provenance-error rewrite. |
| WB-36/WB-08/WB-44 | Complete the existing staged-publication dirty/Processing/projection hook and maintained Workbench obligations in their named order. Reuse actual immutable publication and Candidate command order, without history scans. |
| WB-16 / DR-38 | Daily/booked date label and later tax vocabulary remain their actual owners. No Worker route change is required by WB-04 itself. |

## Checklist disposition at this checkpoint

| Exact WB-04 row | Source/handoff status |
|---|---|
| D01 staged publication | Existing owner retained. Its finite maintained projection/invalidation requirements remain OPEN under the named projection editors; existing publication is not completion of the UI. |
| D02 route | Correct ordinary/expense/qualified Source reuse retained. Genuine Contractless Daily seam remains OPEN until DY-04. |
| D03 publisher | WB-01 available/used authority retained; Daily canonical evidence, genuine contractless authority/capture and projection/tax followups remain OPEN. |
| D04 exact rate | Reuse existing exact nonnegative six-decimal parser; no new parser/calculator needed. Honest NULL unavailable remains required. |
| D05 contract rate rows | WB-01 final canonical five-plus-mileage and actual arbitrary Additional code handling retained. It supplies no proof of genuine Daily resolver completeness. |
| D06 core | Existing real core retained; genuine contractless schema remains OPEN under DY-03. |
| D07 Daily evidence | **OPEN**: genuine supporting reader exists, but all-NULL evidence and complete Client/physical/settings/route/core closure are still required. |
| D08 later approval delta | Current full shared owner retained; later DY-04 must compose the actual ordered definition, not reinstall old 1530 or a contracted-only overlay. |
| D09 output card / ITEM | Documentation is source/handoff evidence only. **ITEM OPEN**, no blanket BUILD/REVIEW/installation/app completion. |

All four actual WB-04 acceptance observations remain **NOT_RUN**: genuine Contractless 7.5-hour one READY work/frozen Client; contracted Daily same path; complete frozen bucket window with missing distinct from zero; hours-only financial lines. DY capture, rotation and combined preparation observations remain NOT_RUN too. No F25 parked producer/protocol/resolver work, early F26, new calculator or fake local acceptance is justified by this handoff.

## Fresh source SHA-256

| Saved source | Raw SHA-256 |
|---|---|
| Daily evidence 0018 | `fa256bfa14b93321e1749c7cbee2505baca4f4da2eef5d11ea6457bdfae120e2` |
| Core 1324 | `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c` |
| Guards 1342 | `d29decd59c88c2620fe658b5d96f0deace19063cd0eaddf150ea7cf09c7b60af` |
| Route 1450 | `a8f9dc118ad75b62b3863820e38bd29cbc4c05aa47facf12c11e45b9013bfc74` |
| Current ordinary publisher 1009 | `34227084b44b53abfb441b2b2826d9ce437fdb20e636e7d3d1d3f54bf324c7d7` |
| Current canonical rate reader 1009 | `07843dcef992a74c2dd2c8c40185d17ce70aba9c33c1c53db5bbcbf6d6952fe2` |
| Final-definer registry | `2dc51e7f79fb45b97905af6bcdc39bef57d172c57e53c111af2ce2ab7b0fd8e4` |
