# DR-07 build investigation — sequence 125

12 October 2026. Read-only source findings and a narrow implementation proposal. No product/checklist change, SQL/business call, fixture, test, render, hosted operation or commit was performed. Future joined/application checks are **NOT_RUN**.

## Exact authority and delivery boundary

Full owning card `03_DRAFT_PAYE_NET_BATCHES_FINANCE_CASES.md:202–222`, every current DR-07 checklist row and `review/R3.md:76–82` require an agency/actor-scoped bounded ACTIVE_PREPARATIONS read, server-recorded actor/start/cutoff/counts, exact progress reattachment, preparing banners and one factual Open banner per resulting confirmed channel. Master `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:209` assigns sequence125, after DR-06/DR-18/F-16; DR-28 is sequence155 and consumes this banner seam later. No approval decision is required. R3 explicitly rejects inferring the agency's active preparation from one browser's last run UUID.

| Checklist delivery | Exact remaining work |
| --- | --- |
| D01, D04, D07 | Context/reference rows only; preserve their actual current parents. |
| D02 | New named `banking_pay_next_active_preparation_index` migration; scoped newest-first native Run access. |
| D03 | Concrete closed ACTIVE_PREPARATIONS request/row/cursor contract and native source references. |
| D05 | Existing `broker/src/bpay-next-preparation.js` page allow-list/input/output/error maps. |
| D06 | Existing NEXT Banking Pay Office banner and server-based own reattachment; reusable banner seam for DR-28; confirmed channel banners from exact CONFIRM addresses. Preserve future host boundaries. |
| D08 | One complete current `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` successor adding this read-only branch. |

The new reader is currently absent from the public dispatch and Worker/Office page enums (0442:550–573; Worker:141–172). The native current Office contains only qualified last-run navigation recovery, not an agency run directory (Office:2276–2290,2618). No "Resume run ID" input/Resume button is presently rendered: `resumeInput` survives only as an unused declaration/reset; remove this dead local if convenient, but do not claim a currently visible paste control still exists (Office:2494,2737,2747).

## Current exact parents

| Exact signature/module | Registered current source |
| --- | --- |
| `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` | `supabase/repeatable/12102026_0442_banking_pay_next_run_review_totals_v1.sql:276` — preserve REVIEW_TOTALS/CASES, DR15/PF08/DR14, audit/list/preview/GUARD and all retained native pages. |
| `private.bpay_next_preparation_progress_v1(uuid)` | `supabase/repeatable/12102026_0414_banking_pay_next_preparation_wait_capacity_reader_v1.sql:5` — stored progress, no read-side status transition. |
| `private.bpay_next_preparation_authority_v1(uuid)` | `supabase/repeatable/04102026_0575_banking_pay_next_preparation_service_v1.sql:8` — service JWT, active admin, NEXT; includes FOR SHARE and is therefore not the new read-only branch's entry helper. |
| `private.bpay_next_create_preparation_v1(uuid,uuid,date)` | `supabase/repeatable/10102026_1621_banking_pay_next_preparation_pay_date_v1.sql:5` — original immutable start/deadline/replay identity. |
| `private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid,jsonb)` | `supabase/repeatable/11102026_2223_banking_pay_next_shift_query_exclusion_v1.sql:61` — actual current accepted-choice Create owner, preserving DR10 policy and ordinary/Source query scope. |
| `private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid)` | `supabase/repeatable/11102026_1358_banking_pay_next_guarded_create_v1.sql:590` — five-argument wrapper delegates to the current six-argument owner. |
| `private.bpay_next_channel_drafts_page_v1(uuid)` | `supabase/repeatable/12102026_0645_banking_pay_next_confirm_refresh_v1.sql:114` — factual confirmed PAYE/Umbrella addresses, completed pre-confirm RELEASE exclusion. |
| Typed transport/UI | BE `broker/src/bpay-next-preparation.js`; FE `js/bpay-next-office.js` and native F-16 options/host. No new route or command. |

0442 is the current **public** reader parent; 0552 is DR-13's private review/capture successor, not a replacement public preparation page. The DR18 0645 SQL source review is already saved; partial future Payment Batches handoff does not block this independent read.

## Native identity, authority, time and counts

`private.bpay_next_pay_run` has native `id`, `actor_user_id`, `status`, `selection_state`, selection/count/review metadata, `created_at_utc`, `confirmed_at_utc`; it has **no agency_id column** (core1324 migration:418–435). Physical agency identity is the existing singleton `private.cloudtms_database_identity.installation_id`, already used as Workbench `context.agency_id` (0235:65–72). Do not invent a Run agency field or join today's Candidate/Contract to infer the agency.

Reuse the current public read-only early-branch pattern: service-role JWT plus the authenticated current active-admin point predicate, plain current NEXT/module-epoch read, native installation identity, strict args and byte bound, before `perform ...preparation_authority` (0442:287–294,503–518,539–550). The Worker supplies authenticated `p_actor_user_id`; request data cannot override it. Reader context/cursor should bind this actor, native agency, owner epoch and finite ALL/MINE scope. Agency-qualified browser state uses the **viewer's** fresh namespace; Run `actor_user_id` is the original initiator and determines its label/own reattachment. Do not substitute an initiator's namespace into a different viewer's progress window.

The original native start/deadline are created together once and exact replay returns the stored Run (1621:29–38,46–58). Migration0501:6–9 requires finite `preparation_expires_at_utc = created_at_utc + 72 hours`. Read these fields, never recompute/extend from now or official payment date. The due index0501:23–24 is deadline-ordered and does not supply newest-first run paging. No reader cleanup, expiry receipt, fresh valuation or monetary recomputation belongs here. A deadline passing does not itself prove holds released: the current native status/cleanup owner remains authoritative.

Actor text is an exact tms_users PK read: `left(coalesce(nullif(btrim(display_name),''),'Unknown user'),250)` (0414:50–52). It is a current identity label, not a falsely frozen historical name. Native maintained buckets are `selected_candidate_count`, `preparing_candidate_count`, `ready_candidate_count`, `review_candidate_count`, `cancelled_candidate_count` (0537 migration:5–8; core1324:427–430). Ordinary pending is selected minus ready/review/removed; preparing is a subset of pending, not an additional disjoint bucket (0414:58–59,77–82). No worker/line summation is needed for this reader. If exposing PARTIAL-discard REVIEW rows, preserve the existing exact discard-request count interpretation using its unique Run point (0414:63–72); do not silently present the raw ordinary buckets as successful resumable preparation.

## Minimal finite reader/index proposal

Use only native statuses PREPARING/REVIEW, newest-first `(created_at_utc DESC,id DESC)`, limit1–100 and strict `<` seek on the exact timestamp/UUID pair. ALL is needed for user B to see user A; MINE is needed for exact own reattachment without scanning all agency pages. Scope is a finite selector, not arbitrary SQL or an actor directory. A compact row needs original Run/initiator identity, current status/selection phase, actor display name, original start/cutoff and maintained counts; the response has complete/next cursor and a closed agency/viewer/epoch/scope context. No amounts, per-channel progress totals, Candidate inventory, lease/job envelope or new revision counter.

The card proposes a partial `(status,created_at_utc DESC)` index. Include the UUID tie. With status leading, obtain separately limited PREPARING and REVIEW keysets and merge at most twice the requested page/lookahead; one ordinary global ORDER BY cannot be claimed as one index walk across both status prefixes. An actor-prefixed equivalent supports MINE directly. Alternatively partial `(created_at_utc DESC,id DESC)` and `(actor_user_id,created_at_utc DESC,id DESC)` indexes with the same status predicate provide the exact two access paths without two-status merging; record this small index-layout refinement in the scope note. Do not leave an actor filter as an unbounded residual against all agency runs. These are supporting indexes on the named table, not a maintained projection/store or trigger system.

Use limit+1 discovery or the original finite next-page convention; never infer completeness from UI banners retained in memory. Bind cursor scope/agency/viewer/epoch and reject cross-scope tuples. Active membership is a live list: a Run can leave between pages; fresh Refresh restarts. No digest/proof/signing/store/version framework is required. ACTIVE_PREPARATIONS should remain independent of Workbench financial projection dirty/pending barriers; it reads native preparation metadata, not projected payable money. Worker and FE parity must validate count conservation, strict timestamp/UUID order, duplicate IDs, cursor/complete relation and finite response bytes.

## Accepted OPEN submission and reattachment boundary

There is a real intermediate state to resolve narrowly. The **current** accepted-choice Create owner2223 calls the original deadline owner (252), receives the original PREPARE command, inserts `run_command`, and freezes descriptor/counts **before** the Office completes uploads/SEAL (2223:577–586). The receive owner1404:32–36 reads the current NEXT epoch;1404:55–57 persists `{command_kind:PREPARE,status:RECEIVED,module_epoch:current}`. Thus a positive accepted Create has an actual PREPARING/OPEN Run, immutable descriptor/command and the original deadline/current epoch. A zero-member empty Create has no received PREPARE command (2223:577–581); do not mistake that metadata-only outcome for started financial preparation. Current PROGRESS normally refuses unsealed ordinary runs (0414:21–40). Do not equate every PREPARING row with a sealed progress offer, and do not silently omit accepted OPEN rows while claiming the all-active contract.

Root's chosen minimum is a tiny same-PROGRESS read-only OPEN support branch, qualified by the exact accepted descriptor/original positive RunCommand/current PREPARE epoch. Expose factual selection-state flags; no financial work starts on this read, and no "complete" or settled Draft result is inferred before SEAL. Show the truthful submission phase, retain/rejoin only the initiating viewer's exact existing submission journal where available, and preserve ordinary sealed progress semantics. Another user's read must not replay Create/upload bytes, synthesize a descriptor or start a fresh command. Existing same-actor recovery APIs are `createChoiceSubmission(...resumeOnly:true)` and original journal continuation; current progress recovery only restores exact pending SEAL and readback (Office:1139,2376 onward). No full journal census or new protocol is needed. The new ACTIVE reader and this small PROGRESS support must not hide accepted OPEN rows.

Exact original progress guard to preserve:0414:21–40 permits SEALED or its two closed OPEN terminal expiry/discard associations, and requires the recorded deadline;0414:41–49 then reads original RunCommand/command and rejects a missing OPEN original association except the completed terminal request, or a command with a noncurrent NEXT epoch. Add only the ordinary accepted OPEN qualification beside those terminal alternatives: PREPARING, unconfirmed, positive selected count, nonnull immutable accepted descriptor/command, exact `rc.run_id=r.id` and `rc.command_id=r.accepted_choice_command_id=c.id`, actual PREPARE/current NEXT epoch and the initial received command state. Native initial command has `expected_member_count IS NULL` and `enrolled_member_count=0` (core1324:795–807;1404:55–57), so enrollment-complete remains false; zero enrollees do not mean complete. Original seal owns `selection_state='SEALED'` and its review increment later (current2223:692–705,707–726,764–765). Do not weaken its uploaded Work/Case/query completeness checks or any terminal OPEN guard while adding observational progress.

On Banking open, query server MINE to select the newest genuine resumable original Run; ALL supplies everyone else's banners. Reuse `openBpayNextPreparationProgress` (Office:2293–2305), viewer actor/agency/epoch checks, existing frame focus/reuse and actor/owner invalidation. Current last-run pointer remains useful for original unknown submission recovery, but is no longer the source of global/own active-run truth. Do not erase pending original bytes merely because the active list has advanced or a row has disappeared.

## Confirmed banners and supplied design

No new confirmed-channel reader is necessary: exact CONFIRM `drafts` plus existing CHANNEL_DRAFTS0645:122–147 supply one native address per non-released original PAYE/Umbrella channel, in fixed order, even after later execution cancellation; no expiry or repricing authority. Existing Office `showCreatedDrafts`, `openConfirmedChannel` and `openPaymentBatches({run_id,drafts})` handoff are real reusable seams (Office:2038–2074). The last is an injected future host seam, not proof DR28 is built. DR27 owns the future maintained channel list; do not add that directory now. Reloaded known confirmed Run addresses can be qualified with the existing CHANNEL_DRAFTS read, without assuming a browser receipt is current.

Supplied immutable design pack `DESIGN_INDEX.md:56`, B4 SCR41/SCR41b specs and `AUDIT_ROOT_B4.md:164–165` govern the preparing and Open banners. SCR41b explicitly says the initiating user auto-reopens progress; other users see the banner. SCR41 supplies Open progress and exactly one Open PAYE/Umbrella action per real confirmed address. Its list body/counts/status/money belong to DR27/DR28, not this reader; `readerAdditions: []`. Invented PB references and future batch facts must not fill the banner. Banking Ready data/filter/export/create controls remain owned by their existing readers. Appearance review will compare the real native banners against these PNGs; no captures were made in this investigation.

## Source checkpoint hashes

| Current source | SHA-256 |
| --- | --- |
| Public reader0442 | `8CBBD0D0BDE4FC93C507EF1002A94B050DD3A85D64107DDF592B1E2DB6FD978E` |
| Progress0414 | `8344BFF1A8ECD96E3FBB289F3B3F16CB37B56E5043B4B9D5999FBF2CE8D7FE9E` |
| Confirm/channel0645 | `5C1E24129CF8A14C97C80D4A7027EAF7F434BFFFEC2FF58872A159FE824552A1` |
| Worker preparation | `542D55D3CB71FB718D230279F39C3EB3516CF4BD09DE607B05F6968130A8154B` |
| FE Office | `3EC63208313C011C49044D37A2F334D9E4DF5D4321AAEBCCC25DAF25C24E7339` |

These are a source investigation checkpoint, not installation evidence. Re-read current bytes before authoring. No unfinished future host/UI work should block the independently buildable reader and exact current Banking banner.
