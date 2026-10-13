# XD-01 server-sort investigation

13 October 2026. Independent read-only source/design checkpoint for seq147, BD179. Full approved card06:1149–1174, the exact current ITEM and D01–D05 rows, `XD-01-02-investigation.md`, the native reader/index/public page/Worker and root `XD-01-build.md` were read. Actual SDP34 and applicable PF/WB10 clauses were then read from the saved approved packs. No product, checklist or BUILD_LOG edit, SQL/RPC/database/business call, test, fixture, rendering, network or Git mutation was performed. This document is the only reviewer write.

**NARROW DESIGN SOURCE PASS for root BD179 as qualified below.** The smallest extension is a four-argument overload of the same private reader, keeping the three-argument 2049 authority untouched, plus the same public page-kind branch, closed optional sort transport and native indexes. This is design review, not a claim that the new sorted reader/index/caller is built, installed or passes whole-server performance. No new money owner, table, trigger, producer or cursor protocol is warranted. XD07 retains the oversized-row successor and its still-open future acceptance clause.

## Exact card and policy boundary

XD01 is Candidate-scoped, masks eleven fields, excludes already-bound native origins and returns `{rows,complete,scan_cursor}`. Card06:1160 explicitly leaves the oversized-row reader successor to XD07; 1167–1172 labels all joined checks future/NOT_RUN. D01 is the native private reader, D02 the existing Worker kind, D03 the Office caller/table, D04 the historical installed2049 provenance, D05 the authored future joined checklist. Context references do not create extra implementations.

Primary policy: `cloudtms-backend/codex_outputs/claude-stage2-continuation-20260925/SCREEN_DESIGN_POLICY_20261010/SCREEN_DESIGN_POLICY.md:172`, SHA-256 `8BC212B174469236F2B46DBF70EADC3664A946636458321CD3C77B7146323DE5`. SDP34 requires every data heading to sort the whole list on the server, ASC then DESC; stable ties, reset to the top, and a server keyset with RQ406 tie-break and a WB10 index. It prohibits loaded-page-only sorting. Its wording does not require every arbitrary full text operand to fit a direct B-tree entry or certify a fixed number of physical index visits.

Approved card02:504 requires deterministic ties and continuation without skipped/duplicated rows; 532 names complete unions at limits1/17/100. WB10:429 requires one bounded page without relying on the120s PostgREST timeout; 449 maps each reader order to an index; 450 specifies local5s statement_timeout. R2 disposition at423 separately names session top-level timeout and real latency evidence.

Applicable PF01 card06:1671–1673 requires row/byte-bounded reads, limit1–100, one bounded page and the same treatment for stored credits. Build1678 requests keyset/index/max-row/byte-cap inventory and removal of unbounded page counts. Remaining1682 separates existing reader source from measured two-second delivery; joined1689 requires DB/Worker/browser timing separately. PF04:1745–1752 prohibits financial-history-driven routine work and permits genuine point-key history guards. The proposed sort reads native current advance/bank facts, not financial outcome/effect history, and invokes no financial caller.

## Existing authoritative source

`04102026_2049_banking_pay_next_current_stored_credit_page_v1.sql:9–12` defines `private.bpay_next_current_stored_credit_page_v1(uuid,uuid,integer)`, STABLE/security invoker with fixed search_path. Lines28–36 take the exact Candidate raw ACTIVE/PENDING manual/non-taxable/one-off-bank advance window, at most limit+1, before full component/bank/origin validation. Lines48–50 bound the component ambiguity probe to two IDs. Lines54–73 retain all current eligibility/bank/hash/native origin clauses, including the UNIQUE bound-origin point at73. Lines84–88 consume every inspected raw identity and advance scan_cursor even if no row is displayed.

The original migration2049 indexes `(candidate_id,id)` under those exact literal enum predicates, plus components `(finance_case_id,id)`. The reader's eleven fields are `legacy_case_id`, `candidate_id`, `principal_source_ex_vat`, `original_source_pay_channel`, `original_tax_treatment`, `original_routing_kind`, `original_created_at_utc`, `bank_version_at_utc`, `bank_details_hash`, `beneficiary_name`, `account_last4`. Full bank values never leave the reader. Original line77 still raises54000 for a first oversized eligible JSON row; XD01 must not claim XD07's consumed typed row is present or add a silent name cap.

Current public `bpay_next_preparation_page_v1(uuid,text,jsonb)` is in `12102026_1646_banking_pay_next_case_channel_resolution_v1.sql`. Native service/admin/NEXT authority precedes the closed kind dispatch at2617. Its stored-credit key list is2634 and exact delegation2680–2686. Keep every other branch and the separate native financial-job owner unchanged in the new dated public dispatch successor.

Worker `broker/src/bpay-next-preparation.js`: input325; exact eleven-field row shape380–382; UUID cursor map397; raw empty/nonterminal cursor checks992–998; Candidate qualification1035; old ID/scan upper-bound1036; raw-cursor continuation1054; closed GET schema779–787. Office's live PAGES map currently lacks this kind, so its small caller/schema integration remains required.

## Smallest sorted extension

Add only `private.bpay_next_current_stored_credit_page_v1(uuid,uuid,integer,text)`, preserving the original three-argument signature/body/ACL. Use eight finite values: CREDIT_ASC/DESC, CREATED_ASC/DESC, AMOUNT_ASC/DESC, PAID_TO_ASC/DESC. Credit is the constant supported label; its only meaningful stable sort operand is the native UUID tie, retaining original default order. No picture-only description/reference or invented count is introduced.

Keep `after_legacy_case_id` and `scan_cursor` UUIDs. Resolve the supplied anchor's actual sort operand through an exact Candidate/UUID point inside PostgreSQL. Do not accept caller amounts, bank names, SQL, hashes or large text cursor values. An absent/wrong-Candidate anchor produces a known typed cursor-changed refusal; sorting starts again from null. An anchor excluded by bound-origin eligibility must still be usable while it remains in the raw seed, so adopting a row does not incorrectly invalidate subsequent raw continuation. No full eligibility test belongs in the raw anchor or before the bounded window.

Use separate initial/subsequent fixed query branches, exact typed bindings and a deterministic UUID suffix. Specify the suffix direction consistently with each native ORDER BY and predicate; the whole tuple must advance in the requested direction. A changed sort/candidate/frame starts a new page chain. Native sort facts are read current at each request; the UUID-only protocol does not promise a frozen cross-request snapshot or detect every legal intervening sort-key edit. Limit-union/continuation claims apply to unchanged native facts; existing current-read refresh/lifetime semantics must remain truthful.

| Sort | Exact source/required supporting path |
| --- | --- |
| Credit | Existing partial `(candidate_id,id)` advance index; UUID order or its reverse. |
| Created | `a.created_at`, then `a.id`; new partial `(candidate_id,created_at,id)` index with the same2049 raw literal predicates. Structural baseline1500 declares created_at TIMESTAMPTZ NOT NULL at3072. |
| Amount | `a.original_amount`, then `a.id`; new partial `(candidate_id,original_amount,id)` index under the same raw literals. Baseline1500:3064 is NUMERIC(12,2) NOT NULL. Original eligibility2049:63 proves displayed component source_amount equals original_amount, so this is comparison of native money facts, not a new calculation. |
| Paid to | Raw bank Candidate seed, bounded128-character C-collated prefix, full unchanged native beneficiary_name C order, finance_case_id UUID tie. Proposed index `(candidate_id,(left(beneficiary_name,128) COLLATE "C"),finance_case_id)`. Full original advance/component/bank/origin qualification occurs only after the returned raw window. |

Bank-driven Paid-to raw seeds include all bank rows for that Candidate; unrelated/ineligible/bound bank identities are consumed after exact native points rather than hidden by a pre-LIMIT eligibility join. Missing-bank advances cannot yield an eligible displayed row. This makes the complete eligible output set the same, without a join-driven filtered-prefix stall. No finance history, rebuild or producer hook is needed.

## Unbounded name and precise access qualification

Baseline structural1500:3476–3488 declares bank beneficiary_name unbounded TEXT NOT NULL. Finishing1501:6053 checks only nonblank; PK4317 is finance_case_id and existing Candidate index9185 is Candidate-only. Native bank upsert baseline routine02:466–476 normalizes whitespace/checks presence without a length cap. Therefore a direct full-name B-tree can introduce an index-entry-size limit for otherwise legal producer values and is rejected for this scope.

The bounded prefix index avoids that limit;128 UTF-8 characters require at most512 bytes for that operand. `ORDER BY prefix COLLATE C,full_name COLLATE C,id` is exact full-name ordering because the prefix is monotone with the full string; the full name breaks prefix ties and is neither truncated nor replaced by a hash.

**Residual limitation:** an arbitrary number of Candidate bank rows may share the same prefix. Full-name ordering before LIMIT may read/sort that tie group. The proposed index supports Candidate/prefix access but does not fully cover the full-name suffix. The raw returned SQL window stays limit+1 and point validations stay ≤limit; those bounds are not a claim that physical visits/memory/latency also stay limit+1. No source or measured two-second PASS is inferred. Preserve local5s statement_timeout as specified, actual top-level timeout and existing dependency/read byte caps; a function-level SET alone is not proof that the already-running PostgREST statement has a5s deadline. Failures remain typed/retryable and current-frame fenced. PF timing/large-prefix behavior stays future joined evidence, with no new local proof programme.

At root's explicitly narrowed BD179 boundary this is an acceptable useful supporting index plus exact whole-list sort; no read clause examined requires the unsupported direct index on the entire uncapped name. It is not acceptable to advertise fully indexed bounded physical full-name seeking. No producer/name cap, projection, table, trigger, digest or new sort system is introduced to conceal the limit.

## Closed transport and required seams

Only when sort is present, public/Worker/Office must accept the exact fourth argument, reject null/unknown values and delegate to the overload; sort absent retains the exact existing unsorted path. Check the generic preparation-args helper's treatment of the new key and keep any sort-specific validation within this named branch. A dated public preparation successor must preserve its current full body/ACL except the two closed stored-credit seams; retain original1646 unchanged and verify the inverse difference.

Sorted Worker/Office decoding must leave the eleven-field schema and raw UUID cursor envelope unchanged. Validate Candidate identity, unique output UUIDs, exact fields/limits/budget, strictly ordered native display tuples and a non-null/different scan UUID for nonterminal progress. Permit empty nonterminal pages. Do not reuse the original ID-monotone or `row.id <= scan_cursor` rules for non-ID sorts. An excluded scan anchor has no displayed row from which the browser could infer its sorting value; only SQL's native raw-window implementation certifies that boundary.

Reuse existing Worker C-byte comparator `caseListTextCompare` at679–682 and microsecond timestamp comparator at684–686 or channelDraftTime33; JavaScript UTF-16/locale ordering and millisecond-only dates are incorrect substitutes. Amount comparison may use exact decimal/BigInt ordering only, without deriving financial values. The original scalar UUID mode retains its old decoder. Office retains the exact originating Candidate/sort/actor/agency/owner/frame/page-chain guard and clears retained continuation before a sort change; it must never locally relabel a cursor from another sort.

Required supporting source artefacts are the new additive read indexes/overload/public branch, exact Worker/Office allow-list/decoder/caller, final-definer/source-gate classification of only these new native templates, genuine last RPC reload, and normal generated contract/source coupling. The old three-argument reader and public financial-job/binder/money owners remain native. XD02 owns actual Adopt mutation/journal/receipt; XD07 owns pre-JSON/consumed oversized output. This design does not complete either.

## Saved identities

| Reviewed source | SHA-256 |
| --- | --- |
| root `XD-01-build.md` | `949CB3D2943BEAC41E31804464AB7265E2F89CC5A0DEE65FB55AC52BBA46CB4A` |
| original private2049 | `94BD5588758782A0D32482D5684E18A790BE67911EB3C8E25DB4EA72CE73310B` |
| original index migration2049 | `AE63BFBE69C7FC3154DBCD5ECBE87800E13F411038F82CF5E7CC63B1E3C6563E` |
| native public/job1646 | `FEC5DD533F37C45F26E46011A551009AE8D62FB93A18862E082B2ACF74197456` |
| Worker preparation | `D5015DAE2CB4E6A2337FFE99D2584A1756E05FEBD6BA795F17C1E47D7EB6985D` |
| exact working checklist CSV | `C6F818898EFE286394F13AEC7F16B0E9FBDC32A720D343C3F2566902EB41F3D9` |

Design verdict is limited to the exact qualified root scope above. Actual implementation review, local compile/catalogue/contract/source gates, A4 and joined/runtime acceptance remain separate. No checklist tick or completion claim is made here.

## Exact shared-argument seam closed by source investigation

Final current `private.bpay_next_preparation_args_v1(jsonb,text[])` is `10102026_2232_banking_pay_next_selection_pages_v1.sql:363–423`, as recorded in the current final-definer registry. Its unknown string key branch at415–418 requires a numeric string. Therefore merely adding `sort` to the central stored-credit key list would refuse every proposed enum. This dependency is concrete and requires no shared-helper amendment.

Smallest dispatch shape: immediately after the original `perform private.bpay_next_preparation_authority_v1(p_actor_user_id)`, handle only `p_kind='CURRENT_STORED_CREDITS'` with sort present; require string plus the exact eight enum values, validate `p_args-'sort'` against the original Candidate/after-legacy-ID/limit keys through the unchanged generic helper, and delegate to the four-argument reader. Unknown extra fields still refuse. Sort absent traverses the original central key/three-argument branch unchanged. This narrows the public successor inverse difference and retains every other page kind/argument policy.

Shared args source SHA-256: `CC1491F55FF1F4AFE48F82E8892015AA2F91500F42D0D41D692F0E6B78DD3E48`.

The existing source-gate `fixedQueryLiterals` at1817–1837 admits one fixed SELECT base with literal-only appended suffixes and typed USING. Multiple/reassigned dollar bases or CASE-generated SQL fragments are not silently admitted. Use that existing closed construction style where possible, or review only the exact new named native sort template; do not loosen the global dynamic-SQL rule. Actual implementation/source-gate review remains pending. No executable source was run or edited by this reviewer.
