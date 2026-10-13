# WB-29 W1 context source review

Read-only independent source review of the BD159/BD160 W1 slice. Root alone authors product. No SQL, business RPC, fixtures, runtime tests, hosted actions or joined application acceptance were performed by this reviewer. W2, transport and captured quote composition remain separate unfinished WB-29 work.

**Current verdict: DESIGN PASS and SOURCE PASS for the W1 slice plus the narrow 2302 correction. C29-W1-S01 is closed by the final receipt below.** The CASE syntax-only amendment is confirmed; all earlier source preservation findings remain applicable. Whole WB-29 is still open.

## Historical checkpoint — source was still being finalised

The complete `public.bpay_next_workbench_page_v1(jsonb)` definition was compared with the current 0235 predecessor. Its only body changes are the early closed `RESOLUTION_CONTEXT` dispatch and propagation of that branch's exceptions alongside existing `SNOOZE_CHECK`; all other existing reader branches are retained. The private reader does not invoke the locking bound-Source snapshot or a Source seed owner.

The current 2241 source validates the raw nested request before native numeric-domain assignment; manual operands are canonical decimal strings or explicit NULL with finite range/precision checks. Genuine Daily availability uses the sealed approval's rate schedule, while ordinary availability uses the existing current-Contract helper and preserves NULL cells. Compatible decision identity is supplied by the exact 2100 helper; other mapped ACTIVE owners are separately enumerated with a cumulative 100-owner bound and 101-row lookahead before compatibility filtering.

Checked anchors in `supabase/repeatable/12102026_2241_banking_pay_next_resolution_context_v1.sql`: raw validation 8–96; service/admin and adopted Candidate authority 98–130; actual current/applied Work and physical Timesheet equality 151–171; distinct available rate sources 175–191; exact compatible and bounded incompatible owners 192–213; native incoming/saved quote calls and qualified zero flag 214–251. BD160's final string DTO projection and the final saved hash are still being checked; this checkpoint does not approve intermediate bytes.

## Preservation and read-only callee evidence

The official date owner remains `public.pay_banking_official_date_context_v1(timestamptz)` in the canonical 26052026 core:185280–185374: STABLE, one settings point read, London statement-time derivation and JSON return. The native basis/ACTIVE owners are 2100; the incoming quote is current 2218 and saved quote wrapper 2213. Their final call tree is being checked for retained pure quote authority and absence of locks/writes. Private additions retain fixed search paths, INVOKER security and no public/browser/service direct grants; the existing public service-only reader remains the sole W1 entry.

That checkpoint preceded the completed BD160 projection. The current verdict and exact bytes follow. All business and joined checks remain **NOT_RUN**.

## Historical W1 slice — SOURCE PASS before the requested snooze trace

Current saved 2241 bytes were reread after Root declared the BD160 projection stable. No essential defect remains in this W1 source slice. This is source approval only; no compilation or first-use execution was performed by this reviewer, and it does not complete WB-29.

* **Closed raw input:** 8–96 validates the action, explicit keys/NULLs, canonical UUIDs, at most 100 rows over distinct Works, exact rate family/code, separate CLEAR owner identity, bounded reason and the three original modes. Rate/amount operands are strings with anchored plain-decimal patterns and admitted range/precision checked before `::numeric` or native composite assignment. Manual NULL, blank, exponent and excess precision cannot silently become a rounded amount or zero.
* **Authority and precedence:** 98–130 requires service-role/admin, active NEXT, actual agency identity, CURRENT global adoption with no dirty/pending work, and the Candidate's adopted metadata equality. The target comes from the actual Candidate and enabled Umbrella/VAT fields; versions, command cut and official London date are native. 151–171 additionally binds the requested actual sealed revision to current/applied Work, current physical Timesheet version, booking/Contract identity and actual Position approval/application. No canonical approval root is substituted for current economic eligibility.
* **Rate facts and owner identity:** 175–191 keeps used approved basis, genuine approval-frozen Daily availability and current ordinary Contract availability separate. Missing `rates_json` is passed as an empty native object, yielding genuine NULL standard cells through current 1009:29–41. Additional code validation and one shared pay field remain 1009:44–63. 192–213 derives the SAVE-compatible ID/version through exact 2100 qualification and discloses bounded mapped incompatible ACTIVE owners separately; the OR mapping emits each physical decision once, with the cumulative 100-owner overflow checked before compatibility filtering.
* **One native quote and truthful zero:** 214–251 constructs an in-memory native intent and delegates incoming/suggested quotes to current 2218; persisted quotes use 2213's wrapper. The reader catches only the two named unavailable Source/residual cases. It repeats the existing live snooze refusal instead of treating excluded residual as available. The zero flag requires a successfully qualified incoming Preview with a literal zero operand or native zero quote; unavailable NULL does not produce zero consent.
* **Exact decimal DTO:** 252–275 projects only the six known per-row objects and their actual `quote` members. Projection occurs after every native equality/semantic check and quote call. Native money, units, rates and version scalars become exact decimal text; NULL remains NULL. Decision manual values/versions are separately projected at 195/206–209. This changes transport representation, not native quote inputs or arithmetic. W2 must continue to derive native basis separately and compare the same projected context under its own locks, as BD160 specifies.
* **Finite response/security:** 276–292 retain explicit per-Work/per-row scope, actual Source-book/binding observations and intermediate/final 120000-byte bounds. Private additions are INVOKER with fixed search paths and revoked direct public/browser/service grants at 297–302; the existing public reader remains STABLE SECURITY DEFINER, original owner/config/ACL and service-only entry at 306–1213. Its whole-function textual comparison with 0235 still has exactly the new early branch and exception extension. Public SECURITY DEFINER membership does not increase.

Read-only call closure was checked against actual current source: basis/ACTIVE 2100 perform point reads; incoming quote 2218 and saved delegation 2213 perform native reads and call unchanged IMMUTABLE `private.pay_finance_selected_quote_v1` in 06102026_1157:10–88. Its original conversion/VAT callees in the canonical 26052026 core are IMMUTABLE arithmetic. Current Contract rows 1009 and exact rate parser 03102026_1510:8–25 are IMMUTABLE. Official date context is STABLE and reads only the settings point. No transitive lock-taking Source snapshot, seed, write, dummy decision row or second financial calculator is introduced by W1.

The source-gate diff adds the three exact new signatures, their finite actual callee lists, and the public reader's single new context edge. BD160 adds only actual `jsonb_each`, `jsonb_set` and `to_jsonb` projection callees to that named helper. It introduces no blanket SQL/history/dynamic waiver. No gate tool was run by this reviewer.

| Reviewed saved file | SHA-256 |
| --- | --- |
| `supabase/repeatable/12102026_2241_banking_pay_next_resolution_context_v1.sql` | `BE4033BD9973DB55F3FC41B96EF29A11B396C508D37BFC6F06DCB7945A1B586F` |
| Complete predecessor `supabase/repeatable/12102026_0235_banking_pay_next_origin_wait_issue_navigation_v1.sql` | `B37228BF39A62C1423CA446C93736495CB3A49A4F7BD7DBD158AAA21B157BE4C` |
| `docs/bpay-next/design/WB-29-context-native-request-design.md` | `588907556DBBD4FAA7E36255155B23F8E738B8E0F5237F8E8D1E8B184806890F` |
| `scripts/bpay-next-source-gates.mjs` | `8B4E0ED72D9B70C09FEA158FAF324FA3F8C44ACFBF077263CECE67E90A5C2EA5` |
| `docs/bpay-next/final-definer-closures.json` | `36D6E11C16E46C95D4515A16C4DB02F9AE2E2632F5E6C5A18704F81639C11D49` |

Receipt housekeeping before closure: current `WB-29-context-successor-provenance.json` still records the earlier `0a512091…` implementation hash and `sourceReview: PENDING`; Root should refresh those to this approved SQL and saved review. This is documentation drift, not a W1 product defect. W2, transport, accepted/captured composition and all joined checks remain **OPEN / NOT_RUN** as applicable.

## CASE syntax amendment and additional live snooze finding

Current 2241 SHA-256 is **`098EF7EE8E20B889B85D9702A374CB381A172D07CB47692E56FE20F7CD9D650E`**. Line84 now parenthesizes the CASE expression used as the comparison operand. Reversing that one literal change in memory yields exactly the previously reviewed `BE4033BD…` full-file hash; no other saved SQL changed. Root reports the initial local PL/pgSQL compile detected the missing parentheses and the corrected source subsequently compiled on sole local PG17 through the existing diagnostic. This is attributed compile-only evidence, not reviewer execution or business acceptance.

### C29-W1-S01 — expired/stale physical active controls over-refuse live quotes

**Essential, P2.** Current 2241:222–223 rejects a matching snooze using only physical `active` plus whole-Work/component identity. The live branch of the current saved wrapper `12102026_2213_banking_pay_next_resolution_input_quote_v1.sql`:183–185 repeats that test. Both omit native inclusive Until-date and worker-pay-basis applicability. W1 then returns `BPAY_NEXT_RESOLUTION_RESIDUAL_AMBIGUOUS` instead of a valid current suggestion/Preview/saved quote.

The counterexample is source-supported without business fixtures: an ordinary dated matching snooze reaches the next London day; its row remains `active=true` until the bounded minute due owner processes it. Current due owner `11102026_0255_banking_pay_next_due_recovery_pages_v1.sql`:9/28–30 selects only `until_date < London today`, then :85–87 changes it to inactive. Native applicability already treats it as unsnoozed before that metadata cleanup: `private.bpay_next_snooze_at_cut_v1` in `10102026_1100_banking_pay_next_snooze_v1.sql`:465–473 requires `until_date IS NULL OR until_date >= business_date` plus the current scope's actual worker-pay basis. Current Work projection2146:210–218 uses that same native date/basis distinction. CURRENT metadata does not prove that the asynchronous due pass already cleared every expired row.

Keep the correction narrow: both W1's live outer admission and the persisted wrapper's **live** branch must qualify the actual applicable whole control or exact current native segment/component control by that same inclusive **London business date** and current native worker-pay basis. Reuse existing readonly revision/segment basis point owners; do not add events, mutate snoozes, schedule cleanup, scan history, quote snoozed units or change allocation. At a component-level read, a single `snooze_at_cut` call with a NULL segment key would miss valid segment controls; any scope test must still bind each matching control to its own current approved segment. Genuine applicable whole/segment snoozes may retain the existing unavailable residual result until the card's exact captured Included composition handles that scope.

Do **not** use the quote's official pay/valuation date in place of London business date. Until Thursday remains snoozed on Wednesday even if the next official pay date is Friday. Do **not** require `snooze.expected_revision_id = current_revision_id`: a retained unchanged native worker-pay basis can legitimately apply across a publication. Actual worker-pay-basis equality is the native authority. Current A2 publication0016:270–307 already cancels changed-basis controls in its SNOOZE phase before installing the applied pointer, so normal completed new publications close most stale-basis cases; this does not remove the expiry counterexample or authorise treating every physical active retained control as applicable.

Changing only W1's outer condition is insufficient: after it admits an expired control, the saved wrapper still throws at2213:183–185. Conversely the captured `p_run_position_id` branch must remain frozen and unchanged: its accepted cut, segment membership and Included Source already own snooze exclusion. No shared1157 arithmetic change is needed.

The verdict at that checkpoint was **OPEN** solely for this concrete snooze qualification issue. Other W1 preservation, exact-decimal, security and finite-gate conclusions remained unchanged. The 2302 receipt below supersedes that checkpoint; whole WB-29 and all business/joined checks remain **OPEN / NOT_RUN**.

## 2302 native live-snooze correction — DESIGN PASS / SOURCE PASS

Read all 245 lines of the new, uninstalled `supabase/repeatable/12102026_2302_banking_pay_next_resolution_live_snooze_v1.sql`. The narrow design is appropriate support for the card's shared current quote: one native STABLE INVOKER boolean helper, used by exactly the live W1 context and saved live-quote paths. It does not introduce a table, protocol, command, event, Source seed, producer change or financial formula.

**C29-W1-S01 CLOSED.** Helper2302:5–16 checks physical active controls only when their Until is NULL or inclusively on/after London business today. It uses the actual revision's whole worker-pay basis for WHOLE_TIMESHEET. For SEGMENT it binds the same component and stored segment key to the actual current revision's native segment, then compares the stored control basis with unchanged `private.bpay_next_segment_pay_basis_v1(s.id)`. It neither uses the official pay/valuation date nor insists that the control's original expected revision UUID equal the current UUID. Thus an expired retained active row or changed-basis stale control does not block a live quote; an applicable unchanged-basis whole/segment control still does.

Complete function-body comparisons against the actual parents show only:

* private W1 context2241 →2302: the two-line broad physical-active test becomes the shared helper call at2302:108;
* saved quote2213 →2302: the live branch's broad test becomes the same helper call at2302:206.

Every other inherited line is retained. In particular the captured Run-position branch2302:212–227, residual/conservation checks:228–235, native quote delegation, pinned identities, policy, decimal DTO projection and security/byte bounds are unchanged. No captured snooze cut is recalculated from current controls.

The helper is write-free. Its only function edge is the existing STABLE INVOKER segment basis owner in current1236:10–25, which reads the native segment/approved-line/revision points and returns the pre-existing worker-pay basis. It introduces no lock-taking transitive call. Existing `bpay_next_snooze_active_work_idx` (migration10102026_1100:47–48) supplies the Work/active scope read, and native `(revision_id,segment_key)` uniqueness (segment migration10102026_1100:25) supplies the exact segment point. There is no new history sweep, array census or index framework. Existing private owner/config/revocations are retained for both successors; the new helper is likewise fixed-search-path INVOKER and has no direct public/browser/service grant at2302:239–244. No public entry or SECURITY DEFINER membership changes.

Current registry resolves the two private successors and new helper to2302, while the public reader, raw validator and offer-context owners remain2241. The source-gate delta adds only this exact helper signature, its existing native segment-basis callee, and one helper edge to each actual caller; it contains no blanket waiver. No checks or business execution were run by this reviewer. Root's local compile evidence stated above applies to2241 only; 2302 was still uninstalled at this review.

| Final correction / controlling file | SHA-256 |
| --- | --- |
| `supabase/repeatable/12102026_2302_banking_pay_next_resolution_live_snooze_v1.sql` | `DD3A2BB6D0E0A262A65CDCB96C0620E6B5A0A84E0A518191DA1D6D15B6075B7B` |
| Installed context predecessor2241 | `098EF7EE8E20B889B85D9702A374CB381A172D07CB47692E56FE20F7CD9D650E` |
| Saved quote predecessor2213 | `37D3DF35BC6297C1B6695B765476965B5AD024BD28AF469E7BEB49BB469A5C2A` |
| Current `scripts/bpay-next-source-gates.mjs` | `4E824DBF1625262353C9B5A98FC777BEA0CD57A7512DC74B42D3DB059115BF4B` |
| Current `docs/bpay-next/final-definer-closures.json` | `0A011921E4FDA12CE4147D64181B322CF5C5D9CEA9868E81C0A9DED6A9AD15E2` |

No essential source defect remains in this correction. Remaining W2, transport, captured composition and whole-item acceptance are outside this receipt and stay **OPEN / NOT_RUN** as applicable.
