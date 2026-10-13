# WB-29 input quote source review

12 October 2026, seq140. Independent read-only review against approved BD156/157 and original2100/1157. **SOURCE PASS — Q01 CLOSED; no remaining finding in this helper/delegation slice.** Current input helper owner is additive supabase/repeatable/12102026_2218_banking_pay_next_resolution_intent_candidate_guard_v1.sql, SHA-256 1F161C8A632DED5985BAC8CD37DB7CCA9DE189C7275B9E2309FE423E98F67A89. Original persisted wrapper remains supabase/repeatable/12102026_2213_banking_pay_next_resolution_input_quote_v1.sql, SHA-256 37D3DF35BC6297C1B6695B765476965B5AD024BD28AF469E7BEB49BB469A5C2A. Registry matches these exact two owners. No load/runtime/business acceptance is claimed by this reviewer.

## Closed finding

**Q01 — CLOSED by additive2218:28–29.** The helper now requires an existing native Work row with id=d.work_id and candidate_id=d.candidate_id before boundary/basis quotation, otherwise raising the typed identity refusal. This restores the FK qualification that an in-memory table composite does not itself enforce. Source comparison confirms exactly one added guard; after removing only those two lines and normalising CRLF/LF for comparison, the complete helper function is identical to reviewed2213. Native STABLE point lookup, owner/search_path/private ACL and every monetary guard remain unchanged. Installed2213 was preserved; no historical source was rewritten.

## Reviewed source boundaries

| Area | Source inspection result |
|---|---|
| Helper and original calculator | One new private STABLE SECURITY INVOKER helper, no writes/dummy row/store; native basis helper and unchanged1157 arithmetic only. Owner/search_path/private ACL retained. |
| Mapping/basis | Additive2218 positively qualifies native Work/Candidate first. Actual boundary selects based-on or published line; complete immutable basis equality and native Work/physical/component/policy checks precede financial quotation. |
| Position input | Separate object admission before jsonb_object_keys; exact four-key tuple, numeric JSON types, finite nonnegative penny A/R/H, approved SOURCE equality, residual and included bounds; nonzero R/H Source channel equality. Wrappers retain actual live/frozen Position authority. |
| Target/policy | Closed PAYE/Umbrella identity/nullability, finite selected date and immutable approval policy; exact native policy window plus same-start ambiguity refusal. No live settings inference. |
| Modes/manual operands | Three existing enum modes; mutually exclusive operands, finite range/precision, required manual reason and reason byte cap. Raw transport admission before the penny-domain/composite remains an explicit forthcoming W1/W2 obligation; it is not implemented or claimed by this helper-only batch. |
| Full STANDARD multi-bucket | Complete native positive bucket set, exact used units/rates, shared1157 internal original-channel Source quote with VAT disabled; source sum reconciles parent approved SOURCE once before selected bucket source is used. Manual full-bucket quote can use exact native units with NULL unproved source split. Unproved suggested split refuses typed. No parent repetition/pro-rata. |
| Partial single bucket | Positive used rate/full units, full native Source quote == parent A; unrounded numeric quotient <=full units, <=6 decimals/in range, exact multiplication equality and shared1157 penny roundtrip. Invalid units become NULL for eligible suggested residual; manual replacement refuses. Partial multi-bucket exits typed before this path. |
| Amount-led/zero | Saved MANUAL_AMOUNT requires original exact A/R/H/channel and full included residual; no narrowed scaling. Mode/policy/basis qualification precedes genuine zero output. Reimbursement is not newly admitted by the taxable native basis helper. |
| Output/conservation | Finite nonnegative penny ex/VAT/inc output and inc=ex+VAT. Remaining units/source scope and immutable policy/window returned distinctly; no new financial-owner formula. |
| Persisted delegation | Original2100 live approval/current/applied/Position/snooze checks retained; original frozen run/worker/component/line/target/date/segment scope retained; original SOURCE basis/residual validation retained. Delegation supplies native A/R/H object and included SOURCE, then adds actual decision ID/version. No changed-offer/E2 bypass or late ACTIVE lookup. |

Source references: original 12102026_2100_banking_pay_next_resolution_remaining_quote_v1.sql:231–324, 06102026_1157_banking_pay_shared_selected_quote_v1.sql:10–87, native basis2100:8–63, persisted tuple FK in migration12102026_2044:34. Approved design SHA AD84968881C9C16468AA12CE360340618F519FC7E781A4F9B8F40F345391BC48.

Root alone authored Q01 and owns subsequent source/normal checks. Reviewer performed saved-source reads, text comparison and this Markdown write only: no product edit, DB/RPC/business call, fixture/test, runtime invocation, hosted action or commit. Whole WB29 and future W1/W2/accepted-consumer joins remain open.
