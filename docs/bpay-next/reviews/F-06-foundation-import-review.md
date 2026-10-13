# F-06 foundation import and F-04 closure review

Independent source review, 9 October 2026, Sol 6.1 Extra High subagent. Re-read the controlling F-06/F-04 cards, their live checklist rows, saved dependency notes and current BUILD-BE sources. No SQL, database calls, tests, runtime, Docker, fixtures or rehearsals were executed by this reviewer. Only this review note was written.

## Verdict

**PASS for this source import batch. No actionable issue found.** The import preserves the reviewed staged bodies, uses the required migration order, records every introduced routine in the final-definer registry, and keeps the ordinary comparison refusal boundary. This is source review, not installed/app acceptance or proof of the complete F-05/F-06 runtime unit.

## Migration order and source provenance

The new canonical migrations have strictly increasing current-date prefixes in the card's dependency order:

| Canonical root | Reviewed staged source |
| --- | --- |
| `09102026_2010_banking_pay_next_source_book_binding.sql` | `06102026_1245_banking_pay_next_source_book_binding.sql` |
| `09102026_2011_banking_pay_next_initial_tsfin_umbrella_work_shape_v1.sql` | `07102026_0018_banking_pay_next_initial_tsfin_umbrella_work_shape_v1.sql` |
| `09102026_2012_banking_pay_next_channel_page_index_v1.sql` | `06102026_0236_banking_pay_next_channel_page_index_v1.sql` |
| `09102026_2013_banking_pay_payment_history_scope_indexes_v1.sql` | `06102026_1206_banking_pay_payment_history_scope_indexes_v1.sql` |

Each differs from its saved RESET source only by the added original-file provenance header. All four independently calculated LF-canonical source hashes match their saved migration-lock entries. The lock diff contains only the four added entries, with no removed/rewritten earlier lock entry. Timestamp naming proves source order; this reviewer did not execute or observe the scaffold commands.

2010 creates the two private binding tables, exact Source/canonical component keys and version/publication/revision links. The circular binding-version constraints remain deferrable initially deferred. Nullable capture/effect provenance leaves existing frozen children unchanged; no backfill, seed or publication call runs at installation. Both new tables retain ENABLE/FORCE RLS, logical repository ownership, current-owner policies and revocation from PUBLIC/anon/authenticated/service_role. The established provider compiler maps only the audited logical owner; the import adds no owner bypass.

2011 admits nullable common-receipt fields only through the exact `INITIAL_AUTHORISED_TSFIN_V1` shape with real authorisation/snapshot/version/generation/signature, six 32-byte evidence digests, approved/classification line identity and one publication. The common HEAD origin keeps its original nonnull shape. Its Umbrella WORK constraint retains the ex/VAT/inc and cash bridge with no Case additions, deductions or recoveries. 2012/2013 contain only the intended channel/history indexes.

## Carried repeatables and object review

Five carried roots are LF-canonically identical to saved RESET source: 1206 history, 1246 binding, 1301 receipt, 0018 initial/Daily evidence and 0718 frozen safety. The other two have only the required changes: 0236 adds schema reload; 1157 corrects its relocation-provenance comment and adds schema reload. No function body changes were hidden in these ports.

- **0236:** Three private STABLE invoker readers retain exact confirmed/SEALED run and frozen channel membership, bounded Candidate keyset pages, exact worker/channel scope and the existing status-reader delegation. All three are owner-only; no browser/service execute grant was introduced.
- **1157:** The complete file carries all three definitions: new immutable invoker shared selected quote, the existing public resolution-apply successor, and the NEXT target valuer. The shared quote delegates to the existing PAYE/Umbrella/VAT conversion helpers; the public caller's six quote assignments use that helper while preserving its selection/context/evidence guards. The valuer retains exact input signature, policy/pay-date checks and one latest applicable window, invokes the shared quote with explicit automatic mode and NULL optional manual/unit arguments, and retains owner-only ACL. Public resolution-apply remains service_role-only.
- **1206:** The history RPC retains service-JWT plus active trusted-admin admission, closed request/section/field registries, exact Candidate/Timesheet and child-parent ancestry, bound cursor/upper keys, bounded pages, UTC/string-or-null amounts and explicit unavailable comparison data. It stays read-only and remains readable after owner rollback. PUBLIC/anon/authenticated are revoked, service_role executes, and reload notification is present.
- **1246:** All nine binding/book helpers retain their complete staged definitions, fixed search paths, SECURITY DEFINER mode and owner-only ACL. The SQL components helper follows its members definition within the same root. Binding identities derive from exact receipt members and real sealed revisions/publications; the lockset precedes agency-clock publication. Five book/binding immutable/no-truncate trigger attachments remain in the same root. Seed helpers are definitions only and require exact applied revisions and typed witnesses; no seed is invoked by import. The bound snapshot retains explicit pending/stale states and `selection_state=UNBOUND`, rather than manufacturing comparison financial authority.
- **1301:** The complete final receipt publisher retains the LEGACY dormant branch, sorted exact one/two receipt members, Candidate identity checks, staging, direct binding lockset before pair publication, and registration of each actual current/prior component. REGISTERED does not claim APPLIED/book/financial readiness. It reasserts the exact AFTER INSERT FOR EACH ROW attachment on `private.weekly_source_entitlement_publication_receipts`; the historical 1570 definition is retained.
- **0018:** Four private initial-TSFIN/Daily helpers retain the actual Source I-1 inventory/basis/certificate chain, exact replay and superseded-pointer handling, real snapshot/line/publication identities and dated resolver/context evidence. The Daily helper uses the existing batch rate authority, including unavailable opposite-channel rates, and computes no money. All are owner-only with fixed search paths.
- **0718:** Exactly four unconditional frozen attachments invoke the existing `private.bpay_next_effect_immutable_v1()` on run_work/run_line: row BEFORE UPDATE OR DELETE and statement BEFORE TRUNCATE on each. Existing-attachment preflight checks exact function/type/enabled/arguments/predicate/constraint state. No whitelist, depth exemption or bypass exists. Its cancel-history index is already `IF NOT EXISTS` with exact conflict preflight, so no extra migration is needed.

The imported roots contain 21 routine definitions (18 newly introduced identities and three existing successors). Every one has exactly one matching current registry entry and the expected final physical root. The ordinary F-07 successor contributes the separate existing case-page successor. Owner/security/search_path and final ACL statements remain attached to the complete definitions. New explicit indexes are the two binding lookups, channel lookup, two history lookups and idempotent cancel-history lookup; unique/FK support indexes remain the schema constraints' responsibility.

## Frozen-writer census and four designated F-04 authorities

A fresh read-only multiline source search across **all** saved repeatable `.sql` roots, including untracked imports, found zero direct UPDATE or DELETE FROM writers of `private.bpay_next_run_work` or `private.bpay_next_run_line`. This is broader than the required NEXT filename census and is source evidence only. Later owner changes must repeat the census before admitting the same protection; no writer is whitelisted here.

| F-04 signature | Current designated final |
| --- | --- |
| `private.bpay_next_value_source_for_target_v1(numeric,text,text,boolean,uuid,date)` | `06102026_1157_banking_pay_shared_selected_quote_v1.sql` |
| `private.bpay_next_prepare_case_page_v1(uuid,uuid,bigint,bigint,integer)` | `09102026_1958_banking_pay_next_ordinary_case_week_applicability_v1.sql` |
| `private.bpay_next_source_receipt_publish_v1()` | `06102026_1301_banking_pay_next_source_receipt_binding_v1.sql` |
| `public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)` | `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` (advanced by the subsequent F-05 batch) |

The source census now finds exactly one target-valuer physical definition, in 1157. Historical 1500 remains an executable relocation-only root with no DROP FUNCTION. Its actual callers are PL/pgSQL runtime calls in 1620, historical 0360 and ordinary 1958; no migration/top-level/SQL-language/intermediate apply caller was found that needs the obsolete inline calculator. The removal is documented explicitly and changes no Source-owned historical definition. Existing 1570, first-authorisation predecessors and 0360 remain preserved under their final-closure rules.

The complete public resolution-apply signature also moves to 1157 in the registry. Earlier physical/root predecessors require that final closure; receipt predecessors require 1301; case predecessors require 1958; first-authorisation predecessors and the recursive 28092026 reassertion require 0557. The reviewed shared selector closes these dependencies for both the post-baseline NEW envelope and ledger-derived changed-only envelope, filters through canonical inventory ordering, and validates before application DDL. The existing static source gate separately checks the intended release range and NEW canonical ownership. No gate was run by this reviewer.

Raw 0549 and its comparison capacity helper remain outside release inventory. 1958 declares only ordinary types/helpers and keeps `BPAY_NEXT_COMPARISON_NOT_INSTALLED`; later approved CM work owns replacing that refusal. No comparison receipt/schema promotion or reservation-correction after-body is inferred from this F-06 import.

## Completion boundary

The current registry contains 704 entries; its four designated finals and all imported routine identities are coherent. Its still-false `foundationComplete` marker is conservative until the root records the combined F-04/F-06 completion. The F-05 caller/successor batch is still required as part of the eventual ordered release unit before business first use. This review does not mark any Installed/App tested row, claim protected verification, or authorise hosted installation or testing.

## Later F-05 metadata update

The subsequent saved F-05 batch advances the first-authorisation designated final to `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql`; a fresh read of the registry confirms that final root and physical file for the exact four-argument signature. Historical 0557 and its predecessors remain retained. The preceding 704-entry count, pending F-05 statement and 0557 closure discussion describe the original F-06 review checkpoint, rather than the later combined build state. This note updates source metadata only and does not add runtime acceptance.
