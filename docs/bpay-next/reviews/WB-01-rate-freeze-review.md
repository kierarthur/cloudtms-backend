# WB-01 rate freeze — independent source review

**PASS for the three 1009 SQL sources below; no open defect found in this finite rate/provenance batch. This does not mark the whole WB-01 card complete.**

Read-only review on 10 October 2026. All 28 migration lines, 81 rate-row lines and 696 current publisher lines were read. The complete publisher routine was compared with its actual F-05 2018 winner, and the helper with 1510. No SQL, runtime, test, fixture, app, data or hosted operation was executed. Only this review document was written.

## Exact reviewed source

| Path | Raw SHA-256 |
| --- | --- |
| `supabase/migrations/10102026_1009_banking_pay_next_ordinary_rate_authority_v1.sql` | `071cf3af4a6ae9ef1c63c93eb32d33b00c9bc93fbc51558d0598d3da5f00b0b4` |
| `supabase/repeatable/10102026_1009_banking_pay_next_canonical_rate_rows_v1.sql` | `07843dcef992a74c2dd2c8c40185d17ce70aba9c33c1c53db5bbcbf6d6952fe2` |
| `supabase/repeatable/10102026_1009_banking_pay_next_ordinary_approval_rate_freeze_v1.sql` | `34227084b44b53abfb441b2b2826d9ce437fdb20e636e7d3d1d3f54bf324c7d7` |
| `supabase/repeatable/03102026_1510_banking_pay_next_rate_rows_v1.sql` | `98db776a421ca42b1b8e5a996f30c9739cdb340509e14f01e00c06908fb08188` |
| `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| `supabase/migrations/03102026_1324_banking_pay_next_core.sql` | `f38878c7b09e3fbac2d21acbf96c4e88bdac1d4c32abac7ee98d2672df7ba52c` |
| `docs/bpay-next/design/WB-01-rate-freeze.md` | `d3002dbfac0d4b11429f32205905bd3183ebb5e80cba248da8d9481e37d69f1f` |

## Checked closure

1. **Honest missing availability and canonical identities.** Migration lines 10–16 remove the actual all-rates-nonnull schedule CHECK and widen the rate-code bound from 128 to 245, consistent with the existing 256-character `ADDITIONAL:` component key. The standard-family, mileage-family, unique identity, unit-label and numeric shape remain. The helper always emits five STANDARD buckets and MILE, including genuine NULL rates. Configured Additional codes use upper/btrim, reject empty/overlong/normalization collisions and retain every arbitrary valid code. They are not relabelled EX1–EX5 or collapsed into generic ADDITIONAL. PAYE/Umbrella/charge remain distinct; Additional and MILE use the genuinely shared pay field already present in the source model.

2. **Available versus approved/used rates.** Publisher lines 203–304 retain Contract availability separately from actual TF effective pay and charge values. All five approved standard rates use the actual TF fields, including effective weekly defaults/overrides. Used-only Additional identities receive availability NULL, not a fabricated Contract rate. The matching TF Additional row provides approved pay/charge, with the genuine `rate` legacy alias retained. Existing approved-line quantity, unit rate and per-bucket rate-detail evidence remain unchanged.

3. **Daily combiner.** Lines 205–215 preserve the existing genuine Daily evidence helper. Its actual 0018 implementation emits available STANDARD rows only where one channel is known (0018 lines 343–352). The new combiner appends only absent standard identities, plus Additional/MILE, so a now-unconditional five-standard helper cannot duplicate the existing Daily rows. Unknown missing standards become NULL rows. The original Daily evidence validation and dated PAYE/Umbrella resolver stay unchanged; no contractless Daily authority is invented by this batch.

4. **Typed missing used rates and real fallback.** Segment and aggregate positive used-hour branches now produce `23514 / BPAY_NEXT_ORDINARY_USED_RATE_MISSING` with exact bucket and actual Contract UUID in JSON DETAIL. Precision/quantity failures remain separate. Fresh lines 199–202 add the actual MILE missing-pay-rate refusal; the original effective TF→Contract→Candidate mileage precedence is preserved. Additional lines 619–629 retain explicit stored amount first, then genuine quantity × genuine rate. Missing quantity/rate no longer silently manufactures zero; positive actual use without pay rate refuses with the normalized actual code. The original Additional amount precision check, original stored totals and final subtotal/total reconciliation remain.

5. **Approval-time source provenance.** The actual Candidate row is read under SHARE when this fresh producer runs; an Umbrella TF requires a currently compatible Umbrella Candidate and a real Umbrella UUID. PAYE records NULL. The captured FK is a real `public.umbrellas(id)` reference. There is no UPDATE/backfill of historical revisions and no lookup by current identity to manufacture an old source. Actual source-reference keys were checked against Worker production source (`broker/src/index.js` 177178–177183: kind/source_kind/override_id/default_id; weekly Contract snapshots retain mode/contract_id) and the genuine Daily 0018 comparison (315–321). The three scalar provenance fields are copied from those real stored keys; absent IDs remain NULL. Existing immutable `valuation_policy_id` default authority is reused, with no TARGET VAT assertion or policy clone.

6. **Immutable support and atomic publication.** Existing 1342 revision guard compares the whole row except seal timestamps (34–37), so the four added revision provenance columns inherit immutability. Existing revision-child guard refuses every non-INSERT and checks the same revision seal, so the two new schedule columns inherit it without replacing frozen guard attachments. The source retains the original exact TF/Timesheet/Contract currentness, work identity, stored daily segment money, shifts, breaks, expense rows, totals and one `bpay_next_publish_staged_revision_v1` call at lines 686–687. No additional agency command, partial publication, early clock, source-paid finance owner, target valuation or queue protocol is introduced.

7. **Other helper consumers and access.** Existing Source stage and protected-source consumers count and insert from the same helper, so unconditional canonical rows preserve their expected count symmetry. The protected calculated-rate check still compares actual nonnull pay/charge equality (0345 lines 776–780); NULL availability does not become a positive qualifier. The new helper/publisher signatures, volatility/security mode, search paths, postgres owner and revoked PUBLIC/anon/authenticated/service_role execution remain bounded to private-owner invocation. The migration alters only the two named revision/rate support tables; existing schema/RLS/guards are retained.

## Explicit remaining work / claim boundary

The reviewed snapshot is an editable, uninstalled source batch. The registry still points to 1510/2018 at the read checkpoint; root must update only the helper and publisher chains when accepting these successors. This is not an installed-current-winner assertion.

BD050 and the master editor chain are preserved: actual Workbench contribution schema/refresh first belongs to WB36/WB08, followed by WB44. WB-01 D01/D04/D09 remain open rather than being satisfied by an undefined/no-op hook. Genuine contractless Daily and later tax/WAIT successors retain their named chain. Worker/Office typed-409 transport and rates navigation were not yet part of these three SQL files and are not certified by this SQL review. No whole-card completion, app journey, hosted installation, local compilation or measured runtime claim is made here.
