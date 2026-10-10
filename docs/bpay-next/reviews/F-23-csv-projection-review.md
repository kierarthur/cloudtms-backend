# F-23 CSV Umbrella row-projection successor review

10 October 2026. Source-only review of the complete newly authored `supabase/repeatable/10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql`, its two current 2018 predecessor definitions, the two-chain final-definer registry diff and existing sealed source-contract signatures/configs/ACLs. No SQL, compilation, test, runtime, fixture or release operation executed. Root alone edits product; no F-25/producer scope is included.

**Current follow-up verdict: PASS / CSV-01 CLOSED** for all four projection-only definitions at latest successor SHA `98067ed7e0ce4a633769f46df1582227cf50239bfc54402bc93d14ed261914a6`. Fresh four-definition/security/registry receipts are recorded in the final section below. Earlier two-function PASS and initial failed snapshot are retained as review history, not current authority.

## Initial captured finding, superseded by the corrected-source PASS below

**CSV-01 — corrupt regex literals in both full successor functions.** At source SHA `3386c143d1a5b1eaaa9a000d04d19743a248216e19dd06b269e2d49a2bf39c71`, lines112–118 of `private.bpay_next_bind_simple_csv_destination_v1` and lines283–289 of `private.bpay_next_issue_simple_csv_v1` replace the original complete `'^[0-9]{6}$'` and `'^[0-9]{8}$'` literals with an unterminated-looking `'^[0-9]{6}` /8 followed by blank lines and injected `commit;` text. Four original anchor/closing-quote occurrences are corrupted. These are changes to executable bank-validation source and destroy the required full-body preservation; this snapshot cannot receive PASS or installation approval.

The corresponding original 2018 lines1959–1960 and2128–2129 retain exact end anchors and closing quotes. Regenerate/correct the uninstalled successor using literal-preserving copying, then compare each entire CREATE statement against the unchanged current 2018 statement with only this one authorised substitution per function:

```sql
select * into strict v_umbrella from public.umbrellas where id=v_worker.target_umbrella_id for share;
```

to:

```sql
select u.* into strict v_umbrella from public.umbrellas u where u.id=v_worker.target_umbrella_id for share;
```

Do not edit the installed historical 2018 source. Root was notified immediately of this exact snapshot before any reviewer execution; no execution was attempted.

## Preserved source intent/evidence

- The two authorised Umbrella projections in the successor do use `u.*` into the actual `public.umbrellas%ROWTYPE`, with the same UUID filter, STRICT and FOR SHARE. No new relation or type is introduced.
- Function identities/arguments are the existing `private.bpay_next_bind_simple_csv_destination_v1(uuid)` and `private.bpay_next_issue_simple_csv_v1(uuid,uuid,uuid)`. Both retain PL/pgSQL SECURITY DEFINER and fixed `search_path=pg_catalog,private,public`.
- The saved successor tail retains logical owner postgres and revokes PUBLIC/anon/authenticated/service_role for both private routines; no service/browser grant or new public RPC is added. Existing sealed contract confirms owner-only EXECUTE and those exact configurations. This does not prove the malformed body compiles or preserves validation.
- Registry diff changes only these two chains to0316 finalRoot/finalPhysical, adds0316 to allowedPhysical, and records2018 as predecessor alongside the historical0140/0150 source. Other chain authority is unchanged in this diff. Correct registry selection is necessary but does not cure CSV-01's body corruption.

Verdict at the captured hash: **FAIL / critical CSV-01 open**. A corrected full-body equality read is required before this successor can be accepted. Whole F-23 remains outside this narrowly scoped review.

## Final regenerated-source review

Fresh-read the complete regenerated0316 source and performed a read-only in-memory string comparison of each complete CREATE statement against the current saved2018 source. Normalize CRLF to LF only; replace the exact original Umbrella query with the authorised alias query once in the expected predecessor text, then require ordinal equality of the entire resulting CREATE statement with the entire successor statement. Both match exactly. Each predecessor contains exactly one original query; each successor contains exactly one authorised query. There is no broader token normalization, ignored literal or body-only comparison.

| Actual function | Complete CREATE equality after only authorised substitution | LF predecessor/successor character lengths |
| --- | --- | --- |
| `private.bpay_next_bind_simple_csv_destination_v1(uuid)` | PASS | 6490 / 6496 |
| `private.bpay_next_issue_simple_csv_v1(uuid,uuid,uuid)` | PASS | 9759 / 9765 |

All four exact bank regex literals `'^[0-9]{6}$'` / `'^[0-9]{8}$'` are intact. The only executable body changes are the two `select u.* ... public.umbrellas u ... u.id` row projections. Original module admission, authorisation, DRAFT/worker/transfer/cancel predicates, lock order, existing receipt replay before current bank checks, destination/one-off attribution, configured columns/format guards, CSV quoting/digests, instruction insertion, transfer updates, money amounts and reply keys are unchanged. No postulated new owner, producer or payment behavior is introduced.

The complete fresh tail contains just the two actual ALTER OWNER statements, the two exact private revocations, and final COMMIT. Both routines retain PL/pgSQL SECURITY DEFINER, same named UUID arguments/JSONB return and fixed `search_path=pg_catalog,private,public`. The current pre-install sealed contract still has these original identities, configurations and owner-only EXECUTE; no new browser/service grant or public RPC is introduced. Subsequent local compilation/seal/coupling remains root-owned work and was not performed by this reviewer.

Fresh registry read/diff confirms exactly the two required chains:0316 finalRoot/finalPhysical/finalClosures, unchanged historical0140/0150 predecessors,2018 now included as predecessor, and0316 added to allowedPhysical. The reviewed release base is unchanged. No third chain/signature is altered by this registry diff.

| Source at this follow-up | SHA-256 |
| --- | --- |
| Corrected `10102026_0316_banking_pay_next_csv_umbrella_row_projection_v1.sql` | `bf58d483dda9c49d0818ae4c8ace06523ed3d0bfcd8e62bfb2ec0cc97849e0eb` |
| Saved predecessor `09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| Two-chain `final-definer-closures.json` | `c7a5c1d2ac41c06128cdb4587b9b373b14400b5badeaace41b0de253df361bbb` |
| Pre-install `supabase/release/current-contract.json` | `b78d9737b4724d8d809a3f4c3ee90742985205193d2c04e7ab44724b9d9c6f07` |

**Final verdict: source PASS; CSV-01 CLOSED; no remaining concrete finding in this two-function/ACL/registry slice.** This is source comparison/readback only. No SQL/scanner/module/gate/test/runtime invocation or product write by reviewer; whole F-23, installed/VERIFIED release and application acceptance are not claimed.

## Latest four-definition source closure

Root added only the two further F-05 D12/D17 full private definitions to the still-uninstalled0316 source. Fresh-read those complete definitions (including recovery's retained effect, position and Source attribution paths), re-read the previous two definitions, all four owner/ACL statements and final transaction wrapper, both current sealed-contract entries for the added routines, and the complete four-chain registry diff.

Repeated the full CREATE-statement ordinal equality comparison against saved2018, normalizing line endings only and applying exactly one intended replacement to each expected predecessor statement. All four complete statements match. One old/one new projection occurs in every corresponding function; no other executable statement or literal is disregarded.

| Actual function | Only changed projection | Complete CREATE equality | LF predecessor/successor lengths |
| --- | --- | --- | --- |
| `private.bpay_next_bind_simple_csv_destination_v1(uuid)` | Umbrella `select *` → `select u.*`, same actual relation/UUID/STRICT/FOR SHARE | PASS | 6490 / 6496 |
| `private.bpay_next_issue_simple_csv_v1(uuid,uuid,uuid)` | Same Umbrella alias projection | PASS | 9759 / 9765 |
| `private.bpay_next_receive_simple_reissue_v1(uuid,uuid,uuid)` | Worker `select * ... where id=v_original.run_worker_id for update` → `select w.* ... w ... where w.id=... for update` | PASS | 4318 / 4324 |
| `private.bpay_next_apply_work_collection_recovery_v1(uuid,uuid)` | Source line `select * ... where id=v_basis.original_run_line_id` → `select l.* ... l ... where l.id=...` | PASS | 7112 / 7118 |

All original financial authority, money calculations, lock/filter/replay ordering, genuine Source attribution tuple/digests, journal insertion and counter/position updates remain exactly copied from2018. CSV regex literals remain intact. Reissue retains SECURITY DEFINER with fixed `pg_catalog,private,public`; collection recovery retains its original **SECURITY INVOKER**, void return and fixed `pg_catalog,private`. Do not classify all four as definers. Original named UUID inputs are unchanged. All four private routines have logical owner postgres and identical explicit revocations from PUBLIC/anon/authenticated/service_role, leaving owner-only execution matching the current pre-install sealed contract. No public RPC, grant, trigger or producer is added.

Registry diff now changes exactly these four signature chains, with0316 finalRoot/finalPhysical/finalClosures and allowedPhysical, and2018 recorded as predecessor after the original0140/0150/0240/1519 authorities. Release base and all other chains are unchanged, apart from terminal newline formatting. Fresh0316 source contains exactly the four expected CREATE statements and four matching ALTER OWNER/revoke pairs.

Read-only linkage check of the source gate's new historical exception: `FOUNDATION_RECONCILED` pins commit `3c91e5e6ce35b5833a5c218b77b741261eeebafa`; saved2018 must equal that exact committed source after line-ending normalization. The four correction signatures must each resolve to a later physical definition rather than2018; the gate extracts each actual final CREATE and checks its special constructs before permitting exact retained ROWTYPE_SELECT occurrences for the pinned2018 signatures. This is a limited linkage read, not execution or a renewed whole-module verdict. It preserves immutable2018 rather than accepting a changed historical body or simply skipping its final authority.

Latest source hashes:

| Object | SHA-256 |
| --- | --- |
| Four-function0316 successor | `98067ed7e0ce4a633769f46df1582227cf50239bfc54402bc93d14ed261914a6` |
| Unchanged2018 predecessor | `7433ac2a1ccfe64613666eed4ff672e81c67a36bb8c9abbc6585833b9dcbe09b` |
| Four-chain registry | `c5b5a6f5790f368bc1d52062f6c847251a8fc729ecebdefd556106968912943c` |
| Gate at the limited linkage read | `aefd66b1673b8f5d774bb1a264f9ad3a33d14fa0a80c0cb991a58853c2a204eb` |

**Latest verdict: PASS for the four full definitions, their unchanged security contracts and four final-authority chains. No remaining finding in this slice.** No SQL/compiler/scanner/module/source-gate/test/runtime invocation, fixture, product write, installed/VERIFIED claim or F-25 work by reviewer.
