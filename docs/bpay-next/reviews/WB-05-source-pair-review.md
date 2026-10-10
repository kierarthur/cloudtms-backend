# WB-05 Source pair lock-set review

Independent source-only review, 10 October 2026. Read the complete WB-05 card (02:212–245), all D01–D09 checklist objects, current registry and successor provenance, the full 1540 predecessor and 1403 successor, and the actual 1346 projection/publisher call placement. No SQL, tests, fixtures, business calls, database, hosted or runtime execution was performed.

**Verdict: PASS for the saved 1403 pair-publisher successor. No remaining concrete source defect found.** This is the narrow pair lock-set review; it is not a claim that the whole WB-05 producer census, later Contract repricing or joined application checks are complete.

| Source | SHA-256 |
|---|---|
| `supabase/repeatable/10102026_1403_banking_pay_next_source_pair_projection_lockset_v1.sql` | `ea53f357817e88504cf3e2c6bad75ca228b1abe264aeeb10c8d58dd068a7fc21` |
| `supabase/repeatable/03102026_1540_banking_pay_next_source_ordered_publish_v1.sql` | `f4af5a88d1ede025fdf227df2442a2c49b801104bf02a99a466366663378a4da` |
| `docs/bpay-next/final-definer-closures.json` | `2732d3593adf166b12c533d15c903784b8d98c71f52c2fec5a17b3c844d3dbd6` |
| `docs/bpay-next/design/WB-05-successor-provenance.json` | `451535c2bf3f620cf80c8ce794a9875f1453d8a3c366d7653c24a2f0194d089d` |

The complete `CREATE ... $function$;` body is byte-identical to 1540 after removing the single added `if v_existing=0` block at 1403:122–135. The signature, return tuple, SECURITY DEFINER and `pg_catalog,private,public` search path are unchanged. The existing source-command UUID mapper is not redefined. Registry changes select only this actual pair signature as its new final owner.

The added block is justified by the actual 1346 path: `mark_processing` resolves `contracts.client_id` (1346:24) and stores that real Client in maintained group/item/summary FK rows. The single publisher invokes this projection barrier at 1346:217 before its command clock at 221. In a two-root call, the first member's clock lock remains held while the second member projects its distinct Contract/Client. Preholding both actual parent mappings before either call prevents a previously unheld second Client parent being first acquired behind that clock.

1403:126–129 acquires `FOR SHARE` on the exact distinct Contracts of the already validated, already locked input works, in native UUID order. This protects their Client mapping from concurrent update. Lines 130–134 acquire `FOR KEY SHARE` on the real mapped Clients in native UUID order. There is no Candidate/history/agency-wide scan, invented Client, broad unrelated lock or new financial calculation. The existing input envelope remains one or two distinct real works under one Candidate (22–47).

The placement preserves the original native prefix: NEXT admission; actual Candidate FK/control; every affected WORK, relevant current/applied/new revision and physical Timesheet lock; then the complete Source revision/event and publication receipt checks (33–120). The new parent locks occur only after those all-or-none checks and before the first publisher/clock. `v_existing=0` excludes an already complete replay from these added locks. A partial replay still raises the same refusal; no missing member is filled in. The original candidate/publication/revision lock behavior on replay is unchanged.

The final member loop and return receipt are unchanged (137–149). Each sorted work calls the same installed publisher with the same real source-event-derived command ID; failures remain in one transaction, preserving atomic pair publication. The single publisher's native receipt/predecessor/physical authority and new preclock projection barrier are reused rather than duplicated.

The successor restores the pair's audited logical owner and exact revoked ACL (153–154); no public/browser/service grant, new routine kind, Source producer, protocol, table or trigger is added. `NOTIFY pgrst, 'reload schema'` is the normal closure addition in this successor; the old 1540 file did not contain a NOTIFY, so it is not described as byte-unchanged predecessor text. Its presence does not change the function's admission or financial behavior.

All actual first-Authorise/manual-review/qualified-hours/protected-before-authorise and Source final/withdrawal paths remain their named owners. OR-45/SP-02 retirement authority still governs the stale legacy Recover prose; this lock-set change does not restore old financial execution. Contract repricing/residual resolution remains with CM cards, and hosted installation, full queued publication, lost-reply/withdrawal/current-pointer joined behavior and application totals remain unobserved by this source review.
