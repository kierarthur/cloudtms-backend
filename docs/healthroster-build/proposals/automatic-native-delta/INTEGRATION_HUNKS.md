# Exact integration

Read `NATIVE_INPUT_PINS.json` first. Reject a differing input and review its final owner; do not overwrite concurrent Money/Protection/Bank changes with an older complete copy.

Install the new private helpers in page order 01–08. All eleven new signatures are owner-only STABLE SECURITY DEFINER functions with fixed `pg_catalog,pg_temp` search paths, logical postgres ownership and EXECUTE revoked from PUBLIC/anon/authenticated/service_role. There are no new tables, indexes, public/service signatures or schema codecs.

The three existing `private.weekly_source_pay_query_{local,c1,next}_receipt_v1(uuid,uuid,uuid)` signatures delegate to their full event-specific cores with a null selector. Each extracted core differs only by its name, one extra private event argument and the exact event selector; the source test mechanically restores the original definition and compares every byte. Do not grant the cores to service_role.

In the complete current `public.weekly_exceptional_pay_complete_local_v1(jsonb)` owner:

1. Add `v_automatic_components jsonb` next to the existing component variable.
2. After native snapshot/identity-manifest checks and before live-authorisation count/PREPARING insertion, when `complete_next_vector_json ? 'automatic_reconcile_proof'`, call `private.weekly_source_automatic_reconcile_delta_assert_v1(v_request.id,v_actor)`.
3. Inside the already-authorised arm, after the actual Local v2 context and before its existing complete-amount/source-expense assertions, use `private.weekly_source_staged_entitlement_components_v1(v_request.id,v_actor)` only for a sealed automatic request. Assign `components` and `expenses` from that result. The `else` is the entire unchanged existing expense/segment/additional producer block.
4. Preserve all subsequent common proposal/publisher/receipt/native chosen-detail/NEXT calls. A returned failure or non-PUBLISHED automatic result continues to raise in the existing atomic Source27 owner.

The exact complete Local successor is page09. It has the existing public signature, existing role/actor/Office guards, existing I-1 admission/locks, exact committed replay before freshness checks and existing first unauthorised save behavior. Review by diff against the pinned current complete owner.

The existing stage call site in page10 is already correct: real request/sources/components/family K+1/COMPLETE `STAGE_C1_REQUEST` step, then `automatic_reconcile_certificate_v1`, then STAGED return. A certificate exception rolls that complete stage back. The existing page11 certificate dynamically calls the exact delta signature and validates its closed seven fields; atomic COMPLETE obtains the certificate before native Local, requires `ok=true/outcome=PUBLISHED`, then writes the sole cleared audit. Preserve its native actor/Source/serial/family/group/cycle gates and exact accepted replay. No Source25 or Money capability successor is supplied here.

These files are source proposals under docs. A release repeatable cannot include them directly outside its Supabase envelope. Root packages reviewed bytes as `.sqlinc` in chronological final owner order, records closure inputs/hashes, adds explicit final-role/ordinary-wrapper/closed-certificate verifiers and generates the final contract before any protected installation. Full Banking integration remains root-owned.
