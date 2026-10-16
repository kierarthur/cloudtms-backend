# Finite native mail claim composition

This proposal replaces only the current contact `082_native_claim_kernel.sqlinc` contribution. Root owns classification, central metadata and mount. It is uninstalled source and requires native verification; no F23 waiver is proposed.

The parent is the complete genuine `04102026_0558_banking_pay_next_legacy_email_claim_boundary_v1.sql` claim function at the exact source pin in `SOURCE_RECEIPT.json`. The new core keeps its one module owner lock, both complete LEGACY and separate indexed factual-only queries, all authority checks, native invoker/RLS, lease bounds, selection order and mail-row return recipe. The Banking owner helper is consumed unchanged.

`build-claim-composition.mjs` applies four finite additions: the existing core name/three operands, exact current query selector in both branches, exact current contact stamp before both lease updates, and bridge-only cleanup after the branch. Existing `083` wrappers and existing core ACL/signature stay in place. Do not mount the historical public parent definition after the wrapper. The old July-derived single-query core is superseded by this complete parent composition.

The tests compare every complete branch byte after removing only the authored additions. Hostile removal/change of either branch, module assertion, factual predicate, financial/Candidate authority, invoker, lease, selector, stamp or SKIP LOCKED fails the exact recipe check. This is source evidence, not a database/provider result. The separate PG17 offline check parses the complete SQL and PLpgSQL body; object resolution, execution, ACL and concurrency remain `NOT_RUN`.
