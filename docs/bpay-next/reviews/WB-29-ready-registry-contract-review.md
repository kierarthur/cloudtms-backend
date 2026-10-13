# WB-29 Ready registry, source gate and contract review

SOURCE PASS — scoped Ready support batch; 0 open findings.

Read-only comparison of current saved files with `8935125b15e998849b805a568fbe7ced95295a45`. This receipt covers one 0113 migration and the five final definitions in 0113/0117/0118. Whole WB-29, subsequent reader/Office joins, screen approval and joined business/runtime acceptance remain open. No SQL, RPC, database, test, fixture, browser, hosted or financial action was performed by this reviewer; root alone writes product.

## Registry and source gate

The registry retains all 1,023 original signatures in their original order and adds exactly one private boolean overload, giving 1,024 entries. Exactly four existing entries change: public workbench page 0055 → 0117, private group display 1822 → 0113, public week rollover 0255 → 0118, and private six-argument current quote 0045 → 0113. Each predecessor remains recorded; finalRoot, finalPhysical and finalClosures agree. The appended seven-argument quote is the sole new signature and has only 0113 as its physical owner. Public signature membership does not change.

The complete gate diff adds only the exact private seven-argument signature and finite callee allowances for the named rollover/display owners. The prior fresh gather callee set moves to the seven-argument implementation; the six-argument entry permits only its same-family delegate. The added official-date, queue-origin, need/current-quote, floor/mod/cardinality and JSON type calls are present in the actual owner bodies. Captured gather/facade mappings remain unchanged. No history, dynamic SQL, structural JSON concatenation, typed constructor, signature matching or other gate algorithm/waiver changes appear in this diff. This is direct static review of the gate change, not a claim that this reviewer executed the gates.

## Contract and access boundary

The contract has 2,398 → 2,399 routines and 588 unchanged relation identities. Exactly four existing routines change only definition_sha256; the sole added routine is private, STABLE, SECURITY INVOKER, with pg_catalog/private search path and owner-only EXECUTE. The existing six-argument quote retains its optional through-segment default and delegates with literal false; the new final boolean has no default, rejects NULL, and is not a public argument.

The complete contract diff contains exactly four new columns: group chosen_display_date_sensitive (boolean NOT NULL DEFAULT true), group chosen_display_pay_date (nullable date), revision chosen_rollover_seek_date (nullable date), and revision chosen_rollover_seek_work_id (nullable uuid). The two validated nondeferrable CHECKs enforce finite display dates and the nullable rollover seek invariant. The sole new partial index is bpay_next_wb_chosen_display_due_idx on date NULLS FIRST/work_id for complete, clean, CURRENT, date-sensitive WORK groups with Ready or Action counts. Only columns/indexes/constraints of these two private relations change. Existing relation ACLs, owners, RLS/force-RLS and other relation properties are unchanged. Enums, schemas, policies, triggers, extensions, default ACLs and contract version are unchanged; there is no new public SECURITY DEFINER membership or grant exposure.

## Saved catalogue reconciliation

All five independently extracted literal source-body MD5s match both sourceMd5 and installed.bodyMd5 in WB-29-ready-display-owner-catalogue.json:

| Final definition | Physical owner | Body MD5 |
| --- | --- | --- |
| public.bpay_next_workbench_page_v1(jsonb) | 0117 | d5fb701c4ff8f4484a805c2bfa9cba40 |
| private.bpay_next_workbench_refresh_group_display_v1(text) | 0113 | 1f4266129e5eaa3e4506a2b295c269e9 |
| public.bpay_next_workbench_week_rollover_page_v1(jsonb) | 0118 | 251fc084ccb2f26ec38901b275b65abd |
| private.bpay_next_current_component_resolution_quote_v1(uuid,uuid,uuid,date,text[],text) | 0113 | dc89b29affbda5bc13704d7bdae97f97 |
| private.bpay_next_current_component_resolution_quote_v1(uuid,uuid,uuid,date,text[],text,boolean) | 0113 | c17586c25b3f5a882853b08afd075a99 |

The saved catalogue reports matching body/configuration/volatility/ACL for all five, matching column/default/index/CHECK metadata, public SECURITY DEFINER membership delta 0, general isolation 839/76/0 and successful general and Candidate/MyTMS isolation DO results. Its local owner mapping is consistent with the logical postgres contract owner. These are root-supplied local PG17 metadata results reconciled against saved source; this reviewer did not rerun database verification and makes no business-data or runtime acceptance claim.

## SHA-256 of reviewed bytes

| File | SHA-256 |
| --- | --- |
| docs/bpay-next/final-definer-closures.json | 87C6E300665D0966E017D7D0065CBD2AEDE9F773D83F6DFBC354AD4457A345BB |
| scripts/bpay-next-source-gates.mjs | 95F3FC52311133B56AC58C1EDB2C88C589C55C4222A173367ED6E8C9DDCCCD8A |
| supabase/release/current-contract.json | C47413EA3796237869C67FFEB78DB940E13C542E0AC3A0AD0633B50B7ADC3A7E |
| supabase/migrations/13102026_0113_banking_pay_next_chosen_display_date_support.sql | 27FA42F6C21758235E1970DC1AA684570636C82822B1A8BDCBB2C249BA232F8D |
| supabase/repeatable/13102026_0113_banking_pay_next_chosen_display_refresh_quote_v1.sql | 9CF9E4054BFD6C7C06BABC1CFD8FDC0FE3280BF291E0AC059C009906C458DDEC |
| supabase/repeatable/13102026_0117_banking_pay_next_chosen_display_read_barrier_v1.sql | E924CB0C55D2AF7C85F9C37C0B6164DEC2EA8532F319EAD1F49B38F0C7E2EE5B |
| supabase/repeatable/13102026_0118_banking_pay_next_chosen_display_date_repair_v1.sql | 03525D56662AAB43919A29DBC45AFAE4E0D36E85D4D5A3D0B82BCA4C8C821BAC |
| docs/bpay-next/design/WB-29-ready-display-owner-catalogue.json | E825F4682E978A48C4B83864F1CD9026C4F2E37CAEFB1767A87D51C95367EBE6 |
