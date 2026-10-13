# F-23 source gates: independent first saved-source review

10 October 2026. Read-only source review of root-authored untracked scripts/bpay-next-source-gates.mjs and tests/bpay-next-source-gates.test.js. No module/gate/test/runtime/SQL/browser/fixture/hosted execution. This is a bounded initial review while root is authoring; it is not a completed F-23 PASS.

## Findings requiring correction

1. **D08 exact actual capability is missed.** The current writer regex requires `provider` inside the relation name. EX-47’s actual declared relation is `private.bpay_next_rail_capability` (04:1297–1302), with `rail='PROVIDER'`, `enabled`, `enabled_by_authority_ref`, `changed_at_utc`; PROVIDER is seeded disabled. An UPDATE of that actual relation setting enabled true passes the current regex. Bind the actual relation/PROVIDER row and reject enabling INSERT/UPDATE/ON CONFLICT/default cases; preserve approved disabled seed and CSV/EXTERNAL availability. Do not invent a bpay_next_provider_capability table.

2. **D12 semantic frozen authority covers CREATE routine definitions only.** Exact seven-path raw-byte comparison and fixed F-01 ancestor are correct. Current `protectedSignatures`/definerInventory omit frozen triggers and destructive/altering authority statements. For example a new `ALTER TABLE private.bpay_next_work DISABLE TRIGGER bpay_next_work_pointer_guard_v1`, DROP/replace of that actual trigger, or ALTER/DROP of a frozen routine is not detected. Freeze actual trigger identity plus owning relation/callback, reject new nonbaseline competing authority, and retain existing exact base statements/includes. Core1324 has four finance-policy triggers on public.settings_finance_windows; guards1342 has fourteen named guards/immutable triggers. This needs a finite identity gate, not a broad ban on feature-owned new schema.

3. **D06 owner-writer exception has the wrong schema and permits a guessed overload.** The code allows any `private.bpay_next_owner_transition_v1(...)`; the exact F-17 owner is `public.bpay_next_owner_transition_v1(jsonb)` (01:853; F17-D02/03). Permit only that actual final owned signature/body once built. Do not grant a private same-name function/overload an owner-write exception. The exact frozen `INSERT private.bpay_next_module_control(id) VALUES(1)` bootstrap exception is appropriately narrow.

4. **D07 second simulation vocabulary is only partly covered.** Checking `evidence_kind =/ := 'SIMULATED_TEST'` misses a genuine `INSERT ... (evidence_kind) VALUES ('SIMULATED_TEST')`, column DEFAULT/check/enum support, or structured payload value. Bind executable actual vocabulary declarations/writes and reject this second label while preserving comments/docs and legitimate `evidence_source='SIMULATED'` plus default BANK. The actual TEST marker target is already explicitly declared by F-22/EX-47; no need to wait for an invented target name.

5. **D09 inherited occurrences do not retain literal data in exception keys.** `scanManagedSql` replaces every string literal with `<QUOTED>`, and the key currently serializes those tokens. A changed inherited hazardous statement with a different literal/filter value can borrow the old allowance. The design requires exact same signature/declaration/type/executable statement including literals and occurrence multiplicity. Retain literal source/data in the finite key or compare canonical exact statement spans. Preserve unchanged historical constructs; no broad Candidate or money-owner style rewrites.

6. **D09 offset and nested-body boundary need correction/classification.** `scanManagedSql` canonicalizes CRLF, but `source.slice(row.start,row.end)` uses the original input in writer, executable-body, RPC and frozen-definition paths. Use `parsed.source` consistently. Three retained changed migrations currently have one CRLF each, so this is an actual saved-input boundary. A nested dollar string containing dynamic EXECUTE SQL is opaque `<BODY>` when the inner body is scanned: any new forbidden dynamic construct needs a finite unclassified/refusal rather than implicit checked status. Quoted identifiers are unescaped before the second token scan, while literal SQL-looking content should not become ordinary executable tokens.

7. **D06–08 executable source coverage and D10 migration-only entry need finite completion.** Current `sources` enumerates managed migration/repeatable closures and tracked seed/verification SQL, but no Worker/config/generated-script defaults, and untracked standalone seed/verification files are absent. The card/B2.19–20 explicitly excludes Worker defaults and automatic enablement. Cover the finite actual source locations without treating docs/reference packs or normal test examples as product writers. D10 selects repeatable public definitions only; a new callable public definition only in an admitted migration lacks the required controlling-root refusal. New/changed migration RPCs should resolve the actual final repeatable root or fail closed. Reuse existing exact caller gates and D05 inventory; no generic new harness.

## Preserved / bounded source conclusions

- Seven frozen raw files and the immutable exact F-01 base are correctly enumerated; no product changes by this reviewer.
- Existing D05 final-definer closure and existing D11 exact normalized method/path route source tests remain appropriate. No duplicate route framework is needed.
- Baseline historical SQL allowances are appropriate in principle; literal/source precision is the remaining issue. Blanket rejection of unchanged inherited constructs would force unrelated owner changes.
- RPC notify check requires the exact top-level channel/payload after definitions/grants and before final COMMIT; that ordering is correctly strict for chosen roots.
- At this read, no CI/local-loader invocation of the new source-gate module was found. Wiring is still builder work; ordinary npm test will discover the .test.js automatically once committed.
- Browser-isolation verifier diff is exactly added explanatory comments plus count824→826 and MD5 2cb2ef197993c05bb0c1566b04c601dd→10128c0eb0ada910eea370f487d5f214. It changes no query, grant, omission or earlier ACL row. The exact two added public jsonb readers are readiness and payment history. Parent reports local catalogue826/76/0 and removal of only those two reproduces824/76/0; source-only reviewer does not independently execute that census or claim installed evidence. Candidate inventory is unchanged by this diff.

## Current source receipts

- `scripts/bpay-next-source-gates.mjs`: `12f3723dab15b3046b2c4e1bd16343928e12b0d5888e0650ccbd870abb6249b8`
- `tests/bpay-next-source-gates.test.js`: `b844e0f30169eff8e3d97152256f5c47d2f3f1ecd4c5c16b405591f8cc2e1c82`
- `supabase/verification/22082026_1302_general_browser_isolation_verification.sql`: `a8f25fde93465f3cc35be3c86f02689309529c1977ecb20c3570fe47bc0a3544`
- `scripts/bpay-next-local-schema.mjs`: `db65145e8b59c0f9abf1d70c32aee0a5f8d63d8e8b8625f1bf9685aa992c6c2a`

Scope remains the ordinary existing source gates plus explicitly named F-23 gates. No testing programme, proof framework, new money owner, parked F-25 work, release/deployment or checklist write was performed.
