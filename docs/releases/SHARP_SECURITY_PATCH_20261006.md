# Approved narrow sharp security correction — 6 October 2026

The protected source gate for run `37479929849` refused `sharp@0.35.4` for `GHSA-wq5f-xc86-pv6w`. Its database job was skipped. This finding is not exempted or bypassed.

The user directly approved applying and testing only `sharp@0.35.5` and its matching dependencies in HANDOVER 2. This chat independently read that human approval before changing the dependency graph.

Compared with predecessor `529dcf23d0a0794ee1ac0843f700080d4fe2ae98`, exactly 27 lockfile package entries changed: sharp and its matching `@img/sharp-*` native packages. The matching libvips package version is 1.3.4. Every other package entry, lockfile metadata, and package setting is identical, except the sharp override itself. Wrangler remains 4.125.0; Miniflare, Node/npm, undici, and application dependencies remain unchanged. The provenance guard now checks the patched sharp and native package versions.

Local qualification:

- Clean `npm ci`: PASS.
- Windows native sharp 0.35.5 / libvips 8.18.7: SVG-to-PNG rendering, decoded dimensions and image format PASS.
- Full backend JavaScript suite: 1,453 PASS, zero failures or skips.
- Weekly-source unit harness: PASS.
- Database source integrity: 293 migrations / 790 repeatables PASS.
- Database contract coupling: PASS; this patch changes no SQL or contract.
- Approved toolchain, dependency provenance, source-secret scan, production/full npm audits: PASS (zero audit vulnerabilities).
- Normal TEST backend, Candidate private API, synthetic private API, and broker deployment dry-runs: PASS; no runtime was deployed by these checks.

These are local results, not hosted release success. The canonical `npm run release:test` coordinator must rerun the complete hosted source/security/database verification, including OSV, at the exact pushed successor commit with fresh connection evidence. Runtime publication is conditional on database VERIFIED. Office and store publication remain conditional on the actual Kier protected-pay Save acceptance test.
