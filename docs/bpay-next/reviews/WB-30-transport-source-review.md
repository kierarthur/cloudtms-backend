# WB-30 closed transport source review

13 October 2026. Independent saved-source review only, against the full card/D01–D09, reviewed build boundary, completed native-reader note, existing2302/2320 contracts and uninstalled0201 draft. Root alone changes product. No product/checklist edit, SQL/RPC, business/database/data, fixture, test, runtime, render/capture or hosted action was performed.

## Initial findings saved immediately

**T30-01 — bind the discovered native row family and optional root line.** `bucketResponse` derives the old explicit scope from `payload.works`, then validates that derived scope. The new review branch checks seed Work/revision but never checks a nonnull requested `bucket_review.approved_line_id` against the root rows. It also constructs the raw-member key without rate family and checks a group member's literal STANDARD without requiring the matched native row's family to equal it. A normally valid explicit ADDITIONAL native row with rate code DAY can therefore be presented as a STANDARD group; a reply for another root line in the same Work can satisfy this validator. Require every discovered native row STANDARD, exact member/native family identity, and all root rows matching the optional requested line. These are finite response/caller checks, not a new server feature.

**T30-02 — qualify saved-choice inheritance and close the signature schema.** The linked/no-saved-owner path compares signatures after deleting `saved_target_rate`/`saved_mode`, before validating the signature's exact keys/types. It also checks the group's `saved_target_rate` against native saved quote only for nonlinked members. A linked-only fresh group can therefore omit/alter saved fields or invent a prefill while passing the inherited comparison. Require exact full signature keys, the group's saved rate matching the signature, and any inherited saved mode/rate to be backed by an actual qualified native member/owner/quote. Linked-only fresh groups must retain NULL saved mode/rate. Reusing a qualified root choice for a genuine linked no-owner member remains permitted.

Both findings were sent to root promptly. Initial status **OPEN for T30-01/T30-02**. Gate/registry static comparison and final reread remain in progress; no whole-card or implementation acceptance is granted.

## Initial raw hashes

| Saved file | SHA-256 |
| --- | --- |
| broker/src/bpay-next-resolution.js | `6a78cc985a42069de886ff37c8aad2fa466e6dddded4bf6ea90ce7f5a949235d` |
| scripts/bpay-next-source-gates.mjs | `031b1dfc4e5b27ab74f6496776bf11fe7b99f3df88e70e0cd16b77cc38fb8cd0` |
| final-definer-closures.json | `816bef21f758e8c0fd9fb70e5f3811090662e10f8ec13b677a7c9c54e538f84e` |
| design/WB-30-native-reader-boundary.md | `382000512f16ebcd74e4c623fdfe9e4fb779550f04b8ab21e61debdcf171b033` |
| 13102026_0201 bucket resolution reader, draft | `533dd53a7bafd89cd2b49a59a72440840ad02b9bee25579608097b38f0128e28` |

## Final current-source override

**SOURCE PASS for the reviewed closed transport, finite source-gate additions and registry promotion; T30-01/T30-02 CLOSED, no remaining essential finding in this scope.** This supersedes the initial OPEN status. Native SQL whole-body correctness, execution, UI/A4, joined acceptance and whole WB-30 completion are not claimed. All observations below are static saved-source checks; AUTHORED_NOT_RUN by this reviewer.

T30-01 is closed by requiring every discovered native row STANDARD, exact member/native family equality, all requested root rows matching a nonnull seed approved-line ID, and header week matching the native root context. T30-02 is closed by the exact nineteen-key signature, displayed saved-rate parity, qualified nullable native saved mode/rate and at least one full exact native signature member per group. Other linked members may inherit only the qualified no-saved-owner fields. Linked-only unsaved groups cannot invent saved prefill.

The request preserves the native aggregate limit of 100 flattened rows and 32,768 serialized bytes; the assembled response is capped at 120,000 bytes. Current and saved decision/revision identities, exact group coverage, nullable rate/source facts, signed six-decimal margins and sixteen-integer-digit units are qualified. BigInt arithmetic checks native totals and ordering without pricing. Header and partial-exclusion facts use closed schemas. Evidence has a closed native parent/cursor tuple, at most 25 rows, strict native ordering, exact last-row continuation and no empty nonterminal page. Foreign parent/cursor identities are refused. The existing native context validation still supplies actor and Candidate scope.

After removing only the new read prefix/dispatch branches, the backend explicit-request normalization, W2 mutation and error-validation source equals HEAD after newline normalization. The entire frontend validation block equals the backend block after removing export syntax, and the retained frontend command-controller body equals HEAD. Its original journal, persistence, unknown-outcome identity and retry logic are retained. Removing only the two new read dispatches from the native context routine reconstructs the complete current 2302 parent body exactly after newline normalization.

The registry semantic census contains only two new private reader signatures and the existing private context owner promotion to 0201; no signature removal, unrelated entry change, public admission or security-inventory change was found. The existing source gate gains only those two exact routine roots, their finite callee lists and the exact two context edges plus needed existing JSON-key grammar. No dynamic SQL, unknown-history, broad type, baseline or delegation allowance was introduced. Root-run checks are outside this independent source receipt.

### Final raw SHA-256

| Saved file | SHA-256 |
| --- | --- |
| BE broker/src/bpay-next-resolution.js | `6ce98d566924c781a960ee9ceeefe7db80ff9215e4d15fb39fd34c7c2cf06be2` |
| FE js/bpay-next-resolution.js | `bbd40ff9ce6e48e7e0ec4a326370fae31aad5a3efdea440043ef65d8f2d8bfd1` |
| scripts/bpay-next-source-gates.mjs | `031b1dfc4e5b27ab74f6496776bf11fe7b99f3df88e70e0cd16b77cc38fb8cd0` |
| docs/bpay-next/final-definer-closures.json | `816bef21f758e8c0fd9fb70e5f3811090662e10f8ec13b677a7c9c54e538f84e` |
| design/WB-30-native-reader-boundary.md | `382000512f16ebcd74e4c623fdfe9e4fb779550f04b8ab21e61debdcf171b033` |
| 13102026_0201 bucket resolution reader, draft | `dadd2dd400af2e36c1e83ddd477191090813d3f82278effb8439591092ea124d` |
