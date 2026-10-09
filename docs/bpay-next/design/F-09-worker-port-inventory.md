# F-09 Worker port inventory

Read-only saved-source investigation, 9 October 2026. F09 and F09-D01–D17 reread. No product code edits, SQL, runtime, tests, fixture/proof runners or hosted calls. This establishes source availability, not syntax/test/deployment acceptance.

Exact17 modules: 15 absent C6 copies and 2 existing dead C5 copies. Both current dead files match cached origin/test LF bytes; their only path commit is420513c6. No later origin/test change affects either. The saved broker source has 0 imports of the two old copies, confirming current dead/unwired status. Do not copy index.js in this item.

## Exact file identities

| Deliverable / broker source | RESET SHA256 / lines | BUILD-BE SHA256 / lines | Action |
| --- | --- | --- | --- |
| F-09-D01 `broker/src/bpay-next-background.js` | `77FAA6D64335F9C54A5508F09DF30FA02763861C9B89B3959B8F43349AD0EDF8` / 126 | `ABSENT` / — | COPY_ABSENT_C6 |
| F-09-D02 `broker/src/bpay-next-command.js` | `95F082F6FEF61932B0498EA007D47966692EC15094D954FC9CBBF4BD1D7ED059` / 337 | `7DF17FA7ACDB77D30411C955553F49C15EBDB3278621E23F64B94D37E21D7152` / 333 | REPLACE_DEAD_C5 |
| F-09-D03 `broker/src/bpay-next-command-rpc.js` | `060CE9BDAB50576AEC731D0B9B3C6499A608E3CD71C17A3C9BDF54483DB1B68E` / 34 | `ABSENT` / — | COPY_ABSENT_C6 |
| F-09-D04 `broker/src/bpay-next-financial-queue.js` | `DE92B5604E8BDDD24EFEDD1DFEAB3A09D052613484E457785412A4E9E72B9C19` / 207 | `ABSENT` / — | COPY_ABSENT_C6 |
| F-09-D05 `broker/src/bpay-next-frozen-advice.js` | `368483F19A7B0523E31AE022DA69A334C22ED4DB83E706F438AF85B3BAD63C1C` / 436 | `ABSENT` / — | COPY_ABSENT_C6 |
| F-09-D06 `broker/src/bpay-next-frozen-advice-rpc.js` | `ACF94F66927911C3252F2F59D5E4BC4AB2EC4AE3D7E550E4D4D0100B8BF80549` / 52 | `ABSENT` / — | COPY_ABSENT_C6 |
| F-09-D07 `broker/src/bpay-next-frozen-read.js` | `3C7D75B485DB6824A38000D580EB96C37864C01A61FDAF114D5551637E10C3B2` / 381 | `ABSENT` / — | COPY_ABSENT_C6 |
| F-09-D08 `broker/src/bpay-next-frozen-rpc.js` | `6DDBC1ECB64873714E9A9C9A05BC4FCB2DAE5D7FCA95236A4F75976594A4E83C` / 25 | `ABSENT` / — | COPY_ABSENT_C6 |
| F-09-D09 `broker/src/bpay-next-history.js` | `81D4CA2FB90B1BBEC7DEA3A41698933F70B306833079484D45A89E4E77738451` / 254 | `ABSENT` / — | COPY_ABSENT_C6 |
| F-09-D10 `broker/src/bpay-next-job-rpc.js` | `D00D086D44BAAEE414CA8509FBE9C30B91015E55982D84C2035E32A3DFBA7FB7` / 24 | `ABSENT` / — | COPY_ABSENT_C6 |
| F-09-D11 `broker/src/bpay-next-job-step.js` | `A103B8E4D6551EBE879392C3FBE2087EA2B8AEEA41B47C8A0E1C0CEFABCFA0C5` / 395 | `ABSENT` / — | COPY_ABSENT_C6 |
| F-09-D12 `broker/src/bpay-next-legacy-email.js` | `3A3684860794FAF2A59DD4A3F7667E5ECB95696D82C48DCCA7784A01DFAFED65` / 72 | `ABSENT` / — | COPY_ABSENT_C6 |
| F-09-D13 `broker/src/bpay-next-legacy-fence.js` | `DF2F676CC9ADE226D8D5DD120696BB15F3B53D8E4B6894E511D7F78424469FEA` / 181 | `ABSENT` / — | COPY_ABSENT_C6 |
| F-09-D14 `broker/src/bpay-next-preparation.js` | `DEFD708E500EA645EE272D326A7B0ED15F03C8B07DC280D3C9590933548C8ED6` / 346 | `54B0A2A9CC0DCCB5BFD13F1BA49134DC1A8700F7BCCFE4304F1CD5C1444E4B70` / 330 | REPLACE_DEAD_C5 |
| F-09-D15 `broker/src/bpay-next-preparation-expiry-rpc.js` | `190AED494B72C87AC9B3579C3CC752D2620E585C2DE24A8BAD20C2DD86D3EA9C` / 309 | `ABSENT` / — | COPY_ABSENT_C6 |
| F-09-D16 `broker/src/bpay-next-preparation-rpc.js` | `642A7D989F526C821B36EB58B1BB00F32439DA953BC260C35AC290601FD37C34` / 16 | `ABSENT` / — | COPY_ABSENT_C6 |
| F-09-D17 `broker/src/bpay-next-runtime-rpc.js` | `9C8C94E9BB1A3A42C126C815D6F896F59DE20BF75ED34D776C089C5628E55A91` / 322 | `ABSENT` / — | COPY_ABSENT_C6 |

RESET root: `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\banking-pay-reset-implementation-20261003`. BUILD-BE root: `C:\Users\KierArthur\OneDrive - Arthur Rai\Documents\GitHub\.codex-worktrees\bpay-next-build-backend-20261009`. SHA256 is saved raw bytes; exact absolute source/target paths and history are in the JSON companion.

## Complete module import graph

| Importing module / line | Specifier | Target in selected17 |
| --- | --- | --- |
| `bpay-next-background.js`:12 | `./bpay-next-financial-queue.js` (STATIC) | True |
| `bpay-next-background.js`:11 | `./bpay-next-preparation-expiry-rpc.js` (STATIC) | True |
| `bpay-next-background.js`:10 | `./bpay-next-runtime-rpc.js` (STATIC) | True |
| `bpay-next-command.js` | none | self-contained |
| `bpay-next-command-rpc.js` | none | self-contained |
| `bpay-next-financial-queue.js`:28 | `./bpay-next-job-step.js` (STATIC) | True |
| `bpay-next-frozen-advice.js`:15 | `./bpay-next-frozen-read.js` (STATIC) | True |
| `bpay-next-frozen-advice-rpc.js` | none | self-contained |
| `bpay-next-frozen-read.js` | none | self-contained |
| `bpay-next-frozen-rpc.js` | none | self-contained |
| `bpay-next-history.js` | none | self-contained |
| `bpay-next-job-rpc.js` | none | self-contained |
| `bpay-next-job-step.js` | none | self-contained |
| `bpay-next-legacy-email.js` | none | self-contained |
| `bpay-next-legacy-fence.js` | none | self-contained |
| `bpay-next-preparation.js` | none | self-contained |
| `bpay-next-preparation-expiry-rpc.js` | none | self-contained |
| `bpay-next-preparation-rpc.js`:2 | `./bpay-next-preparation.js` (STATIC) | True |
| `bpay-next-runtime-rpc.js` | none | self-contained |

All relative module imports are present in RESET and remain inside the selected17 set. Port the complete set before syntax/import checks. Background statically imports preparation-expiry-rpc.js; omitting expiry is an actual broken import. job-step.js:54 preserves both SOURCE_PAY_QUERY_OPEN and SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE as explicit visible recoverable issues. The module import graph alone does not wire the Worker; F10–F14 own route/queue/cron/fence/shared-hook hunks.

## Available ordinary source-unit tests

The normal package source gate is `npm run candidate:bindings:check && node --test tests/*.test.js`. Its tests/*.test.js glob admits these saved tests. Inspect and port only applicable normal tests; never execute proof runners or create business fixtures. Request/RPC tests use controlled in-memory dependencies/fetch mocks and invalid local URLs; that is source-unit test evidence, not a database or provider programme. No tests were executed in this investigation.

| Recommended F09 test | Raw SHA256 | Existing BUILD-BE | Module coverage | Fixture/runner reference |
| --- | --- | --- | --- | --- |
| `tests/bpay-next-background.test.js` | `C3D93771CD1DB2AEE9F5C067ABF7CABCF4CCA9649F308FF4CFB906EAEC3859BE` | False | bpay-next-background.js | none |
| `tests/bpay-next-command-rpc.test.js` | `F1A40A82BF92862CD6494A3244E51DE4BE2E4D5894D0B30AB53ECC02CBF59B9D` | False | bpay-next-command-rpc.js | none |
| `tests/bpay-next-command.test.js` | `0D847330D80E8C8E3FAD9225E00726E26AFD364E7903C810D71CA66CB58FCA3D` | False | bpay-next-command.js | none |
| `tests/bpay-next-financial-queue.test.js` | `95A2183B9C323772742525A4C3A21347DB359AF258AF02D5750DD51F7E25257E` | False | bpay-next-financial-queue.js | none |
| `tests/bpay-next-frozen-advice.test.js` | `70CD2F2D4172963D289A351F99C7123B230AC5A85908FD470007BA4A6DA44150` | False | bpay-next-frozen-advice.js; bpay-next-frozen-advice-rpc.js | none |
| `tests/bpay-next-frozen-read.test.js` | `0963A0B474C6874EB3D57DEC0AF0683524F500E7067EE021AAC0BFCFD73B56D7` | False | bpay-next-frozen-read.js | none |
| `tests/bpay-next-frozen-rpc.test.js` | `A7C4DD574F9CE713CE2B62C9076294902939E6E5BE7B249F3A3FE3ADD8F2DEE5` | False | bpay-next-frozen-rpc.js; bpay-next-frozen-read.js | none |
| `tests/bpay-next-job-rpc.test.js` | `42EF10973B8A409F42A2DC459E1D46AF918A5ED178E68C03980AB1E3FFCFDDC8` | False | bpay-next-job-rpc.js | none |
| `tests/bpay-next-job-step.test.js` | `1DAAAA0736FC14E3384EDFE56725CF1ED8132AEF9AAD3C6C00AFE1E20798B656` | False | bpay-next-job-step.js | none |
| `tests/bpay-next-legacy-email.test.js` | `7F62D8ED70027ADC9D76BC3634DA5014FCCAC66DDA183C3C1055F0B5057AECF4` | False | bpay-next-legacy-email.js | none |
| `tests/bpay-next-legacy-fence.test.js` | `A806CB57ACD15BFEA895209D1579CF8243828B9949EA039C0D7594544D144253` | False | bpay-next-legacy-fence.js | none |
| `tests/bpay-next-payment-history-caller.test.js` | `B5BF563A511D3A24437C922F31EA4A31D4B0130EDA8F253998610DB7A34C798D` | False | bpay-next-history.js | none |
| `tests/bpay-next-preparation-expiry-rpc.test.js` | `84E4672E8BC12C60440337B66D4A9511CED840A99ADEE9C6828A48E074D86D16` | False | bpay-next-preparation-expiry-rpc.js | none |
| `tests/bpay-next-preparation-rpc.test.js` | `DC65CF3E1B0D0D8934C57226357E70FCC00324809D5F537F42667EB4725676E7` | False | bpay-next-preparation-rpc.js | none |
| `tests/bpay-next-preparation.test.js` | `C1526ACD3B6F57F4C60CCF39E2D59D6B21F012F9E258CF9025F224A45FDDDEB7` | False | bpay-next-preparation.js | none |
| `tests/bpay-next-runtime-rpc.test.js` | `89C252719C568A0DBC0A8FB7CF9B81DE2A0BBC1D0EFD64FA456473D1C0DE6332` | False | bpay-next-runtime-rpc.js | none |

RESET has 45 bpay-next-*.test.js files; recommend the16 module caller/RPC tests above for F09. The remainder are source SQL/feature/wiring assertions owned by later cards (for example payment-history SQL, worker-entry, frozen-entry-method, preparation expiry boundary, current Cases/credit/destination/recovery). Their full hashes/imports/source references are preserved in JSON, not implicitly selected. Older staged migration filename assertions may need source-path adaptation after F06 rescoping; adapt exact source references only under the relevant owner rather than porting obsolete migration files to satisfy a test.

## Source gate entry points

- `.github/workflows/candidate-expense-carrier-release.yml`:40: `- run: npm test`
- `.github/workflows/candidate-manager-email-verify.yml`:59: `run: npm test`
- `.github/workflows/candidate-source-bootstrap-guard-component-release.yml`:40: `- run: npm test`
- `.github/workflows/candidate-source-self-hours-component-release.yml`:40: `- run: npm test`
- `.github/workflows/database-release.yml`:70: `CLOUDTMS_FINAL_DEFINER_BASE="$base" node --test tests/bpay-next-final-definers.test.js`
- `.github/workflows/supabase-migrate.yml`:54: `# npm test includes sealed ancestry/rollback proofs that read exact`
- `.github/workflows/supabase-migrate.yml`:86: `run: npm test`

## Port constraints and follow-up

Replace command/preparation from reviewed RESET saved bytes; there is no newer same-path upstream body to merge. Copy the other15 files as one source set. Preserve closed typed keys, response/error mapping, owner/epoch containment, request/page bounds and SCX049 issue strings. A normal source-unit test is permitted by F09/B11; a first-use fixture/proof runner is excluded even if named similarly. Existing tests are available but not a passed check. Root alone copies modules/tests and later runs mandated syntax/import/caller checks.

Available for read-only F10–F14 index hunk investigation after this inventory; no index.js product hunk was edited or ported here.
