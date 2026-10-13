# WB-28 Worker and Office source review

13 October2026. Independent read-only review of root's draft closed DECISIONS_CLEAR transport/controller/UI and existing-route/store/caller changes against complete WB28 card, all exact rows, BD175 and supplied design integration note. Root alone edits product. No SQL/RPC/database/business/runtime/test/fixture/network or product writes. **SOURCE OPEN** at this saved checkpoint; initial essential findings were sent immediately. Actual A4 remains separate.

## Current findings

| ID | Current source / precise required correction |
| --- | --- |
|O28-01: retained operation unreachable after date/epoch change|Controller key includes entire current namespace (including today's owner_epoch and official_pay_date). Reopening from today's HEADER therefore misses an old pending operation and can start new FREEZE rather than recover its original bytes. Keep recoverable original agency/actor/filter identity accessible across mutable epoch/date, validate its original stored namespace/body internally and use that original date/scope store for any accepted local reset. Never reset today's store because its revision happens to equal an old local_revision. The newly added original date/epoch request fields qualify the body but do not fix this key/reopen seam.|
|O28-02: excluded ownerless rows shown as saved decisions|reason(row) checks owner.exclusion_code and outcome_code but ignores row.exclusion_code when owner is NULL. Original frozen excluded member rows then say Saved decision instead of In a Draft/Changed/Unavailable, hiding exact disclosed exclusions. Use native row exclusion as the fallback; never infer that excluded amount is clearable.|
|T28-01: impossible saved-owner tuples accepted|Closed owner validation currently accepts arbitrary STANDARD rate_code and manual mode/operand mismatches (e.g. MANUAL_AMOUNT with no amount or simultaneous manual rate/amount). Match native five STANDARD codes and mode-specific null/value rules in the identical BE/FE block. Historical unavailable owner/mode semantics remain nullable; genuine zero remains accepted. Do not tighten arbitrary Additional identities to five legacy EX codes.|

Root was editing the original date/epoch identity concurrently. The fresh files now include native owner_epoch/official_pay_date request identity, receipt equality and internal namespace binding; those additions were reread and are not reported missing. Further byte/page/currentness review remains in progress. Current initial source hashes follow.

| File | SHA-256 |
| --- | --- |
|BE broker/src/bpay-next-decisions-clear.js|7807D4BE3440A7CB95E76947CF1F525CC52C8D764622371F0A258910A9F0BDFE|
|BE broker/src/bpay-next-workbench.js|F5C7A9AA75370911602466806DEC323114FAAB51C42E4AEE195CCF248746935F|
|FE js/bpay-next-decisions-clear.js|E16C52E5480AC1200E3B8FBAE20980C7C7A618BEABEFF61DD2295C3931F837DB|
|FE js/bpay-next-workbench.js|687AC237FFA2DC628C3F27FC674DC004BAC14AD3F5909DEC7327655032E5023E|
|FE js/bpay-next-office.js|173F02F5FA0CA3B235443BBDCA22B5BAD616AF33E7AA84090BA246C75BF18BBB|
|FE css/bpay-next-office.css|43ACF881F17CED3BC5B48BB79BB4D13BBC51597C6C276D8DAA4D27AFAB8DC679|
|FE index.html|5C1EEEA487008B3FADEB0F154E151A938586542AF3969333F08822ED1FAC3B15|

The existing SAVE/CLEAR original command controller, original request schemas and money owners are separate and must stay unchanged. This review does not approve the complete SQL operation or infer whole-card/app/business/A4 acceptance.

## Fresh repair and remaining continuation checkpoint

O28-01/O28-02/T28-01 are CLOSED at current BE typed module SHA `CCA0BEFE84B805D6E15460399C211E63BD8828D64BBD4D66601D39C056498BCA`, FE module `B7319D401F4728A615B5AA3BB50E52E876D91FA6855026FED87B6C03A5E1EE27`, workbench caller `4E00A8E59DA0CAD210D502388FEC3021F8D6859697E464D72964BD965239C82F`. The closed BE/FE DTO block is statically text-identical after module-export/EOL normalization. Stable agency/actor/filter key loads and internally validates retained original date/epoch; fresh mount namespace is compared before a local store reset, preventing today's coincident revision from being reset by an old result. Ownerless exclusions now use row.exclusion_code. STANDARD codes and manual mode/null operands are strict, preserving genuine zero/arbitrary Additional identities. Original request/result date/epoch equality is present.

Three finite points remain OPEN and were sent promptly:

| ID | Actual source / minimal correction |
| --- | --- |
|O28-03: variable-height continuous-scroll anchor|readList sets Back position to list.scrollHeight/3 and Forward keeps oldTop after evicting the oldest retained page. Long actual labels/variable row heights make this jump or skip visible disclosed rows. Retain first visible member_no/owner_no/offset or measure actual inserted/removed page height, preserving <=3 pages and exact failed cursor/history. No new list reader or sorting API.|
|T28-02: native phase/step result qualification|Outer request phase/step is matched, but result next counters/state are not phase-qualified and directly govern controller continuation. Require received FREEZE state DISCOVERING/READY and freeze_step=request.step+1; received APPLY state APPLYING/COMPLETE/PARTIAL and apply_step=step+1; processing APPLY is nonterminal and keeps apply_step=step. VIEW retains the existing read-only states/cursor contract. These are finite native receipt relationships, not a new protocol or browser business arithmetic.|
|O28-04: native unconfirmed stale refusal has no Refresh|run maps only BPAY_NEXT_CHOICE_CONTEXT_CHANGED to the unconfirmed stale path. Native BPAY_NEXT_DECISIONS_CLEAR_STALE (including original date/epoch change) instead loops Check result against a rejected discovery identity. Map that exact code too only when !consented, using the existing explicit Refresh/cancelUnconfirmed readback; confirmed/unknown operations stay retained and no automatic newer-scope substitution is allowed.|

Store reset binds captured local revision and exact draft-filter namespace, detaches remembered/inherited defaults and clears only relevant local arrays; a later edit returns false and is retained. Reset's repaint/selection label callback is local, not onRefresh. Existing financial journals/old SAVE/CLEAR controller and money owners are not changed. All new CSS is scoped; actual pixel/copy/44px/native confirmation style remains A4's separate review. No runtime performed.

## Final source closure — 13 October 2026

**SOURCE PASS, zero open findings.** This final amendment supersedes the OPEN checkpoints above for the exact saved hashes below. All six findings O28-01–04 and T28-01–02 are CLOSED. This is an independent static Worker/Office source verdict; complete SQL operation, joined business acceptance and A4 remain separate. No product changes or runtime/test/SQL/RPC/network calls were performed.

O28-03 now captures the actual first visible member/owner DOM row and its measured offset before adopting a page, then finds that exact row after prepend or eviction and adjusts by its measured position. The retained window remains at most three pages, with original failed cursor/history retained. T28-02 now qualifies received FREEZE and APPLY states and their exact next step, while processing APPLY remains READY/APPLYING at the original step; VIEW remains read-only. O28-04 now includes only the exact native DECISIONS_CLEAR_STALE code in the existing staff Refresh path when the original operation is unconfirmed. Confirmed or uncertain operations retain their original identity and bytes.

The closed BE/FE DTO blocks remain statically text-identical after export/EOL normalization. Stable agency/actor/filter recovery, internally validated original epoch/date, exact pending-page readback before retry, original namespace guarding of local reset, later local-edit preservation, and cancellation/retirement returning a new operation to the initial current namespace remain intact. The final icon/wording and scoped CSS edits add no quote, pricing, financial scope or command owner. Their visual policy verification belongs to A4.

| Final reviewed file | SHA-256 |
| --- | --- |
| BE broker/src/bpay-next-decisions-clear.js | CCA0BEFE84B805D6E15460399C211E63BD8828D64BBD4D66601D39C056498BCA |
| BE broker/src/bpay-next-workbench.js | F5C7A9AA75370911602466806DEC323114FAAB51C42E4AEE195CCF248746935F |
| FE js/bpay-next-decisions-clear.js | CF1AAF3CD5974A53E2B96F7A28CD8CC370A2D7FE6226014DB73BD0C055CF1C18 |
| FE js/bpay-next-workbench.js | 8A525750584F9E7793094DADED696073B9629E2456C8E66A1125F56B40B34E0C |
| FE js/bpay-next-office.js | 173F02F5FA0CA3B235443BBDCA22B5BAD616AF33E7AA84090BA246C75BF18BBB |
| FE css/bpay-next-office.css | 84F22E7F0F704DFDDF0EDC88CAFAA53A240F52702206BC209305B5EEB3B8ED6D |
| FE index.html | 5C1EEEA487008B3FADEB0F154E151A938586542AF3969333F08822ED1FAC3B15 |

## Final A4 presentation source amendment — 13 October 2026

**SOURCE PASS retained, zero open source findings.** Fresh saved bytes were reread after the final A4 repairs. Only the native question/result labels, singular grammar, state-specific native icon/result/row tones, confirmation-header visibility, safe initial Cancel focus, reset result role/status live marker, fieldset specificity, and existing truthful empty-line visibility changed. These changes preserve all original journal/controller/dispatch/namespace/local revision predicates and the identical closed BE/FE DTO block. The exact later owner changed-list wording remains literal. No pricing, new command, rediscovery or payment authority was added.

The final 30 actual PNGs were independently inspected, including final replacement empty pixels; the other 27 hashes remained unchanged after that last empty-line condition repair. P28-01–04 are closed. Separate full 42-row A4 APPROVED receipts are saved in SCR-02/review.md, SCR-18/review.md and SCR-19/review.md, preserving older SCR-02 history. This source amendment is not joined runtime/business acceptance; the reviewer performed no product changes, rendering, SQL/RPC/database/test/business/network actions.

| Final current Office file | SHA-256 |
| --- | --- |
| js/bpay-next-decisions-clear.js | 97F9B291ED0CF24C7CCB14E73C20189B97C7BA4C961DF8B97220E85AEBE12A4C |
| js/bpay-next-workbench.js | 83BD53D7CBD721D9A098DE705748EEEC6068C85CE618B78BF7C94FA6C9F8F1CD |
| js/bpay-next-office.js | 173F02F5FA0CA3B235443BBDCA22B5BAD616AF33E7AA84090BA246C75BF18BBB |
| css/bpay-next-office.css | A1D16E05192FCEF7380A5746621FA0DBD97A66AF567EDFBDBF54580E16FFEB76 |
| index.html | 5C1EEEA487008B3FADEB0F154E151A938586542AF3969333F08822ED1FAC3B15 |

