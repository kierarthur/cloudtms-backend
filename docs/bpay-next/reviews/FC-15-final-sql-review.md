# FC-15 final SQL/design/source-gate review

12 October2026. Independent read-only source review. **Final SOURCE PASS for the reviewed1822/1849/1858 chain and narrow source-gate/design deltas; no whole-item PASS.** F15-S08 below is historical and closed by the final1858 amendment. Root alone edits product. Reviewed installed1822,1849 and uninstalled1858 source, BD149, owner ruling, prior source receipt and saved registry. No compiler, SQL/RPC/business/data/test/fixture/hosted calls by this reviewer. Business/application acceptance **NOT_RUN**. Root's local1822/1849 compilation evidence is attributed only to Root.

## Historical finding F15-S08 — manual display can infer taxable stage or lose original non-tax identity (closed)

1849:235–248 copies the native WITHDRAWN row for the extra manual amount. That row's kind/tax come from the retained applied revision's approved-line LEFT JOIN203–207. A genuinely sealed certified-zero revision is explicitly accepted by helper50–51 and can have no current approved line. The original paid line remains available through `v_manual.original_approved_line_id`, but the extra row then carries NULL kind/tax and line248's ELSE invents GROSS_DEDUCT. A real NON_TAXABLE original therefore gets the wrong displayed stage, and a genuinely unknown original is treated as gross. The inherited new approved manual fallback459 likewise maps original NULL tax to GROSS_DEDUCT.

Smallest correction in the still-uninstalled1849 Work-core successor: for the manual extra row, point-read the exact immutable original approved line when the retained member lacks kind/tax; use its genuine captured kind/tax for that informational row without attaching its line UUID to a different revision. Use explicit TAXABLE→GROSS_DEDUCT, NON_TAXABLE→NET_DEDUCT, otherwise NULL for manual informational stages (including the approved fallback). Preserve the native WITHDRAWN row unchanged, NULL historical tax, original source units and all zero financial contributions. This needs no money owner, producer, new reader or classification backfill.

## Source findings closed / retained behavior

- **S06 withdrawal retention:** helper1849:21–29 admits only prior known MANUAL/AUTOMATIC_COLLECTION_UNAVAILABLE, same observed sealed applied revision and approved Position, while Work current revision is NULL and approval WITHDRAWN. It cannot bootstrap an AUTO or unknown legacy observation. Current APPROVED Source authorization remains the complete52–72 branch; only the separately qualified retained observation skips that live requalification. Original typed line/source identity45–51 and channel41–43 still apply. No approved amount is replaced by zero.
- **BEFORE guard:** installed collection trigger1519:688–690 is BEFORE INSERT/UPDATE. Guard1822:718–724 calls the helper while its exact collection query sees the OLD known row; NEW manual reason/amount must equal the current native calculation. Immutable basis/case association565–578, genuine paid tuple594–637, typed current leased owner654–701 and exact Position observation702–706 are retained. CREATE OR REPLACE1849 changes the helper used by that existing guard; it does not exempt a money cause, lease, job or revision.
- **Actual balances:** native component/Case equality1849:33–39 and source/actual-target/segment conservation73–108 remain. The exact original1236 gross-balance2678–2688, debt-basis4076–4095 and restricted segment-collection-basis4102–4140 do not require Work APPROVED/current pointer, so an old-applied ordinary PAYE segment remains usable without replacing those helpers. The actual signed recovery effects, write-off shares and ACTIVE recovery/Work hold shares remain the native operands. Linked principal856–861 preserves actual R/W and native outstanding/hold floor; recovery/hold release does not freeze the former free amount.
- **Replay:** reconcile728–736 retains the exact same-job/observed revision/A/C/effect refusal and permits distinct actual WORK_COLLECTION_RECOVERED effects. No economic history is deleted or reinserted. Current instruction uniqueness0300:270, active-hold uniqueness440–441 and DR37 cancel inventory uniqueness1215migration:236 prevent a single current instruction from contributing multiple active hold releases to the same collection. The potential same-job free-amount concern was traced against those native constraints; no concrete duplicate active-release path was found. No replay weakening is recommended.
- **Withdrawal projection:** original native WITHDRAWN/WAIT row200–234 owns A/C/H and reauthorisation marker. The extra235–249 is existing AVAILABLE/INFORMATIONAL/ACTION, nonselectable/unselected, positive manual nominal and zero A/C/H/residual/due/splits/recovery/overhang/write-off/deduction contributions. It satisfies the existing AVAILABLE shape and avoids RECOVERY's required positive recovery contribution (1252:165,170). Exact component selection plus unique collection identity gives one unlinked row; linked Case rows do not receive a second Work money display. Existing group display1822:2036 includes only manual informational nominal.
- **Accepted/RESERVED and unavailable:** original Work-core accepted lookup, saved `v_base`, RESERVED row413–423, conservation425–436 and current positive-query/hold/snooze classifications remain. Manual recovery398–405 clears only its own accepted flags/amount and uses a zero linked-Case nominal, closing S07. Existing unbound Evidence442–446 remains unbound with explicit manual unavailable reason; no positive offer is certified. FE amount hiding is separate presentation evidence, not a database amount proof.
- **Prior S01/S02/S03/S04/S05/S07:** the installed1822 receipt records corrected native target-share columns, confined alias predicates, exact existing Expense-carrier admission/origin support, tax-qualified zero WAIT pair, retained strict Source READY authority, and isolated informational selection identity. Those claims retain their prior source evidence;1849 does not reopen their money paths. Genuine unbound representations/removal evidence remain BLOCKED.

## Exact successor and scope preservation

Whole-function line comparison of1849 against installed1822 found exactly three definitions: observation helper adds retained predicate/live-auth gate; reconcile changes only the AUTOMATIC/MANUAL mode expression815–816; Work core adds the withdrawal manual extra row. The rest of those complete bodies, signatures, INVOKER/DEFINER/STABLE properties and `search_path=pg_catalog,private` are retained. CREATE OR REPLACE preserves existing ownership; private/browser/service execution revokes904–906 are retained. No new public SECURITY DEFINER membership, table, public trigger, economic command or producer.

The registry points those three exact finals to1849; guard/origin/tax/expiry/release/discard/plan/Case projection/group display remain1822. Original FC13 future-channel decision/consumption fence is not broadened; no Source/Umbrella automatic owner or captured Case/Work repricing is promoted. BD149 expressly keeps D06 **BLOCKED** because locked1822's generated-name drops are not the card's audited-expression discovery. Root's catalogue proves the named expressions' observed correctness, not compliance with that literal discovery requirement. No immutable-file bypass is approved.

## Source-gate review

Current gate diff adds one exact helper signature; its explicit segment helper/sum/cardinality/lateral callees; exact helper edges from guard, reconcile and Work-core; existing refresh-many edge from reconcile; and two exact indexed-anchor statements at1476–1477. The anchor has Work/component equality, positive PAYROLL_SETTLED predicate and `(occurred_at_utc,id) LIMIT1`, supported by the migration1822 partial index. No wildcard routine, dynamic SQL/history waiver, relaxed financial alias rule or blanket query exemption was added. Existing native scoped segment sums are unchanged, and no constant physical history-scan cost or timing is claimed.

## Initial checkpoint byte hashes

| File | SHA256 |
|---|---|
| repeatable1849 | `DE67314666B76892EF6B4AC25554FA44E7F7A89A96068EABE6B7EA9A3B79A830` |
| repeatable1822 | `B9650E9DA89A3DCA3B59BC33F73EFC8ACF132038DAE163DF826D1F18730990FD` |
| migration1822 | `A7563BC0C03E983A0DE2B502946BEDFE1C874331805BD5E94F8AEBB872CBC00C` |
| BD149 FC-15.md | `6819DB6C7FC805B28D227EFCACB89595A788EF3AEA5BD8F8C25E679DD99EFFEC` |
| source gates | `6CD3EECD249D8E173ED3FF708149D2A0C6E88093E790EF18A15B4710137A758B` |
| final-definer registry | `44AEBEC364B3A5E840D2DA42BC710E4C6EDF47FEF32C442D3A34622768F647EC` |

The initial checkpoint was OPEN for F15-S08. Its final closure follows; D06 and genuinely unbound amount branches remain explicit completion limits. This receipt does not claim joined/runtime acceptance.

## Final1858 source closure

Root reports1849 had already compiled locally, so the display correction is a new dated successor; no installed repeatable is edited. Full Work-core function comparison against1849 finds only the expected immutable original-line kind/tax fallback134–136 and three explicit finite informational payroll-stage assignments137,299,349. TAXABLE maps to GROSS_DEDUCT; NON_TAXABLE to NET_DEDUCT; unknown remains NULL. The original approved-line UUID is not assigned to a different retained revision. Both WITHDRAWN native A/C/H/WAIT row and extra manual zero contributions remain unchanged. F15-S08 is closed.

Current registry has observation/reconcile1849 and Work core1858. Complete bodies/signatures/search-path/security and private execute revocation remain; all other owning1822 parents and future FC13 consumption boundary stay unchanged. No further essential source defect was found in this narrow final review. The helper/BEFORE OLD semantics, native R/W/H/segment calculations, replay and indexed-anchor scope conclusions above remain controlling.

| Current file | SHA256 |
|---|---|
| repeatable1858 | `455E61CB1DF71F04DD3394A4B03E3320F9CCDAECF1C4D32B096E6185A24D20FF` |
| repeatable1849 | `DE67314666B76892EF6B4AC25554FA44E7F7A89A96068EABE6B7EA9A3B79A830` |
| final-definer registry | `DBCDF1FCEB5AF79CCBE8EE88B634259973D2DBED7E0ACB4FD2900EBF1263DF16` |
| source gates | `6CD3EECD249D8E173ED3FF708149D2A0C6E88093E790EF18A15B4710137A758B` |
| BD149 FC-15.md | `6819DB6C7FC805B28D227EFCACB89595A788EF3AEA5BD8F8C25E679DD99EFFEC` |

**SOURCE PASS** is a source-only verdict. Literal D06 expression discovery and truly unbound representations remain explicitly BLOCKED; business/app/queue acceptance remains NOT_RUN. Reviewer performed no local compilation, catalogue or business call.
