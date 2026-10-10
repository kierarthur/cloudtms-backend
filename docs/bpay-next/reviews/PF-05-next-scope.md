# Sequence 20: PF-05 exact source scope inventory

Read-only inventory, 10 October 2026. No product edit, source-gate/test execution, SQL/runtime, fixture, hosted call or application acceptance. This is the next-item inventory requested after the bounded F-23 case/workflow closure; it does not build or complete PF-05.

The exact next item is **PF-05 — No admission tariffs, processing budgets or fee graphs**, sequence20 in `IMPLEMENTATION_CHECKLIST.md:491–509` and the normal plan `08_BUILD_ORDER_AND_CODEX_INSTRUCTION.md:104`. Full controlling card read: `06_CONTRACT_MOVES_DAILY_PAYMENT_SUMMARY_PERFORMANCE.md:1764–1778`. Dependency F-09; reviewer R6; design note not required. Nothing visible is added to the user flow.

| Exact checklist object | Required bounded delivery | Current inventory conclusion |
| --- | --- | --- |
| PF-05-D01: proposed forbidden tariff/per-step pricing source check | Targeted semantic DB/Worker source check rejects NEXT admission allowances/tariffs, processing budgets/per-step pricing RPCs and abandoned G01/C1 architecture. Its normal source test lists the rejected vocabulary and must pass on the reviewed build commit. | F-23 now has a named finite source-gate module/test and approved final-definer inventory; those are reusable locations/discovery rather than a reason to introduce another harness. No separate PF-05 function/semantic check was found in the current gate read. Actual implementation remains builder work. |
| PF-05-D02: future joined in-app TEST checklist | No admission budget/fee graph, no NEXT pre-pricing call; legitimate bounds and existing money helpers remain permitted. Reuse an actual joined receipt across explicit clauses later. | Full Worker/queue/minute-tick/application acceptance remains NOT_RUN. No local business scenario, synthetic pricing/financial fixture or new proof programme is required now. |

Card rule: no request needs a theoretical R/C/Q/W/B/E (or renamed) admission allowance, another round-trip to price a processing step, or an ancestor graph deciding who pays for processing. Failure is measured timeout/stall/wrong money. Therefore a raw word ban on `budget`, `price`, `C1` or financial rates would not meet this card.

Keep the semantic boundary precise:

- Admission/pricing requests and step-fee authority are forbidden; ordinary page/row/byte/lease/time bounds, existing named money valuers/rates and operational retry limits are permitted. Retained old `budgetProfile` scheduling identifiers are bounded scheduling source, not automatically an admission tariff.
- Genuine Weekly Source C1 publication/history/replay semantics remain required. `C1` alone is not the abandoned architecture proof and must not trip this gate. No Source or money-owner rewrite is implied by PF-05.
- Discover actual NEXT public route/handler and adapter calls in `broker/src/index.js`, its existing `bpay-next-*` modules and their designated final SQL closures. Use actual called RPC/relation/argument semantics and the F-04 typed identity inventory; do not infer a new pricing service, graph or RPC name from prose.
- The current financial transport has fixed intake/original-credit endpoints (`bpay-next-command-rpc.js:7–31`), and preparation has fixed intake/page endpoints (`bpay-next-preparation-rpc.js:3–16`). A limited literal census of current `bpay-next-*` modules found no tariff/pre-pricing/G01/processing-fee terms. This is useful source inventory only; it is not a complete semantic call-path proof or a passed gate.
- Reuse existing normal source discovery and finite source-unit assertions when implementing the one named check. Include actual source-generated SQL/Worker callers where they can introduce an admission pricing call, without scanning documentation/negative examples as product behavior. No broad framework, build-time external request or scenario test is justified.

Both PF-05 check rows remain unbuilt/unreviewed/uninstalled/app-not-tested in this inventory; no checklist changes by reviewer. Current owner Rule B remains the accepted F-25 scope; this performance card does not reopen the parked producer/resolver protocol.
