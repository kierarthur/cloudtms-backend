# WB-34 screen source follow-up

13 October 2026. Independent read-only checkpoint; OPEN source findings. This reviews saved Office source and the original SCR29/SCR29b/SCR29c/SCR21b pictures before the new native captures are available. It does not approve A4 or claim application/business acceptance. Root owns product repairs, captures and BUILD_LOG changes.

## Concrete findings

**S34-01 — Retained choices disappear during an unknown result.** Rates `render()` returns from its pending-journal branch before the rate table, Reason and linked consent; Component Choice and Pay Channel likewise return before their original inputs. Check result/Try again therefore appears without the exact retained choice, including when reopening. Keep those controls bound to the original controller and show its original request values read-only, with unavailable human labels stated honestly. Do not reconstruct a new request or reread live money as the original choice. The supplied failed editor explicitly keeps the user's rates visible.

**S34-02 — Changed-evidence money and labels are unfinished.** `paintChangedEvidence()` renders raw rate codes such as DAY/NIGHT and bare Authorised rate/Charge rate decimal strings. Use the existing native human rate labels and exact presentation-only currency formatting, retaining all fractional precision and missing-value dashes. The renderer's Before/Now facts must stay native; this asks for no pricing or new reader fields. Check the comparison table's native frame, phone labels and money alignment in actual captures before A4.

**S34-03 — Refresh can be a dead action after a stale refusal.** Rates `reviewAgain()`, Component `refreshParent()` and Channel `reviewAgain()` return a cannot-check message when `stale` is true and `changedEvidence` is absent or UNAVAILABLE. A W1 preview can become stale before W2 dispatch, so the visible Refresh currently performs no read in that state. Preserve the typed choice, but make the explicit bounded read/requalification action real; otherwise present a truthful exit instead of a functioning Refresh promise. Never retry a financial mutation to obtain evidence or automatically adopt a fresh version for SAVE.

**S34-04 — A saved receipt can be described as a failed update.** Each SAVE path assigns its validated receipt before awaiting the parent adoption/handoff. If that follow-up throws an ordinary error rather than an error carrying `acceptedReceipt`, the generic copy says Could not update these rates/payments. Qualify copy from the already-known receipt and state that the choice was saved but the refreshed result cannot yet be checked. Keep the accepted request final and do not offer a replacement financial retry.

## Supplied design and control guidance

Original B2 pictures were viewed directly: SCR29 pending, SCR29b refused, SCR29c failed and SCR21b result. The pending picture shows the original parent navigation and factual Updating state; the refused/failed editors retain typed rates. Normal accepted SAVE must return to the same parent under the actual actor, namespace and lifetime guards, with truthful pending rows until coherent native completion. CLEAR continues its WB33 own-editor restoration. The illustrated three-of-four partial SAVE must be omitted because the native SAVE is atomic; record that deliberate difference in BUILD_LOG. Do not add confirmation or payment date controls, toast messages or picture-only financial values.

Check result reads the original receipt; Try again repeats original bytes only after the existing controller permits it. Review again performs explicit fresh evidence adoption while preserving the choice for staff review. Apply saves only the disclosed current offer. Keep editing/Discard protects unsaved input. Header Close returns focus to the still-current caller. Each visible control and every actual picture difference needs an explicit purpose/justification in the final screen receipt and root's BUILD_LOG.

Final A4 remains pending every current dark/device-light/phone capture and all 42 policy rows. Item 25 and the readerAdditions subpart of 26 are owner-exempt; native-value proof in the rest of 26 still applies. Item 16 uses actual `tools/policy.json.wording`, including no technical codes or internal identifiers. Preserve the owner's literal changed-list text, “The list has changed. Refresh and try again.”, despite the general one-sentence copy rule.

## Source checkpoint hashes

SHA-256 of reviewed saved FE bytes:

| File | SHA-256 |
| --- | --- |
| js/bpay-next-rates.js | B0249380B1E87705172204DFA082F077877A28F7EB59C36A6A1932D26F94F7AA |
| js/bpay-next-component-choice.js | D33C241116A2A30B015B7B72DBE8BA90EA2594641A55E7762C119583E11980EF |
| js/bpay-next-pay-channel.js | C483A5C36041A5FDA6A098011FD2500F2D96B01F3872972D8192CBB5194BC8B1 |
| js/bpay-next-clear-choice.js | 918E42A23501F3D45759C466A1ACC1DA29CA4085A4FB2ED83CA978EF09E4EC89 |
| js/bpay-next-resolution-adoption.js | 960D5304DB19B02ECEE3D63B86BDB56A96192EFDC7A026BCAB8D740EBF8ED1D9 |
| js/bpay-next-workbench.js | B08C09CA5195414E8690940BF2F07B0C18B86D4A6FF771DF6558D188E368D2F5 |
| js/bpay-next-office.js | 0EA800B1167BF1CB254A001B38A0D99E5B12750575103B6C2507FA85F52B5B8B |
| css/bpay-next-office.css | A1D16E05192FCEF7380A5746621FA0DBD97A66AF567EDFBDBF54580E16FFEB76 |
| index.html | 5B2092D7BF93E3B5F857D681F8837B36E86A5D71C0B4FBB55A4C5E37E38A6129 |

Supplied PNG SHA-256: SCR29 D967B9F4A3ECC3D0903B214F11D6A3FF3A9BA69ACE1E571D62F93F2FE512E19F; SCR29b 902D7B4A937E0FD954D60DB924E22EA8012E8F1263D95E51D8198548F9008D28; SCR29c 1D68EB93697F8D1BE9BF5FF7C6B8CC083D0A56500F4E9D171B802016816C4A5F; SCR21b BA504BFEB2326001FD77D8BEFDA317794713BCB5D12181F3281FB16EFF8F840E.

No product, design-pack, checklist or screenshot changes; no SQL/RPC, business data, rendering, runtime tests or network calls were performed.

## Concurrent saved-source amendment

The next read found root's pending-choice paragraphs and comparison formatting already saved. Rates, Component and Channel now display values from `controller.originalRequest()` plus the retained native source, and the original Reason, before Check result/Try again. The missing-value portion of S34-01 is closed; group the retained rows by their known Timesheet/Work human labels so repeated linked Day choices remain identifiable. No new request is constructed by this presentation. S34-02's raw rate-code/bare-money defects are closed in source: the comparison uses human labels, £ and at least two pence places while retaining extra native fractional precision. Actual table/phone presentation remains for A4.

S34-03 and S34-04 remain OPEN at this read. `adoption.accepted()` can still throw an ordinary Parent unavailable/recovery error after the editor sets its known result, independently of the controller's earlier `onResolutionReceived` wrapper. The accepted-result copy finding is therefore a concrete second-stage handoff path, not a claim that the original receipt callback lacks its acceptedReceipt guard.

Fresh SHA-256: Rates 9C08DB29D55DDFF7F4A70A412AD2C9F7E617E56635DDB25FF9F470101103B99B; Component AD5EEF8083CF6753B4A4ED0B604F38C0C4ED4CFF22906E185E273433A019FBB5; Channel 1D378EA5A80CC582BE8457C06EF400CCAC8A239B6647A9ED9DBCFD7C7C27BE76; Clear 73C9ABF157CF1347FBE749114A5AC4342DD0531E6366672BB6840A4B550B8F9B; Adoption 9518FDB9821B6C1CC4980FDCC0C30DBF3C464B8F26AAC36D1D11CDCA07D61E87; CSS remains A1D16E05192FCEF7380A5746621FA0DBD97A66AF567EDFBDBF54580E16FFEB76. These replace the earlier hashes for this amendment only; root may continue concurrent repairs.

## Independent final source delta review

The latest root-requested read closes **S34-01–04 in source**. The pending display groups the original retained rows by the actual typed Timesheet week where known and displays the original choice/Reason; it does not replace journal bytes or read live values as the original choice. Check result remains original-controller readback and Try again remains controller-qualified exact retry. Those controls moved into the native footer; each render removes its prior recovery controls, preventing accumulation.

The shared comparison now uses human labels and exact £ formatting. Rates/Component/Channel explicit Refresh really invokes the existing bounded native reader at the original address when CURRENT changed evidence is unavailable, after preserving input with a null guard. CURRENT evidence is explicitly adopted through its mapped immutable identities; successful reads restore retained input by native component identity. No financial request is dispatched by Refresh, and failed reads keep native refusal/error qualification.

The editors' plain error functions now treat an already-known `result` as authoritative even if a later handoff error lacks `acceptedReceipt`; the shared adoption callback also attaches the receipt to a callback error. Known accepted fallback says the choice/rates were saved and offers real Check progress, which reuses the accepted receipt and existing read-only continuation. It does not execute or retry SAVE. Normal successful handoff still closes the child and preserves the caller/actor/owner fences.

**S34-05 — OPEN, narrow remaining copy defect.** Component `load()` qualifies a successful fresh native response and sets `stale=false`, but does not clear the previous `error`. Rates and Channel already clear it for their fresh reads. Following a stale refusal and Review again, Component can therefore retain the changed-list error beside a newly enabled Apply. Clear that old error after the current successful read is qualified; a failed read must still show its actual refusal. This is display-state repair only.

Current SHA-256: Rates 7F8217F6DDFEE1C0D213E579C7A3A6720774AC0EED71EAB910B8F6718BAA982F; Component 09EB6F940E5E88460F0F4B6DA33D3182B72E6685676EBCF60F63FC0D5106C006; Channel 3FA33A123438FE82DA0F7CA782A2500652717E8FEDDFC7B103EED01691ED604D; Clear C78B7A5CC83F05178EF44B1E698F9E88D7AC4FEDF2B73C066F1C1FE5D0FFE60D; Adoption 3EEA662002EF60163A267EF2914E3115259F2EB15A4CF62966092387A2972297; Workbench 476588018C1410523288A5FA80D3D177A6DC9715B8A0B777EE01FC5069A278B9; Office 3808B4B9EE2778C54B3179217BEFC42D6973EDAB963D42A5D460F44A25C70323; CSS A1D16E05192FCEF7380A5746621FA0DBD97A66AF567EDFBDBF54580E16FFEB76; index 5B2092D7BF93E3B5F857D681F8837B36E86A5D71C0B4FBB55A4C5E37E38A6129.

This amendment is source-only. **A4 remains NOT REVIEWED against the new captures**; no pixel, phone/device-light, business-flow or performance acceptance is inferred from these code checks.

## Latest A4 support source closure

**S34-05 CLOSED, scoped SOURCE PASS for S34-01–05.** Component clears `error` only after its fresh current native read is qualified, including the restored Ready path. A failed read retains actual refusal handling. The three editors keep the original address and input on explicit Refresh when no current mapping exists; no new SAVE is issued by the read action.

Root's recovery heading addition is a bounded presentation field in the existing cache. Its exact four keys are Candidate display name/reference, Client display name and week ending; nullable text is UTF-8 bounded and week must equal the original root Work's typed context. It is obtained only from the already validated native reader header, is text-rendered, and does not change the original request or receipt authority. The three editors restore that heading, original Timesheet week and chosen values from retained original body/source for accepted fallback. The narrow pending badge CSS uses scoped nowrap/no-shrink; To apply is now neutral in both initial render and input-change render.

Source SHA-256 at this read: Rates 0B8821D5621F8010AAD5786CB69DD3E615E056CEF4537F6AC1DDBD190F70FA0B; Component 05FBF23BC23CCD09F413939933047B7BBF750D758BF66CA1A0E05D662A730B4F; Channel 6E5A50945CB82D21B649ECC5D34021B8322F8DA2FD0B2A746E56DD6055F56D81; Clear C78B7A5CC83F05178EF44B1E698F9E88D7AC4FEDF2B73C066F1C1FE5D0FFE60D; Adoption 44E56DE766C0221039E80004BFFF56CFE1F75E7B90F8DC95CB2C0ECB17FBD86B; Workbench 71B8A0063A8F38511CCA6F650769F01E925342C8D09CCF3FC51A591EC3BCAAE6; Office 3808B4B9EE2778C54B3179217BEFC42D6973EDAB963D42A5D460F44A25C70323; CSS 994AE663B512B9DCA487341D2EB0A67B6D60B5C9B14EA2A21CF1DE819D808DAA; index EDDA7E76704446D9A2A7DDB0D5518D8CD0FF2011DAB5F996B507045CC91E5DED.

The initial 43 A4 images were independently viewed and yielded P34-01–03; root is recapturing these repairs. **SOURCE PASS does not approve A4**. The final pixel receipt in SCR-29/review.md must independently close every visual failure against current PNG bytes.

**S34-06 OPEN — recovery heading field bound:** the new `record.header` text limit is 4096 UTF-8 bytes, but the existing native header validators in `js/bpay-next-resolution.js` allow 8192 for Candidate name/reference and Client name. Legal native labels of 4097–8192 bytes would fail local prepare/persist before SAVE. Use the original 8192 limit while retaining the 524288 whole-cache cap. This is one bound alignment within the same presentation cache; no request/receipt/reader change is required. Root was notified immediately. S34-01–05 remain closed; complete current source approval requires S34-06 closure.
