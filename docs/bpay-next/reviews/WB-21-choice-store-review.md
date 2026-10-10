# WB-21 first choice-store source review

10 October 2026. Reviewed the complete new `createChoiceStore` block (`FE js/bpay-next-workbench.js:676–710`) and existing `shape`/`field` helpers, native segment-key input validator and current Ready store. Reviewed SHA-256: **`63e8b6f49f84ffba3ba19225e4d5c2aa19dd4f7ddbe8be6f0080b09050cc4e35`**. Read-only source review; no product edits, imports, tests, runtime, SQL, application/data/network/hosted/fixture operations. First store slice only; Ready/submit/persistence UI joins are pending. Verdict: **findings open; not yet SOURCE PASS**.

## Concrete findings

### CS01 — same-namespace handles lose each other's updates and generation

`createChoiceStore:691,698–700` obtains a state object once, then each handle replaces its own closure and the Map entry on save. An already-created second handle continues using the older object. Source sequence: create A and B for the same context; A excludes Candidate C (revision1); B chooses Work W from its retained revision0. B's save writes revision1 without C's exclusion. A still sees its own older state and `acknowledge(1)` can clear dirty state despite a different generation from B.

Use one live canonical state per namespace (or a shared singleton handle/entry with subscribers), and have every read/change/acknowledge operate on that live state. Preserve monotonically distinct generations across all handles. This matters for the actual shared Ready/child-window store and same-namespace reopen.

### CS02 — `freeze()` returns a mutable submitted descriptor

`:707` clones but freezes only the outer object. `offer.intent.candidates[0].selected`, its arrays, Work rows and `offer.namespace.scope` remain mutable. Detaching the snapshot protects the store, but does not make the submitted offer immutable. A later callback can alter it between request/receipt stages.

Recursively freeze the finite cloned JSON tree returned for submit, or retain private immutable canonical bytes and expose detached data that the sequencer cannot use as mutable authority. The first option is a small supporting helper over this bounded tree. Keep raw choice state separate from accepted request bytes.

### CS03 — explicit Candidate exclusion does not yet dominate frozen child choices

`:704–707` permits explicit Candidate `selected:false` alongside selected Work/component/segment/Case entries and copies all entries unchanged into the offer. `candidateSelected()` does not qualify child entries. Keeping remembered subordinate choices locally can be useful when reselecting a Candidate, but the submitted effective descriptor needs an explicit deterministic Candidate-exclusion precedence.

Filter or qualify selected subordinate choices in the frozen effective descriptor, or establish that exact precedence in the genuine sequencer/native descriptor contract before calling the store slice complete. Do not treat a NONE-mode default as an explicit Candidate exclusion: NONE still permits explicit Work choices. No money or new server protocol is required to resolve the finite local precedence.

### CS04 — missing storage initially reports persistence available

`:691–693` starts `persistent=true`; when no storage/window is supplied, `storage=null` leaves that flag true until the first save (`:698`). Thus `persistenceAvailable()` claims availability for a newly opened memory-only store. Set the initial flag accurately from the actual storage surface and retain the existing failure-to-memory fallback. A failed write must continue to report false. This does not authorise dispatch of an accepted command without its original journal safety.

### CS05 — segment-key validation differs from the genuine native input

`:688` accepts any nonempty string of at most245 UTF-8 bytes, including NUL and leading/trailing whitespace. The current genuine `broker/src/bpay-next-preparation.js:206` requires no NUL, exact trimmed value and 1–245 code points (with request byte bounds separately). The store can therefore freeze keys that its existing SEGMENT_PAGE caller refuses.

Use the same finite native key predicate, retaining the total32KiB store bound. Do not invent a new segment-key syntax or derive keys from labels.

## Confirmed sound scope and pending support

The boolean helper is real (`field:396`), and Work ALL/SUBSET mode is correctly validated explicitly instead of calling a nonexistent field mode. UUIDs use the existing canonical lowercase-only validator. Namespace derives real validated agency/epoch/actor/date plus normalized Candidate/Client/channel scope, and omits presentation sort/search. Exact row key allowlists reject money/prose extras. Per-array≤1000 and whole-record≤32KiB bounds are finite, IDs are de-duplicated, next local changes increment revision, and acknowledgment preserves later edits for a single live handle. Candidate count conversion deliberately maps ALL_MATCHING to the existing ALL counter contract.

The module-wide `choiceMemory` Map has no total namespace cap or lifetime eviction. Each entry is bounded, but repeated filters/dates/accounts can accumulate entries. A finite cache policy is a required boundedness consideration at the real persistence/lifetime join: do not evict dirty memory-only intent or uncertain journals silently. This first-slice review does not require an agency-wide persistence programme.

Current `createChoiceStore` is not yet exported/used by Ready. That is a stated pending join, not an additional first-slice defect. No whole WB-21 implementation, native accepted-boundary completion or runtime acceptance is asserted. Rule B remains unchanged; parked F25 producers/resolver/protocol remain parked.

## Fresh store fixes and Ready join

Re-read SHA **`b8416f67f6fccf9e973152748b11cccbc3734bfd5ba50336553a72829c95ff8f`**. CS01 is closed: save now mutates the one shared Map state with `Object.assign`, so existing handles observe newer generations and acknowledgment does not clear a later revision. CS02 is closed: a finite recursive helper freezes cloned namespace/snapshot/submit trees, not merely the outer wrapper.

CS03 is partly closed: freeze filters subordinate rows belonging to explicit false Candidate entries while preserving raw remembered intent. **One remaining concrete NONE-mode branch:** setMode(NONE), choose a selected Work for Candidate C, then chooseCandidate(C,false). Because false equals the mode default, `chooseCandidate` removes that override instead of retaining an explicit exclusion; freeze consequently keeps the selected Work. An explicit Candidate untick must dominate it. Preserve a false marker or equivalent qualified suppression for that explicit action, while allowing selected Works for Candidates that are only default-false in NONE mode. CS04 and CS05 remain unchanged in this fresh source.

Ready now creates/restores the store from validated coherent page/header context (`:758–761`), keeps it through sort/refresh, and obtains a different keyed store after actual scope/date/epoch changes. Scope switches and close no longer destroy prior namespace choices. Selected state/count intent use the canonical store; count requests capture both handle and revision, reject a scope-handle replacement or later edit, and retain exact server counts. The store is now exported, and the actual store accessor/onChoiceStore seam exists. Native debt-only eligibility, bounded read blocks and actor/NEXT barriers are preserved. This closes the earlier pending export/Ready attachment observation. No per-tick network or monetary calculation was introduced.

Verdict for this fresh first slice: **CS03 NONE-mode, CS04 and CS05 open; no whole-item or runtime PASS asserted.** Parent/child listener fanout and finite Map lifetime remain actual subsequent UI-lifetime considerations, not a new server programme.

## Final fresh store and Ready source verdict

Re-read complete store and Ready join at SHA-256 **`047e6b7e16b1e4d0545ce7563fc4c24fa0927b10d1239b699aa81266fdd9ff46`**. **CS01–CS05 are closed.** NONE mode now retains an explicitly false Candidate row (`:705`), so the frozen effective descriptor filters that Candidate's Work/component/segment/Case rows while preserving raw remembered choices. A default-false Candidate with no explicit false marker can still have deliberately selected subordinate choices. Missing storage immediately reports false (`:693`); read/write failures preserve the bounded live in-memory state. Segment keys now use the existing native no-NUL, exact-trimmed, 1–245-code-point predicate (`:689`) alongside the independent 32KiB record limit.

The finite lifetime concern was reported as **CS06** and is also closed in this fresh source: a new namespace is refused once the Map has256 entries (`:694`), using the existing local-too-large refusal. Existing namespaces remain available at that bound. No entry is evicted, so active handles and dirty memory-only intent cannot split into two canonical states or disappear. Per-record32KiB and total256-entry bounds are explicit; no cache or recovery framework is required.

Namespace remains derived only from the validated agency, actor, owner epoch, official pay date and Candidate/Client/channel scope. Reauthorization/presentation revisions are captured in the immutable submit offer rather than allocating new stores. Ready restores that keyed live store after coherent reads, keeps it across sort/refresh/close, switches it only for a different namespace, and checks both captured handle and generation before accepting selection counts (`:761,770–772`). Shared state mutation, recursive immutable clones, exact typed row allowlists and generation-safe acknowledgment remain intact. No per-checkbox network call, money calculation, financial-authority change or accepted-journal deletion was added.

**Verdict: SOURCE PASS for the choice-store and current Ready attachment slice, with no open concrete finding.** This does not mark whole WB-21, later child selection/sequencer joins, native acceptance or runtime behavior complete. Source inspection only; no product edits, tests, imports, runtime, SQL, business/data/network or hosted operations.

## Fresh closed-intent and Close re-review — 10 October 2026

Workbench SHA-256 `b5ce284a12e1d923a7fbd775c01f1010c12600357e48bea35afc95950a98df1a`; Office `ea7737dffe26e74d4e064bb1a147be0f8e8459c392d43435423968f53857b646`; UI `34e3286cc697c150542bb7f2ab61b5d6033e2cdfdc36272c3620d9b445a5dfd1`; main `e66e48eed68cd2c8db817bb0838df0cc126bedc8f147a50f952d93bf665acf1b`.

The store now has exactly `mode,candidates,works,components,cases`; the unassigned `segments` collection and `chooseSegment` API are absent (`Workbench:685–708`). This is a narrow removal, not a substitute implementation of the later segment selector. The store, Office descriptor and Worker descriptor have matching native fields and ALL/SUBSET vocabulary. Explicit false Candidate dominance still removes the Candidate's subordinate Work/component/Case rows only from the frozen effective offer; raw remembered choices remain retained. CS01–CS06 remain closed: shared live handles, recursive immutable snapshots, NONE dominance, honest persistence failure, native typed identity and finite 256-namespace/no-eviction lifetime are preserved.

Re-read the actual Ready join and count-generation checks and the exact Office Close blocks (`:580–599`). Same scoped intent survives coherent reread/sort/Close. The unchanged UI/main hashes bind the already reviewed actual frame/prefix/token/Escape/Keep/Discard behavior. The Office changes are typed transport additions; safe local dismissal after lost authority, generation-qualified local discard, actual onDismiss-only cleanup, actor recheck after successful record handoff, and untouched accepted/unknown journal retention remain intact. The existing SCR17 visual review continues to cover the unchanged confirmation presentation; no new capture or visual execution was performed by this reviewer.

**Verdict: SOURCE PASS for this fresh store/Ready/Close slice, with no open concrete finding.** The descriptor/receipt/page mirrors are separately reviewed in `WB-21-submit-transport-review.md`. The whole submit sequencer, later owned joins, native business acceptance and whole WB21 remain outside this verdict. No tests, runtime, imports, SQL, hosted/data/network operations or product edits were performed.
