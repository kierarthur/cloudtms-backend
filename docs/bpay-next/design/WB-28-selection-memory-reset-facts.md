# WB-28 selection-memory reset: exact current callers and minimal support

13 October2026. Read-only focused census requested by root. Current saved registry is authoritative. No product edits or business/SQL/RPC/runtime/tests/network. Design facts only; actual implementation remains root-owned.

## Every current direct resolve call

Static census of current finalPhysical function definitions found exactly four caller bodies (historical superseded copies excluded; two Create overloads remain genuinely current):

| Current function signature | Current source / direct call | Reference passed and purpose |
| --- | --- | --- |
| public.bpay_next_workbench_page_v1(jsonb) |13102026_0117_banking_pay_next_chosen_display_read_barrier_v1.sql194 |v_ns from actual agency/NEXT epoch/actor/current official date/normalized filter191–193; p_request.selection_memory. Omitted/JSON null chooses latest implicit namespace; explicit reference is retained. Returns selection_memory actual run_id and original ref scope195,233. All default-enriched HEADER/CANDIDATES/GROUP/estimate kinds share this gateway.|
| private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid) |11102026_1358_banking_pay_next_guarded_create_v1.sql361 |Explicit {run_id:descriptor.memory_baseline_run_id,scope:descriptor.namespace.scope}. The original descriptor/body/cut is authority; no request for latest implicit memory.|
| private.bpay_next_create_preparation_v1(uuid,uuid,date,jsonb,uuid,jsonb) |13102026_0016_banking_pay_next_accepted_eligible_leaf_v1.sql407 |Same explicit descriptor reference, under original held/eligible cut. Capture405 creates retained HEADER/LEAF evidence before default-choice materialization408. Original request replay/current admission order is retained.|
| private.bpay_next_paye_guard_page_v1(uuid,uuid,jsonb) |11102026_1358_banking_pay_next_paye_same_week_guard_v1.sql276 |Unaccepted descriptor branch constructs explicit reference275. Already accepted branch257–262 uses accepted_choice_descriptor and captured selection_candidate counts; never resolves latest or rereads live defaults.|

Retained three-arg Create(uuid,uuid,date) is1621 and has no direct memory resolve. Accepted choice guard and native queued/PIN/capture consumers are not extra implicit default resolve callers. Do not infer current ownership from all rg hits in old repeatables.

## Resolver semantics and downstream default owners

Current private.bpay_next_selection_memory_resolve_v1(jsonb,jsonb),2306:46–81:

- p_namespace contains agency_id, owner_epoch, actor_user_id, official_pay_date and scope {candidate_id,client_id,channel}. The native caller, not browser money, supplies authority.
- SQL NULL/JSON null p_reference means latest acknowledged HEADER in the exact namespace, ordered by original accepted agency_sequence DESC,run_id71–76.
- An explicit reference has exactly run_id/scope. Client/channel must equal requested namespace. Its Candidate may be NULL (a broader original baseline narrowed by a Candidate child) or must equal the requested Candidate62–65.
- Explicit run_id NULL returns NULL immediately66, meaning detached fresh defaults; never lookup latest.
- Explicit nonnull run must be acknowledged and exactly match the reference's original agency/epoch/actor/date/scope. Missing refuses78. This original reference is retained authority; a later reset must not delete or reinterpret it.

Current private.bpay_next_selection_memory_choices_v1(jsonb,uuid,jsonb)2306:83–131 consumes a supplied baseline, not latest resolve. It compares native LEAF basis, applies unchanged/new/changed semantics, inherited HEADER ALL_MATCHING/NONE and exact four-kind exclusions97–116, then current local overlay precedence124–130. Candidate false dominates. Current private.bpay_next_selection_memory_row_v1(text,jsonb,jsonb,uuid),0135:5–83, calls choices25 and publishes native default/changed/basis/count facts for Candidate/Work/component/Case rows. Current private.bpay_next_workbench_kind_page_v1(text,jsonb,jsonb,integer),0132, uses the gateway's _selection_memory/_count_selection. These indirect default consumers must stay consistent if any partial-scope reset overlay is necessary; changing latest resolver alone cannot selectively change a leaf inside one retained run.

Current selection_memory_capture_v1(uuid,jsonb),0016:54ff, saves exact original descriptor HEADER59–60 and eligible immutable leaves62–74, inherited and explicit intent under original accepted cut. choice_leaf_eligible_v1 at4–50 later reads those exact retained run leaves, including quantity/member dispositions; never current latest defaults. Native guard2306:1815 rejects DELETE/TRUNCATE; OPEN/PREPARING header command/ack transition alone is mutable1817–1833. Seal acknowledgment2306:708–709 retains original accepted agency_sequence rather than using wall-clock seal order. Hence reset cannot delete old records or make a late old seal restore earlier defaults.

## Smallest whole-namespace reset marker

Where the actual reset scope is one complete original memory namespace, a marker in the root's new immutable clear-operation header is sufficient; no separate money/default cache or fabricated Draft:

1. Record original agency/owner_epoch/actor/official-date/normalized **memory scope**, exact clear operation request and its original accepted sequence. Make the marker effective only at the genuine accepted reset outcome (including an empty clear closure if reset is still accepted). Marker existence before confirmation is not a reset.
2. For implicit/latest resolve only, seek the exact matching actor/date/scope effective reset marker via an index and compare to the acknowledged HEADER's native original agency_sequence. A baseline accepted at/before that reset is obsolete as future default; return NULL unless a genuinely newer accepted HEADER exists. An older Create acknowledged/sealed later still has its old sequence and cannot undo the reset. A later accepted Create must supersede normally.
3. Preserve explicit old nonnull run references and explicit detached-null behavior. No reset in queued accepted descriptors/PIN/capture/retained leaf eligibility, no rewrite of Create command/body or original accepted offer.
4. The marker/receipt and its sequence publish atomically with the real reset outcome, replay is a no-op and another actor/date/epoch/scope cannot match. Own ordered sequence is not a new clock or timestamp heuristic.
5. Existing public W1 context changes through the actual native reset result/appropriate original revision seam, so old cursors and clean adoption cannot silently mix before/after defaults. A zero-owner operation still needs a real reset outcome/currentness publication; do not rely on a Work publication which never happened.

Native agency sequence comparison needs the actual root operation's recorded accepted authority, not arbitrary last-page pubtime or a SELECT max over financial history. The exact new operation header schema/index/admission is root's design; no such marker exists in current2306.

## Candidate-null inheritance is a real scope limit

A Candidate-specific child can use explicit reference {run_id:R,scope:{candidate_id:NULL,client_id:C,channel:P}} while its current requested Candidate is A. The scalar R represents all original baseline members in that broader scope. Resetting R wholesale also changes defaults for B; retaining R unchanged keeps A's inherited exclusions. A marker keyed merely to new narrow A does not affect implicit broader R and cannot implement a selective filtered reset inside it.

Choose and disclose the actual **memory namespace reset** separately from financial clear closure if resetting that complete original broader namespace is intended. If card/current-filter consequences require A-only reset while B remains, do not claim resolver-only scalar logic solves it. The smallest additional native support would be a reset-scope qualification in existing default choices/row logic, based on the original actor/date/reset sequence plus native leaf Candidate/Client/channel membership; apply only to future implicit default semantics, not old captured/explicit financial authority. It must not use a new all-Work browser vector, global digest, producer rewrite or changed immutable baseline. Root must settle that exact filtered-reset interpretation before product code. This is an actual representational limit, not permission to silently expand reset consequences.

## Exact Office adoption and later edits

Current FE js/bpay-next-workbench.js has the needed fences and a narrow support seam:

- createChoiceStore998–1065 is agency/epoch/actor/official-date/scope keyed; current shared state/storage v3 is bounded32768. memory and remembered references are distinct from four-kind local intent, observed bases and dirty revision.
- Ready refresh read1120–1122 captures previousStore/revision. A clean fresh page has reference NULL (or exact continuation context), so it already asks implicit latest. CANDIDATES and HEADER must agree1139–1140; local revision is rechecked1127/1137 before adoption.
- adopt1035 updates only !dirty store: it can receive run_id NULL and replace memory/remembered/mode with fresh defaults without history deletion. No new generic sync framework is needed for clean reset adoption.
- Dirty stores deliberately retain their explicit original reference1121; restored dirty reference mismatch reissues native reads for that old reference1131–1137. adopt cannot erase them. Counter/observed identity qualification and unresolved native basis remain original.
- For accepted Clear reset, capture namespace/local revision at original confirmation. If unchanged when the exact reset receipt returns, a small store reset method can detach inherited memory, reset only the relevant four local arrays/mode/remembered baseline, increment local revision and invalidate estimates/counters; no server call to reset local selection. setMode1060 demonstrates detached-null mechanics, but dirty=false/remembered reset must be deliberate so a later discard cannot reattach the obsolete baseline.
- If later staff edits changed the local revision, do not replace those overlays. Retain their original observed bases and local intent; qualify them through current native reads and existing unresolved mechanism. Detach/refresh obsolete inherited defaults only with an explicit method preserving these later edits, or leave a visible reconciliation state requiring staff action. Do not automatically reseal/reprice/retry an accepted request.
- A clean Candidate child must receive the refreshed exact parent memory reference; its current explicit old reference intentionally remains stable until normal parent adoption/refresh. No late result may change a retired parent or other actor's namespace.
- Original pending Create/Resolution journal bytes are independent of local store state and must never be rewritten by reset/adoption. Current accepted descriptors continue to use original explicit run refs.

Typing/reset itself remains local. No automatic count/estimate/financial call is needed just to render the resulting ticks. Subsequent explicit normal refresh/Create uses the existing gateway and native default machinery.

## Saved source hashes

| Current file | SHA-256 |
| --- | --- |
|0016 accepted_eligible_leaf_v1.sql|35648FDE73F982466ED72C065EB9B6916D6EF998364BD64AF0F491625A73CFD3|
|1358 guarded_create_v1.sql|6D13CD0A482BE586E49AAEB2FB67BCA82613AA092657CBCC208E6F012ACBC0A6|
|1358 paye_same_week_guard_v1.sql|5982FEB781685D84929F15403DCACE89E7AD02BC22131FC96D823615986A2A23|
|0117 chosen_display_read_barrier_v1.sql|E924CB0C55D2AF7C85F9C37C0B6164DEC2EA8532F319EAD1F49B38F0C7E2EE5B|
|0135 selection_complete_counts_v1.sql|5F8D2E1CC3AD80460C840AAC028CB76A499F3DE2A7496B721BD8C7369ED9BA85|
|2306 actor_selection_memory_v1.sql|3F77BBB57500F1320D3873935C8504264A46E50287BE9740267F838F8D1DC750|
|FE js/bpay-next-workbench.js|C19E85817F57AEC3AEE0BCF2F9985DF32E9D7CCD7C7D42A6C6CE772551711F57|

Current full card/assigned deliverables/other sources are recorded in WB-28-current-investigation.md. This focused file does not author a reset implementation or claim runtime/whole-card completion.

