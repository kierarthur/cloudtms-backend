# WB-22 local intent and acknowledged defaults reconciliation

10 October 2026. Read-only design investigation for sequence69. Full WB22 card (`02_WORKBENCH.md`:857–894), all current checklist rows, `WB-22-persistence-investigation.md`, current choice store, selection journal and strict W1 mirrors read. This is design evidence, not implementation, installation or application acceptance. No product, SQL, tests, gates, modules, browser or business calls were executed or edited.

## Required current scope and actual seams

WB22 owns three deliverables: typed `private.bpay_next_selection_exclusion_memory`, actor/date/scope `private.bpay_next_selection_mode_memory`, and `js/bpay-next-workbench.js` default/untick reconciliation. The public preparation intake is the existing contextual entry, not a new route. HEADER/CANDIDATE_GROUPS must expose the needed acknowledged defaults through existing W1; ordinary service-only CREATE and the named seal merge persist the already accepted boundary. WB23 upload, RuleB selection snapshots/PIN and all frozen Source producers remain intact. Later WB24/25/26/28 controls remain actual ordered joins, not reasons to omit four-kind memory now.

| Saved source seam | Current behavior and required bounded addition |
| --- | --- |
| FE Workbench906–938 `createChoiceStore` | Local state has exact namespace, four bounded intent arrays, local revision and dirty boolean. It has no acknowledged baseline identity or financial basis per override. Add those reconciliation facts without replacing shared state-object identity or evicting another namespace. |
| FE Workbench909–910 | Namespace is agency/owner epoch/actor/pay date plus Candidate/Client/channel scope. Sorting, search and displayed section do not create separate choice namespaces. Use this same exact namespace for server memory, local memory and submission. |
| FE Workbench930–936 | `countIntent`, `candidateSelected`, Candidate inclusion and freeze currently use only local mode/boolean overrides. Centralize the inherited server default and local overlay decision here so ticks, counts and frozen submission agree. |
| FE Workbench993–1006 | Ready reads CANDIDATES then HEADER, checks one coherent revision, restores only when namespace key differs, and fences the local revision across count reads. Add acknowledged-baseline agreement to that same adoption fence; a response must not replace choices changed while awaiting it. |
| FE Workbench1246–1277 | Candidate child uses the existing parent choice store; it must use the same baseline and reconciliation state, rather than create a Candidate-scoped replacement store from its narrower reader scope. |
| FE Workbench13–44,348–402,430,525–567 and BE Workbench matching schemas | HEADER/CANDIDATES/CANDIDATE_GROUPS, CONTEXT and countSelection are closed schemas. They have no remembered/default/basis fields. Update the finite corresponding declarations and invariants together; unknown extra fields must still refuse. |
| FE Office493–700 `createChoiceSubmission` | Restored journal retains its original descriptor, run/command IDs, pending bytes and SHA. Its own original local revision is acknowledged at665–673. New server defaults must not rewrite these accepted/pending identities. |
| BE preparation descriptor decoder; FE Office descriptor decoder | Current descriptor has only namespace/intent/local revision/workbench revision/adoption revision. A memory baseline or retained raw memory intent is a real closed-contract addition; it cannot be silently read from browser storage by SQL. |

Current registered W1 final public reader is `10102026_2150_banking_pay_next_workbench_retry_v1.sql`; common fixed-kind executor is `10102026_2128_banking_pay_next_workbench_export_v1.sql`, while HEADER and CANDIDATE_GROUPS wrappers remain `10102026_1634_banking_pay_next_workbench_read_limits_v1.sql`. Existing choice CREATE is `10102026_1832_banking_pay_next_accepted_choice_intake_v1.sql`:114–507. Current seal is `10102026_2232_banking_pay_next_selection_pages_v1.sql`:269–361, with2240 receipt cleanup successor. Current bounded retained-input reader is2245. Select final definitions through the registry before authoring successors, preserving the current other reader fields, Source fields, export semantics and HEADER job-progress envelope.

## Minimum reconciliation model

Keep three distinct facts: an acknowledged server baseline, the bounded local overlay, and an immutable in-flight submission. A local revision is an edit generation, not an accepted run boundary; workbench/adoption revisions are coherent-read fences, not changed-financial-basis evidence. A metadata rename/ref/bank-warning update can advance projection revision without changing a payment, so it must not release an untick. A Work revision alone also cannot prove unchanged Case/component terms.

The acknowledged baseline needs one exact server-issued identity tied to the accepted original run/command and the full namespace. Null means no acknowledged baseline for this namespace. The server reader must return defaults and typed current basis for each bounded row, derived from that baseline and the maintained native financial origins; no browser amount comparison or list of every agency ID. Candidate memory must cover its native eligible member basis; Work memory covers that Work/revision; component memory includes its Work/revision/approved-line identity; Case memory uses its real component/rule/basis identity. Existing GROUP_LINES IDs and CANDIDATE_GROUPS current_revision_id/financial_view_revision are useful factual references but are not, by themselves, proof that every relevant remembered basis is unchanged.

The local state needs the acknowledged baseline identity it inherited plus explicit bounded overrides and their originally observed typed bases. Preserve an untouched, missing or stale basis as such. Do not upgrade an old dirty v1 untick by assigning today's basis: that would turn an unknown older choice into a newly qualified exclusion. Retain it for visible reconciliation and keep submission unavailable until the actual existing owner can qualify/reconcile it. An old clean local v1 state may adopt exact server defaults after coherent read; an old pending submission must continue its unchanged original request/replay contract.

One decision path must serve Ready ticks, child ticks, count requests and the descriptor. Conceptually:

1. Validate full namespace, acknowledged generation and current coherent reader context.
2. Obtain the server-owned default and current typed basis for the native row.
3. Apply explicit local overrides from the current local edit generation. A stale or unknown override remains visible and unresolved; do not silently replace it with either checked or unchecked defaults.
4. Apply Candidate=false dominance to effective financial selection, while preserving subordinate raw memory.
5. Freeze that exact resolved baseline plus bounded intent once for CREATE. Original pending/accepted journals continue their own retained generation.

No RPC is required for an individual click or Keep. The acknowledged tables change only at the named accepted/sealed boundary or the named reset boundary; existing W1 reads return bounded defaults. The card does not require a second public reader, per-click write, agency-wide browser array, resolver/protocol, Source certificate or earnings recalculation.

## Four-kind precedence, including suppressed raw intent

Current `freeze()`936 removes every Work/component/Case intent beneath Candidate=false, while snapshot/raw local intent retains those rows. That is correct for effective financial membership but insufficient as the sole input to new server memory. If a user unticks a component and then its Candidate, the newly made component untick disappears from the submitted effective descriptor. Persisting only that descriptor loses it on accepted Draft/reopen. Preserving only *previous* server exclusions also loses newly made hidden overrides.

Retain the bounded original raw memory intent separately from effective financial membership, or capture an equally exact accepted representation of those false native tuples. Validate native scope and basis under the existing acceptance locks. Such memory rows must never create a selected member, hold or money job. Candidate dominance must suppress output selection without destroying the memory audit. This is a necessary WB22 supporting contract change, not authority to broaden RuleB or alter upload ordering.

For the effective hierarchy, Candidate=false wins over subordinate true choices; Work=false wins over subordinate component choices; component choices apply only within the exact native parent Work/revision/SUBSET. NONE retains the existing1832 eligibility semantics: an explicit Work=true can select its Work without Candidate=true, while component rows alone cannot promote a Work. Case inclusion must retain the actual1832 Candidate/payable-parent predicates. Do not invent a blanket NONE Candidate gate or independently make every Case payable.

An acknowledged unchanged explicit false remains false. When its genuine financial basis changes, that old exclusion no longer applies to the new basis; the server can return the new/changed default and the exact stale-memory fact. A dirty Keep choice made on an older basis must not be quietly treated as a fresh exclusion. An explicit current-generation false remains meaningful after a new server baseline arrives. Local explicit true similarly retains exact native scope and basis; it is not permission to include a replacement revision implicitly.

## NONE and unloaded rows

The current local NONE rule931 returns false for every Candidate absent from its local array. The current HEADER `count_selection`430 is only `{mode:ALL|NONE,candidate_ids:uuid[]}`. Merely returning new/changed markers for loaded rows therefore cannot meet the card: unloaded new rows remain unticked, HEADER counts disagree, and1832 CREATE still applies its original NONE predicate to the submitted arrays.

Existing W1/default/count and CREATE need the *same acknowledged mode baseline* plus the bounded overlay. For acknowledged NONE, unchanged rows use the retained baseline selection; genuinely new/changed eligible native rows use the server's default unless an applicable explicit false overrides it. Determine that in SQL from exact actor/date/scope financial basis, not from a loaded-page ID set. Keep bounded defaults on CANDIDATES/CANDIDATE_GROUPS and typed component/Case joins; do not collect all matching IDs in Office.

A fresh local Clear all is an explicit new local mode generation. It must not be confused with an older acknowledged NONE baseline and immediately tick every never-drafted row as “new”. Preserve the difference between that local clear/reset generation and inherited acknowledged mode. Capture its exact current coherent basis frontier through the existing native boundary and retained descriptor; after acceptance, new/changed detection must compare with that accepted original frontier. Global Select all deliberately clears subordinate local exclusions only as the existing explicit user action; a passive refresh must not do the same.

`countSelection()`1006 already checks local revision after await. Extend it to the same baseline/default generation used for ticks/CREATE. A Candidate count-only boolean overlay cannot truthfully describe component/Case exclusion member totals; use the already bounded typed overlay in the existing count contract where those totals are requested, retaining authoritative server counting. No browser financial totals.

## Adoption, reset and retry rules

| Situation | Safe behavior |
| --- | --- |
| First coherent read, no local or acknowledged memory | Existing ALL_MATCHING default for the exact new namespace; repayments follow genuine eligible Case defaults. |
| Clean local state, matching acknowledged baseline | Adopt server defaults once; preserve shared object identity and notify the existing consumers. |
| Dirty Keep, same baseline and basis | Preserve local mode and all explicit true/false overrides through refresh, reopen and page eviction. |
| Dirty Keep, newer server baseline or changed native basis | Keep the original bounded overlay and original basis, expose reconciliation, and refuse an unqualified submit. Never overwrite dirty state merely because a HEADER reply is newer. |
| Background reply after a local click/global action | Adopt only if actor/namespace/store identity/local revision and baseline still match the captured request; otherwise preserve current choices and disregard the obsolete reply. |
| Candidate child reads a narrower scope | Use its parent store/baseline; validate row scope and reader context but never change the parent's namespace to Candidate-only. |
| Pending or accepted submission while local choices change | Original journal replays exact original bytes and retained input readers. New dirty choices remain a separate later local generation. Acknowledging original local revision must not clear a newer revision. |
| Concurrent session accepted a newer baseline | Do not regress memory with an older run finishing later. Use the native accepted boundary order in the named tables; a stale original request remains its own replay, not a fresh current-baseline mutation. |
| Different actor, agency, epoch, date or Candidate/Client/channel scope | Separate namespace; no inherited unticks/defaults, no silent migration and no cross-namespace reset. |
| Keep when storage is unavailable | Existing current-tab retention remains honest; no durable-storage claim or automatic server write. |
| Close Discard | Discard local unacknowledged edits and restore acknowledged defaults for this namespace. Current938 always substitutes empty ALL_MATCHING, so it needs reconciliation with the server baseline. |
| Named later clear/reset action | Clear the appropriate acknowledged and local baseline under its existing explicit owner; no per-click/reset RPC invented here. Never erase or rebuild a pending original journal. |
| Metadata-only newer projection | Refresh factual display/coherence as needed, but do not classify unchanged financial tuples as New/Changed or release their exclusions. |

Before mutating local state after an await, use current actor/NEXT/root lifetime and existing namespace/store identity checks. Keep256 namespace/32768-byte local caps, four arrays<=1000 each, and the131072-byte original journal cap. Do not introduce eviction, mutable exposed snapshots or automatic business retries. Restore only exact validated prior local metadata; malformed storage still refuses rather than resetting valuable intent.

## Acceptance boundaries and remaining handoffs

CREATE1832:333–407 captures exclusions/membership once; it omits excluded Candidate/Work rows from the real member set. Consequently exclusion-memory basis cannot be invented later at seal from live current rows. Capture required memory-native tuples with the original accepted boundary, including suppressed false intent, under its genuine current owner locks. Seal2232:295–298 already returns exact SEALED replay before refresh; its memory merge must use retained accepted facts and be idempotent. It must not refresh settings, Source/query data, membership or financially reinterpret a new revision.

Use the accepted baseline identity in the finite W1 cursor/context contract if cursor-visible defaults can change without a projection revision. A cursor must not retain a stale baseline while its rows now use newer defaults; never retag an old cursor. Existing actor/scope/revision checks and response limits remain. Strict BE/FE mirrors must bind the same nullable baseline, exact native basis shape, mode/default booleans and stale-state union; no free-form server state or whole financial row transport.

The intended narrow implementation is: two named memory tables/guards; exact retained acceptance basis support where the existing descriptor lacks it; seal merge of that original evidence; bounded fields/defaults in existing W1 and matching BE/FE closed mirrors; one choice-store reconcile path used by Ready/child/count/freeze. No producer/version/resolver work, new route, parked ACCEPTED_* calls, monetary calculation, per-click persistence or all-agency browser enumeration. Full application journeys from the card remain NOT_RUN. WB24/25/26/28 actual controls remain their later owners.

Source checkpoint SHA256: FE Workbench `38baef63cb6c9e21d70b2544aecc5977c0f94d52972c48d4c62eb74679aa930e`; FE Office `6ea2bf26501c62c20dd4f556ebe8f2bb68495d88e866f4b0eb1dd9c6bcf87fcc`; BE Workbench `cbd2a1dd5ed494f747c76355e06a39ecc34a7883f06aa29f75719929e726b144`; BE preparation `249ec176f83f512d5ea1b521bcc8bdb6c49c0a872f3c37b1c8650115f22dc5a8`; existing persistence investigation `3f325c6d29e70f5a7a19531428b99b75771debc6347413f87a2b59950102959e`.
