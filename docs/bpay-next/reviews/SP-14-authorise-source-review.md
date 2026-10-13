# SP-14 independent Authorise source review

Source review on 11 October 2026. Product is read-only. Reviewed the current 2223 migration/repeatable, BD122 owner amendment and provenance against the current 0806 First Authorise and 0939 generic Authorise parents. No SQL execution, business calls, fixtures, tests or runtime acceptance.

**Final verdict: SOURCE PASS — A14-01/A14-02/A14-03 closed.** The original Source predicate, admission order and mutation/publication tail are retained. This is source approval under BD122, not runtime or whole-item acceptance.

## A14-01 — NULL issue family loses the original First Authorise refusal

`11102026_2223_banking_pay_next_shift_query_exclusion_v1.sql:2010` replaces the current 0806:130 `comparison.issue_family IS DISTINCT FROM 'CANDIDATE_TIMESHEET_MISSING'` with `NOT IN (...)`. For a NULL issue family the new predicate is NULL and the OPEN incident does not enter the refusing EXISTS. The original predicate returned TRUE and refused it. This promotes unknown evidence to an Authorise exemption contrary to BD122's retained unknown-query safety.

Smallest correction: retain the original NULL-safe missing-Timesheet predicate and exclude the three new families only through a NULL-safe, positively qualified exception. Keep the original query refusal code and generic-authorise/publication tail.

## A14-02 — the new three-family exemption needs its incident-owned current comparison

At 2223:2002–2012 the comparison is joined by `incident.current_comparison_revision_id` alone. The pointer FK references comparison ID, not `(incident_id, comparison_id)`; it does not establish that this is the incident's actual latest comparison. A foreign or stale comparison reporting one of the three new families therefore grants an exemption. Native factual ownership explicitly treats those cases as unavailable (`05102026_1417_weekly_source_coverage_support/qualified_coverage.inc:71–78`). The repaired manual exception at 2223:1990–1994 already supplies the narrow required proof.

Smallest correction: grant the new three-family exemption only when `comparison.incident_id=incident.id` and no newer comparison for that incident exists. Otherwise preserve the original whole-root refusal/qualified-protection behavior. Preserve the pre-existing CANDIDATE_TIMESHEET_MISSING exemption exactly. No new facts table, protocol or producer is needed.

## A14-03 — A3 undecidable metadata must not omit the key

At 2223:2211–2221, NULL `sheet_scope` or `line_type` falls through into the Contract/week/group branch. With an actual Contract and zero active memberships this sets `v_query_key_required=false`; the subsequent native classifier can return FALSE simply because those metadata predicates do not match. That is not a positive ordinary-scope proof. BD122 requires undecidable scope to take the existing key.

Smallest correction: leave the default TRUE when the native scope/type tuple is NULL; evaluate omission only for known values and the reliable current canonical tuple. Similarly retain the key when the canonical requested row itself is ambiguous. The existing `contract.self_bill`, installed Source-family classifier and live authorisation checks must remain before omission.

## Confirmed source boundaries

The A3 pre-read uses plain reads before the original first Timesheet `FOR UPDATE` (2223:2264–2269). It resolves an obsolete requested row only when exactly one current booking member exists, defaults to requiring the key, and errors in the read-only classifier also require it. Omission requires the actual Contract's self_bill=FALSE, native `_candidate_expense_source_family_v1`=FALSE and no live root authorisation; active Office group absence alone does not prove non-Source ownership. Revoked/archive restoration remains admitted. The ordinary mutation/rate-refusal/result/exception tail follows afterward, including the exact named55P03 escape.

The First Authorise caller still invokes the original generic owner exactly once with the original arguments. The native canonical family/actor/protected authority and immutable authorisation/publication tail remain. The owner amendment explicitly records the whole-Candidate and old queue-envelope conflicts. D01–D03 require the existing current native pin/admission/replay owners, not reinstallation of stale card literals; final direct service owner remains 2041, and the original PIN/guard ownership is preserved.

## Reviewed bytes

| Source | SHA-256 |
| --- | --- |
| supabase/repeatable/11102026_2223_banking_pay_next_shift_query_exclusion_v1.sql | `6d4274c4a9b388b60faad264dc4baa10e79fc397505f560ac9e81c5c27cb96e5` |
| supabase/migrations/11102026_2223_banking_pay_next_shift_query_exclusion.sql | `939cfaf247eabcbcdb54495f18f08d86beaaaeed607147a25a8e202ff4db89aa` |
| design/SP-14-owner-amendment.md | `5d25272f359b2dfb433bb2f83707b40b9b2a92cef1dbfd750c99fa982f775065` |
| design/SP-14-successor-provenance.json | `2877ac2c1fee09c7832295d4a6105289dfd6016047dfde9fd99160034114646f` |

These hashes identify the findings' source. A fresh closure is required after corrections. Compilation, catalogue and all financial/joined/queue contention outcomes remain NOT_RUN by this reviewer.

## Fresh correction check

Re-read saved repeatable SHA-256 `b6ac0d7edac3f8c3ebf835125b8b1ff9c706e926bfe305e0aa850074db193089` and provenance `6554e31f276641074d65cdfc5487de473acfece99f7f211362bb2aefc6ae1889`. A14-01 is closed: the original `IS DISTINCT FROM CANDIDATE_TIMESHEET_MISSING` remains and the new qualified exception is tested `IS NOT TRUE`. A14-02 is closed: the new exemption requires comparison ownership, actual event Candidate/Client equality and no newer comparison. A14-03's NULL scope/type fall-through is closed by non-NULL predicates in the group branch.

The remaining A14-03 edge is the resolved settings tuple. The installed native classifier is a plain STABLE read and returns FALSE unless `settings_authority_version='CONTRACT_SETTINGS_AUTHORITY_V1'` and `settings_authority_resolved_at IS NOT NULL` (0202:195–196). Missing settings therefore returns FALSE without establishing an ordinary Source domain. A WEEKLY/HOURS current row with actual self_bill=FALSE, group count zero and unresolved settings currently omits admission at 2223:2219–2243. Retain default TRUE for that unknown tuple; allow omission only after the existing native resolution metadata is known. This is an admission condition only, not a new accepted proof or producer change.

## Final saved-source closure

Fresh final bytes independently read: 2223:2234–2239 retain the admission key for WEEKLY/HOURS when the existing CONTRACT_SETTINGS_AUTHORITY_V1 version, resolved timestamp or authority object is unavailable. Non-NULL native scope/type remains required before the group branch. The final actual Contract/self-bill, native Source-family and live root-authorisation checks remain. Any read-only predicate error keeps the key. Thus A14-03 is closed without a new proof object or producer.

A14-01 and A14-02 remain closed at 2223:2010–2017: the original missing-Timesheet predicate is NULL safe; the three new exceptions require actual incident ownership, event Candidate/Client identity and current latest comparison, and the complete exception is tested `IS NOT TRUE`. A NULL/foreign/stale comparison cannot grant the new exemption. The manual exception retains the previously repaired exact Source group and comparison qualification.

Static complete-body comparison confirms the entire First Authorise suffix from its original generic call through the immutable authorisation result is unchanged from 0806. Generic Authorise's entry before admission (apart from the new local declarations) and its complete mutation/validation/rate-refusal/response/exception tail are unchanged from 0939. The new pre-read performs no row/advisory locks; the existing Source admission remains before the original Timesheet/TSFIN locks and exact named busy escapes before diagnostic writes. No publication function or raw five-key gate is replaced.

D01–D03 retain their current owners and native scope: the 0559 PIN owner takes the existing admission before module/business locks only on NEW/PINNING fresh work, and preserves advanced/replay paths; its current immutable pin guard remains unchanged. Position capture consumes only the original exact snapshot/segment array added at Create, preserving the genuine positive Source WORK predicate and missing-companion/UNAVAILABLE refusal. The current 2041 direct service owner retains the early key and native replay/phase exclusions. No Source key is added to CLAIM/RESERVE/ALLOCATE/FINANCE_ALLOCATE. Original parked F25/Rule-B producer, financial amounts, Source entitlement and Source publication ownership are unchanged.

The migration remains two nullable columns on existing private relations with array/finite-reason CHECKs. Existing RLS and immutable snapshot/selected-member guards retain their ownership. New helper is private STABLE/security invoker; ACL tail keeps its execution owner-only, and the existing public generic function retains service_role-only execution. The BD122 conflicts and future NOT_RUN acceptance are explicit.

| Final saved source | SHA-256 |
| --- | --- |
| supabase/repeatable/11102026_2223_banking_pay_next_shift_query_exclusion_v1.sql | `b3d7b39a31d35f573dbcc4ea21e9fc0015c801d09a02f1508fe8c9b3ed4535dd` |
| supabase/migrations/11102026_2223_banking_pay_next_shift_query_exclusion.sql | `939cfaf247eabcbcdb54495f18f08d86beaaaeed607147a25a8e202ff4db89aa` |
| design/SP-14-successor-provenance.json | `547a38584382236d059499366cd47deff850306a10349581b832f76c0da9e73c` |
| design/SP-14-owner-amendment.md | `5d25272f359b2dfb433bb2f83707b40b9b2a92cef1dbfd750c99fa982f775065` |
| final-definer-closures.json | `a482c107d56b32b2bac71027ad933327463e9838d42f2d04228821bd9b70527e` |

No essential source finding remains within this review. Root performs normal compile/catalogue/contract checks separately. Business outcomes, real both-order Source contention, timing/no-starvation, queued application and joined Office acceptance remain AUTHORED_NOT_RUN; this reviewer performed none.
