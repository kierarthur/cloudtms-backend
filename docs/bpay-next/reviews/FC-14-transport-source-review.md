# FC-14 Worker and frontend finite contracts — source review

Current verdict: **SOURCE PASS for the reviewed finite contracts and preparation-page adapters.** F14-T01 and F14-T02 are closed on the final saved source below. No remaining essential defect was found in this scope. No product edits, SQL/database calls, fixtures, tests, hosted business endpoints, browser actions or joined acceptance were performed. All runtime and joined checks are NOT_RUN.

## Essential finding

**F14-T01, CLOSED: truthful unknown snooze summary was rejected.** The server report deliberately emits `summary.snoozed_items = null` when any admitted Case has unknown applicable snooze membership (`supabase/repeatable/11102026_2129_banking_pay_next_case_audit_v1.sql:382`). The first result contract required a non-null integer, rejecting a valid report. Both saved copies now admit `integer?` in their report summary branch, retaining unknown rather than turning missing evidence into zero. The same branch now uses `aggregateMoney` for the two server summary sums, preserving exact decimal strings when an aggregate exceeds one native Case's numeric(18,2) magnitude.

**F14-T02, CLOSED: admitted uppercase UUIDs disagreed with native scope.** The initial `CF_UUID` was case insensitive, but `caseFinanceQuery` returned raw identifiers and the result decoder compared exact scope/Candidate strings. Native PostgreSQL UUID output is lowercase, so an admitted uppercase Candidate/Case/cursor ID could turn a successful native read into RESPONSE_INVALID. Both final copies now require canonical lowercase UUIDs (:2 Worker / :4 frontend); the native window already normalizes its seed IDs. This does not add identity, token, route or protocol machinery.

## Checked current seams

The two new page kinds use the existing GET preparation-page route and RPC adapter. The new request parser requires the exact native Candidate/Case or active-only scope, sort, direction, bounded limit, after and include-count fields; it rejects duplicate/extra URL keys. No command, money formula, mutation route, queue wake or producer is added (`bpay-next-preparation.js:35–36,470–485,792–838`; frontend `bpay-next-office.js:605–614`). Office reads send no command body and keep the existing explicit read failure path.

The Worker derives the actor from the existing active-admin Office guard, injects it into the native RPC and verifies it in the returned context. Frontend independently verifies the actor after the existing actor/owner guard around the asynchronous request. Report continuation additionally binds native agency/owner/date/workbench/adoption/policy context; Audit binds agency/actor/owner. The exact scope is echoed and checked; cursor context must match the page context.

Cursor shape is closed to context/scope/section/key/source/source_id/event_ordinal, at most 16 KiB. Result shape is closed, rows are at most request limit100, rows at most90,000 bytes and payload at most120,000 bytes. Worker limits both streamed native response and final wrapper; Office streams into its existing finite body limit before the new decoder's stricter payload check. Request JSON and actual URL encoding are separately bounded at32KiB. Header and rows remain native scalar facts; money/count values stay strings.

Money ordering uses the server's sign plus padded-penny key; text keys are UTF-8 hex, avoiding browser UTF-16 collation. Native server order and decoder's comparator preserve fixed sections, selected sort direction, then source/UUID/ordinal ties. The decoder rejects duplicate source identities and non-increasing rows, verifies the exact last-row cursor, and requires a null cursor for complete pages. These match the current native key construction/order at SQL2129:280–298,405–423; no browser monetary Number conversion or loaded-row sorting is added.

Static index order loads the frontend contract immediately before Office (`index.html:3029–3030`) and the native viewer afterwards (:3032). Only export/wrapper form differs between Worker and browser contract copies; independent body comparison is recorded in the final amendment.

Review uses the Workers review skill and current official [Workers best practices](https://developers.cloudflare.com/workers/best-practices/workers-best-practices/) for bounded asynchronous response handling. No Worker platform binding, configuration or API surface is added by these pure validators, and no package installation or tool execution was needed.

## Final saved-source receipt

Independent static comparison removed only ES module `export` keywords and the browser factory wrapper, then normalized CRLF/LF: **closed contract bodies match exactly**. No validator expressions, schema fields or limits were omitted from this comparison.

| Saved source | SHA256 |
|---|---|
| BE `broker/src/bpay-next-case-audit-contract.js` | `1798D3A1622B68EFE4E89D2A1E3B527642CA92513F72E03C62DDCE364E907FA1` |
| BE `broker/src/bpay-next-preparation.js` | `3773D8BD0BF0C659BA4ACED7F0A66440D228EC4FBCDE7CDF6B07B51E67892C26` |
| FE `js/bpay-next-case-audit-contract.js` | `145CE37B807CC31CF6D2E3F2B6E5672A4110A61C59615562D8966484943A8EFF` |
| FE `js/bpay-next-office.js` | `2A53EC2ADB75C94E9AEA95321B8F372882142DB0B948BEE2F0F535A48F650314` |
| FE `index.html` | `8F4561AD8B84AE49BE7793662BBE8E2E4480BC0AAA49BBA64DC74EAEFA68913F` |

The Worker derives the exact admin actor at preparation:796–799 and injects its lowercase ID into the existing READ RPC at :827–832. Scope/cursor context validation rejects a changed agency/owner/native report context and mismatched Candidate/Case. It does not certify an across-page financial snapshot or turn a read into a mutation. Existing response streaming cancels oversized/failed reads and remains deadline-bound (:487–500,803–838); final Worker serialization rechecks its byte ceiling. Frontend independently bounds the body (:508–520), validates the same closed DTO (:606–613), and retains the existing sanitized native error envelope (:533–550). Known finance-context/not-found/request/row-size errors remain finite mapped codes at preparation:748–749. No raw SQL error, worker lease/cursor, financial command, new API route, secret or request-scoped module global was introduced. Visual/native interaction is covered separately and NOT_RUN here.

## Late source-byte confirmation

**SOURCE PASS continues.** Fresh hashes of both finite contracts, preparation adapter, Office adapter and index match every entry in the final table above. The native viewer's late failure-footer, live-copy/money markers, native badge/sort classes, actual snooze timestamp and row-key navigation change no transport field, request/response/cursor limit, context check or error mapping. Its current SHA256 is `ECC2F2C7C6512CB4696FC4A7F9D46AF9F2467F2596D3EA7B1F742CD664AF9BF5`; full native source review is recorded separately in `FC-14-native-source-review.md`. No new essential transport defect was found. No product edits, tests, database/hosted calls or visual/native/joined acceptance were performed for this confirmation; those checks remain **NOT_RUN**.
