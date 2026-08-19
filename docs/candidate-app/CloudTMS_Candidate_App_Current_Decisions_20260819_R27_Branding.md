# CloudTMS Candidate App — current cumulative decisions

**Authority date:** 19 August 2026  
**Current implementation boundary:** Candidate Daily Phase 3 R25 / R18E assurance closure  
**Decision range:** complete historical authority through AV-501

## Authority and format

This Markdown file is the current machine-readable decisions authority. It preserves the complete text of every page of the final historical 186-page R22 Decisions PDF, including all earlier decisions and later-controlling addenda. From this point forward, new decisions must be incorporated directly into the cumulative Markdown authority. No new Decisions PDF is required unless the user explicitly requests one.

The `Historical decision page` headings below preserve source-page traceability only; the decisions are cumulative and later-controlling amendments override conflicting earlier positions exactly as stated in the text.


## Historical decision page 1

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 1 
DECISION AUDIT 
CloudTMS Candidate App 
Current Agreed Decisions 
Status Approval-ready current-decision register 
Scope DB/RPC → backend/private API → public broker → CloudTMS frontend → 
iPhone/Android/web 
Rule Current decisions only; superseded positions removed 
How to approve this document 
Read the short sections as the current product handbook. Exact route-warning and rejection wording 
appears once near the end. Anything not genuinely settled is isolated under Decisions still to be 
approved. 
  
Audit date
Page 1Decision audit · 12 August 2026
12 August 2026
~~~


## Historical decision page 2

~~~text
CLOUDTMS CANDIDATE APP  |  CURRENT DECISIONS
Page 2
Decision audit - 13 August 2026
Contents
1. System ownership and boundaries
2. Environments, access and security
3. Candidate accounts and authentication
4. Entitlement, navigation and history
5. Route families and candidate capability
6. CONTRACT weekly timesheets
7. DAILY booked shifts and timesheets
8. Local Save for Later
9. Expense entry and evidence
10. Expense claim lifecycle
11. Expense separation, carriers and invoice routing
12. Electronic submission and official documents
13. Manager approval
14. Paper signing and QR lifecycle
15. Processing, authorisation, payment and invoice
progression
16. Rejection, amendment and no-work
17. Notifications and communication
18. Candidate App functional layout
19. CloudTMS office presentation
20. Route conversion and office intervention
21. Concurrency, recovery and audit
22. Delivery order and verification
23. Route-conversion warning catalogue
24. Decisions still to be approved
25. Omissions and limitations
26. Source precedence used for this audit
27. Office switch to Manual: incomplete expense claims
28. Exact replay and PAPER execution authority
29. Authentication and operation-replay closure
30. Public credential replay and push-token identity
31. Public authentication boundary, replay and key rollout
32. Final public-authentication throttle and key authority
33. Mixed-version concurrency ownership
34. Credential winner and security receipt
~~~


## Historical decision page 3

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 3 
This register contains current decisions only. Superseded wording is excluded. Anything not fully 
approved is isolated in Decisions still to be approved. 
 
Approval summary 
This document contains the current controlling decisions only. Earlier positions that were later 
changed have been removed rather than repeated. The original policy, UI specification, presentation 
addendum, later audit instructions, route-warning catalogue, installed DB/RPC authority and current 
backend plan were reconciled before drafting it. 
The overall design is now: 
• CloudTMS owns all canonical timesheet, financial, evidence, processing, authorisation, invoice 
and route truth. 
• The Candidate App is a controlled new interface over CloudTMS, not a second timesheet or 
finance system. 
• The database/RPC authority, CloudTMS Candidate backend, separate public Candidate broker 
and private Candidate API foundation are installed/deployed in TEST with every Candidate 
feature flag disabled; independent final API-freeze verification remains the next gate. 
• The CloudTMS office frontend and Candidate iPhone/Android/responsive-web interface remain to 
be implemented against the finally approved contract. 
• Seven Candidate App tables and fourteen public Candidate business RPCs are the fixed 
architectural limit. No duplicate current function definitions are permitted in the installable SQL 
set. 
The items under Decisions still to be approved are the only matters this audit could not honestly 
classify as already agreed. They concern implementation technology, branding/publishing details 
and pixel-level app presentation—not the core CloudTMS timesheet policy. 
1. System ownership and boundaries 
CloudTMS remains the sole authority for contracts, contract weeks, timesheets, TSFIN, evidence, 
route validity, processing, authorisation, invoice locks, payment locks, retained financial history, 
deletion, archive and invoice routing. 
The Candidate App and its broker submit factual information only: actual starts and finishes, breaks, 
additional units, expense claim amounts and source evidence. They never supply authoritative rates, 
pay, charge, VAT, ERNI, margin, invoice breakdown, TSFIN totals, issue codes, import classification 
or authorisation eligibility. 
All candidate-facing channels and office-facing CloudTMS screens must receive the same server 
decision for the same facts. Frontend controls may guide a user, but the backend/database 
revalidates the final proposed state under lock. 
The following existing CloudTMS authorities remain the only authorities for their jobs: 
• the existing WEEKLY calculation and lifecycle path; 
• one shared canonical DAILY Save/Recalculate calculation body used by office create, office edit, 
additional DAILY, the TSFIN worker and Candidate finalisation; 
Page 3Decision audit · 12 August 2026
~~~


## Historical decision page 4

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 4 
• the existing Process functions, with timesheet_daily_manual_process_atomic remaining 
Process-only; 
• the existing Authorise authority; 
• the existing Invoice Generator/Issuer pipeline; 
• one SQL route/version authority; 
• one official CloudTMS timesheet renderer with controlled document variants; 
• one manager workflow and approval authority used by both phone and email presentation routes. 
Candidate-specific expense placement, evidence, approval, rejection and notification orchestration 
may remain bounded Candidate workflow logic. It must compose the existing financial and lifecycle 
authorities and must not duplicate their calculations. 
The retained Google Availability/rota service remains responsible for the existing DAILY rota and 
availability behaviour. DAILY rota data is not migrated into a new CloudTMS rota system. 
Banking Pay, Policy X, payment execution, settlement, remittance, PAYE/Umbrella economics, rate 
calculations and the legacy Google DAILY business behaviour are outside this project and must not 
change. 
2. Environments, access and security 
TEST and LIVE are separate environments with separate broker deployments, application identities, 
storage, secrets, push credentials and CloudTMS/database authorities. The Candidate App initially 
uses TEST and later uses the separately configured LIVE environment. Any environment mismatch 
fails closed. 
The iPhone app, Android app, responsive candidate web app and public manager pages 
communicate only with the separate public Candidate broker. They do not access Supabase, 
CloudTMS database tables, R2, Google Apps Script, Google Sheets, email providers or push 
providers directly. 
The broker communicates with a versioned private Candidate API owned by the CloudTMS 
backend. It may also mediate the retained Google Availability API for DAILY rota functions. 
The seven Candidate tables force row-level security. Browser roles have no direct Candidate table 
access, and Candidate business RPCs are service-owned rather than browser-callable. 
Every protected request is bound to its environment, account, selected candidate and current 
session. Manager tokens are hashed, time-limited, version-bound and single-use. Mutations use 
idempotency keys and expected row/workflow versions so retries cannot create duplicate claims, 
carriers, workflows, evidence, emails, notifications or submissions. 
Source evidence, generated review documents, signatures, returned signed documents and final 
signed documents are immutable and auditable. Route changes may supersede them as current 
truth, but must not physically purge signed or issued history. 
3. Candidate accounts and authentication 
All Candidate App users start with a new Candidate App password. Existing Google password 
hashes and salts are not migrated or used as a legacy login path. 
Page 4Decision audit · 12 August 2026
~~~


## Historical decision page 5

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 5 
The email must exist on an eligible CloudTMS candidate record. Activation and password recovery 
begin with an enumeration-safe email challenge so the public response does not reveal 
unnecessary account information. 
First activation consists of creating and confirming a new password, receiving an email verification 
code, verifying that code and creating the first session. Password establishment and first-session 
creation are one transaction: a person cannot be left with a password but no completed activation 
session. 
CloudTMS creates and verifies the modern password credential. Plaintext passwords are never sent 
into SQL. Access tokens are short-lived; refresh tokens are opaque and rotated; only refresh-token 
hashes are stored. Presenting an old token after rotation triggers token-family replay protection and 
revocation. 
One login can access only candidate records whose normalised CloudTMS email matches the 
authenticated email. In TEST, where more than one matching record exists, the user selects the 
candidate record they are using. LIVE must not permit duplicate active candidate accounts sharing 
one email. 
Switching the selected candidate resets all server and local candidate context. No timesheet, draft, 
notification or rota information from the previous selection may remain visible. 
Change Password, Notification Preferences and Logout are app-level Account actions. They are not 
buried inside the DAILY menu. 
4. Entitlement, navigation and history
DAILY entitlement requires the selected active candidate to have a non-empty Global Candidate Key in
candidates.key_norm. The server rechecks entitlement when a DAILY workflow is created and again at
finalisation; hiding a DAILY card is not sufficient authority.
The retained Google Candidate_ID continues to identify rota/booking data. It does not replace the CloudTMS
GCK or become the login authority.
CONTRACT entitlement exists when the selected candidate has an eligible contract-week or timesheet
occurrence in the current six-calendar-month window. Unprocessed, processed, authorised, invoiced and paid
occurrences all count for entitlement.
App-level navigation contains Home, DAILY/Rota where entitled, CONTRACT Timesheets where entitled,
Notifications and Account. Home shows a large DAILY card and/or CONTRACT card. If only one module is
available, the app may open it directly while keeping an app-level way to switch module.
The Timesheets page uses Current and History tabs. Current is selected by default.
Current. Starting with each contract's effective current week ending and moving backwards, it shows every
not-paid timesheet with no age limit, including unprocessed, processed, authorised, rejected or action-required,
and invoiced-but-not-paid records. It also shows a paid timesheet while its authoritative paid_at_utc is within
the preceding seven days. No later contract week or genuinely future timesheet appears.
History. Shows paid timesheets older than seven days within a 16-week window: the effective current contract
week and the preceding 15 contract weeks, calculated separately for each contract. A timesheet appears in
one tab only.
Both tabs order cards strictly by week-ending date from most recent to oldest. Each card displays Week
Ending 1 January 2025. Archived rows never appear; different contracts appear separately.
Canonical financial state wins over historical workflow state when producing the visible status. Paid and
authorised states cannot be hidden by an older rejected, refused or awaiting workflow. Workflow audit history
remains available separately from the Current and History tabs.
Page 5Decision audit · 12 August 2026
~~~


## Historical decision page 6

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 6 
Each CONTRACT row provides candidate-safe display facts without the broker querying CloudTMS 
tables directly: client, week-ending day/date, job title, band, stable status, route/capabilities, hours, 
expense overlay, manager state, rejection scope/reason and invoice/payment state. 
5. Route families and candidate capability 
The server assigns one effective route family for every materialised timesheet in this order: 
1. import-authoritative; 
2. actual current QR/paper facts; 
3. ELECTRONIC submission authority; 
4. otherwise genuine MANUAL non-QR. 
The app and future broker consume the returned route and capability flags. They do not infer route 
from submission_mode, a paper-fallback setting or raw QR fields. 
Import-authoritative and genuine MANUAL non-QR hours are candidate view-only. The candidate 
cannot start or finalise hours, declare no work or mutate those records. Import-authoritative 
expenses use the mandatory separate expense route. 
Paper fallback permission does not transform an existing MANUAL non-QR timesheet into a 
candidate paper timesheet. A QR/paper route requires actual route/version facts or a valid new route 
intent. 
Candidate PAPER/QR is WEEKLY-only. DAILY always uses electronic manager approval by phone 
and/or email and never gains a Candidate paper route. 
An unsigned DAILY row may remain physically MANUAL to preserve the existing two-signature 
database constraint, while a constrained server-owned pending route intent records that it is logically 
awaiting a fresh ELECTRONIC submission. That intent is consumed by reads, workflow creation, 
finalisation and office route context, then cleared when the final signed ELECTRONIC row is 
created. 
Route/capability authority is rechecked at list/detail, missing-week creation, workflow creation, 
worker submission, paper preparation, expense placement, no-work and finalisation. 
6. CONTRACT weekly timesheets 
The backend returns all seven authoritative dates from the contract-week snapshot and effective 
week-ending rule. The app never assumes the week ends on Sunday and shows days even when 
no work was planned. 
Non-ad-hoc contracts prepopulate the effective planned schedule. Ad hoc shifts start without 
schedule prepopulation. Candidates may change permitted start/finish times, add or remove multiple 
segments on the same day, enter overnight work and enter breaks as start/finish or duration. 
The existing WEEKLY authority remains responsible for overlap checks, date boundaries, daylight-
saving behaviour, time buckets and every financial value. 
A candidate can explicitly record No break. The canonical factual input is break_minutes = 0 with 
no break start or finish. WEEKLY accepts this without creating the DAILY-specific non-60-minute 
break issue. 
Page 6Decision audit · 12 August 2026
~~~


## Historical decision page 7

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 7 
Additional Units appear only where the contract supports them. Any non-zero Candidate-entered 
additional units are HOURS-side content, require office checking and block automatic authorisation. 
They also count as worked activity when deciding whether an expense claim can be made for that 
week. 
There is no percentage-deviation calculation for WEEKLY Additional Units. They have no agreed 
scheduled baseline against which a percentage comparison can be made. Percentage-based 
unexpected-hours checking applies only where this document explicitly defines a planned-versus-
actual hours rule; it must not be extended to WEEKLY Additional Units. 
Candidates can submit a complete timesheet without entering expenses. The app offers Continue 
with Timesheet Only or equivalent approved wording and explains that expenses may be added 
later. An unfinished receipt or mileage form never blocks an otherwise complete hours submission. 
Add missing week is available only for an eligible ELECTRONIC or candidate-enabled WEEKLY 
paper route where the full seven-day week overlaps the candidate's contract. It is unavailable for 
import-authoritative or MANUAL non-QR routes and cannot duplicate an existing/hidden base week. 
Effective settings are resolved separately for each generated week. A successful request uses the 
existing CloudTMS contract-week/planning authority to create the ordinary base week: OPEN when 
the week has reached the current Europe/London date, otherwise PLANNED. The Candidate App 
introduces no separate missing-week lifecycle or creation engine. 
I did not work this week is available only on a candidate-operable, unprotected WEEKLY 
record. It uses the existing planned-week delete, materialised-record delete or retained-history 
archive authority as appropriate. It creates no new no-work tombstone. 
7. DAILY booked shifts and timesheets 
The existing Google-backed availability tiles, status choices, booked shifts, refresh, Past Shifts, 
running-late/emergency functions, Hospital Addresses and Accommodation Contacts remain 
functionally unchanged. Their appearance may later be improved, but their communication 
semantics and Google-side behaviour do not change. 
The existing retained DAILY availability application/service and existing DAILY timesheet integration 
remain operational and unchanged throughout Candidate App TEST and pilot unless replacement is 
separately approved. The new Candidate broker may mediate the retained Google Availability API, 
but this project is not authority to decommission or modify either retained DAILY deployment during 
TEST/pilot. 
The ordinary DAILY timesheet entry is a booked, eligible, not-authorised shift tile. The old generic 
Send a timesheet by email menu item is removed. 
Opening a shift shows date, client/site, ward, role, booking reference and booked start/finish. The 
candidate is asked whether those hours are correct. Yes reuses the booking; No opens actual 
start/finish controls while keeping the booked values visible. 
The candidate records a break using start/finish, an explicit duration, or No break. No break is 
stored as zero minutes with no start or finish. The factual value is accepted by the canonical DAILY 
calculation and existing TSFIN route. 
The normal planned break used for DAILY deviation comparison is 60 minutes unless a more 
specific authoritative planned break exists. If planned net time cannot be resolved safely, automatic 
authorisation is withheld rather than guessed. 
Page 7Decision audit · 12 August 2026
~~~


## Historical decision page 8

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 8 
A DAILY actual break other than 60 minutes—including an explicit no-break value of zero—creates 
the DAILY checking issue. The app shows the calculated duration and says office checking may 
delay payment, with Change Break Times and Yes, Continue. The actual break is preserved when 
the candidate continues. 
Unexpected-hours comparison is performed per Europe/London work date. Multiple planned 
segments are combined, planned breaks are subtracted, multiple actual segments are combined 
and actual breaks are subtracted. The issue is raised only when actual daily net minutes are strictly 
greater than planned daily net minutes by more than the current configured percentage. Exactly at 
the threshold, fewer hours and under-reporting do not trigger this issue. Positive actual work against 
zero planned net time does. 
The same Europe/London work-date derivation is used by DAILY read, workflow creation, schedule 
validation, issue calculation and finalisation, including overnight and daylight-saving boundaries. 
The candidate reviews one shift and signs it. The manager methods are controlled by Care 
Packages settings: phone, email or both; at least one must be enabled. DAILY manager name and 
position are entered each time and are never prepopulated. 
The functional Care Packages settings remain Allow manager to authorise on phone and Allow 
manager to authorise by email. CloudTMS must reject a settings save that would leave both 
disabled. The configured hours-deviation percentage remains settings-owned and server-enforced; it 
is never exposed as an editable Candidate value. 
DAILY contains no expenses, mileage, expense evidence, Candidate paper/QR, later expense claim 
or Add missing week. The app never displays the internal deviation percentage. 
Final DAILY materialisation follows one sequence: save the frozen manager-approved factual 
schedule through the shared canonical DAILY Save/Recalculate owner, obtain the fresh row 
signature, call the existing Process authority, then call the existing Authorise authority only when 
automatic authorisation is allowed. The economic calculation itself is unchanged. 
HealthRoster DAILY validation remains authoritative. Where validation is required, Candidate 
finalisation must not call Authorise and the existing validation/READY_FOR_HR route continues. 
8. Local Save for later 
Save for later is entirely local and makes no broker or CloudTMS mutation. Native apps use 
protected application storage; the responsive web app uses candidate-bound IndexedDB. 
The local draft may hold hours, breaks, additional units, expense values, evidence images, mileage 
progress and current screen progress. It is not visible on another device, to a manager or in 
CloudTMS. 
The draft remembers the last server context. If the contract, route, settings or row version changes, 
the candidate must review the draft before continuing. 
If hours are submitted while an expense draft remains unfinished, only the submitted hours part is 
cleared. The local expense draft remains under Continue Expense Claim and may be removed with 
Discard Expense Claim. Discarding it removes only unsent local information. 
The confirmation is Saved on this device. Nothing has been submitted yet. Returning to a 
draft offers Continue Draft and Discard Draft. 
Page 8Decision audit · 12 August 2026
~~~


## Historical decision page 9

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 9 
9. Expense entry and evidence 
The candidate claims only the needed categories: Mileage, Accommodation, Travel and Other, plus 
any later category that is already supported by canonical CloudTMS economics. Zero fields for 
unused categories are not required. 
Category choice may remain optional to the candidate where the current screen makes the context 
unambiguous. For example, evidence uploaded inside the Accommodation flow can be labelled 
Accommodation by the server without asking the candidate again. The immutable server component 
must nevertheless always contain exactly one derived category. A conflicting caller-supplied 
category is rejected. 
Travel, Accommodation and Other require at least one valid source image for that category. Mileage 
requires a completed Mileage Claim Form. Evidence requirements are derived from all accepted 
economic fields—units, pay values, charge values and general expense values—so an alternate or 
negative economic field cannot bypass the rule. 
One uploaded source component supports exactly one category. Several clear receipts may appear 
in one photograph, and multiple components may support the same category. Candidate source 
evidence may be a validated image/photo or a dedicated one-page PDF for that category. Mixed-
category evidence and multi-page candidate evidence PDFs are not accepted. 
The backend computes the authoritative content hash. The same image bytes cannot be reused as 
another claim, category, week or generation. When evidence already exists, a different image must 
be identified as additional or replacement evidence. 
No expense amount becomes canonical until the relevant workflow has the required evidence, 
manager approval is complete, every required final signed page is ready and atomic finalisation 
succeeds. 
The Expense and Mileage Approval Summary does not count as Other source evidence. Generic 
TIMESHEET evidence and generated unsigned documents are not automatically treated as signed 
returned evidence. 
Only one active TIMESHEET evidence item may exist for a timesheet. More than one source image 
per expense category remains supported. 
Mileage Claim Form 
When mileage is entered, the app asks whether the candidate already has a completed and signed 
Mileage Claim Form. If not, it offers Download, Email to Me, Save for Later and—during initial hours 
entry—Submit Timesheet Now and Finish Mileage Later. 
The branded A4 form is titled Mileage Claim Form for week ending DD/MM/YYYY. It contains 
Date, Postcode From, Postcode To and Number of Miles, with Total Mileage prepopulated and 
Manager Name, Position, Signature and Date at the bottom. A returned form is canonical MILEAGE 
evidence with document role MILEAGE_CLAIM_FORM. 
The returned Mileage Claim Form is handwritten evidence, not a machine-readable economic 
source. CloudTMS does not automatically interpret or reconcile handwritten journey rows against the 
candidate-entered or prepopulated mileage total. Suspected discrepancies use the existing manual 
office review and whole-record rejection/correction process. 
Page 9Decision audit · 12 August 2026
~~~


## Historical decision page 10

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 10 
Expense and Mileage Approval Summary 
When any mileage or expense exists, CloudTMS generates one branded A4 summary for the actual 
expense record. It identifies candidate, client, contract and week, lists each non-zero total and 
shows evidence counts by category. It requires one manager name, position, signature and server 
date but no candidate signature. 
The summary uses evidence kind OTHER with document role 
EXPENSE_MILEAGE_APPROVAL_SUMMARY. It never creates an Other amount, never satisfies Other 
source evidence and appears only once in invoice evidence. 
10. Expense claim lifecycle 
Candidates may add expenses during the initial timesheet flow or return later to the visible worked 
row. Hours do not need to be resubmitted for a later standalone expense claim. 
When Add Expenses is selected for a worked week that already has an expense claim, the app first 
shows candidate-safe information about that claim: previously submitted amounts/categories, 
submitted date, evidence count and current status where available. With no prior claim it starts a 
new claim. With an active claim it offers Continue Existing Claim or Cancel Entire Claim and 
Start Again. After an authorised/completed claim it may offer a subsequent claim under the rules 
below. Paid or invoiced history creates a new generation, shows Requires Checking for 
Duplicate and prevents automatic authorisation. 
A candidate may have only one not-yet-authorised expense claim for the same candidate, contract 
and worked week. This remains true after the first claim is finalised but still unauthorised. Concurrent 
attempts are serialised so only one succeeds. 
An active claim offers Continue Existing Claim or Cancel Entire Claim and Start Again. 
Cancellation invalidates outstanding manager links and generated paper documents while retaining 
immutable audit history. 
Another claim can start only after the previous canonical expense claim is actually authorised. A 
later claim after paid or invoiced history uses a new generation, displays Requires Checking for 
Duplicate and never auto-authorises. 
Worked eligibility uses the authoritative predicate: positive clock-hours or positive additional units. 
Expense-only carrier rows remain hidden; there is no standalone expense card in the Candidate 
App. 
11. Expense separation, carriers and invoice routing 
Expense separation is mandatory for NHSP import-authoritative hours and HealthRoster 
CREATE/no-timesheet-required hours. It cannot be disabled. A valid effective Expense Invoice 
Email is required when import-authoritative client or contract settings are saved. 
For optional non-import routes, the functional setting is Send expense-only invoices to a 
different email address. When enabled, separation applies, an effective valid Expense Invoice 
Email is required, expenses use a separate expense-only record where the policy requires it and 
only the expense-only invoice uses that address. For import-authoritative routes, Separate expense 
invoicing is required; it cannot be disabled and an effective valid Expense Invoice Email is 
mandatory. 
Page 10Decision audit · 12 August 2026
~~~


## Historical decision page 11

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 11 
For other routes, separation is controlled by the effective client/contract setting. Where contract 
settings do not override the client, the client value applies. Where contract override is active, an 
explicit contract value—including false—wins. Import authority always overrides an attempt to turn 
separation off. 
The existing fields remain the internal storage to minimise change: 
send_manual_invoices_to_different_email is the separation switch and 
manual_invoices_alt_email_address is the Expense Invoice Email. 
Where separation applies, the proposed final economic state classifies a mutable additional row: 
Final state Server role 
Hours or additional units non-zero; 
expenses zero Hours 
Expenses/mileage non-zero; hours 
and additional units zero Expenses 
Both sides zero and no active 
expense evidence Flexible 
Both sides non-zero Conflict and reject 
 
Positive and negative values count as non-zero. Protected import, correction, authorised, paid, 
invoiced, archived or retained-history rows are not silently reclassified because a current total 
happens to be zero. 
On a flexible mutable row, both sides may initially be available. Entering hours, schedule segments 
or additional units disables expenses and expense evidence. Entering mileage or expenses disables 
schedule and additional-unit controls. Clearing one complete side can make the other available 
again; the UI never silently deletes values or evidence. 
When separation is off and the current timesheet is mutable, expenses may use it. When separation 
is required, CloudTMS reuses exactly one eligible expense carrier for the same 
candidate/contract/week or creates one atomically through the existing additional-timesheet 
authority. It never reuses an additional-hours, ambiguous, authorised, paid, invoiced or archived 
row. 
Each hidden expense carrier is resolved to a visible display anchor independently: active workflow 
anchor, then parent_timesheet_id, then a unique base worked row, then a unique additional 
worked row. Zero possible anchors or more than one possible anchor fails closed; the server never 
guesses the first row. 
Only economically expense-only invoices use the Expense Invoice Email. Hours and hours-only 
adjustments use the normal invoice route. Negative expense corrections use the Expense Invoice 
Email; negative hours corrections remain on the normal route. 
Expense-only sources do not consolidate with hours/import/self-bill sources. They may consolidate 
with one another only when client, recipient, effective policy and consolidation mode match. Stream 
and recipient are resolved before grouping, so a mixed-recipient invoice cannot be created. 
Page 11Decision audit · 12 August 2026
~~~


## Historical decision page 12

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 12 
A separate expense-only invoice associated with import-authoritative work remains independently 
sendable to the effective Expense Invoice Email. It must not inherit, and must not be suppressed by, 
a self-bill or no-send rule applying to the corresponding imported-hours source. 
The existing Invoice Generator/Issuer READY/BLOCKED and diagnostic systems remain the visible 
workflow. A missing Expense Invoice Email blocks only the affected expense work; unrelated ready 
work continues. No separate Candidate expense invoice engine or visible internal stream labels are 
added. 
Approved and barred manager domains are trimmed, lowercased, stripped of one leading @, syntax-
validated and deduplicated. 
The configured hours-deviation percentage and barred public manager-email domain list remain 
settings-owned server authorities. Their values and defaults are not re-derived or hard-coded 
independently by the Candidate App, broker or CloudTMS frontend. 
12. Electronic submission and official documents 
The workflow types have these fixed page/signature requirements: 
Workflow Candidate signature Official hours page Expense pages 
CONTRACT hours Required Required None 
CONTRACT combined Required Required Required 
CONTRACT standalone 
expense Not required Forbidden Required 
DAILY Required Required Forbidden 
 
The candidate signs the exact immutable factual submission. Where manager approval is needed, 
CloudTMS stores a pending versioned workflow rather than prematurely committing canonical 
financial truth. 
For every electronic hours workflow, CloudTMS renders the official one-page timesheet twice from 
the same immutable submission: 
• the manager-review version contains exact WEEKLY or DAILY facts and the candidate 
signature/date, but no manager signature or approval date; 
• the final signed version contains the identical business content and candidate signature plus 
manager name, position, one manager signature and authoritative server approval date. 
The manager reviews CloudTMS-generated documents. The broker or manager page does not 
redraw a timesheet from JSON. 
Every required review component—hours page where applicable, expense summary, mileage form 
and each evidence page—must be rendered, stored, hashed and present in the immutable all-ready 
manifest before a manager email request is created. The manager does not wait for review 
images/documents to be generated after opening the link. 
After manager approval, final signed derivatives may render asynchronously. Approval is committed 
first and remains durable if rendering/finalisation fails. The office can retry final rendering/finalisation 
Page 12Decision audit · 12 August 2026
~~~


## Historical decision page 13

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 13 
idempotently without asking the manager to sign again. Canonical finalisation occurs only when 
every required final signed derivative is ready. 
13. Manager approval 
Phone and email are two presentations over the same workflow generation, required component 
manifest, approval request, signature event and finalisation authority. 
The manager is told how many pages must be reviewed. They review the official hours page where 
present, Expense and Mileage Approval Summary, Mileage Claim Form and every source evidence 
page separately. Each required page must be recorded as viewed before approval. 
The manager does not sign each page. After all pages are reviewed, the manager confirms/enters 
name and position and draws one signature. That signature and the server approval date are 
applied to every page in the approval transaction. 
For CONTRACT, the last approved manager name/position for the same candidate and contract 
may be prepopulated with Not you? Change, subject to current policy revalidation. DAILY never 
prepopulates manager identity. 
Phone approval requires no manager email. Manager mode shows only the pages being approved, 
not the candidate account. Success shows a large green tick, MANAGER APPROVED, then returns to the 
timesheet list/rota. No separate push is sent for immediate phone approval. 
After on-phone approval succeeds, control may be handed directly back to the candidate. No 
biometric prompt, password prompt or app reauthentication is required solely because the phone 
has just returned from isolated manager mode. Manager mode must remain isolated from Candidate 
account controls while the manager is using it. 
Email approval uses one secure link for the complete workflow, including all expenses. The link lasts 
seven days. Each request permits the initial send plus five resends. A reminder is allowed only after 
24 hours since the previous send, while the request is live and resend allowance remains. 
An expired unchanged submission may be renewed with a new token and a fresh send allowance. 
Candidate cancellation requires a reason, invalidates the request, notifies the manager and is 
audited. Manager refusal also requires a reason and refuses the whole current workflow/record 
scope. First complete approval wins; completing phone approval invalidates a competing live email 
token. 
Before completion, changing the approval method retires the previous active method atomically. 
Moving from a live email-manager request to paper signing supersedes the request and invalidates 
its token. Moving from an active paper-signing generation back to ELECTRONIC approval retires the 
paper workflow, pack and code and requires a fresh electronic generation. Only one current 
approval route/generation may remain live; historical requests and packs remain auditable but 
cannot later complete the superseded workflow. 
Allowed manager email identities are the union of exact client addresses/domains and exact 
contract addresses/domains. Domain matching is exact; subdomains require their own entry. Client 
settings can allow free entry; contract settings are INHERIT, ALLOW or DISALLOW and an explicit 
contract value wins. 
Free entry permits valid non-public business addresses. Public consumer domains are barred by the 
system default list unless the exact address or exact domain is explicitly approved. The agreed 
starter list is: gmail.com, googlemail.com, hotmail.com, hotmail.co.uk, outlook.com, outlook.co.uk, 
Page 13Decision audit · 12 August 2026
~~~


## Historical decision page 14

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 14 
live.com, live.co.uk, msn.com, yahoo.com, yahoo.co.uk, ymail.com, icloud.com, me.com, mac.com, 
aol.com, proton.me, protonmail.com, mail.com, gmx.com and gmx.co.uk. 
If an office route change withdraws a live emailed request, the link is invalidated atomically. The 
approved email is: 
Subject: Timesheet approval request withdrawn Body: The approval request for this timesheet has 
been withdrawn by CloudTMS. No further action is required. 
 
14. Paper signing and QR lifecycle 
Candidate-facing wording says Paper signing or Print documents for signing; it never requires 
the candidate to understand QR. 
When electronic approval is available, the main choice presents phone and email. Paper is behind 
the secondary wording Choose another option - this process can delay payment, followed by 
Go Back to Electronic Approval and a less prominent Continue with Printed Documents. 
When electronic approval is unavailable but candidate paper is permitted, paper is the normal route 
and no discouraging comparison is shown. 
CloudTMS creates separate branded signing pages: the existing unsigned hours timesheet, 
Expense and Mileage Approval Summary where required, Mileage Claim Form where required and 
one evidence approval page for each source image. 
The QR code appears only on the official main hours timesheet. Supplementary expense-summary, 
mileage-form and evidence-approval pages do not receive separate QR codes; they are bound to 
the same immutable workflow through their manifest page identity, generation, content hash and 
required order. 
The candidate signs the hours page. The manager signs the hours page, summary, mileage form 
and every evidence approval page. The candidate uploads every expected returned page against 
the workflow/QR-bound manifest. A missing, duplicate, foreign or wrong-generation page fails 
closed; one returned page cannot complete the pack. 
Paper preparation is asynchronous. The lifecycle distinguishes route active, QR code generated, 
durable pack ready, pack issued/sent and signed return received. A QR token or generation 
timestamp does not prove that a PDF exists or was issued. 
The initial paper-preparation request queues the existing QR document operation and a held 
candidate email. The candidate does not make a second email request. The email and documents 
ready notification are released only after the official pack is durably ready. 
While waiting, the app shows Preparing documents, checks the Candidate notification feed and 
bounded paper-pack status endpoint while the screen is active, and refreshes on resume/push. 
Download documents is enabled only after server readiness. The broker streams the authorised PDF 
without exposing an R2 key. 
A generated unsigned PDF, manual_pdf_r2_key, generic document asset or generic TIMESHEET 
evidence does not prove a signed return. Signed-return state requires explicit current return 
provenance such as QR scan/signature facts, a Candidate SIGNED_RETURN component or 
evidence/document role that explicitly identifies a returned signed timesheet. 
Page 14Decision audit · 12 August 2026
~~~


## Historical decision page 15

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 15 
Paper submissions never auto-authorise. After a complete return, CloudTMS attempts canonical 
WEEKLY finalisation and leaves any controlled blocker in a retryable received state. An office user 
reviews the signed pack and uses the existing Authorise action. 
Reissuing/replacing a pack retires the old active Candidate workflow, invalidates the old code/pack, 
creates a fresh generation/code/pack and retains all issued/signed history. Ordinary UI does not 
restore a revoked historical QR generation. 
15. Processing, authorisation, payment and invoice progression 
Electronic candidate submission creates a pending workflow; it does not immediately make 
candidate-entered facts canonical. Manager approval plus complete final signed documents allows 
atomic finalisation. 
WEEKLY finalisation uses the existing WEEKLY authority and moves the contract week/current 
financial row into the normal submitted/PENDING_AUTH lifecycle. DAILY finalisation uses canonical 
Save/Recalculate, then existing Process, and reaches PENDING_AUTH. 
Automatic authorisation is allowed only where the effective setting permits it and no checking 
blocker exists. The policy precedence is contract setting when present, otherwise client setting, 
otherwise the global default; the global default is false. 
Automatic authorisation is withheld for applicable issues including unexpected hours, DAILY break 
not equal to 60 minutes, Candidate-entered additional units, HealthRoster validation, duplicate 
expense review and required evidence review. Those informational issues do not prevent an 
authorised CloudTMS office user manually authorising after review unless an existing lifecycle rule 
separately blocks it. 
Paper is never auto-authorised. Import-authoritative hours do not enter Candidate hours approval. 
HealthRoster validation remains a gate where applicable. 
Once authorised, the record enters the existing invoice and payment-eligibility checks. Candidate 
finalisation never executes payment, creates a new payment pathway or bypasses invoice 
readiness. 
16. Rejection, amendment and no-work 
Reject Candidate Submission is whole-record rejection. CloudTMS does not reject an individual 
receipt/page while keeping the rest of the same record active. 
The action is unavailable while the record is authorised; the existing Unauthorise process must run 
first. It is blocked for invoiced, paid, archived or otherwise financially protected records. 
For a combined hours-and-expenses record, rejection requires the candidate to resubmit the 
complete timesheet and every expense on that record. For a separate expense carrier, rejection 
applies to the complete expense claim on that carrier and does not affect the separate hours record. 
An hours-only record requires the hours timesheet to be resubmitted. 
Successful rejection removes/supersedes the active candidate-submitted evidence and clears 
applicable hours, breaks, additional units, expense values, manifests, signatures and authorisation 
on the new/current record. The editable current lifecycle ends OPEN/UNPROCESSED. Rejected 
historical versions and evidence remain for audit but are not current truth. Old manager links, paper 
packs and QR tokens become unusable. 
Page 15Decision audit · 12 August 2026
~~~


## Historical decision page 16

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 16 
The candidate message states both the problem and the complete resubmission scope: 
• Combined record: Your timesheet and expense claim have been rejected. Problem: 
[specific category/document]. Reason: [office reason]. Because the hours and 
expenses are stored on the same timesheet, resubmit the complete timesheet and all 
expenses on it. Primary action: Resubmit Timesheet and Expenses. 
• Separate expense record: Your expense claim has been rejected. Your hours are not 
affected. You do not need to resubmit your timesheet. Resubmit the complete 
expense claim containing [listed amounts/categories]. Reason: [office reason].  
Primary action: Resubmit Expense Claim. 
• Hours-only record: Your timesheet has been rejected. Show the reason and action Resubmit 
Timesheet. 
Awaiting-manager workflows may be cancelled or replaced as a whole. Received/processed but 
unauthorised amendments follow the existing CloudTMS route. An authorised record must be 
unauthorised first; any later Unprocess requirement remains whatever the ordinary CloudTMS 
lifecycle already requires. Paid/invoiced hours cannot be Candidate-amended; later expenses use a 
new claim generation. 
17. Notifications and communication 
After OS/browser permission, transactional push categories default on. Account > Notifications 
provides a master switch and separate controls for manager approval updates, timesheet/expense 
needs attention, authorisation, payment, approval reminders and resubmission required. 
Turning push off never removes in-app notification truth or record badges. 
Action-required notifications include manager refusal, expired approval, CloudTMS rejection and 
fresh route resubmission. They deep-link to the exact workflow/record and state the complete 
required action. They never deep-link to an individual evidence component because rejection is 
whole-record. 
Informational notifications include manager email approval, authorisation and payment. Ordinary 
Received, processing and evidence upload do not generate noisy pushes. Immediate on-phone 
approval uses the green success screen instead of another push. 
Fresh ELECTRONIC or paper resubmission notifications are idempotent. CONTRACT candidate 
identity is resolved from contract candidate, current TSFIN candidate, workflow candidate and only 
then occupant/GCK fallback; a CONTRACT candidate does not require a GCK to receive the 
notification. 
QR documents ready and replacement-document pushes are released only after the corresponding 
pack is durably ready. 
18. Candidate App functional layout 
The agreed functional layout is clear even though final brand styling and pixel-level designs have not 
yet been approved. 
Page 16Decision audit · 12 August 2026
~~~


## Historical decision page 17

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 17 
App shell 
The login screen shows agency branding, Email, Password, Sign in, First registration and Forgotten 
password. Home presents the entitled DAILY and CONTRACT cards. Top-level navigation also 
includes Notifications and Account. 
Candidate screens use large touch targets, native time controls, one primary decision per screen, 
visible progress and Back navigation. Colour is never the only status indicator. Candidates never 
see carrier IDs, additional_seq, raw timesheet IDs, QR terminology or invoice-stream internals. 
Disabled controls provide a plain-English reason and a valid next action where one exists. Review 
flows show page progress and Back. Validation moves focus to the first error without discarding the 
current work. 
CONTRACT list and entry 
Each card shows client, week-ending day/date, job title/band, hours total/status, expense 
summary/status, relevant approval/checking badge and the current View/action. Hidden expense 
carriers are overlaid beneath the correct worked row. 
Opening a week shows all seven dates, then day/segment editing, break entry, Additional Units 
where configured, total review and the optional expense question. Expense entry asks What would 
you like to claim? and presents only the selected category flow. After a category, the user sees 
the claim summary and Add another expense type or Continue. 
Approval selection presents phone and/or email according to policy. On-phone manager review is a 
controlled handover mode showing only the required documents. Email status shows sent time, 
expiry and eligible reminder/cancel actions. Paper presents one signing set but keeps each 
expected page separate for return. 
Candidate-visible states and recovery actions 
The candidate-facing status vocabulary includes, where applicable: No timesheet submitted yet, 
Saved on this device, AWAITING MANAGER APPROVAL, Awaiting signed documents, Preparing 
documents, Awaiting office review, Received, Authorised, Paid, Rejected, Invoiced - not 
paid, Hours needs checking and Requires Checking for Duplicate. Canonical paid, authorised 
and invoiced state takes precedence over historical workflow state. 
The corresponding recovery/action vocabulary includes, where applicable: Review & Resubmit, 
Resubmit Timesheet, Resubmit Timesheet and Expenses, Resubmit Expense Claim, Request 
Approval Again, Continue Existing Claim, Cancel Entire Claim and Start Again, Continue 
Expense Claim and Discard Expense Claim. The server-provided capabilities and lifecycle 
determine which action is available; the app does not infer eligibility from the label alone. 
DAILY 
The existing rota tile meanings remain. An eligible booked tile shows a clear timesheet action, then 
the single-shift identity, booked-versus-actual hours question, break/no-break choice, review, 
candidate signature and permitted manager approval route. It returns to the rota when complete. 
Page 17Decision audit · 12 August 2026
~~~


## Historical decision page 18

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 18 
Notifications and Account 
Notifications show action-required and informational items with direct navigation to the affected 
record. Account contains candidate selection where TEST requires it, password, notification 
preferences and logout. 
19. CloudTMS office presentation 
Existing Simple Timesheet, Timesheet Summary, Bulk Process, Bulk Authorise, Invoice Generator 
and Invoice Issuer remain recognisable. No replacement modal family or internal carrier-navigation 
scheme is introduced. 
Use existing View Related Records and Add Additional Manual Timesheet. Do not add Open 
Linked Hours/Expense buttons, general ELECTRONIC/HOURS/EXPENSES SEPARATE/LINKED 
badges, linked-carrier banners, a QR-signature-check badge or visible invoice-stream labels. 
Approved presentation changes 
• Simple Timesheet shows Awaiting Manager Approval and Manager Approved in the Overview 
Stage area and Issues tab. Manager Approved uses the existing positive/green style. 
• Timesheet Summary shows Awaiting Manager Approval, a green Manager Approved badge 
and Unexpected hours - needs checking using the existing warning style. 
• Bulk Authorise shows red Awaiting Manager Approval and green Manager Approved in both the 
left-row and selected right-pane locations. 
• Bulk Process uses a 4px border around the middle timesheet preview: existing success green 
when the viewed timesheet may be attached, existing danger red when it may not. The border 
has no tooltip or explanatory copy. 
• Unavailable ATTACH choices remain visible but disabled. TIMESHEET is enabled only when the 
record can accept one and has no active TIMESHEET evidence. It is always disabled for an 
expense-only record. 
• Bulk Process preserves row selection, list scroll, current tab, preview and evidence selection 
across save/process refresh, and a problem on one row does not prevent work on another 
eligible row. 
• The existing Expenses action is disabled on a separated hours-only record. Its exact hover text 
is: This timesheet cannot have expenses attached. Create an additional timesheet to 
add expenses. 
• The action label is Reject Candidate Submission. The exact generic confirmation is: This will 
reject the evidence submitted and require the candidate to resubmit. Are you sure 
you wish to continue? Combined and expense-only records do not add more copy to that 
CloudTMS modal; the Candidate App explains scope. 
• Send Manager Reminder appears in Simple Timesheet Overview > Actions. Send Manager 
Reminders appears next to Focus in the Timesheet Summary toolbar and operates on ticked 
rows. The server silently filters to eligible live email requests. The action remains visible but 
disabled when no selected row is eligible. 
• The existing Detailed/Simple Timesheet workflow area retains candidate-safe manager workflow 
information where available: approval route/method, current approval status, request/send timing, 
expiry, resend/reminder count or remaining allowance, eligible Send Manager Reminder, and 
Page 18Decision audit · 12 August 2026
~~~


## Historical decision page 19

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 19 
permitted cancellation status/action. This remains inside the existing timesheet UI; no 
replacement workflow screen or modal family is introduced. 
• Invoice Generator and Invoice Issuer show the existing-style badge Expense Email missing 
where applicable. No new invoice screen or stream label is added. 
• The replacement paper/QR email includes: Please remember to sign the replacement 
timesheet before returning it. 
All route-conversion actions use one server preview and one shared frontend warning renderer. The 
first click never mutates route state. The exact warning wording is in the appendix. 
No Candidate or CloudTMS warning/confirmation may use the browser or operating-system native 
alert, confirm or prompt window. The existing styled CloudTMS confirmation-modal family must be 
used after its exact current source function is verified. Every changed modal must be visually 
inspected at the supported desktop sizes and corrected if it looks untidy, clipped or inconsistent with 
the existing product. 
20. Route conversion and office intervention 
Route conversion is a server-owned preview → warning → confirm operation. Preview returns the 
permitted action, warning, reason requirement, current lifecycle row signature and context hash. 
Confirmation carries that exact signature/hash. CloudTMS locks and recomputes the context before 
mutation; a manager approval, authorisation, invoice lock or route change between preview and 
confirm produces a controlled conflict rather than a partial transition. 
ELECTRONIC/QR to MANUAL is exceptional office intervention, not the normal correction route. 
When the candidate can simply correct and resubmit, the office uses Reject Candidate Submission. 
Exceptional conversion requires one closed reason: 
• Candidate supplied manual timesheet; 
• Candidate reported hours incorrect; 
• Hiring manager reported hours incorrect; 
• Electronic submission technical failure; 
• Other exceptional office intervention, with a mandatory note. 
Leaving an active Candidate submission atomically cancels/supersedes every matching live 
workflow, manager request, token and active component before the clean new route generation 
becomes current. More than one unexpected active workflow fails closed rather than leaving a live 
token behind. 
MANUAL to ELECTRONIC creates a fresh unsigned logical ELECTRONIC generation, preserves 
factual hours/economics, reuses no old signatures/documents and notifies the worker to resubmit. 
MANUAL to QR creates a fresh generation, token and pack and notifies only after the pack is ready. 
Ordinary QR restore actions are removed when the confirmed route feature is enabled. Exact 
ELECTRONIC restore is an internal-only technical undo and is available only when CloudTMS 
proves every component in the immutable manager-approved manifest, both signatures, final 
document identity and financial fingerprint are unchanged. If any required page is missing, replaced 
or superseded, the normal fresh ELECTRONIC route is required. 
The full approved warning catalogue is reproduced once in the appendix and is controlling for the 
later frontend. 
Page 19Decision audit · 12 August 2026
~~~


## Historical decision page 20

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 20 
21. Concurrency, recovery and audit 
Manager approvals for different workflows do not take a global application lock. Fifty managers 
approving fifty different shifts are handled independently; PostgreSQL serialises only conflicting work 
on the same workflow/timesheet. 
A single workflow uses request/workflow idempotency, immutable manifest hashes, workflow 
generation and row signatures to reject double-clicks, retries and stale approvals. First complete 
approval wins. 
Approval commits before asynchronous final rendering/finalisation. A failure on one workflow does 
not roll back or block unrelated approvals. The failed workflow retains durable approval and a 
controlled retry state; office retry does not require another manager signature. 
Carrier allocation, one-claim enforcement, route changes, manager first-complete-wins, token 
rotation and finalisation use appropriate row/advisory locks and idempotency. No partial canonical 
timesheet or partial paper return is accepted. 
Audit records include account, selected candidate, session, workflow generation, manager 
route/name/position, cancellation/rejection/intervention reason, correlation identity and CloudTMS 
service actor. 
22. Delivery order and verification 
The agreed delivery sequence is: 
1. DB/RPC authority; 
2. CloudTMS backend and private Candidate API; 
3. separate public Candidate broker security/contract closure; 
4. CloudTMS frontend; 
5. Candidate App for iPhone/Android/responsive web and public manager pages. 
The DB/RPC and backend stages remain part of the living implementation plan and are not 
discarded when later stages start. Every audit-driven correction updates that cross-stage plan so 
frontend, broker and app behaviour stays coherent with the installed authorities. 
Every remaining implementation stage must consult this current-decisions document before 
changing behaviour. The latest complete PDF must be included in every Candidate App handover 
pack supplied for independent verification; a handover is incomplete without it. 
No stage is considered complete only because code compiles. Verification includes clean 
PostgreSQL installation, runtime and concurrency suites, ACL and feature-off compatibility, focused 
and complete backend regression, frontend local-patched-asset/browser tests, 
route/renderer/invoice regressions, device/browser testing, accessibility, security, performance/load 
and an independent audit gate. 
SQL handovers and repository install sets contain only the latest current schema/functions. A 
qualified function appears once; existing functions are replaced in their authoritative repeatable file 
rather than duplicated as historical alternatives. 
All rollout begins in TEST. Production deployment, Candidate activation, live manager email/push 
delivery and app-store publication require separate explicit approval. 
  
Page 20Decision audit · 12 August 2026
~~~


## Historical decision page 21

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 21 
23. Route-conversion warning catalogue 
The warning catalogue below is the exact approved copy for one shared CloudTMS frontend 
renderer. Warning codes are existing product/API codes, not decision-register numbering. 
ELECTRONIC, nobody signed — ELECTRONIC_UNSIGNED_TO_MANUAL 
Convert this timesheet to Manual? 
The candidate will no longer be able to submit this version electronically. CloudTMS staff will be 
responsible for entering and processing the timesheet. 
Use this only where the candidate has supplied the timesheet outside the electronic route, or 
CloudTMS staff need to intervene. 
Buttons: Go Back / Convert to Manual. A mandatory intervention reason is shown before final 
confirmation. 
Candidate signed; manager pending — 
CANDIDATE_SIGNED_MANAGER_PENDING_TO_MANUAL 
The candidate has already signed this electronic timesheet 
It is awaiting hiring-manager approval. Converting it to Manual will cancel the current electronic 
approval request and the manager's approval link will stop working. 
The candidate's signed electronic submission will remain in the audit history but will not apply to the 
new Manual version. 
Use Reject Candidate Submission instead where the candidate can simply correct and resubmit 
the timesheet. 
Buttons: Go Back / Continue to Manual conversion. A mandatory intervention reason is shown 
before final confirmation. 
Manager approved; CloudTMS not authorised — MANAGER_APPROVED_TO_MANUAL 
The hiring manager has already signed and approved this electronic timesheet 
Converting it to Manual will retire the completed electronic approval. The candidate and manager 
signatures and the signed electronic timesheet will remain in the audit history, but they will not apply 
to the new Manual version. 
This action should be used only where the candidate or hiring manager has reported that the 
submitted hours are wrong, or another exceptional office intervention is required. 
Buttons: Go Back / Continue to Manual conversion. A mandatory intervention reason is shown 
before final confirmation. 
CloudTMS-authorised — ROUTE_CHANGE_REQUIRES_UNAUTHORISE 
Unauthorise this timesheet first 
This timesheet has been authorised in CloudTMS. It cannot be converted to Manual until the existing 
Unauthorise process has been completed. Unprocess it afterwards where the normal lifecycle 
requires this. 
Page 21Decision audit · 12 August 2026
~~~


## Historical decision page 22

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 22 
Button: Close. An existing Unauthorise action may be shown, but route conversion remains blocked. 
Invoiced or paid — ROUTE_CHANGE_FINANCIAL_HISTORY_BLOCK 
This timesheet cannot be converted 
This timesheet has financial history and its submission route cannot be changed. Use the 
appropriate additional-timesheet, correction, credit or reversal process. 
Button: Close. 
Import-authoritative — ROUTE_CHANGE_IMPORT_AUTHORITATIVE_BLOCK 
This timesheet is controlled by an import 
Candidate-entered hours are not permitted for this timesheet. The submission route cannot be 
changed. Expenses must use the separate expense-timesheet route. 
Button: Close. 
Reject or office intervention decision 
Does the candidate need to resubmit instead? 
Use Reject Candidate Submission where the candidate can correct and resubmit the timesheet 
themselves. 
Convert to Manual only when CloudTMS staff need to enter or process the replacement timesheet 
on the candidate's behalf. 
Buttons: Go Back / Use Reject Candidate Submission / Continue to Manual conversion. 
Issued paper pack not returned — QR_ISSUED_TO_MANUAL 
A timesheet pack has already been issued 
The candidate may already have printed the documents. Converting this timesheet to Manual will 
invalidate the current code and the issued pack can no longer be returned. 
CloudTMS staff will become responsible for entering and processing the replacement timesheet. 
The issued pack will remain in the audit history. 
Buttons: Go Back / Continue to Manual conversion. A mandatory intervention reason is shown 
before final confirmation. 
Signed paper evidence returned — QR_SIGNED_TO_MANUAL 
A signed timesheet has already been returned 
Converting it to Manual will retire the signed returned evidence. The signed document will remain in 
the audit history but will not apply to the new Manual generation. 
Continue only if the candidate or hiring manager has reported that the submitted hours are wrong, or 
another exceptional office intervention is required. 
Buttons: Go Back / Continue to Manual conversion. A mandatory intervention reason is shown 
before final confirmation. 
Page 22Decision audit · 12 August 2026
~~~


## Historical decision page 23

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 23 
MANUAL to fresh ELECTRONIC — FRESH_ELECTRONIC_RESUBMISSION_REQUIRED 
Switch this timesheet back to Electronic? 
The current timesheet will be opened as a fresh electronic submission. Previous signatures and 
signed documents will remain in the audit history and will not be reused for changed content. 
The worker will be notified that the timesheet must be reviewed and resubmitted. 
Buttons: Go Back / Switch to Electronic and notify worker. 
Worker notification title: Your timesheet needs to be resubmitted. Body: Open the app to 
review and submit your timesheet again. Deep-link to the new current timesheet/workflow. 
MANUAL to fresh paper submission — FRESH_PAPER_RESUBMISSION_REQUIRED 
Create new signing documents? 
A new timesheet pack and a new code will be created for the current hours. Any older pack or code 
will remain historical and cannot be returned. 
The worker will be notified that new documents are ready and the timesheet must be resubmitted. 
Buttons: Go Back / Create new documents and notify worker. 
Worker notification title: Your timesheet needs to be resubmitted. Body after durable pack 
readiness: New documents are ready for signing. Open the app to continue. 
QR replacement/reissue — QR_REPLACEMENT_PACK_REQUIRED 
Issue a replacement timesheet pack? 
The current pack and code will be invalidated. Any printed copy can no longer be returned. A 
replacement pack with a new code will be generated. 
Please remember to tell the worker to sign the replacement timesheet before returning it. 
Buttons: Go Back / Issue replacement pack. 
Candidate email: Please remember to sign the replacement timesheet before returning 
it. Recommended push after readiness: Replacement documents are ready / Please sign and 
return the replacement timesheet. 
Internal exact ELECTRONIC undo — EXACT_ELECTRONIC_RESTORE_PROVEN 
This action is not shown in ordinary route actions. 
Restore the previous electronic approval? 
CloudTMS has proved that the current hours, signatures, signed document and financial content are 
identical to the previous electronic submission. Restoring it will make that exact approved electronic 
generation current again. 
No worker resubmission will be requested. 
Buttons: Go Back / Restore exact electronic version. If exact proof fails, use the fresh 
ELECTRONIC route. 
  
Page 23Decision audit · 12 August 2026
~~~


## Historical decision page 24

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 24 
24. Decisions still to be approved 
The following were not found as final approved decisions in this task history or its supplied 
documents: 
Candidate App visual design 
The functional screen hierarchy, journeys, labels and required controls are agreed. A pixel-level 
visual design is not. Brand assets, final colour palette, typography, iconography, spacing, card 
appearance, motion and final iPhone/Android/web responsive mock-ups still require approval before 
the app frontend is treated as visually frozen. 
Cross-platform implementation technology 
No cross-platform implementation technology has been explicitly approved. That choice must be 
presented for approval after the backend/frontend contract audit and must not be recorded as settled 
policy beforehand. 
Push provider and application publishing 
The push provider/token format, Apple Developer organisation, Google Play organisation, app 
identifiers, signing identities, store listings, privacy-policy URL, support contact, screenshots and 
final pilot/publishing dates remain delivery decisions. The notification rules and timing are already 
agreed; the provider and publishing details are not. 
Final public domains and configuration 
The final Candidate App/web URL, public manager-approval URL and LIVE broker configuration are 
not yet frozen. TEST/LIVE separation and fail-closed environment binding are already fixed. 
Independent backend/API audit findings 
The current living plan records the DB/RPC authority, CloudTMS Candidate backend, private 
Candidate API and separate public broker foundation as implemented and deployed to TEST with all 
Candidate flags disabled. Independent final API-freeze verification remains the next gate. Any new 
verified finding may require implementation remediation, but it does not become a product-policy 
change unless the user explicitly approves changed behaviour. 
  
Page 24Decision audit · 12 August 2026
~~~


## Historical decision page 25

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS 
Page 25 
25. Omissions and limitations 
This audit used the complete recoverable history of this Codex task, the original engineering pack, 
the original Final Presentation Decisions Word document, later pasted independent audits/addenda, 
the 10 August suggested inclusion pack, the current installed SQL/repeatables, current Candidate 
backend documents and the current route-warning catalogue. 
Superseded decisions are intentionally omitted, as requested. Only the current replacement rule is 
stated in the decision sections above. 
Two earlier interpretations remain expressly superseded: WEEKLY Additional Units do not use a 
planned percentage-deviation comparison, and CloudTMS does not automatically interpret/reconcile 
handwritten Mileage Claim Form journey rows. Their current replacement rules appear in sections 6 
and 9. 
Raw SQL bodies, backend source code and detailed function inventories are not reproduced in this 
decision document because they are implementation evidence rather than product decisions. They 
remain in the backend repository and living implementation plan. 
The audit cannot include decisions made outside this task and not supplied in an attachment, or 
private discussions that were never recorded here. No such omitted decision is known, but their 
absence cannot be proven from this task alone. 
The original Final Presentation Decisions Word document was extracted structurally, rendered 
through Microsoft Word and visually checked across all four source pages before its current 
decisions were reconciled here. 
26. Source precedence used for this audit 
Where sources conflicted, the latest explicit approved instruction governed. The reconciliation order 
was: 
1. later explicit user decisions and controlling addenda in this task, including the approved 10 
August inclusion review; 
2. later independent audit findings that the user explicitly accepted and instructed to implement; 
3. the current approved W01–W13 route-warning catalogue; 
4. the current living implementation plan, backend API contract and authority map; 
5. the Final Presentation Decisions addendum; 
6. Controlling Policy v7.0 and Full UI/App Flow Specification v3.0 for matters not later changed; 
7. implementation-plan details only where they did not create new product behaviour. 
Current code and database truth were used to confirm implementation status and authority shape. 
Code did not silently override an explicit product decision. 
Page 25Decision audit · 12 August 2026
~~~


## Historical decision page 26

~~~text
CLOUDTMS CANDIDATE APP   |   CURRENT DECISIONS
27. Office switch to Manual
Incomplete expense claim confirmation
This rule applies when CloudTMS office staff switch an ELECTRONIC or QR/PAPER timesheet to MANUAL and the
candidate has one separate expense claim that has started but is not complete. The office must not be permanently
blocked, and the claim must not be left active against a timesheet version that is about to become historical.
Mandatory office warning
The candidate has started an expense claim but has not completed it. Do you want to remove the
incomplete claim and continue?
If the office selects No
Nothing changes. CloudTMS keeps the existing timesheet route and the incomplete expense claim exactly as they are.
No workflow, approval request, email, notification, QR identity, evidence or timesheet version may be changed.
If the office selects Yes
CloudTMS must revalidate and lock the preview context, then complete one atomic transaction: retire any obsolete
unsent PAPER delivery and readiness notification; invalidate the obsolete QR identity where applicable; cancel or
supersede a live manager request and queue the established manager-cancellation message where required;
supersede the incomplete expense workflow and its mutable lineage; and only then create the new current MANUAL
timesheet version. Sent mail, signed documents, R2 objects and other immutable audit history remain retained.
REFUSED and safety boundaries
REFUSED remains a recoverable state because AMEND is available. It therefore receives the same warning. Yes
supersedes that amendable lifecycle but retains the refusal reason, refusal timestamp and audit history; No leaves it
available to amend. More than one affected workflow, an ambiguous identity, an unrelated claim, protected financial
history or a live provider handoff must still fail closed. A temporary provider-handoff conflict may be retried; it must not
silently discard the claim.
Presentation and future implementation
The office frontend must use the established professional CloudTMS confirmation-modal family, with clear Yes and No actions.
Native browser or Windows alerts are not permitted. The modal must remain uncluttered and must be browser-tested with
realistic populated data. Before frontend work, the DB/RPC/backend contract and regression matrix must independently prove
No as zero mutation, Yes as complete atomic removal-plus-conversion, REFUSED history retention, provider fencing, and zero
stranded active expense claims. This decision is mandatory input to the detailed office-frontend plan and, later, the Candidate
broker, responsive-web, iOS and Android plans.
Page 26
Page 26Decision audit · 12 August 2026
~~~


## Historical decision page 27

~~~text
CLOUDTMS CANDIDATE APP  |  CURRENT DECISIONS
Page 27
Decision audit · 12 August 2026
28. Exact replay and PAPER execution authority
Caller-owned semantic request identity
Every factual Candidate mutation requires the caller's bounded idempotency key. The backend must reject a missing key and
must not substitute a random UUID. The durable receipt binds workflow, action, expected generation, caller/channel and factual
inputs. Exact reuse returns the first durable result; another action or materially different payload returns
CANDIDATE_IDEMPOTENCY_CONFLICT.
WORKER_SUBMIT builds its semantic request and probes the receipt before rebuilding mutable workflow, financial or
presentation material. The Candidate's signing time and factual claim/signature identity are included. Regenerated official
presentation is excluded. Manager replay similarly excludes generated email, token and delivery values while reminder and
renewal remain bound to the exact approval-request ID and generation.
Canonical finalisation completion
WEEKLY, PHONE, PAPER and DAILY completion is bound to the original workflow generation and one immutable approval
identity, not to whichever valid trigger key first completes the work. EMAIL/PHONE binds approval request ID, request
generation, method and immutable review digest. PAPER binds the exact returned manifest.
If PAPER_RETURN key K reaches RECEIVED but its immediate finalisation is blocked, a later retry key R may complete
finalisation. Replaying K must then return the same committed FINALISED result. Mutable workflow generation/state, row
signature and regenerated DAILY materialisation are considered only when no completion exists. A different approval or
manifest cannot inherit another approval's result.
Exact manager-request ownership
Reminder and renewal use the request identity in the server action envelope. A newer request cannot silently replace the
request the user acted upon. Generated mail/token material does not change semantic replay identity.
Canonical office rejection
The rejection authority locks actor plus key and binds environment, target, expected current identity, expected row signature
and trimmed reason. Exact retry returns the durable result. Another target, signature or reason under that key fails before
timesheet, workflow, notification, QR or mail mutation.
~~~


## Historical decision page 28

~~~text
CLOUDTMS CANDIDATE APP  |  CURRENT DECISIONS
Page 28
Decision audit · 12 August 2026
28. Exact replay and PAPER execution authority
Shared PAPER preparation states
- PREPARING - source document or pack work is still incomplete; pack retry is unavailable.
 
- BACKOFF - a retryable assembly failure exists but the next attempt is not due.
 
- FAILED_RETRYABLE - a closed transient assembly code, durable attempt count and due retry time exist.
 
- FAILED_TERMINAL - scheduler and Office execution stop; unsent mail remains inert and evidence is retained.
 
- READY - the exact complete-pack receipt is installed and normal provider fencing may continue.
 
- RETIRED / STALE - no preparation or retry authority remains.
 
Source-document readiness and failure ownership
The 15-minute preparation deadline detects stalled work but does not make an upstream source document retryable. A missing
or non-ready source document remains PREPARING, including after the deadline, because pack retry cannot make that
document ready. Only after the exact source document is READY can a transient pack-assembly failure become
FAILED_RETRYABLE. A terminal source-document failure remains terminal.
One executor, backoff and crash recovery
Every actual pack attempt has a unique inner attempt identity and acquires the exact database lease for one workflow, delivery
generation and manifest. Only a newly acquired claim may perform R2 reads or assembly. Replaying an earlier CLAIMED
receipt is observational and must return claim_acquired_new=false. Only the matching operation ID and token can release
the pack or record failure.
Retryable assembly failures use bounded backoff of 1, 5, 15 and then 30 minutes. Office must not bypass next_retry_at_utc. If
an executor crashes after claiming, another attempt remains blocked while the ten-minute lease is active and may acquire a
fresh attempt only after expiry. Attempt count advances only for an actual newly claimed execution.
Office retry outcome and preserved decisions
The Office retry UUID remains the durable outer operation. A concurrent exact call may return transient RETRY_IN_PROGRESS, but
that response is not frozen as final. Completion stores the full READY, FAILED_RETRYABLE or FAILED_TERMINAL result and
HTTP status; every later exact retry receives that result. No response exposes R2 storage identity.
Current/History, the incomplete-expense Yes/No warning, recoverable REFUSED rule, finalised history, provider fencing and the
seven-table/fourteen-public-RPC boundary remain unchanged. No DAILY/WEEKLY economics, pay, charge, VAT, ERNI, margin,
TSFIN, invoice, payment, Banking Pay, Policy X, settlement, remittance or production authority changes.
~~~


## Historical decision page 29

~~~text
CLOUDTMS CANDIDATE APP  |  CURRENT DECISIONS
Page 29
Decision audit · 12 August 2026
29. Authentication and operation-replay closure
Caller-owned mutation identity
Every factual Candidate authentication, account and session mutation requires one caller-owned bounded
idempotency_key. The backend never substitutes a random operation key. The mandatory set is:
- challenge start, resend and verify;
 
- password activation/reset completion, login (including failed-login lockout mutation), refresh and logout;
 
- TEST candidate selection, notification preferences, notification read acknowledgement, push-token registration and
 password change.
The Candidate OpenAPI requires the key on every listed request. Same key plus the same factual request returns the
same durable result. The same key with a different action, email/purpose, token identity or payload fails with
CANDIDATE_IDEMPOTENCY_CONFLICT.
Secrets and lost-response recovery
Passwords, raw refresh tokens, raw challenge tokens and push-token ciphertext are not stored in ordinary audit receipts.
Generated refresh, challenge and phone handoff material uses retained versioned key authority so an exact replay remains
usable across an approved key rotation.
If refresh succeeds but its response is lost, repeating the old session/token with the original key returns the same
successor session and refresh token without revoking the token family. Reusing that rotated token with a different key
remains a security event. A failed login may advance the lockout counter once; its exact replay returns the same rejection
and cannot increment the counter again.
Finalisation and phone replay
Finalisation performs a key-only durable receipt lookup before reading current approval state. A committed WEEKLY
EMAIL/PHONE, PAPER or DAILY result therefore replays after its approval request becomes historical or superseded,
without duplicate financial placement, final documents or notifications. Phone semantic identity excludes generated
token/hash/expiry facts and an exact replay returns the originally usable token under its recorded key version.
PAPER observation, execution and Office crash recovery
A missing or non-ready source document remains PREPARING, non-retryable and attempt-count zero, including after its
observation deadline. Only a READY source may acquire the exact pack attempt. Genuine render, source-read, R2 or
assembly failures after that claim use the closed retryable/terminal catalogue and bounded backoff.
For Office PAPER retry, the canonical inner READY or failure transition and the outer Office UUID result are written in one
database transaction. The database can reconstruct a missing legacy outer receipt from the exact inner operation state
after a Worker crash; every later exact call replays that result. No response exposes an R2 storage key.
Preserved boundaries
Current/History, route/rejection, resubmission, manager-mail, immutable evidence, provider permits and the
seven-table/fourteen-public-RPC boundary remain unchanged. No DAILY/WEEKLY economics, pay, charge, VAT, ERNI,
margin, TSFIN, invoice, payment, Banking Pay, Policy X, settlement, remittance or production authority changes.
~~~


## Historical decision page 30

~~~text
CLOUDTMS CANDIDATE APP  |  CURRENT DECISIONS
Page 30
Decision audit · 12 August 2026
30. Public credential replay and push-token identity
One stable public authentication result
A successful login, password activation/reset or refresh has one public result. The public Candidate session ID is a stable
opaque identifier derived from the environment and immutable private session identity. Repeating the same factual request
with the same operation key returns the same public session ID, access token, refresh token and frozen expiry facts; the
broker must not generate a fresh public UUID or wall-clock timestamp on replay.
Deterministic authenticated public envelopes
New public access, refresh and PHONE handoff credentials use the reviewed v2 deterministic authenticated envelope.
Canonical plaintext receives a purpose-separated HMAC identity and its own HMAC-derived AES-GCM key. Distinct
canonical plaintext therefore does not reuse one key/nonce pair; an exact replay intentionally reproduces the identical
authenticated string. Existing valid random-IV v1 envelopes remain readable during rollout.
Public access/refresh claims use the private database receipt's issued, session-expiry and absolute-expiry facts.
Selected-candidate access uses the initiating access credential's frozen issue time. PHONE handoff uses the approval
request's frozen creation and expiry times. Changed purpose, payload, device or session binding must fail validation.
Concurrent database winner
For activation/reset, login and refresh, two simultaneous exact requests may propose different private session IDs. After the
database returns the durable winner, each handler derives the returned refresh token from the result.session_id, recorded
token-key version, action and caller operation key. A losing provisional token is never returned beside the winning session.
Push-token storage versus factual identity
The raw push token remains broker-only and is encrypted for storage with a fresh random AES-GCM IV. Random storage
ciphertext is not factual idempotency identity. The broker separately supplies a stable, purpose-separated HMAC over:
- environment and provider;
 
- public Candidate session identity;
 
- the exact raw push token.
 
The private request receipt hashes that semantic identity and key versions, not randomized ciphertext. Same raw
token/provider/session plus the same operation key replays one success; a changed token, provider or session under that key
conflicts. Neither the raw token nor its randomized ciphertext is written into ordinary audit receipts.
Concurrent mutable-precondition recovery
If an exact simultaneous logout reaches an already-revoked session, or an exact simultaneous password change reaches the
newly written password verifier, the private API performs one second exact receipt lookup. Matching receipt returns the first
durable result; a different factual request conflicts; absence of a receipt preserves the original authentication failure.
Preserved product and authority boundaries
The Current/History split, 16-week History limit, week-ending ordering and labels, row-tap detail/action hub, incomplete-claim
warning before MANUAL conversion, Office actions, route/rejection rules, seven Candidate tables and fourteen public
Candidate RPCs remain unchanged. No frontend implementation, DAILY/WEEKLY economics, pay, charge, VAT, ERNI,
margin, TSFIN, invoice, payment, Banking Pay, Policy X, settlement, remittance or production authority changes.
~~~


## Historical decision page 31

~~~text
CLOUDTMS CANDIDATE APP  |  CURRENT DECISIONS
Page 31
Decision audit - 12 August 2026
31. Public authentication boundary, replay and key
rollout
Closed public authentication routes
Unauthenticated Candidate authentication is a closed catalogue: challenge start/resend/verify, password completion, login and
refresh only. Logout is authenticated. The broker must open the public access credential and forward the exact internal bearer to the
private logout receipt owner. Missing or invalid public access returns 401 and cannot reach the mutation.
Challenge result truth
Challenge start and resend require one caller-owned bounded idempotency key. Missing, blank or oversized keys return 400. Same
key plus changed email, purpose or challenge identity returns 409 CANDIDATE_IDEMPOTENCY_CONFLICT. Only Candidate
eligibility and account-state outcomes are enumeration-masked to the generic 202 accepted response; outages and throttling remain
visible as service/429 outcomes.
Versioned public credentials
New access, refresh and same-phone PHONE credentials use deterministic authenticated v3 envelopes containing the issuing key
version. Each credential authority has one current writer version and an explicit reader catalogue. Login, activation/reset and refresh
freeze the chosen access, refresh and public-session mapping versions in the durable private result. Exact replay therefore
reconstructs the byte-identical public result after a supported rotation. Refresh retains the initiating public-session mapping version.
The broker proves all selected wrapping secrets before the private mutation starts.
Retained v1/v2 readers support controlled rollout. Removing a version from the reader catalogue makes that version unacceptable
even if its secret remains bound. The approved rollback target is the separately published v3-compatible reader deployment, with
former writer versions restored while every version issued by the newer deployment remains readable until its bounded credentials
and receipts expire. A pre-v3 binary is not an approved rollback target.
PHONE and push semantic binding
PHONE selection binds the workflow/generation/key to the initiating public-session digest and, where supplied, device digest before
any handoff token is generated. Exact replay returns the same usable private and public handoff using the recorded key versions;
another workflow/session or changed supplied device conflicts.
Push storage and factual identity remain separate. Random-IV ciphertext embeds its positive database-safe storage-key version. A
versioned HMAC binds environment, provider, public session and raw token. During approved identity-key overlap the broker
supplies proofs for configured reader versions and the private durable receipt reselects its frozen semantic version. Storage drift or
supported key rotation replays; changed token/provider/session conflicts.
Release verification and preserved boundaries
Before independent GO, each affected public mutation must prove:
- public broker -> signed private API -> RPC/database -> durable private result -> public response;
 
- exact and concurrent replay, changed-input conflict, generated-execution drift and crash recovery;
 
- supported writer rotation, reader retirement and approved rollback compatibility;
 
- one factual mutation and no duplicate communication/provider side effect.
 
The Current/History split, 16-week History window, row action hub, incomplete-claim MANUAL warning, Office modal behaviour,
seven Candidate tables and fourteen public Candidate RPCs remain unchanged. Candidate and Office frontend wiring remains
blocked until independent API GO. No DAILY/WEEKLY economics, rates, pay, charge, VAT, ERNI, margin, TSFIN, invoice,
payment, Banking Pay, Policy X, settlement, remittance, production or future Office-role policy is changed.
~~~


## Historical decision page 32

~~~text
CLOUDTMS CANDIDATE APP  |  CURRENT DECISIONS
Page 32
Decision audit - 12 August 2026
32. Final public-authentication throttle and key authority
Resend acceptance and durable throttling
A public challenge resend returns 202 accepted only when the database has durably accepted the operation. A resend attempted too
soon returns a visible 429 response with server-owned retry timing. A resend that has exhausted the permitted allowance returns a
stable terminal 429 response. Neither condition is enumeration-masked as successful acceptance.
A throttle result consumes the supplied idempotency key and is recorded as the durable result. The same key and same factual request
always replays that result, even after time advances. A later permitted resend must use a new caller-owned key. Same key plus
changed challenge identity, email or purpose remains an explicit CANDIDATE_IDEMPOTENCY_CONFLICT.
Challenge-token issuing version and rollback
Every accepted challenge start or resend freezes the exact challenge-token key version in its durable receipt. Challenge-token authority
has one current writer version and an explicit retained-reader catalogue. Exact replay uses the recorded issuing version directly; it does
not infer the version by scanning only up to the current writer.
The supported rotation contract is:
- writer 1 -> writer 2: retained version-1 receipts remain readable while reader 1 is present;
 
- approved writer 2 -> writer 1 rollback: version-2 receipts remain readable while reader 2 is present;
 
- reader retirement: a receipt using the removed version fails with a stable unavailable result.
 
Public credential version integrity
New public access, refresh, public-session and PHONE credentials use deterministic authenticated v4 envelopes. The issuing key
version is cryptographically included in the identity HMAC, per-message AES-GCM key derivation and authenticated data. Changing
only the textual version label therefore invalidates the credential, including where two version slots were accidentally assigned the same
secret material.
Bounded readers for v1, v2 and v3 remain available for controlled compatibility. The broker rejects a credential-authority configuration
where distinct version slots resolve to identical secret material; this also prevents retained legacy v3 credentials from being relabelled
across aliased slots. Removing a version from its reader catalogue makes credentials issued by that version unacceptable.
Unknown-account login idempotency
An unknown-account login receives the same durable, enumeration-safe generic failure receipt as other login outcomes. No Candidate
account is created and no account failure counter is changed. Same key plus the same factual login replays the durable failure;
changed email or password under that key returns an idempotency conflict.
Cutover and preserved boundaries
The v4 writer cutover is approved only under the verified dormant state: all Candidate client flags false, all seven Candidate business
tables empty and Candidate-bound mail empty. Production remains untouched. Any future environment containing retained v3
credentials or receipts must keep the compatible readers and prove its bounded transition before changing its writer.
This decision adds no Candidate business table and no public Candidate RPC. It does not change Current/History, Office row actions
or modals, Candidate workflow economics, DAILY/WEEKLY calculations, rates, pay, charge, VAT, ERNI, margin, TSFIN, invoices,
payments, Banking Pay, Policy X, settlement, remittances, production or future Office role policy. Candidate and Office frontend API
wiring remains subject to independent backend/API GO.
~~~


## Historical decision page 33

~~~text
CLOUDTMS CANDIDATE APP  |  CURRENT DECISIONS
Page 33
Decision audit - 13 August 2026
33. Mixed-version concurrency ownership
Database winner owns challenge delivery
During an approved rolling private-Worker deployment, two instances may propose different challenge-token writer versions for the
same factual START or RESEND. The local token is only a proposal. Every successful or replayed canonical result returns the
database-frozen token hash and issuing version to the private service. After that RPC returns, only those winner facts may control
delivery.
Each handler reconstructs the token from the exact retained winner version, verifies its SHA-256 hash and then attempts the
deterministic create-only mail identity. A losing Worker must never queue its local token, even if its mail write arrives first. Exact replay
may repair a database-committed/mailer-missing crash, but the repaired mail must contain the database winner's token.
Authentication request version is frozen before hashing
For every authentication/account operation, the first caller reserves one request-HMAC key version under the existing environment and
idempotency-key database lock before any password proof, refresh-token proof or factual request hash is calculated. A concurrent
Worker proposing another current writer receives and uses the already frozen version.
The reservation applies to:
- activation/password completion, login success and generic login failure, and refresh;
 
- logout, TEST Candidate selection and notification preference/read mutations;
 
- push-token registration and password change.
 
The reservation and completed durable response are one receipt lifecycle in the existing audit authority. It stores no raw password,
refresh token, challenge token or push token. Same key plus another action or a main request that attempts to replace the frozen
version returns CANDIDATE_IDEMPOTENCY_CONFLICT.
Rotation, rollback and retirement
The current writer version is automatically readable. Additional retained readers are explicit. An in-flight operation continues using its
frozen version across writer rotation or approved rollback while that reader remains configured. Deliberate reader retirement fails closed
with a stable unavailable result before mutable preconditions or business mutation.
Mandatory release proof
Verification must combine concurrency and rotation rather than test them separately: START and RESEND with writer versions 1/2,
either database winner and either mail arrival order; identical login success/failure, activation and refresh requests across
request-HMAC writers; representative authenticated account actions; changed factual input/action; rollback; and reader retirement.
PostgreSQL 17.6 and 18.1 and a real private-handler/database chain are required.
Preserved boundaries
This decision adds no Candidate business table and no public Candidate RPC. It does not change public Candidate routes, Office
modal behaviour, Current/History, workflow or PAPER policy, DAILY/WEEKLY financial calculation, rates, pay, charge, VAT, ERNI,
margin, TSFIN, invoices, payments, Banking Pay, Policy X, settlement, remittances, production or future Office role policy. Candidate
flags remain false and frontend API wiring remains subject to independent GO.
~~~


## Historical decision page 34

~~~text
CLOUDTMS CANDIDATE APP  |  CURRENT DECISIONS
Page 34
Decision audit - 13 August 2026
34. Credential winner and security receipt
Database winner owns PHONE handoff material
During an approved rolling private-Worker deployment, two instances may propose different manager-token writer versions for the same
factual SELECT_PHONE_APPROVAL operation. A locally generated token is only a proposal. The canonical workflow result freezes and
returns an internal approval-token hash and the winning handoff-token key version. Those database facts are authoritative after the RPC
returns.
Every successful or replayed private handler reconstructs the deterministic token from the workflow ID, pre-action generation, caller
operation key and returned winner version; verifies its SHA-256 against the returned winner hash; discards any local proposal; and
removes the internal hash before returning. A losing Worker and the public broker therefore receive the same usable winner token.
Refresh-token reuse is an atomic security operation
Reuse of a rotated predecessor refresh token revokes every ACTIVE or ROTATED session in its token family and returns
CANDIDATE_REFRESH_TOKEN_REUSE with family_revoked true. Family revocation and that negative response must complete the
same durable authentication receipt in the same database transaction.
An exact or concurrent retry using the same factual request and security-event key returns the stored negative security result. It must not
re-evaluate the predecessor after it has become REVOKED, substitute CANDIDATE_SESSION_EXPIRED, derive a successor
credential or repeat a family mutation. A different key remains a separately evaluated security event under the established
theft-protection contract.
Mandatory release proof
Verification must include:
- PHONE first execution through two real private handlers on manager-token writers 1 and 2;
 
- both possible database winners and both returned private/public token-validity checks;
 
- refresh success, rotated-predecessor reuse, deliberately lost security response and exact retry;
 
- simultaneous exact refresh-reuse security requests with one family revocation and one durable result;
 
- PostgreSQL 17.6 and 18.1, the complete backend suite and all three Worker builds.
 
Preserved authorities
Challenge database-winner delivery, authentication request-HMAC version reservation, public v4 credential binding, unknown-account
failure receipts, public session/access/refresh replay, PHONE public-session/device binding, push semantic identity, Current/History,
resubmission, manager mail, PAPER and Office retry remain unchanged. Internal winner hashes are service-only and never public
response data.
No-change boundary
This decision adds no Candidate business table, public Candidate RPC or financial authority. It does not change DAILY/WEEKLY
financial calculation, rates, pay, charge, VAT, ERNI, margin, TSFIN, invoices, payments, Banking Pay, Policy X, settlement, remittances,
production, Office modal functionality or future Office role policy. Candidate flags remain false and frontend API wiring remains subject to
independent GO.
~~~


## Historical decision page 35

~~~text
CLOUDTMS CANDIDATE APP  |  CURRENT DECISIONS
Page 35
35. Account-session invalidation concurrency
One bounded session-lifecycle concurrency owner
Every Candidate session create, rotate or family/account-wide invalidation for one account uses
private._candidate_auth_account_session_lock_v1. The lock is transaction scoped, keyed by environment and account ID, and
unavailable to public, anon, authenticated and service roles. It creates no Candidate business table and no public Candidate RPC.
Mandatory lock order and revalidation
The stable order is authentication receipt/idempotency-key ownership, account-session advisory lock, account row lock, session row
locks, mutation and durable result receipt. If only a session is initially known, its account ID is read as identity only; account and session
state, environment, status, refresh proof, expiry and family are re-read after the shared lock is held.
Participating operations and closed postconditions
- Normal refresh and rotated-token reuse share the same per-account lock. family_revoked=true requires zero ACTIVE or ROTATED
rows in that token family.
- Existing-account activation/password reset leaves only the new reset session ACTIVE.
- Password change leaves only the initiating policy-retained session ACTIVE.
- REVOKE_SESSIONS, LOCK and DISABLE leave zero ACTIVE sessions.
- LOGIN_SUCCESS session creation and five-session cap enforcement use the same lock, so an account invalidation cannot be
bypassed by a concurrent login.
Mandatory adversarial release proof
Disposable PostgreSQL tests must use real independent transactions and execute both operation orders for:
- rotated-predecessor reuse versus legitimate current-successor refresh;
- password change and verified password reset versus another-session refresh;
- REVOKE_SESSIONS, LOCK and DISABLE versus refresh;
- LOGIN_SUCCESS versus DISABLE.
PostgreSQL 17.6 and 18.1 must both prove the exact permitted final session set, no escaped successor, no partial mutation and no
deadlock. Existing PHONE winner, durable refresh security receipt, challenge/key rotation, Current/History, resubmission, provider
permit, PAPER and Office regression authorities remain green.
No-change boundary
This decision does not change Candidate workflow, Office modal functionality, frontend role policy, financial calculations, invoices,
payments, Banking Pay, Policy X, settlement, remittances or production. Candidate feature flags remain false and frontend API wiring
remains subject to independent GO.
Decision audit - 13 August 2026
~~~


## Historical decision page 36

~~~text
CLOUDTMS CANDIDATE APP  |  CURRENT DECISIONS
Page 36
36. Locked password-authority revalidation
Worker verification is preflight, not mutation authority
The private Candidate Worker may load a password verifier and perform PBKDF2 work before the database call, but that mutable
REST snapshot never authorises login session creation, failed-login accounting or password replacement. The canonical database
transition revalidates the exact authority after its transaction locks.
Non-plaintext proof and locked comparison
- For a known account, the Worker sends a derived presented-password digest and a SHA-256 fingerprint of the exact verifier
authority it inspected: account, scheme/version, salt, digest and canonical parameters.
- No plaintext current or new password enters PostgreSQL or an ordinary durable receipt.
- After receipt ownership, the shared account-session advisory lock and account-row FOR UPDATE, PostgreSQL recomputes the
fingerprint and compares the presented digest with the locked current digest.
Closed concurrent outcomes
- A stale positive login cannot create a session after password reset or change.
- A stale negative login cannot increment the failed-login counter after the verifier changes; only a genuine mismatch against the
current locked verifier may increment once.
- Of two concurrently preverified password changes, only the proof matching the current locked verifier may commit. The other returns
the durable generic invalid result and cannot overwrite the winner or revoke sessions.
- Exact same-key successful replay continues to return the durable winner after the verifier has moved; changed factual input still
conflicts.
Mandatory adversarial release proof
The real private-handler/PostgreSQL chain must run reset/login and change/login in both commit orders, stale failed login versus reset,
and two password changes whose Workers both preverify the old password before either database mutation. PostgreSQL 17.6 and
18.1 must prove no stale session, wrong counter change, verifier overwrite, deadlock or partial mutation.
No-change boundary
This decision adds one private helper only. It adds no Candidate business table or public Candidate RPC and does not change
Candidate workflow, Office modal functionality, frontend role policy, finance, invoices, payments, Banking Pay, Policy X, settlement,
remittances or production. Candidate feature flags remain false and frontend API wiring remains subject to independent GO.
Decision audit - 13 August 2026
~~~


## Historical decision page 37

~~~text
CLOUDTMS CANDIDATE APP  |  CURRENT DECISIONS
Page 37
Decision audit - 13 August 2026
37. Office Candidate frontend implementation
API-freeze gate status
The independent Candidate DB/RPC/backend/API and integrated Office Candidate API review issued GO on 13 August 2026. Earlier
page statements that Office frontend wiring remained blocked until independent API GO record a gate that has now been satisfied;
they are not current blockers. The Office Candidate frontend may therefore be implemented and deployed to TEST against the frozen
contract. Candidate responsive web, iOS and Android implementation remains separate future work.
Authority, compatibility and presentation ownership
The normal authenticated CloudTMS Office remains the sole Office surface. Candidate sessions never receive Office authority. The
frontend consumes the frozen server projection and typed action envelopes and does not infer lifecycle eligibility, row identity,
financial protection, reminder timing, QR ownership or idempotency rules. The existing Office authentication and authorisation
boundary is preserved and Candidate App user authority is unchanged.
Office staff do not perform manager approvals or refusals. A manager decision remains manager-owned. Office displays the resulting
status and evidence only. EMAIL and PHONE are implementation methods and are never shown to the Office user as status labels,
badges, method fields or Office decision controls.
Approved Office status catalogue
Only the following Candidate Submission labels may be rendered. A new label requires explicit product approval before
implementation:
- Awaiting Candidate Submission
 
- Candidate Submitted
 
- Awaiting Manager Approval
 
- Manager Approved
 
- QR Awaiting Signed Return
 
- QR Pack Preparing
 
- Finalising Submission
 
- Finalisation Needs Attention
 
- QR Pack Needs Attention
 
- Refused by Client
 
- Rejected by Agency
 
- Candidate Submission Cancelled
 
- Candidate Submission Complete
 
- Status unavailable
 
Refused by Client means that the client/manager refused the submitted claim. Rejected by Agency means that authorised Office staff
rejected the Candidate submission before Authorisation and the Candidate must follow the server-owned recovery/resubmission
route.
Timesheet Summary
Timesheet Summary contains one dedicated, read-only Candidate Submission column immediately after Processing Status and
before Issues. It is compact, uses at most two visible lines and participates fully in the existing Summary-grid preferences. The Office
user can sort it by the approved display label, drag it to another position and resize it; the chosen order and width are persisted
exactly like peer columns. Sorting applies to the complete filtered result before pagination. Resizing grows the table into the
established horizontal-scroll region so columns to its right remain independently draggable and responsive. It contains no action
buttons. Candidate state is never duplicated into Issues; Issues contains actual issues only. Unknown or unavailable Candidate
authority fails closed as Status unavailable.
One winning Candidate and QR status
Summary, Simple Timesheet, Bulk Process and Bulk Authorise use one shared presenter and render only the single approved status
furthest along the authoritative lifecycle. QR Pack Preparing is replaced by QR Awaiting Signed Return once the pack is
ready/issued. Once signed return evidence is received, neither earlier QR badge remains: Finalising Submission is current until later
Candidate Submission Complete or terminal Candidate truth wins. Mutually exclusive QR progress badges are never stacked or
shown simultaneously.
~~~


## Historical decision page 38

~~~text
CLOUDTMS CANDIDATE APP  |  CURRENT DECISIONS
Page 38
Decision audit - 13 August 2026
37. Office Candidate frontend implementation
Simple Timesheet modal
The established Overview, Lines, Expenses, Evidence, Issues, Finance and Audit tabs and the existing modal family are preserved.
Candidate Submission status is placed in the existing Overview stage/workflow area. Candidate actions are inserted only when the
canonical projection returns that exact action as enabled for the exact current row. Existing Authorise, Unauthorise, Process, finance,
invoice and payment controls remain unchanged.
Evidence and QR terminology
The Office interface uses QR Pack for the complete pack and QR Timesheet when referring only to the timesheet. It does not use
PAPER as user-facing terminology. The combined issued pack is shown in Evidence as Unsigned QR Pack and is retained for audit
only; it is not authorisable evidence. Returned signed documents are displayed separately and classified by their server-owned
document type, including Timesheet and the established expense evidence classifications such as accommodation. Evidence retains
the existing View and Download controls and every existing column; narrow layouts use an internal horizontal scroll rather than
removing information.
QR Pack Preparing means CloudTMS is assembling the official pack in the background and no Office retry is yet available. Retry QR
Pack Preparation appears only for an explicit server-owned retryable failure. QR Awaiting Signed Return means that the issued pack
exists but the official signed documents have not yet been returned, so the timesheet is not eligible for Authorisation.
Simple Timesheet action catalogue
The following Candidate actions may appear only where the server says that exact action is eligible:
- Send Manager Reminder - only for a live manager request that is currently eligible for another reminder.
 
- Request Manager Approval Again - only for an expired, otherwise unchanged manager request.
 
- Reject Candidate Submission - only before Authorisation and outside protected financial states; never on an authorised timesheet.
 
- Retry Finalisation - retries only the failed technical finalisation and does not repeat or replace a valid manager decision or signature.
 
- Retry QR Pack Preparation - only for an explicit server-owned retryable failure.
 
- Create Replacement QR Pack and Notify Worker - invalidates the old pack/code and creates a replacement; Simple Timesheet only.
 
- Convert to Manual - only through the approved preview, warning and revalidation flow.
 
- Enable Electronic Submission - only where a manual record is eligible for electronic submission.
 
- Enable QR submission - only where a manual record is not eligible for electronic submission but is eligible for QR submission.
 
Enable QR submission is never shown where electronic submission is eligible, because the Candidate chooses QR or electronic
submission in that case. Resend QR Pack is present only as a disabled Simple Timesheet placeholder until the backend gains a
separately approved resend authority. Cancel Manager Approval Request is not shown anywhere in the Office frontend.
~~~


## Historical decision page 39

~~~text
CLOUDTMS CANDIDATE APP  |  CURRENT DECISIONS
Page 39
Decision audit - 13 August 2026
37. Office Candidate frontend implementation
Bulk Process and Bulk Authorise
Bulk Process may expose only: Send Manager Reminder; Request Manager Approval Again; Reject Candidate Submission; Retry
Finalisation; Retry QR Pack Preparation; Convert to Manual; Enable Electronic Submission; and Enable QR submission. Each action
remains row-specific and appears only when the server marks it eligible. Replacement and resend QR Pack actions are not available
in Bulk Process.
Bulk Authorise may expose only Reject Candidate Submission as a Candidate action, and it is absent for authorised rows.
Replacement and resend QR Pack actions are not available in Bulk Authorise. Existing Bulk Process and Bulk Authorise financial,
processing and authorisation controls retain their existing authority and behaviour.
Send Manager Reminders workspace
The Timesheet tools sidebar contains Send Manager Reminders immediately below Bulk Process. It opens a dedicated professional
modal showing only reminder-eligible manager requests across the server-owned Office reminder catalogue; the current Timesheet
page filters do not narrow this eligibility set.
- Columns are selection, Candidate and Last request or reminder sent. Candidate and last-sent headers sort their complete result; the
 selection header remains the all-pages select/clear control rather than a competing sort control.
- A live surname search filters as the user types. Clearing or changing the search does not clear selections, so several candidates can
 be selected across successive searches.
- Pagination uses Previous, Next and Page n of n. The header checkbox selects or clears all eligible rows across every page, not only
 the visible page.
- The catalogue fails closed rather than partially displaying or sending when more than 1,000 requests are actually eligible.
 
- Send Reminders and Cancel are the only workspace actions. Send Reminders opens a styled CloudTMS confirmation describing
 the exact selected count before any mutation.
- The result view reports Sent, no longer eligible and Failed outcomes and has a Close action. Eligibility is revalidated by the server at
 execution time.
Confirmations, conflicts and responsive quality
Every Candidate mutation uses the established styled CloudTMS confirmation family with clear plain-English consequences and
never a native browser or Windows alert, confirm or prompt. No permanent Refresh button is added. Refresh current state appears
only inside a controlled stale/conflict message. The UI must remain keyboard accessible, responsive and professionally laid out;
desktop and narrow Playwright tests must prove the patched assets, modal behaviour, action gating, method hiding, status wording
and absence of native dialogs.
Durable manager-reminder result and lost-response recovery
An accepted reminder batch always returns top-level ok=true because a durable result exists even when individual items are
PARTIAL or FAILED. Per-item success, no-longer-eligible and failure counts remain authoritative inside that durable result. A
top-level false response means no durable batch result. If the execute response is lost or uncertain, the browser first reads the exact
batch identity. Only an authoritative not-found permits one exact retry with the same batch ID, frozen selection and idempotency key;
no second batch or key is invented. Continued uncertainty preserves that identity and exposes only the controlled Refresh current
state recovery.
Preserved app-ready and no-change boundaries
The Office implementation is prepared for the future Candidate App by consuming the same canonical status, evidence, action and
identity contracts. It does not add Candidate authority or change Candidate App users. It adds no Candidate business table or public
Candidate RPC and changes no DAILY/WEEKLY calculation, rate, pay, charge, VAT, ERNI, margin, TSFIN, invoice, payment,
Banking Pay, Policy X, settlement, remittance or production authority. Frontend wiring remains subject to the verified TEST rollout
and independent re-audit gate.
~~~


## Historical decision page 40

~~~text
CLOUDTMS CANDIDATE APP  |  CURRENT DECISIONS
Page 40
Decision audit - 14 August 2026
38. Manager reminder continued-uncertainty ownership
Controlling outcome
The manager-reminder workspace owns at most one unresolved batch for the current authenticated Office session. A lost or uncertain
HTTP response never authorises a replacement batch, a replacement idempotency key, a changed selection or another confirmation.
The original frozen batch remains the only operation until CloudTMS obtains an authoritative result or an authoritative rejection proves
that the operation was not accepted.
Frozen operation identity
The retained recovery receipt freezes all facts needed for exact recovery:
- batch ID and identical idempotency key;
 
- selection mode, selected row identities and selection fingerprint;
 
- catalogue revision and exact preview facts;
 
- operation phase and whether the single permitted exact retry has been consumed;
 
- the authenticated Office user that owns the unresolved operation.
 
Recovery-only presentation
After continued uncertainty, the ordinary reminder catalogue is replaced by one professional recovery-only view. It displays the retained
batch facts and exposes only Refresh current state. Surname search, row selection, Select All, sorting, paging, Send Reminders, Cancel
and the modal Close control are unavailable. A function-level guard also blocks send execution before UUID generation, preview or
confirmation whenever an unresolved batch exists.
One exact retry, then status-only
After an uncertain execute response, CloudTMS reads the exact batch status first. The first authoritative
CANDIDATE_REMINDER_BATCH_NOT_FOUND response permits one exact POST retry using the unchanged frozen request. Retry
consumption is persisted before that POST begins. If uncertainty continues, every Refresh current state action is GET-only; no further
execute POST and no new operation identity may be produced.
Modal and Office-session lifecycle
The unresolved receipt is owned outside disposable modal state and survives forced dismissal, reopening and page refresh within the
same authenticated Office session. Reopening restores the exact recovery-only view instead of loading a fresh sendable catalogue. The
receipt is cleared when an authoritative durable result is rendered, when a stable non-uncertain rejection proves safe non-acceptance,
or when the Office session is cleared or replaced. A different Office user cannot adopt the receipt.
Durable outcomes and preserved boundaries
COMPLETED, PARTIAL and FAILED remain durable structured results with authoritative item counts; PARTIAL or FAILED is not a
transport uncertainty. Once any durable result is recovered, the protected state clears and a genuinely new batch may be started. This
correction changes only the Candidate Office reminder workspace and its frontend cache identity. It changes no Candidate database
definition, RPC, backend/API contract, Office action authority, Candidate public feature flag, Office role, financial calculation, invoice,
payment, Banking Pay, Policy X, Worker or production resource.
Mandatory verification
Acceptance requires unit and real-browser proof of: execute uncertainty plus status uncertainty; authoritative 404 plus one exact retry
and later status-only refreshes; blocked close, Escape and ordinary controls; exact restore after dismissal/reopen; eventual PARTIAL
and FAILED recovery; no second UUID or confirmation; desktop and narrow professional layout; deployed-asset parity; and preservation
of the complete Candidate Office status, QR-precedence and Timesheet Summary regression matrix.
Implementation authority: TEST-Frontend runtime commit 760477f45dd897e4d05d07af90d75b1fabcb0b2c; final evidence head
81a33e79a80e211ef91c0d8031d3244d8334645e. Candidate public features remain disabled.
~~~


## Historical decision page 41

~~~text
CLOUDTMS CANDIDATE APP  |  CURRENT DECISIONS
Page 41
Decision audit - 14 August 2026
39. Office runtime authority and closed-world reminder
recovery
Closed-world authoritative reminder outcomes
After reminder execution may have reached the server, only two status-read outcomes may change the protected operation: an exact
durable COMPLETED, PARTIAL or FAILED result resolves it; and the exact stable CANDIDATE_REMINDER_BATCH_NOT_FOUND
result may permit the one frozen retry where that retry is still unused. Every other status-read failure is non-authoritative about whether the
original batch was accepted.
The browser therefore retains the exact batch and recovery-only view after, without limitation:
- network loss, timeout, malformed or unavailable status truth and HTTP 5xx;
 
- HTTP 400, 401, 403, 409, 429 or any future non-404 error;
 
- permission, authentication and rate-limit failures affecting the status request itself.
 
Such errors never clear the receipt, re-enable the reminder catalogue or authorise a replacement UUID, key, selection or confirmation. A
later exact not-found may consume the single retry; once consumed, all later Refresh current state actions remain GET-only.
Normal TEST Worker environment authority
The normal TEST backend Worker must define CANDIDATE_APP_ENVIRONMENT as TEST in its checked-in TEST Wrangler
configuration. The Office Candidate service capability and projection routes use that binding to select the approved TEST environment.
Missing or invalid authority fails closed and must not be mistaken for an ineligible timesheet.
Candidate public feature flags remain a separate client boundary. All 12 stay false while the authenticated Office service surface remains
enabled for authorised Office users. This configuration does not enable Candidate login, reads, writes, notifications, manager flows or QR
processing for Candidate users.
Deployment and browser acceptance
Every normal TEST Worker publication must prove:
- a repository regression test finds exactly one TEST binding with the exact value TEST;
 
- the Wrangler TEST dry build includes that binding and the normal TEST Worker deploy is healthy;
 
- the real authenticated capabilities endpoint returns the enabled Office contract rather than CANDIDATE_ENVIRONMENT_INVALID;
 
- real eligible Electronic rows display Awaiting Candidate Submission in Summary and Simple Timesheet, and applicable Bulk surfaces
 initialise;
- Candidate flags and Candidate business data remain disabled/empty throughout harmless verification.
 
Preserved boundaries and implementation authority
This closure changes no Candidate database definition, table, public RPC, Office adapter, OpenAPI, action policy, status catalogue,
Candidate user authority, timesheet economics, finance, invoice, payment, Banking Pay, Policy X or production configuration. It changes
only the normal TEST Worker environment binding, its regression test, the Candidate Office reminder error classification, its tests and
frontend cache identity.
Implementation authority: frontend b06efa29966a28fbb9a5b548239b558d3ca6d71f; backend
c0b5d86a1b2ba155f466cc1168a169964d9c8b9a; normal TEST Worker 4fc999ec-983c-4314-8303-e05929ec04ff. Candidate public
features remain disabled.
~~~


## Historical decision page 42

~~~text
CLOUDTMS CANDIDATE APP  |  CURRENT DECISIONS
Page 42
Decision audit - 14 August 2026
40. Candidate applicability, Daily identity and immediate
Summary truth
Candidate lifecycle applicability
Candidate submission status is displayed only for the canonical ELECTRONIC or QR route families. Manual non-QR, Manual adjustment,
HealthRoster/NHSP import-authoritative and import-authoritative adjustment rows remain blank in Timesheet Summary, Simple Timesheet,
Bulk Process and Bulk Authorise. A current Manual row may still expose an exact server-enabled route-conversion action; that does not
manufacture Candidate lifecycle truth before conversion.
Durable completion proof
Authorised, Invoiced and Paid are Office financial facts, not proof that the Candidate used or completed the Candidate submission workflow.
Candidate Submission Complete requires a durable Candidate workflow in FINALISED state. Historic financially protected ELECTRONIC/QR
rows with no Candidate workflow remain blank. A current eligible unprocessed ELECTRONIC/QR row with no workflow may show Awaiting
Candidate Submission.
Daily and Weekly exact identity
DAILY Candidate identity is owned by the exact current timesheet and its booking/version family. A DAILY projection must not require or
manufacture a contract_weeks row. WEEKLY Candidate identity remains the exact current timesheet plus its single exact contract-week
record. Mixed, ambiguous or moved identities continue to fail closed.
Immediate Timesheet Summary transport
When the authenticated Timesheet Summary requests Candidate truth, the normal backend composes the existing canonical Office
PROJECT_BATCH projection into the same Summary response before returning the rows. Composition is partitioned into batches of no
more than 100 exact identities. Duplicate display row keys are never collapsed; each exact identity is preserved in a separate valid batch
where necessary.
- The browser validates embedded contract version, exact row identity and row signature before first-paint rendering and cache seeding.
 
- A row carrying the embedded-result marker never starts a second after-render Office projection request for its initial grid cell.
 
- Established Manual and import-authoritative routes are resolved as immediate blank values without canonical projection work; the exact row
 route outranks broad client/family hints.
- Real TEST measurement recorded 153 ms median with Candidate projection versus 133 ms without it: 20 ms added median cost and zero
 browser follow-up projection requests.
- Full Simple Timesheet detail, explicit refresh and mutation recovery continue to use the established Office routes.
 
- The Issues column remains sourced from issue_codes in the main Summary payload and does not become a Candidate after-render field.
 
- No Candidate table, public RPC, economic calculation, finance, invoice, payment, Banking Pay, Policy X or production authority changes.
 
Implementation authority
Frontend 7cab427272a7d21eb922c51765442548d0c3ab53; backend 869bd349644f16c02decd33c5076bdb536e39a56; normal TEST
Worker b2f36fcc-6c11-4336-9dd2-5bc8ccd8df7c. Candidate public feature flags remain disabled.
~~~


## Historical decision page 43

~~~text
CLOUDTMS CANDIDATE APP  |  CURRENT DECISIONS
Page 43
Decision audit - 14 August 2026
41. Office route, processing and Candidate authority closure
One server-owned applicability decision on every Office surface
Manual non-QR, HealthRoster-authoritative, NHSP-authoritative and import-authoritative rows have no Candidate submission lifecycle. Summary,
Simple Timesheet, Bulk Process and Bulk Authorise receive the same server-owned not-applicable marker, display a blank Candidate state and
perform no Candidate projection request. A current eligible Electronic/QR row with no workflow may show Awaiting Candidate Submission; financial
completion alone never proves Candidate completion. Candidate Submission Complete requires durable FINALISED workflow truth.
Route, Processing Status and Candidate Submission are separate
- Route displays canonical QR only for a QR/QR-only route, Manual only for Manual non-QR, and Electronic for an Electronic-capable route. A later
 Candidate QR Pack choice does not relabel an Electronic route.
- Processing Status is limited to Unprocessed, Processed, Authorised for Invoicing, Partially Invoiced, Invoiced, Archived and fail-safe Processing
 Delayed.
- Candidate and QR lifecycle wording appears only under Candidate Submission. Legacy Awaiting signed QR timesheet and legacy QR
 issue/projection gates do not control Processing Status or Office lifecycle actions.
Authorise and signed QR return authority
Office Authorise and Unauthorise consume only the canonical server action flags and TSFIN lifecycle authority. A QR row can reach
PENDING_AUTH or READY_FOR_HR only after the canonical Candidate finaliser has accepted the complete signed return manifest and completed
the established financial/Process composition. An issued unsigned QR Pack, partial return, QR code, retained legacy QR field or legacy waiting label
never enables Authorise. A deliberate conversion to Manual removes Candidate/QR gating and follows the unchanged Manual rules.
Import-authoritative rules remain unchanged.
Timesheet Summary transport and column behaviour
- Candidate projection truth is embedded in the initial authenticated Summary response and paints with the normal row; the browser does not fan out
 after render. Existing Issues projection remains part of the same Summary response.
- The Candidate Submission column defaults after Processing Status and before Issues, contains no actions, and behaves like peer columns:
 persisted drag/reorder, persisted resize and approved-label sorting. The user's saved position is authoritative after the first change.
- Embedded identity/signature validation, maximum-100 server batches, duplicate display-key separation and blank/not-applicable short-circuit
 remain mandatory.
Legacy retirement boundary and verification
Legacy QR/Electronic presentation and gating may be retained only as inert compatibility transport for historic payload parsing; it cannot veto or
enable current Office actions. Current server action flags, Candidate workflow projection, signed-manifest finalisation and canonical route/version
authority remain intact. Mandatory regression covers PostgreSQL 17.6/18.1, complete frontend/backend JavaScript, patched-asset Playwright at
desktop and narrow viewports, all four Office surfaces, QR/Manual/Electronic route labels, Authorise visibility, Summary interaction and
manager-reminder recovery.
No-change and activation boundary
No Candidate business table or public RPC is added. DAILY/WEEKLY economics, rates, pay, charge, VAT, ERNI, margin, Process implementation,
invoice, payment, Banking Pay, Policy X, settlement, remittance and production authority are unchanged. Candidate public feature flags remain false
until separate activation approval.
~~~


## Historical decision page 44

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
CORRECTED R5 ADDENDUM - 16 AUGUST 2026
Sections 42–56 - Candidate Daily availability integration R5
Addendum page 1
Current Decisions
Candidate Daily availability integration
Later-controlling corrected Phase 0 R5 addendum - Sections 42–56
Decision date
16 August 2026
Document status
Corrected Phase 0 R5 ready for independent re-audit
Base document
43-page Final Office Acceptance decisions dated 14 August 2026
R5 verdict
Phase 0 remains pending independent R5 audit. Phase 1A runtime
implementation, Phase 2 SQL authoring/installation, Phase 1B, Candidate Daily
activation and production are NO-GO. This document grants corrective
specification and audit authority only.
Precedence: sections 42-56 supersede the earlier Phase 0 addenda and control only
Candidate Daily availability, minimal-change legacy containment/coexistence, Google
routing, the additive DB/RPC boundary, route policies, retained specialist functions,
complete legacy-client scope and booked-tile timesheet path. All other Candidate workflow,
Office, finance, Banking Pay and Policy X decisions remain unchanged.
The original accepted document is reproduced unchanged as pages 1–43 of the combined PDF. This addendum
follows it and is the current authority for Candidate Daily availability coexistence.
~~~


## Historical decision page 45

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
CORRECTED R5 ADDENDUM - 16 AUGUST 2026
Sections 42–56 - Candidate Daily availability integration R5
Addendum page 2
Precedence
The accepted Candidate App Decisions PDF pages 1–43 remain unchanged. Sections 42–55 below are
later-controlling only where they explicitly govern Candidate Daily availability storage, broker/Google coexistence,
legacy authentication, Daily capability gating, retained specialist functions and cutover. The original decisions
continue to control every other subject, including Candidate DAILY workflows, manager PHONE signing, Office
authority, finance and Policy X.
This R5 addendum supersedes every earlier Phase 0 addendum, including R3, in full.
~~~


## Historical decision page 46

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
CORRECTED R5 ADDENDUM - 16 AUGUST 2026
Sections 42–56 - Candidate Daily availability integration R5
Addendum page 3
42. Authority and topology
Supabase becomes the canonical candidate-entered availability authority only through a recorded
environment/candidate cutover. Google remains a synchronized Master Rota projection during coexistence. The
Candidate client speaks only to the public Candidate broker. The public broker uses the existing private Candidate
API/service binding, which calls service-owned Supabase RPCs. No second public intermediary, direct
app-to-Supabase connection or direct app-to-Google connection is introduced.
~~~


## Historical decision page 47

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
CORRECTED R5 ADDENDUM - 16 AUGUST 2026
Sections 42–56 - Candidate Daily availability integration R5
Addendum page 4
43. Legacy compatibility by containment, not
modernisation
Do not modernise the legacy app. Contain it. The old Availability browser, UI, login flow, internal msisdn lookup,
tile fields, statuses, save controls, mixed per-date outcomes and retained specialist journeys remain unchanged
wherever reasonably possible. The temporary browser receives no CloudTMS session/token, canonical Candidate
UUID, HMAC secret or direct Candidate API authority. Existing Apps Script remains its server boundary. A small
removable compatibility adapter resolves the existing legacy source, creates/reuses stable server-owned operation
identity, signs an Apps Script-to-CloudTMS request, and CloudTMS independently maps the approved source
identity to exactly one canonical Candidate. No unrelated legacy defect is repaired as part of this integration.
Legacy save keeps mixed accepted/rejected per-date semantics. The accepted subset commits once under a single
canonical version and durable receipt. A busy/update-window response cannot claim application before that commit;
deferral is explicit and retryable.
~~~


## Historical decision page 48

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
CORRECTED R5 ADDENDUM - 16 AUGUST 2026
Sections 42–56 - Candidate Daily availability integration R5
Addendum page 5
44. New-app Daily availability
The new product renders fourteen Daily tiles from one complete active Supabase rota generation overlaid with
canonical preferences. It saves availability through an authenticated, versioned, idempotent, all-or-none broker
command. Booked and blocked dates are not editable. The route never reads Google. Every result has a correlation
ID, stable error code and retry classification.
iOS, Android and first-party browser web are presentation surfaces over the same public Candidate broker contract.
None owns independent authorization, availability, Emergency or timesheet business logic. The browser web
surface is not an installed PWA and does not reproduce the old browser installation journey.
~~~


## Historical decision page 49

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
CORRECTED R5 ADDENDUM - 16 AUGUST 2026
Sections 42–56 - Candidate Daily availability integration R5
Addendum page 6
45. Candidate DAILY timesheets and booked-tile actions
Booked tiles provide closed, server-authored action locators only when the current Candidate action-hub record
identity and concurrency token are unambiguous. The token is the accepted opaque 32-character lowercase
row_signature; Daily does not name a digest algorithm, derive another signature or create a second concurrency
authority. The app refreshes the accepted action hub immediately before acting. Missing, ambiguous, stale or
moved ownership omits the locator.
Viewing/editing, Save/Recalculate, candidate signing, EMAIL approval and PHONE manager handoff use the
accepted Candidate DAILY and Candidate Manager route family. The legacy Apps Script SEND_TIMESHEET and
shape-detected authorization callback remain old-app compatibility only and cannot authorize the new workflow.
Candidate-specific callbacks require signed system authentication or canonical CloudTMS database projection.
Candidate-facing terminology is exact: any action that produces documents to be signed is labelled “Print
documents for signing”. Legacy/internal action identifiers or labels are never displayed in the Candidate App.
~~~


## Historical decision page 50

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
CORRECTED R5 ADDENDUM - 16 AUGUST 2026
Sections 42–56 - Candidate Daily availability integration R5
Addendum page 7
46. Retained Daily specialist functions
Emergency/cannot-attend/leave-early, running-late options/preview/send, DNA/cohort/exclusion rules, tried-calling
states, acknowledgements, messages, Past Shifts, Hospital Addresses and Accommodation Contacts remain
available to both apps. They share one CloudTMS business/effect authority. New-app access is through individually
allowlisted Candidate broker routes; temporary legacy access is through individually allowlisted signed Apps Script
adapters that preserve existing response shapes. There is no generic Apps Script action tunnel and no duplicated
emergency/specialist authority. Current effective emergency eligibility uses a 600-minute grace for parity. A
contradictory 30-minute source comment is non-authoritative until deployed boundary tests and an explicit product
decision change the rule.
External effects use stable effect keys and the exact durable owner
private.candidate_daily_external_effect_receipts. Apps Script must claim an effect before acting
and complete it afterwards; it never owns a second receipt authority. A retry checks the database receipt and, where
needed, provider state; it does not blindly send again. UNKNOWN is a durable non-success state requiring
reconciliation, not permission to resend.
~~~


## Historical decision page 51

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
CORRECTED R5 ADDENDUM - 16 AUGUST 2026
Sections 42–56 - Candidate Daily availability integration R5
Addendum page 8
47. Database and version authority
The availability extension is additive and environment-scoped. It contains twelve tables and thirteen service-only
RPC owners. The increase from R2 supplies explicit batch replay, source mapping and external-effect authorities; it
does not broaden candidate functionality. No legacy browser-session table/RPC family is introduced. One
authority-scope row per environment/candidate owns the canonical version. An accepted command locks it,
increments once and stamps changed days, receipt and outbox with the same version. Generation publication that
clears a preference follows the same lock/increment/stamp rule. Browser roles receive no table/RPC access.
Environment comes from trusted deployment context, never request body.
The design count is descriptive, not a license to weaken security. A future count change requires a recorded
decision and compatible migration.
~~~


## Historical decision page 52

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
CORRECTED R5 ADDENDUM - 16 AUGUST 2026
Sections 42–56 - Candidate Daily availability integration R5
Addendum page 9
48. Rota generation, projection and freshness
There is exactly one ACTIVE fourteen-day generation per environment/candidate. Activation requires fourteen
unique consecutive rows and atomically supersedes the old generation. booking_id is bounded text, not UUID.
Freshness has accepted canonical, delivered-visible, overlay-proof, effective-required-visible and effective-visible
monotonic progress. DEFERRED_OVERLAY advances no delivered cursor and consumes no delivery-failure attempt.
A proven current active-generation overlay may satisfy effective visibility while it safely hides the canonical
preference; removing or changing it invalidates proof and wakes the parked work. A genuine unprojected visible
change or noncontiguous gap remains blocking. Normal new-product reads remain database-only.
~~~


## Historical decision page 53

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
CORRECTED R5 ADDENDUM - 16 AUGUST 2026
Sections 42–56 - Candidate Daily availability integration R5
Addendum page 10
49. Route-class policy, global switch and pilot
entitlement
The global switch is the exact JSON boolean candidate_daily_enabled in
settings_defaults.candidate_app_feature_flags_json. Missing/null/malformed/read error is false. The
accepted bootstrap field entitlements.daily is preserved with its pre-existing meaning and is not the new
Availability decision.
The additive object capabilities.daily_availability is the sole authority for the new Candidate Daily
Availability surface. Its enabled value is true only when the global switch, explicit current-environment/candidate
entitlement, immutable source/candidate binding and Candidate-surface readiness are all true. One private
route-policy authority assigns BASELINE_BOOTSTRAP, CANDIDATE_SURFACE, LEGACY_COMPAT or
SIGNED_SYSTEM_SYNC. Only CANDIDATE_SURFACE consults the new-product switch. Legacy compatibility and
signed system synchronisation continue under their own validated session/HMAC, source/effect, mode and
transition prerequisites while the product flag is false. Missing, malformed, unavailable or unreadable inputs fail
closed within the selected policy. If entitlements.daily and the new capability differ, the new capability controls
Daily Availability. False hides/unmounts Daily on all three new surfaces, leaves Weekly available and denies direct
Candidate Daily requests; it does not stop legacy or system continuity.
Entitlement is an explicit private record with environment, candidate, enabled state, validity, actor, timestamp,
reason and evidence. It is never inferred from active candidate state, source links, account rows, keys or cohort.
~~~


## Historical decision page 54

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
CORRECTED R5 ADDENDUM - 16 AUGUST 2026
Sections 42–56 - Candidate Daily availability integration R5
Addendum page 11
50. Google system trust and replay protection
Google-system routes have no browser CORS or candidate-cookie authority. HMAC v1 signs method, normalized
path/query, timestamp, nonce, content digest, idempotency key, correlation ID and raw body bytes exactly as
specified by the Phase 0 canonicalization vectors. The private API verifies signature and conditionally consumes a
nonce in a dedicated durable versioned prefix. The public broker has neither secret nor replay store. A duplicate
nonce is rejected. Signed system calls must supply one exact valid signed correlation ID and it is never replaced.
Candidate/browser calls may receive a generated correlation ID before authority processing. Database receipts own
business idempotency.
Candidate user sessions and the legacy compatibility boundary are separate trust mechanisms. The legacy browser
keeps its existing login; only the Apps Script-to-CloudTMS hop uses Google-system HMAC and nonce authority.
~~~


## Historical decision page 55

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
CORRECTED R5 ADDENDUM - 16 AUGUST 2026
Sections 42–56 - Candidate Daily availability integration R5
Addendum page 12
51. Idempotency, receipts and external effects
Idempotency-Key is the sole transport authority for commands; command bodies do not duplicate it. Pure reads
do not require it. Same key and same request hash returns the original completed result without another
version/outbox/effect. Same key with a different hash conflicts. Receipt state advances IN_PROGRESS ->
COMPLETED or IN_PROGRESS -> FAILED_FINAL; terminal result is write-once. A stale IN_PROGRESS owner is
recovered under a bounded lease.
Multi-candidate generation publish is per-candidate atomic and returns ordered COMMITTED, REPLAYED or
REJECTED outcomes. One candidate failure cannot partially activate that candidate or roll back another candidate
already committed.
A multi-item or multi-candidate command also has one immutable batch receipt containing the canonical request
hash, deterministic input order, linked item receipts, recovery lease and exact write-once terminal response.
Projection claims are commands with a stable claim request ID. The receipt freezes claimant, request hash,
target/scope, requested limit, ordered claimed row identities, lease tokens, expiries and terminal claim result. Exact
replay returns the same still-valid lease set; a claimant must recover, complete, expire or explicitly reconcile every
active lease before new work. Completion requires the exact claimant, lease token and row/version and rejects
stale, reassigned or changed duplicate completion.
private.candidate_daily_external_effect_receipts remains the one effect authority. For running late,
cannot attend, leave early, DNA, acknowledgement and escalation stop it records exact
candidate/shift/source-event identity, immutable request hash, idempotency key, first claim, lease
owner/token/expiry, attempts, provider request/result evidence, UNKNOWN, terminal result, reconciliation and
retention metadata. Same key/hash replays; same key/different hash conflicts before effects; ambiguous provider
outcome never blindly retries.
~~~


## Historical decision page 56

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
CORRECTED R5 ADDENDUM - 16 AUGUST 2026
Sections 42–56 - Candidate Daily availability integration R5
Addendum page 13
52. Cutover, rollback and evidence gates
Every authority transition is recorded durably with environment, candidate/cohort, prior/new mode, effective time,
canonical version, active generation, accepted/visible cursors, in-flight disposition, actor, approver, reason and
evidence hash. Cutover and rollback are atomic service operations, never manual flag-only events. Rollback does
not create dual authority.
Phase 0 uses replayable sanitized fixtures. Effectful evidence is listed separately and requires explicit TEST
mutation/communication approval, synthetic identifiers, cleanup and no-production proof. Rollout requires the
measurable soak/error budget frozen in the test plan.
Before Apps Script editing/deployment or Phase 1B Google closure, an authorized read-only gate records effective
project file order, deployed version, hashes, triggers, manifest, property names, winning duplicate functions and the
complete Master Availability consumer/writer graph.
~~~


## Historical decision page 57

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
CORRECTED R5 ADDENDUM - 16 AUGUST 2026
Sections 42–56 - Candidate Daily availability integration R5
Addendum page 14
53. Phase structure and activation
R5 is awaiting independent Phase 0 re-audit. Phase 1A runtime implementation, Phase 2 SQL
authoring/installation, Phase 1B, Google changes and activation remain no-go until the preceding gate is explicitly
passed and authorized. After Phase 0 pass, Phase 1A implements dark broker contracts; Phase 2 implements the
additive database/RPC authority under separate TEST mutation authority; Phase 1B implements broker-to-RPC and
narrow Google adapters against Phase 2. These are one dependency-controlled work package and are not
complete until integrated runtime tests pass together.
The new Candidate App follows the frozen broker contract. Candidate Daily remains inaccessible until global switch,
explicit entitlement, identity, freshness, parity, cutover and rollout gates all pass. No Phase 0 statement authorizes
migration execution, TEST mutation, Apps Script/Worker/frontend deployment, communication, commit/push or
production action.
~~~


## Historical decision page 58

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
CORRECTED R5 ADDENDUM - 16 AUGUST 2026
Sections 42–56 - Candidate Daily availability integration R5
Addendum page 15
54. Complete legacy-client preservation boundary
The old Availability browser was audited as a whole. Authentication/password/logout outcomes,
resilience/accessibility, local drafts and refresh, all tile/save states, welcome/seen state, Past Shifts, retained
content and every Emergency/attendance operation are preserved through either accepted Candidate core routes
or explicit Daily routes. The old PWA/browser installation prompt and instructions are not reproduced. The native
iOS app is installed through Apple App Store, the native Android app through Google Play, and the first-party
browser web application runs without presenting the old install journey. Debug allowlists, console diagnostics and
unimplemented queue stubs are not product features.
The old email-timesheet menu action is retired from the new app by explicit user decision. The embedded legacy
digital-timesheet implementation is replaced by the already-accepted Candidate DAILY/action-hub workflow; it is not
copied as a second workflow. These are the only candidate behaviours found in the static old-client audit whose old
implementation is not reproduced. Any newly discovered user-visible behaviour is USER DECISION REQUIRED
before omission, replacement or addition.
~~~


## Historical decision page 59

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
CORRECTED R5 ADDENDUM - 16 AUGUST 2026
Sections 42–56 - Candidate Daily availability integration R5
Addendum page 16
55. Executable closure rules introduced by R5
CANDIDATE_API_OPENAPI_V1_MERGED_R5.yaml is the sole implementation contract. The baseline and
addendum are build inputs/evidence only. The merged contract must pass semantic OpenAPI 3.1 validation,
complete local-reference resolution, unique operation IDs and deep preservation of every non-Daily baseline
path/method/operation/security/request/response/error/content type and baseline component. The only accepted
baseline operation overlay is the additive bootstrap composition described above.
The exact new feature authority field is capabilities.daily_availability.enabled. Candidate source links and key
rotation follow Source Identity Canonicalization V1. These plain-text identifiers are included deliberately so the
combined Decisions PDF remains searchable and independently testable.
R5 retains one minimal signed legacy-adapter boundary, stable save IDs, one batch/claim receipt, one
external-effect receipt, one source-link authority, one cursor model and one transition model. It introduces no legacy
browser-session authority. It extends the shared HMAC corpus with method, path, key-version, duplicate-signature
and percent-normalisation attacks and requires the unchanged corpus to pass in Worker JavaScript and Apps Script
JavaScript. 32_SINGLE_AUTHORITY_AND_NO_DUPLICATION_REGISTER.md is an implementation stop gate.
The accepted base decisions on pages 1–43 are unchanged. Where an earlier Phase 0 statement names ten
tables, nine RPCs, replaceable signed correlation IDs, optional effect receipt storage, session/time-bucket legacy
idempotency, a public mirror escape hatch, a combined cohort transition row, capabilities.daily_enabled, a
universal global gate, duplicated cursor ownership or a 64-character Daily action signature, this R5 addendum is
later-controlling and the earlier statement is superseded.
~~~


## Historical decision page 60

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
CORRECTED R5 ADDENDUM - 16 AUGUST 2026
Sections 42–56 - Candidate Daily availability integration R5
Addendum page 17
56. R5 semantic closure and certified-source authority
The exact authority mode is GOOGLE_PRIMARY, ROLLBACK_PENDING or SUPABASE_PRIMARY.
Shadow/observation is evidence, not a mode; a completed rollback is recorded as GOOGLE_PRIMARY with
immutable rollback history. private.candidate_daily_authority_scopes owns only authority mode,
canonical version, active-generation pointer and transition reference. private.candidate_daily_sync_state
alone owns accepted_canonical_cursor, required_visible_cursor, delivered_visible_cursor,
overlay_proof_cursor and effective_visible_cursor; readiness requires effective visible progress to
meet required visible progress.
Every candidate-specific legacy adapter request is server-to-server, HMAC/nonce protected and contains only the
approved candidate_source_hmac plus factual operation data. Apps Script may retain its existing internal
msisdn resolution temporarily, but the browser cannot send a canonical Candidate UUID or choose a CloudTMS
source. CloudTMS independently resolves the source HMAC to exactly one Candidate and rejects missing,
ambiguous or mismatched mappings before business work. Stable request identity is created and retained at the
Apps Script adapter boundary before first submission, allowing exact replay without a legacy-browser patch.
HMAC v1 uses real raw URL/header/body parser paths and rejects malformed percent/UTF-8, controls, encoded
separators, dot/empty segments, duplicate/ambiguous headers, CRLF and framing ambiguity. Source Identity V1
uses the explicit code-point normalizer in section 27 and identical canonical bytes across languages. Each of the
twenty-five operations has its own exact status/error/retry triples. Every closure fixture has a stable mandatory ID
and complete precondition/result/postcondition, including every retained effect/state combination and the
minimal-change adapter boundary.
The unredacted user-certified Apps Script sources are authority for static function identity and hashes but are
excluded from this distribution because they contain sensitive literals. The pack contains line-order-preserving
sanitized sources, source hashes and a function-index equality proof. ai_startDailyPings occurs only as a
menu reference and has no declaration in either certified source. It is therefore an unimplemented legacy reference,
creates no Candidate requirement, and is not implemented or revived by R5. A later authorised effective-project
read-only inventory must classify its deployed state before any separate repair/removal decision.
R5 remains documentation and executable-contract evidence only. It performs no Apps Script edit, Worker change,
SQL authoring/installation, feature enablement, TEST mutation or production action. Independent Phase 0
acceptance remains mandatory before Phase 1A runtime work.
~~~


## Historical decision page 61

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
CORRECTED R5 ADDENDUM - 16 AUGUST 2026
Sections 42–56 - Candidate Daily availability integration R5
Addendum page 18
Implementation-start determination
Corrected Phase 0 R5 remains pending independent audit. Phase 1A runtime implementation, Phase 2 SQL
authoring or TEST installation, Phase 1B Google or broker integration, Candidate Daily activation and production
remain NO-GO. Only independent re-audit and any resulting corrective specification work may proceed from this
document.
No database migration or data/configuration mutation, Candidate Daily flag change, Google
edit/execution/deployment, Worker/frontend deployment, communication send, commit, push, pull request or
production action is authorized by this document.
The supervised source-identity artifact and live Apps Script file-order/deployment/trigger/property/winning-function
inventory remain gates before identity backfill, Google edits and Google closure. Installing Phase 2 in TEST still
requires explicit migration/mutation authority. Candidate activation also requires the global exact-true switch, explicit
entitlement, cutover ledger and quantitative soak gates.
~~~


## Historical decision page 62

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
PHASE 1A R6 ADDENDUM - 16 AUGUST 2026
Sections 57–64 - Google evidence gate and dark broker implementation
R6 addendum page 1
Current Decisions
Candidate Daily availability integration
Later-controlling Phase 1A R6 addendum - Sections 57–64
Authority
Current fact
Decision date
16 August 2026
Base authority
Accepted 61-page Phase 0 R5 Decisions PDF; AV-001–AV-154 remain controlling
R6 additions
Google Evidence Gate complete; AV-155–AV-180; Phase 1A source implemented dark
Runtime status
Local source/tests only; no Worker deployment, SQL installation, Google change or feature
enablement
Next gate
Independent R6 review before Phase 2 additive SQL/RPC authoring
Precedence: Sections 57–64 add evidence and Phase 1A implementation decisions. They do
not weaken or replace AV-001–AV-154. If a historical status statement says Phase 1A is not yet
authorised, the accepted independent R5 GO plus this R6 implementation record supersede
only that status; all substantive R5 constraints remain intact.
The first 61 pages are the accepted R5 Decisions document reproduced unchanged in the combined PDF. This
addendum begins on the next page and is the current authority for the effective Google evidence and Phase 1A source
state.
~~~


## Historical decision page 63

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
PHASE 1A R6 ADDENDUM - 16 AUGUST 2026
Sections 57–64 - Google evidence gate and dark broker implementation
R6 addendum page 2
57. Google Evidence Gate
The authorised read-only Google Evidence Gate is complete for the Availability API and NEW MASTER ROTA System.
No Sheet data, Apps Script file, manifest, property, trigger, deployment or OAuth configuration was changed; no
function was executed. Property values and unredacted source remain excluded from audit distribution.
System
Effective source/deployment evidence
Availability API
Sheet 1BSom…; current Head SHA-256 eacd1875…bbb3b3f; exact certified-source match; active
web version 215 matches Head; 0 installed triggers; 50 property names inventoried without values.
NEW MASTER ROTA
Sheet 1eEnr…; current Head SHA-256 c3ae9c48…19fa0a8; exact certified-source match; active
web version 100 SHA-256 f41dad2e…682099d differs from Head; 12 trigger entries; 28 property
names inventoried without values.
- Effective project file order and duplicate-function ownership are known and must be rechecked immediately before a later
 Google edit.
- Master trigger execution uses current Head while the deployed web version is older; Phase 3 must distinguish
 trigger-source and web-deployment authority.
- ai_startDailyPings has no declaration or installed trigger. Its one historical reference is not revived or repaired by
 Candidate Daily.
- The Google projects do much more than Candidate Daily; unrelated functions remain outside scope and untouched.
 
Binding legacy rule: Do not modernise the legacy app. Contain it. Preserve the old
browser/UI/login and internal msisdn behaviour; strengthen only the later trusted
server-to-server Apps Script-to-CloudTMS boundary.
~~~


## Historical decision page 64

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
PHASE 1A R6 ADDENDUM - 16 AUGUST 2026
Sections 57–64 - Google evidence gate and dark broker implementation
R6 addendum page 3
58. Phase 1A source authority
Phase 1A implements the frozen public/private transport and policy contracts from the sole merged R5 OpenAPI. It
does not implement the Phase 2 database/RPC authority or any Phase 3 Google adapter. The exact baseline is
cloudtms-backend origin/test commit 5386d3d2504d86a0366d66f4096a2f7a8912b2e9.
Source owner
Bounded responsibility
candidate-daily-contract-v1.js
Closed 24-route catalogue; policy, limits, correlation, idempotency, capability and error
authority.
candidate-daily-hmac-v1.js
Raw-target canonicalisation; HMAC v1; key overlap; private R2 nonce owner and cleanup.
candidate-daily-phase1a.js
Additive disabled bootstrap and effect-free dark dispatch.
candidate-app-backend.js
Authenticated Candidate Daily dispatch plus additive bootstrap only.
candidate-private-worker.js
Private signed-system prefix, service auth, HMAC verification and nonce cleanup.
candidate-broker.js
Credential-free signed-system forwarding; Candidate transport/rate/deadline boundary;
stable safe responses.
candidate-broker/wrangler.jsonc
Four TEST rate-limit bindings; no HMAC secret value.
Dark means safe: all Candidate Daily calls are globally disabled, signed system calls cannot
reach business work, and no Daily table/RPC or Google executor exists. A successful HMAC
check is not product enablement.
~~~


## Historical decision page 65

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
PHASE 1A R6 ADDENDUM - 16 AUGUST 2026
Sections 57–64 - Google evidence gate and dark broker implementation
R6 addendum page 4
59. Routes, policies and limits
The runtime catalogue matches all 24 new merged-R5 operations exactly: 11 Candidate routes and 13 trusted
Google-system routes. Existing bootstrap is the additive 25th Daily-related merged operation.
Route family
Policy/class
Count
Bound
Candidate Daily reads
CANDIDATE_SURFACE /
CANDIDATE_DAILY_READ
7
60/min/Candidate; 6 in flight; 32 KiB; 12
s
Candidate Daily commands
CANDIDATE_SURFACE /
CANDIDATE_DAILY_COMMAND
4
12/min/Candidate; effects 6/min; 1
operation/effect; 32 KiB; 10/20 s
Legacy compatibility
LEGACY_COMPAT_READ/COMMAND
4
Signed system transport; 120/min/key; 8
in flight; 256 KiB
Signed Google sync
SIGNED_SYSTEM_READ/COMMAND
9
Signed system transport; 120/min/key; 8
in flight; 256 KiB; 10/12/20 s
The four policy authorities are fixed:
- BASELINE_BOOTSTRAP preserves the accepted Candidate bootstrap.
 
- CANDIDATE_SURFACE requires readable inputs, global exact true, explicit entitlement, source identity readiness and
 authority readiness.
- LEGACY_COMPAT is independent of the Candidate global flag and requires signed transport, nonce, trusted
 environment, stable operation identity, approved mapping, compatible mode and transition readiness.
- SIGNED_SYSTEM_SYNC is also independent of the Candidate global flag and requires signed transport, nonce, trusted
 environment, source-scope readiness, compatible mode and transition readiness.
Distributed in-flight enforcement is not simulated with unsafe Worker-isolate counters. The cardinalities are frozen in the
route authority; Phase 2 receipts/leases and Phase 1B integration must enforce them before any route can be activated.
~~~


## Historical decision page 66

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
PHASE 1A R6 ADDENDUM - 16 AUGUST 2026
Sections 57–64 - Google evidence gate and dark broker implementation
R6 addendum page 5
60. HMAC, nonce, correlation and replay
The public broker has no Google-system secret and no replay store. It bounds and forwards the exact signed bytes
through the existing private service-authenticated binding. The private Worker alone verifies HMAC and atomically
consumes the nonce.
Invariant
R6 decision
Signed bytes
Method, normalised path/query, timestamp, nonce, body SHA-256, Idempotency-Key, correlation
ULID, blank separator and exact raw body.
Key rotation
One PRIMARY and one optional OVERLAP key slot; unknown/misconfigured IDs fail generically.
Clock
Private-server ±300 seconds.
Framing
Exact Content-Length, UTF-8 JSON content type, no transfer/content encoding, no BOM, strict
path/query/header grammar.
Nonce
Atomic create-if-absent under
candidate-daily-google-nonces/v1/{environment}/{key_id}/{timestamp}/{nonce}; retained at least ten
minutes.
Business retry
Fresh nonce/signature; same caller Idempotency-Key. Phase 2 receipt owns exact business replay.
Correlation
Candidate value may be generated before processing; signed system value is mandatory, signed
and never replaced.
No raw password, token, Candidate identity, Script Property value, HMAC secret or Google
payload is written into the nonce record. Only bounded creation/expiry metadata and the
signed-message digest are retained.
~~~


## Historical decision page 67

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
PHASE 1A R6 ADDENDUM - 16 AUGUST 2026
Sections 57–64 - Google evidence gate and dark broker implementation
R6 addendum page 6
61. Legacy, Master Rota and Emergency coexistence
The temporary legacy system must remain fully functional until the new Candidate App is proven. Phase 1A deliberately
changes no legacy client or Google source. The later Phase 3 adapter translates existing server-side behaviour into
bounded signed CloudTMS requests and translates results back into the existing browser response shape.
Boundary
Must remain true
Legacy browser
No new login/session/token/UUID/Supabase/HMAC knowledge; existing UI and msisdn lookup
preserved.
Availability Apps Script
Smallest server-side compatibility facade; stable request identity across timeout/reload; no direct
browser CloudTMS write.
Master Rota
Generation publisher/consumer barriers added only in Phase 3; existing triggers and unrelated
consumers preserved.
Emergency/specialist
Both old and new clients ultimately use one CloudTMS receipt/effect authority; no blind resend after
uncertainty.
Decommissioning
Legacy browser and adapter are removed only after successful proving and separate authorisation.
- Do not repair unrelated legacy defects during coexistence work.
 
- Do not infer a Candidate UUID from browser input; the later trusted source mapping resolves and revalidates it
 server-side.
- Do not revive ai_startDailyPings or add a trigger merely because a historical reference exists.
 
- Do not let Emergency, running-late, cannot-attend, leave-early, DNA or acknowledgement actions acquire a second
 effect receipt.
~~~


## Historical decision page 68

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
PHASE 1A R6 ADDENDUM - 16 AUGUST 2026
Sections 57–64 - Google evidence gate and dark broker implementation
R6 addendum page 7
62. Verification and safety result
Verification gate
Final result
Focused production-module TAP
13 passed; 0 failed
Complete backend TAP
576 passed; 0 failed
Runtime/OpenAPI route parity
24/24 exact; merged API SHA-256 1e4362f3…f765fa
HMAC Node/Python
3 positive, 2 route-valid, 24 negative, 5 query, 20 raw-parser — PASS in both runtimes
Source identity Node/Python
6 positive, 5 normalisation, 2 malformed, 9 negative, 2 bindings — PASS in both
runtimes
R5 pack validator
154 decisions, 25 operations, 4 policies, 63 effects, 28 adapters — PASS
Worker dry builds
Candidate public, Candidate private and normal TEST Worker — PASS
Syntax/diff
All changed JavaScript parses; git diff --check PASS
Safety action
Result
Google data/source/deployment mutation
None
Supabase/SQL/RPC mutation
None
Candidate flag/entitlement change
None
Worker/frontend deployment
None
External effect or communication
None
Production access
None
Finance/Banking Pay/Policy X change
None
The locked dependency installation reported inherited npm advisories. No broad dependency rewrite was authorised or performed. They
remain a separate repository-maintenance concern and do not alter the Phase 1A source diff.
~~~


## Historical decision page 69

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
PHASE 1A R6 ADDENDUM - 16 AUGUST 2026
Sections 57–64 - Google evidence gate and dark broker implementation
R6 addendum page 8
63. Decision register extension AV-155–AV-180
AV-001–AV-154 remain in the accepted R5 ledger and matrix. R6 adds 26 decisions. The full row-by-row wording and
proof owner are in the R6 compliance matrix included with this PDF.
IDs
Decision family
Disposition
AV-155–AV-160
Google gate, certified source/deployment relation and ai_startDailyPings
Evidenced; no mutation
AV-161–AV-162
Minimal-change legacy containment and dual-client Emergency
compatibility
Binding; existing behaviour
preserved
AV-163–AV-167
Sole merged OpenAPI, exact routes, additive bootstrap and dark capability
Implemented and tested
AV-168–AV-173
Correlation, idempotency, body/rate/deadline and distributed concurrency
ownership
Implemented except durable
in-flight owner deferred to Phase
2/1B
AV-174–AV-177
HMAC v1, key rotation, private nonce and separate business replay
Implemented and vector-tested
AV-178–AV-180
No deployment/mutation, Phase 2 boundary and remaining full-app phases
Gates retained
No R6 decision authorises Daily activation, SQL installation, Google editing/deployment,
Candidate UI publication, communication effects or production. A later GO must be granted at
each named gate.
~~~


## Historical decision page 70

~~~text
CLOUDTMS CANDIDATE APP — CURRENT DECISIONS
PHASE 1A R6 ADDENDUM - 16 AUGUST 2026
Sections 57–64 - Google evidence gate and dark broker implementation
R6 addendum page 9
64. Remaining phases to full implementation
Phase
Required outcome
Current status
Independent R6
review
Audit manifests, source diff, HMAC vectors, Google evidence and AV-001–AV-180.
Next
Phase 2
Author 12 additive tables and 13 service-only RPC owners; PostgreSQL 17/18
disposable verification; separate TEST-install approval.
Not started
Phase 1B
Wire all 24 operations to Phase 2, including receipts/leases, concurrency, Google
projection, effects and real public-private-RPC tests.
Blocked on Phase 2
Phase 3
Minimal Availability/Master server-side adapters, consumer barriers, outage/recovery
proof; no legacy browser redesign.
Evidence gate
complete; edit not
authorised
Phase 4
Daily UI for responsive web, iOS and Android from the same broker contract; no client
business authority.
Not started
Phase 5
Controlled TEST authority cutover, explicit global/cohort enablement and rollback
rehearsal.
Not authorised
Phase 6
Full Emergency/specialist/workflow acceptance through both old and new clients with
explicit effect approval.
Not started
Phase 7
Gradual cohorts, continuous gates, then separately authorised legacy app/adapter
decommissioning.
Not started
Current verdict: Phase 1A source is ready for independent review, but the Candidate Daily
product remains disabled and incomplete. The full Candidate App includes the already
accepted Weekly workflows plus every remaining Daily, Google coexistence, native/web UI,
specialist, cutover and rollout phase above.
~~~


## Historical decision page 71

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 1A R7 CORRECTION - 16 AUGUST 2026
Sections 65-69 - R6 transport correction and coexistence clarification
R7 addendum page 1
Current Decisions
Candidate Daily Phase 1A correction
Later-controlling R7 addendum - Sections 65-69
Authority
Current fact
Decision date
16 August 2026
Base authority
Accepted 61-page R5 plus the 9-page R6 addendum; Sections 1-64 remain controlling except where
R7 expressly supersedes an operational fact.
R7 purpose
Close all nine bounded findings from the independent R6 audit without beginning Phase 2.
Product state
Candidate Daily remains globally disabled, database-dark, Google-unchanged and effect-free.
Next gate
Independent R7 review; only a GO may release Phase 2 additive SQL/RPC authoring.
R7 is a correction, not an expansion. It changes the public failure boundary, pre-auth system
throttling, nonce retention, framing and canonicalisation proofs. It adds no Daily table/RPC,
Google adapter, Candidate UI, entitlement or business effect.
~~~


## Historical decision page 72

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 1A R7 CORRECTION - 16 AUGUST 2026
Sections 65-69 - R6 transport correction and coexistence clarification
R7 addendum page 2
65. Independent R6 findings and closure
The independent R6 audit issued a bounded NO-GO for Phase 1A source. It accepted the 24-route catalogue, additive
bootstrap success, access topology, HMAC verification order, body/rate/deadline metadata and dark/no-mutation
boundary, but identified nine exact transport defects. R7 retains the accepted design and closes each defect directly.
Finding
R7 controlling correction
R6-ERR-001
Every Daily/bootstrap error has exactly ok, error_code, correlation_id, retry_class and required safe message;
details is absent or one typed closed variant.
R6-RESP-001
The public broker validates route/status/code/retry/correlation/schema and rebuilds the body; any private drift
becomes one conforming generic dependency error.
R6-ORIGIN-001
Disallowed Origin, client and preflight/header policy failures remain public 403 FORBIDDEN /
DO_NOT_RETRY.
R6-RATE-001
Signed-system pre-auth traffic consumes both a source-IP bucket and a trusted-key/shared-invalid-key
bucket before private HMAC work.
R6-NONCE-001
Ten-minute replay retention starts at successful server consumption, never at the caller-signed timestamp.
R6-CORR-001
A valid signed correlation is preserved; missing/invalid input is rejected at the public pre-auth edge with a
newly generated valid response ULID.
R6-FRAME-001
When Content-Length is supplied, Candidate and system paths require exact equality with actual bytes.
R6-HMAC-QUERY-001
Percent-encoded query names/values use explicit ASCII/code-unit tuple ordering in JavaScript and Python.
R6-RAW-001
The deployed contract is stated at the Fetch-observable header boundary; raw pre-normalisation properties
are never claimed without a platform-level HTTP probe.
~~~


## Historical decision page 73

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 1A R7 CORRECTION - 16 AUGUST 2026
Sections 65-69 - R6 transport correction and coexistence clarification
R7 addendum page 3
66. Closed response and transport authority
The merged R5 OpenAPI remains the sole API authority. R7 does not edit its route or error matrices. Runtime tests load
the frozen 25-operation matrix and compare every permitted status/error/retry triple exactly, including bootstrap.
Boundary
Required behaviour
Daily error
additionalProperties=false; fixed public message; correlation is a valid ULID; no untyped
unavailable_reason object.
Public response
Never forwards private JSON bytes. It reconstructs allowlisted public fields and safe headers only.
Nonconforming private body
Returns CANDIDATE_DAILY_NOT_READY/STATUS_CHECK on Candidate routes or
DEPENDENCY_UNAVAILABLE/RETRY_AFTER on bootstrap/system routes.
Phase 1A success
No new Daily business success is possible. A wrong or unexpected success schema fails closed to
the generic dependency result.
403 policy
Origin, native-client and preflight/header rejection stay 403 FORBIDDEN / DO_NOT_RETRY, never
500 retryable.
Framing
Candidate maximum remains 32 KiB; system maximum remains 256 KiB; supplied declared length
must equal bytes read.
Private implementation data cannot cross the public broker merely because it is valid JSON.
Internal stack, database, token, storage, diagnostic and unknown future fields all cause safe
generic reconstruction.
~~~


## Historical decision page 74

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 1A R7 CORRECTION - 16 AUGUST 2026
Sections 65-69 - R6 transport correction and coexistence clarification
R7 addendum page 4
67. Rate, nonce, HMAC and platform boundary
Invariant
R7 decision
Pre-auth rate
One IP bucket always applies. An accepted PRIMARY/OVERLAP key ID may have its own second
bucket; every unverified/unknown ID shares one invalid-key bucket. Rotating attacker labels cannot
multiply private work.
Nonce lifetime
R2 uploaded time or the stored server consumption epoch owns age. At 599 seconds it remains; at
600 seconds it is eligible for cleanup, including requests accepted at both +/-300-second clock edges.
Correlation
The HMAC signs a valid supplied ULID. The public edge does not alter it. Missing/invalid signed input
never reaches private and the error envelope still carries a valid generated ULID.
Query ordering
Canonical percent-encoded strings are sorted by explicit ASCII/code units, then value; locale collation
is prohibited.
Raw headers
Workers can enforce only the Fetch Request/Headers representation they receive. Duplicate lines
may already be combined and outer whitespace may be normalised. The verifier rejects the resulting
ambiguous/invalid observable value; direct raw-pair vectors remain reference-parser evidence, not a
claim that wire bytes survive Cloudflare unchanged.
- No public HMAC secret or nonce store is introduced.
 
- No unknown key ID receives an independent throttle bucket.
 
- The R5 signed prefix/body algorithm remains unchanged; only deterministic query ordering and replay-age ownership are
 corrected.
- Future routes with a non-empty query must use the same corrected Node/Python vectors before activation.
~~~


## Historical decision page 75

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 1A R7 CORRECTION - 16 AUGUST 2026
Sections 65-69 - R6 transport correction and coexistence clarification
R7 addendum page 5
68. Google coexistence and decommission boundary
The user deployed NEW MASTER ROTA current Head as active web-app version 101 on 16 August 2026. That later
operational fact supersedes the R6 evidence statement that active version 100 differed from Head. The current Head
hash and certified-source relationship remain the recorded authority; Phase 3 must recheck them immediately before
any Google edit.
Decommissioning the temporary legacy Availability browser and its LEGACY_COMPAT facade
does not decommission the Availability Sheet/Apps Script service, Emergency functions,
Master Rota publication, signed system synchronisation, projection/freshness routes or effect
authority.
Component
Lifecycle decision
Legacy browser/UI/login
Preserve unchanged during coexistence; remove only after the new Candidate app is proven and
separately approved.
LEGACY_COMPAT facade
Temporary server-side translation for the old browser. It may be removed with the old browser after
accepted cutover.
Availability Sheet/Apps Script
Continues after browser retirement until its emergency/specialist responsibilities are separately
migrated and accepted.
NEW MASTER ROTA
Continues publishing generation/working truth required by Availability and specialist flows; it sends
to the signed CloudTMS system boundary as Phase 3 settles.
Emergency/specialist flows
Must work with both old and new clients during coexistence and retain one CloudTMS receipt/effect
authority. No flow is silently retired with the old browser.
Final specialist retirement
A separate later decision, migration and acceptance gate; never inferred from Candidate app
launch.
~~~


## Historical decision page 76

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 1A R7 CORRECTION - 16 AUGUST 2026
Sections 65-69 - R6 transport correction and coexistence clarification
R7 addendum page 6
69. Decision register AV-181-AV-192 and remaining gate
IDs
Decision family
Current disposition
AV-181-AV-183
Closed Daily/bootstrap errors, public response rebuilding and exact 403
policy
Implemented; adversarially
tested
AV-184-AV-188
Pre-auth throttling, consumption-age nonce, correlation, exact framing,
ASCII query and honest Fetch boundary
Implemented;
Node/Python/platform tests
AV-189
Master Rota active web deployment v101 now represents current Head
User-deployed operational fact;
future edit recheck required
AV-190
Legacy browser retirement is separate from continuing
Availability/Emergency/Master/system services
Binding
coexistence/decommission rule
AV-191
Any R7 TEST deployment remains dark; flags false; no SQL, Google, UI or
external effect
Required safety boundary
AV-192
Phase 2 remains the next phase only after independent R7 GO
Not started / separately gated
Current verdict requested: GO or bounded NO-GO for corrected Phase 1A only. Do not infer
Phase 2 SQL approval, Google edit/deploy approval, Candidate UI completion, feature
enablement, production authority or legacy/specialist decommissioning.
~~~


## Historical decision page 77

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R8 - 17 AUGUST 2026
Sections 70-79 - installed Daily authority and broker-to-RPC integration
R8 addendum page 1
Current Decisions
Candidate Daily Phase 2 and Phase 1B
Later-controlling R8 addendum - Sections 70-79
Authority
Current fact
Decision date
17 August 2026
Base authority
Accepted R5 plus R6/R7; Sections 1-69 remain controlling unless this R8 addendum expressly
records the implemented later phase.
R8 purpose
Install the exact twelve-table/thirteen-RPC Phase 2 authority and deploy complete Phase 1B
broker-to-RPC integration.
Product state
Installed/deployed in TEST; all thirteen Candidate flags false; no entitlement, source, generation,
availability, receipt or effect row.
Next gate
Independent R8 review. A GO releases Phase 3 work only; it is not activation, full-app completion or
production authority.
R8 establishes business authority without enabling it. The database and Workers are present,
but Candidate Daily remains unavailable until the complete feature, entitlement, mode,
generation, freshness, parity and rollout gates pass.
~~~


## Historical decision page 78

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R8 - 17 AUGUST 2026
Sections 70-79 - installed Daily authority and broker-to-RPC integration
R8 addendum page 2
70. Phase disposition and full-product boundary
Phase 0 R5 and corrected Phase 1A R7 are accepted. R8 implements Phase 2 and Phase 1B as the next
dependency-controlled package. The full Candidate application remains broader than Daily availability and retains all
accepted authentication, session, submission, approval, QR/electronic, evidence, notification, Office and workflow
authorities.
Phase
R8 disposition
Phase 2
Twelve-table/thirteen-RPC additive authority implemented and installed in TEST.
Phase 1B
Candidate/system operations wired public broker -> private Worker -> installed RPCs; deployed disabled.
Phase 3
Not started. Minimal Google server adapter and Master dual publication remain mandatory.
Phase 4
Not started. Complete responsive web/iOS/Android Daily UI and shadow parity remain mandatory.
Phase 5
Controlled TEST cutover not authorised; rollback, parity, soak and error budgets required.
Phase 6
Full Emergency/specialist and DAILY signing/EMAIL/PHONE acceptance remains mandatory.
Phase 7
Gradual rollout and separately authorised legacy-browser retirement remain mandatory.
A Phase 2/1B GO is not a claim that the app is complete and must never enable a Candidate
flag, create an entitlement, edit Google, run a real effect or begin production rollout.
~~~


## Historical decision page 79

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R8 - 17 AUGUST 2026
Sections 70-79 - installed Daily authority and broker-to-RPC integration
R8 addendum page 3
71. Exact additive database authority
Table owner
Purpose
authority_scopes / entitlements
One mode/version fence and exact Candidate/cohort enablement.
source_links
Trusted legacy-source digest to one canonical Candidate; no raw browser secret or arbitrary
Candidate nomination.
command_receipts / batch_receipts
Factual idempotency key, request hash and durable exact result.
rota_generations / rota_days
Immutable versioned generation and complete fourteen-day day facts.
availability_days
Canonical Candidate/day value bound to a generation.
sheet_projection_outbox / sync_state
Lease/retry/park plus sole durable/effective cursor and freshness authority.
authority_transitions
Immutable append-only cutover/rollback transition record.
external_effect_receipts
Exact Emergency/specialist effect claim, lease, completion and status.
- Exactly twelve additive tables; no table is a replacement for the seven accepted Candidate core business tables.
 
- Every Daily table has RLS and no direct anon/authenticated/service-role DML.
 
- Exactly thirteen service-role-only security-definer RPCs own all business access.
 
- The migration adds candidate_daily_enabled=false only; it creates no business row or entitlement.
~~~


## Historical decision page 80

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R8 - 17 AUGUST 2026
Sections 70-79 - installed Daily authority and broker-to-RPC integration
R8 addendum page 4
72. Identity, receipts and exact replay
Candidate calls derive identity from the authenticated private Candidate session. Legacy calls resolve one approved
environment/source identity through the private source-link catalogue. Neither request may nominate an arbitrary
Candidate UUID.
Invariant
Binding decision
Command key
Caller-owned Idempotency-Key identifies one factual Candidate/legacy operation.
Batch key
Generation publication has its own durable batch receipt and canonical request hash.
Exact replay
Same key plus same factual request returns the stored database result; an internal replay marker
never crosses publicly.
Conflict
Same key plus changed Candidate/source/generation/date/value/action facts conflicts.
Transport nonce
Always separate from database idempotency; legitimate retry uses a fresh HMAC nonce and the
same business key.
Sensitive data
No password, raw Candidate token, Apps Script secret or legacy browser session is stored in
receipts/source links.
~~~


## Historical decision page 81

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R8 - 17 AUGUST 2026
Sections 70-79 - installed Daily authority and broker-to-RPC integration
R8 addendum page 5
73. Generation, availability and Candidate capability
A Candidate Daily capability is true only when every server-owned prerequisite is true.
Electronic/DAILY shape alone is never evidence of availability capability.
Required proof
Owner
Global switch
settings_defaults candidate_daily_enabled
Exact entitlement
private.candidate_daily_entitlements
Candidate identity
authenticated Candidate session/account
Authority mode
private.candidate_daily_authority_scopes
Complete current generation
generation plus exact fourteen-day rota rows
Freshness/cursor
private.candidate_daily_sync_state
Operation preconditions
the exact read/write RPC
The existing bootstrap reads this same database helper and deep-preserves all accepted baseline fields. TEST has the
flag false and the entitlement/source/generation/availability tables empty, so the capability is false.
~~~


## Historical decision page 82

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R8 - 17 AUGUST 2026
Sections 70-79 - installed Daily authority and broker-to-RPC integration
R8 addendum page 6
74. Projection, freshness and deferred overlay
Rule
Controlling behaviour
Single outbox
All Sheet projection work is durable and claimable by one bounded lease owner.
Completion fence
Only the exact claim lease may complete/retry/park the row.
Dual cursor
One sync-state row owns durable cursor and effective-visible cursor; no browser/App Script memory
owns either.
Deferred overlay
Permitted only with exact current generation/hash proof.
Retreat
Overlay removal/change retreats effective visibility and requeues eligible parked work.
Freshness
Candidate reads fail closed on stale/missing generation/cursor state; signed-system status/projection
remains independently authenticated.
~~~


## Historical decision page 83

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R8 - 17 AUGUST 2026
Sections 70-79 - installed Daily authority and broker-to-RPC integration
R8 addendum page 7
75. Mode transition, rollback and effect authority
The only authority modes are GOOGLE_PRIMARY, ROLLBACK_PENDING and SUPABASE_PRIMARY. One atomic
RPC checks expected mode/version, generation, reconciliation and cursor/freshness barriers before appending the
immutable transition record.
Boundary
Decision
Direct flip
Prohibited when the required transition/reconciliation proof is absent.
Rollback
Explicit and auditable through ROLLBACK_PENDING; receipts/history are not deleted.
Effect claim
One factual effect key/request hash and bounded lease owner.
Effect complete
Exact lease/factual identity owns terminal result or retry state.
Lost response
Status reads the same durable result; another key is not invented.
Concrete provider
Fails typed unavailable until Phase 3/6 adapter exists; R8 runs no real effect.
~~~


## Historical decision page 84

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R8 - 17 AUGUST 2026
Sections 70-79 - installed Daily authority and broker-to-RPC integration
R8 addendum page 8
76. Phase 1B private and public integration
Layer
Sole responsibility
Public broker
Origin/client/rate/access-token/body/schema boundary; rebuilds strict allowlisted public responses.
Private Worker
Service authentication, Candidate session, Google HMAC/nonce and closed RPC composition.
Database
Canonical identity, policy, generation, availability, receipt, cursor, transition and effect truth.
Specialist seam
Typed dependency interface only; never a test-only fake or invented success in production.
- All accepted Daily operations map to an installed RPC or one explicitly later-gated specialist seam.
 
- Public responses never expose receipt hashes, source mappings, lease tokens, database errors or internal replay
 markers.
- Signed-system continuity does not consult the Candidate product switch.
 
- Candidate Daily reads/writes consult the complete database-owned capability conjunction.
~~~


## Historical decision page 85

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R8 - 17 AUGUST 2026
Sections 70-79 - installed Daily authority and broker-to-RPC integration
R8 addendum page 9
77. Minimal legacy change and continuing Google services
Do not modernise the temporary legacy app. Contain it. Preserve its browser UI, login and
msisdn lookup; strengthen only the later server-to-server Apps Script -> CloudTMS boundary.
Component
Lifecycle decision
Legacy browser
Unchanged through coexistence; receives no CloudTMS token/HMAC secret/Supabase
access/Candidate UUID selector.
Availability Apps Script
Phase 3 adds the smallest signed compatibility/projection/effect adapter while retaining existing
response behaviour.
NEW MASTER ROTA
Active current Head is web v101. Continue Availability publication and later add signed CloudTMS
generation publication.
Availability/Emergency
Continue during and after legacy-browser retirement until separately migrated and accepted.
Specialists
Must work through both clients during coexistence with one CloudTMS receipt/effect authority.
~~~


## Historical decision page 86

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R8 - 17 AUGUST 2026
Sections 70-79 - installed Daily authority and broker-to-RPC integration
R8 addendum page 10
78. Verification, deployment and safety
Gate
R8 fact
PostgreSQL
42 Candidate suites PASS on 17.6 and 18.1.
JavaScript
605 complete and 35 focused tests PASS.
OpenAPI/builds
62-path OpenAPI PASS; both Candidate Worker dry builds PASS.
Installed TEST
12 tables, 13 RPCs, exact ledgers/ACLs; 0/13 flags enabled; all Candidate/Daily rows zero.
Private Worker
Version 689bbe95-bf31-4f91-8e5a-40289558cefa at 100%.
Public broker
Version 18f67f8e-3ca2-46ad-9599-8512894de6c3 at 100%; health/readiness 200/200.
Safe migration
Candidate install succeeded; broad run then stopped only on three declared pre-existing James
definition hashes.
No change
No Google/frontend/normal Worker/finance/Banking Pay/production change; no Candidate
data/effect/communication.
~~~


## Historical decision page 87

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R8 - 17 AUGUST 2026
Sections 70-79 - installed Daily authority and broker-to-RPC integration
R8 addendum page 11
79. Decision register AV-193-AV-228 and next gate
IDs
Decision family
Disposition
AV-193-AV-200
Exact tables/RPCs/ACL/flag/system continuity/Candidate
capability/bootstrap
Implemented and installed;
product disabled
AV-201-AV-208
Legacy source mapping, no browser authority, generation and exact
receipts
Implemented; no real
source/data
AV-209-AV-216
Outbox/lease/cursors/overlay/modes/rollback/effect receipt
Implemented and PG17/18
verified
AV-217-AV-222
Typed specialist seam, Phase 1B mapping, session identity, response
reconstruction, replay separation, rates
Implemented; specialist
execution later-gated
AV-223-AV-228
Disabled/empty TEST, no Google change, Master lifecycle, full-app
remaining phases and no unrelated drift
Proved/preserved
Current verdict requested: GO or bounded NO-GO for Phase 2 and Phase 1B R8. A GO releases
Phase 3 implementation only. It does not enable Candidate Daily, complete the Candidate UI,
authorise real effects, deploy production or retire the legacy browser.
~~~


## Historical decision page 88

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R9 - 17 AUGUST 2026
Sections 80-84 - database-owned authority transition correction
R9 addendum page 1
Current Decisions
Candidate Daily Phase 2 and Phase 1B
Later-controlling R9 correction - Sections 80-84
Authority
Current fact
Decision date
17 August 2026
Base authority
Accepted R5/R7 plus the R8 implementation. Sections 1-79 remain controlling except where this
addendum corrects R8 transition proof.
Independent finding
R8 documented cutover/rollback barriers but the transition RPC could commit without independently
proving the complete locked database state.
R9 correction
One existing RPC now owns the complete source, generation, cursor, reconciliation, overlay and
in-flight proof in the same transaction as the immutable transition.
Product state
Installed/deployed in TEST but all Candidate flags remain false and Candidate/Daily tables remain
empty.
Next gate
Fresh independent R9 operation-level review. No feature activation, Google edit, Candidate data,
production or legacy retirement is authorised.
The request supplies expectations, not authority. PostgreSQL must lock, derive and freeze the
database-winner facts before any mode change can commit.
~~~


## Historical decision page 89

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R9 - 17 AUGUST 2026
Sections 80-84 - database-owned authority transition correction
R9 addendum page 2
80. R8 finding and bounded R9 disposition
The R8 package materially passed schema, RPC, broker/private mapping, access control and disabled-state review. Its
single blocker was limited to candidate_daily_authority_transition_atomic_v1. The function accepted sparse caller
transition facts and did not prove the complete cutover/rollback barrier described by AV-214 and AV-215.
Boundary
R9 decision
Schema
No change: exactly twelve Daily tables.
Public RPC catalogue
No change: exactly thirteen service-role-only Daily RPCs.
HTTP/OpenAPI
No change: existing Phase 1B mapping and response contract remain exact.
Runtime source
Only the existing transition definition and executable regression workflow change.
Features/data
Remain disabled and empty; R9 creates no real scope, source, entitlement or effect.
Protected owners
No Google, Office, finance, Invoice, Banking Pay, Policy X, provider or production change.
~~~


## Historical decision page 90

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R9 - 17 AUGUST 2026
Sections 80-84 - database-owned authority transition correction
R9 addendum page 3
81. Locked database proof and derived disposition
Locked owner
Required proof
Feature/scope/entitlement
Exact existing scope, expected mode/version/entitlement and closed global-switch relationship.
Source links
Exactly one time-current PRIMARY in exactly one active link group.
Generation/days
Exact active expected generation/version, published and complete with fourteen day rows.
Sync state
READY plus accepted, required-visible and effective-visible cursors all equal the locked canonical
version.
Current facts
Reconciliation timestamp is no older than generation, availability and projection facts.
In-flight owners
Commands, other batches, effects and all projection rows are locked and classified.
PostgreSQL derives DRAINED, RECONCILED or NONE. Caller CANCELLED is never derived
and cannot authorise a transition.
~~~


## Historical decision page 91

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R9 - 17 AUGUST 2026
Sections 80-84 - database-owned authority transition correction
R9 addendum page 4
82. Generation, cursor, overlay and rollback rules
Rule
Fail-closed behaviour
Generation
Missing, BUILDING, partial, stale, wrong ID/version or incomplete day set rejects.
Cursor
Missing sync, cursor lag, non-READY state, pending/retry/terminal count or missing
revision/reconciliation rejects.
Overlay
DEFERRED_OVERLAY counts only with exact generation ID/version, date and source-row hash.
Unresolved work
PENDING, CLAIMED, RETRY, TERMINAL, command, other batch, IN_PROGRESS effect and
UNKNOWN effect all block.
Forward
Google to Supabase uses the full strict barrier; entitlement may remain false for dark proving.
Rollback
Disable globally first, enter ROLLBACK_PENDING with entitlement false, then use the full strict
barrier before Google becomes primary.
~~~


## Historical decision page 92

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R9 - 17 AUGUST 2026
Sections 80-84 - database-owned authority transition correction
R9 addendum page 5
83. Replay, concurrency, cohorts and immutable evidence
- A missing authority scope is rejected and never created by transition.
 
- An exact no-op returns NO_CHANGE and appends no transition.
 
- Same key plus the same request returns the exact durable batch result; changed facts under that key conflict.
 
- Concurrent exact callers serialize on one batch receipt and receive one result.
 
- Concurrent different-key cutovers serialize on deterministic scope locks; one commits and the other receives an explicit
 stale-precondition rejection.
- Every cohort item resets all local state and runs in an isolated subtransaction; one rejection cannot leak a source row,
 entitlement or transition fence.
- Every authority-changing ledger entry freezes the locked generation version, complete sync snapshot and
 database-derived in-flight disposition.
Decision IDs
Later-controlling family
AV-229-AV-232
Existing scope, locked proof, derived disposition and one source authority
AV-233-AV-236
Complete generation, exact cursors/latest facts and exact overlay
AV-237-AV-240
All in-flight owners, entitlement/mode, two-stage rollback and winner snapshots
AV-241-AV-244
Partial cohorts, parallel replay/single winner, no-op and dual-engine executable proof
~~~


## Historical decision page 93

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R9 - 17 AUGUST 2026
Sections 80-84 - database-owned authority transition correction
R9 addendum page 6
84. Verification, deployment and next gate
Gate
R9 fact
Runtime commit
b3ee95cfc5f2cdfef09a85810f525d8912dec438
PostgreSQL
43 Candidate SQL suites plus real-chain and parallel transition proof PASS on 17.6 and 18.1.
JavaScript
613 complete tests PASS; focused transition source contract PASS.
Worker builds
Normal TEST, private Candidate and public Candidate dry builds PASS.
Candidate DB workflow
32019728370
Safe migration/install
32019728348
Installed repeatable SHA-256
0c3a67fd4bdbf49bfaee7c7604f5f8ae31f84dafb6e1f5978baf93244cafdabb
TEST Worker versions
normal 8411682c-489e-4ad4-a591-b021c5e0bf4d; private
9d73bbff-5099-4f12-a58d-64cb9dbb4889; public 09ac826b-d7da-4932-b0ad-a5fe6e194779
Safety
All Candidate flags false; Candidate core/Daily/mail rows empty; production untouched.
Requested verdict: GO or one bounded evidence-backed NO-GO for the R9 authority-transition
correction. A GO grants the outstanding Phase 2 authority verdict while retaining the existing
Phase 1B transport GO, and permits the already-planned Phase 3 gate only. It does not activate
Candidate Daily or complete the full Candidate App.
~~~


## Historical decision page 94

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R10 - 17 AUGUST 2026
Sections 85-88 - first-rollback unresolved-work correction
R10 addendum page 1
Current Decisions
Candidate Daily Phase 2 and Phase 1B
Later-controlling R10 correction - Sections 85-88
Authority
Current fact
Decision date
17 August 2026
Base authority
Accepted R5/R7, R8 architecture and R9 locked transition proof. Sections 1-84 remain controlling
except for the bounded first-rollback correction here.
Independent finding
R9 derived unresolved work as NONE, but the first SUPABASE_PRIMARY to ROLLBACK_PENDING
edge could still commit when the caller truthfully supplied NONE.
R10 correction
Every changed authority mode now rejects database-derived NONE as
CANDIDATE_DAILY_NOT_READY after caller/database disposition equality.
Transport status
Phase 1B GO remains in force. R10 changes no private/public HTTP mapping.
Product state
Installed in TEST for review only; all Candidate flags remain false and Candidate/Daily tables remain
empty.
Next gate
Fresh independent R10 review. Phase 3 remains blocked until Phase 2 receives GO.
NONE accurately describes unresolved work. Accuracy does not make unresolved work safe
for an authority switch.
~~~


## Historical decision page 95

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R10 - 17 AUGUST 2026
Sections 85-88 - first-rollback unresolved-work correction
R10 addendum page 2
85. R9 finding and exact R10 disposition
The R9 package materially passed its locked database proof, Phase 1B mappings, access control, disabled-state safety
and dual-engine matrix. Its remaining blocker was one missing semantic barrier on the first rollback edge. The equality
check rejected a false DRAINED claim, but a truthful NONE claim could pass because that edge did not use the later
strict parity block.
Boundary
R10 decision
Runtime
Add one database-owned guard after derived/caller disposition equality.
First rollback
SUPABASE_PRIMARY to ROLLBACK_PENDING rejects NONE before writing entitlement, mode or
ledger.
Final rollback
Existing complete R9 Google parity barrier remains unchanged.
No-op
Same-mode exact no-op with NONE remains NO_CHANGE.
Schema/RPC/API
No table, signature, RPC, route, request or response change.
Protected owners
No Google, Office, finance, Invoice, Banking Pay, Policy X, provider or production change.
~~~


## Historical decision page 96

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R10 - 17 AUGUST 2026
Sections 85-88 - first-rollback unresolved-work correction
R10 addendum page 3
86. Stable semantics and preserved settled paths
Locked outcome
Required result
Caller DRAINED; DB NONE
SEMANTIC_REJECTION because the caller assertion is false.
Caller NONE; DB NONE; mode
changes
CANDIDATE_DAILY_NOT_READY because unresolved work blocks authority movement.
Caller NONE; DB NONE; exact
mode/entitlement no-op
Existing NO_CHANGE result; no transition ledger row.
Caller DRAINED; DB DRAINED
Eligible to continue through every other applicable R9 barrier.
Caller RECONCILED; DB
RECONCILED
Eligible only with the existing exact deferred-overlay generation/date/hash proof.
The first rollback stage is not final Google cutover. It may accept settled DRAINED or exact
RECONCILED truth, but never unresolved NONE.
- The global Candidate Daily flag must already be false.
 
- Candidate entitlement must become or remain false.
 
- Expected mode, version and entitlement must equal locked database truth.
 
- The item subtransaction rolls back its transition fence and all local writes on rejection.
 
- The final ROLLBACK_PENDING to GOOGLE_PRIMARY edge retains full source, generation, cursor, reconciliation and
 overlay proof.
~~~


## Historical decision page 97

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R10 - 17 AUGUST 2026
Sections 85-88 - first-rollback unresolved-work correction
R10 addendum page 4
87. Complete first-rollback adversarial matrix
Unresolved database owner
R10 proof
Projection PENDING
False DRAINED conflicts; truthful NONE is not ready.
Projection CLAIMED
False DRAINED conflicts; truthful NONE is not ready.
Projection RETRY
False DRAINED conflicts; truthful NONE is not ready.
Projection TERMINAL
False DRAINED conflicts; truthful NONE is not ready.
Candidate command IN_PROGRESS
Truthful NONE is not ready.
Other Candidate Daily batch
IN_PROGRESS
Truthful NONE is not ready.
External effect IN_PROGRESS
Truthful NONE is not ready.
External effect UNKNOWN
Truthful NONE is not ready.
Two concurrent different-key attempts
Both reject; mode/fence/ledger remain unchanged.
Decision IDs
Later-controlling family
AV-245
NONE never authorises a changed authority mode
AV-246
Every unresolved owner blocks the first rollback edge
AV-247
False DRAINED and truthful unresolved NONE remain distinct
AV-248
No rejected/concurrent rollback authority, entitlement, ledger or fence drift
AV-249
Valid settled/no-op/R9 barriers remain intact
~~~


## Historical decision page 98

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
PHASE 2 + PHASE 1B R10 - 17 AUGUST 2026
Sections 85-88 - first-rollback unresolved-work correction
R10 addendum page 5
88. Verification, deployment and next gate
Gate
R10 fact
Runtime commit
304ff61ba3f6870caa43928fd11d8ddeb7914e9e
PostgreSQL
PostgreSQL 17.6 and 18.1: 43 suites + 2 concurrency tests PASS per engine
JavaScript
Complete backend JavaScript: 613 passed, 0 failed
Candidate DB workflow
32027528703
Safe migration/install
32027528744
Repeatable file SHA-256
ba297193832c9f2b9e4f3dad894bcee039deafe3d2a8096818839e5f8b518b85
Installed function SHA-256
bc0da2bc78a2df454aa3d658454af42b5014401562cf9afce2d2cb5b5b03096a
TEST Worker versions
normal 6ce0838a-862b-4abb-9fd1-d86e0202d5f4; private
9d73bbff-5099-4f12-a58d-64cb9dbb4889; public 09ac826b-d7da-4932-b0ad-a5fe6e194779
Safety
All Candidate flags false; Candidate core/Daily/mail rows empty; no real transition/effect;
production untouched.
Requested verdict: GO or one bounded evidence-backed NO-GO for R10. A GO closes the
outstanding Phase 2 rollback blocker while retaining Phase 1B GO and permits the planned
Phase 3 gate only. It does not activate Candidate Daily or complete the full Candidate App.
~~~


## Historical decision page 99

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R11 - 17 AUGUST 2026
Sections 89-94 - minimal Google coexistence bridge
R11 addendum page 1
Current Decisions
Candidate Daily Phase 3
R11 minimal Google coexistence bridge - Sections 89-94
Authority
Current fact
Decision date
17 August 2026
Base authority
Accepted Sections 1-88 and AV-001 through AV-249 remain controlling.
Phase 3 result
Complete, unredacted and copy/paste-ready Availability API and NEW MASTER ROTA source
package.
Legacy boundary
Do not modernise the temporary browser. Contain it behind the existing Apps Script projects.
Disabled state
A missing or false bridge flag is an exact legacy-only path with no bridge request, retry, state, log or
Sheet write.
Installation state
Source prepared only. No live Google edit, save, version, deployment, trigger or Sheet mutation was
performed.
Feature state
Candidate Daily flags and entitlements remain false. Phase 3 source does not activate a user journey.
Next gate
Independent R11 source review, then controlled Google installation and coexistence proving with the
flag initially false.
CLOUDTMS_CANDIDATE_BRIDGE_ENABLED=false means the certified legacy path continues
exactly as it is.
~~~


## Historical decision page 100

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R11 - 17 AUGUST 2026
Sections 89-94 - minimal Google coexistence bridge
R11 addendum page 2
89. Phase 3 scope and minimal-change authority
The legacy Availability browser is temporary and will be retired after the new Candidate App is proven. Phase 3
therefore adds no browser authentication redesign, CloudTMS client token, Supabase client, canonical Candidate
identifier, new UI or unrelated legacy repair.
Boundary
Controlling decision
Legacy browser
UI, login, msisdn lookup and response contracts remain unchanged.
Availability API
Certified business logic and legacy Sheet/cache path remain authoritative while disabled.
NEW MASTER ROTA
Existing Availability publication remains first; signed CloudTMS generation is additive.
Security boundary
Trusted Apps Script to CloudTMS Worker only; the browser never calls CloudTMS or Supabase.
Emergency/specialists
Continue on existing paths; Phase 3 adds receipt primitives but invokes no provider.
Triggers
No trigger is created, deleted or changed; ai_startDailyPings remains orphaned and untouched.
Do not fix unrelated legacy defects during coexistence. Only the bounded compatibility seams
are authorised.
~~~


## Historical decision page 101

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R11 - 17 AUGUST 2026
Sections 89-94 - minimal Google coexistence bridge
R11 addendum page 3
90. Strict disabled-state equivalence
Flag state
Required behaviour
Missing, blank or false
Return before HTTP, retry, status, logging, Script Property, Script Lock or bridge-owned Sheet work.
Helper file absent
Every guarded seam returns the already-built certified legacy result.
True
Permit the additive server-to-server bridge; legacy work still executes first.
Any other value
Disabled.
Phase 3 configuration is installed as Google Apps Script Project Script Properties. These are not JavaScript globals and
are not pasted into Code.gs.
Project Script Property
Controlled TEST value or owner
CLOUDTMS_CANDIDATE_BRIDGE_ENABLED
false initially in both projects; only case-insensitive true enables.
CLOUDTMS_CANDIDATE_BASE_URL
https://test-cloudtms-candidate-broker.kier-88a.workers.dev
CLOUDTMS_CANDIDATE_ENVIRONMENT
TEST
CLOUDTMS_CANDIDATE_GOOGLE_HMAC_KEY_ID
New Phase 3 TEST ID; none is installed now. Recommended:
candidate-daily-google-test-v1
CLOUDTMS_CANDIDATE_GOOGLE_HMAC_SECRET
New random signing secret after source GO; private verifier plus both
Google projects only.
CLOUDTMS_CANDIDATE_SOURCE_HMAC_SECRET
Different new random source-identity secret for both Google projects and
source-link bootstrap.
CLOUDTMS_CANDIDATE_EXECUTOR_ID
Availability API only: availability-api-google-test
- Legacy tiles continue from Master Rota-backed Availability and EmailHistory data plus the existing cache.
 
- Legacy availability changes continue through the existing queue and Sheet mapping.
 
- Master Rota continues the existing Availability event publication.
 
- Emergency, messages, specialist, timesheet and retained Google behaviours remain the certified code.
 
- Rollback is immediate by setting the flag false; exact certified Code.gs rollback files are also supplied.
~~~


## Historical decision page 102

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R11 - 17 AUGUST 2026
Sections 89-94 - minimal Google coexistence bridge
R11 addendum page 4
91. Signed transport, identity and replay
Apps Script uses the existing trusted msisdn lookup to locate Public ID - Credentially, derives an environment-bound
non-reversible source HMAC with a separate property-owned secret, and signs exact UTF-8 request bytes with the
frozen CLOUDTMS-HMAC-V1 protocol. Raw public ID, mobile number and email are not sent.
Authority
Required property
Source mapping
CloudTMS independently maps the signed source HMAC to one approved Candidate source link.
Request signing
Timestamp, fresh nonce, ULID correlation ID, body SHA-256 and HMAC-SHA-256 use frozen
canonical bytes.
Operation identity
Request/batch ID, idempotency key, correlation, source HMAC and factual body are frozen under
Script Lock.
Recovery
Status first; first authoritative not-found may consume one exact retry; continued uncertainty is
status-only.
Logging
Enabled-only structured control facts; no identity, payload, token, signature or secret.
The supplied helpers call the public Candidate broker only with POST under /candidate-system/v1/google-availability.
The exact Phase 3 suffixes are legacy/tiles, legacy/availability, legacy/availability-status, rota-generations,
projection/claim, projection/complete, effects/claim, effects/complete and effects/status. The public broker maps that
prefix to the private service binding; the private Candidate Worker validates the closed body and invokes the installed
Phase 2 RPC owner.
~~~


## Historical decision page 103

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R11 - 17 AUGUST 2026
Sections 89-94 - minimal Google coexistence bridge
R11 addendum page 5
92. Availability reads, writes and projections
Operation
Controlling sequence
Tile read
Build legacy envelope; signed Worker read of canonical Phase 2 tiles; merge by date; retain legacy
cohorts/welcome/emergency facts; fail open to unchanged envelope.
Availability change
Complete existing legacy Sheet path first; mirror factual dates/codes with one stable key; legacy result
remains the browser result.
Projection
Claim up to fifty rows; map exact candidate/date; preserve booked/system-blocked overlays;
otherwise use existing map-write; complete the exact lease.
Projection scheduling
No trigger in Phase 3. Controlled scheduling is a later TEST proving decision.
Effects
Claim/complete/status primitives only. No Emergency or specialist provider is invoked merely by
installation.
Canonical tiles come from Supabase through the CloudTMS Worker, never from direct browser
or Apps Script Supabase access.
~~~


## Historical decision page 104

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R11 - 17 AUGUST 2026
Sections 89-94 - minimal Google coexistence bridge
R11 addendum page 6
93. Master Rota dual publication and retained services
Fact
Decision
Publication order
Existing Availability event first. Mirror only after accepted legacy AVAILABILITY_UPDATE_END.
Generation
Exactly fourteen days per candidate with stable day/item hashes and batches of at most fifty.
Booked shifts
EmailHistory remains source truth; existing parser is reused; unresolvable booked time fails closed.
Uncertainty
Persist and reuse the same batch, key, correlation and exact body.
After browser retirement
Master Rota still feeds Availability and Emergency and also mirrors CloudTMS until separately
migrated and accepted.
Decommission scope
Retiring the temporary browser does not retire Availability API, Emergency, Master Rota, projections,
freshness or specialist services.
The new app does not make Availability or Emergency independent of Master Rota. Dual
publication is deliberate retained infrastructure.
~~~


## Historical decision page 105

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R11 - 17 AUGUST 2026
Sections 89-94 - minimal Google coexistence bridge
R11 addendum page 7
94. Verification, installation, rollback and next phases
Gate
R11 fact
Base repository authority
ea2ad4c5b23cf72cff21ecebae5f0ee5d4c85115
Focused JavaScript
48 passed, 0 failed
Complete backend JavaScript
625 passed, 0 failed
Availability revised/helper
4113dadcbd4044f222fafd51c78ff5bea8905cc22c8517107ba26866b269d905 /
4bc6cb8eaa77ef21ba98d90b52b0d05cc6363f9e41c800e430d95361869d85f9
Availability rollback
eacd187564ea9b0f00c1830f9240c6afcfe1a0d0611162c1bdf9b9fd6bbb3b3f
Master revised/helper
6d742f8fac4f9b98630f2afb44d4c2d7c7dc085a0c56c26391c6b921ee70db03 /
58e8da3948f2890b42abd802485776169ff500dedcf060d27b449a60597bcb2c
Master rollback
c3ae9c480a97ad2771312f5f453adbe7049c07219f89624f75df543d319fa0a8
Google install/deploy
Not performed; manual controlled TEST gate remains.
Current Google HMAC authority
Absent on deployed TEST public/private Candidate Workers; create new only after source GO.
Safety
No feature enablement, Candidate mutation, provider effect, finance change or production
action.
Decision IDs
Later-controlling Phase 3 family
AV-250 to AV-255
Strict disabled equivalence, minimal legacy containment, signed boundary, legacy-first and fail-open
AV-256 to AV-261
Canonical tiles, dual publication, fourteen-day generations, durable identity, one exact retry and
HMAC v1
AV-262 to AV-266
No identity/secret leakage, projection overlays, receipt-only effects, no trigger drift and retained
services
AV-267 to AV-270
Complete rollback source, no activation, later live Google gates and no financial drift
A source GO permits only the controlled Google installation/proving gate. It does not enable
Candidate Daily, start Phase 4, retire the legacy browser or authorise production.
~~~


## Historical decision page 106

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R12 - 17 AUGUST 2026
Sections 95-97 - durable accepted subset and disabled Google deployment
R12 addendum page 1
Current Decisions
Candidate Daily Phase 3
R12 correction and disabled Google deployment - Sections 95-97
Authority
Current fact
Decision date
17 August 2026
Base authority
Accepted Sections 1-94 and AV-001 through AV-270 remain controlling except where R12 is
explicitly later-controlling.
R11 audit result
Three bounded defects existed in the Availability write/mirror/recovery family; the R10 database/API
GO remains intact.
R12 correction
Mirror only durable accepted legacy rows; never mirror deferred/rejected rows; retain every
STATUS_CHECK or uncertain operation.
Google installation
Availability API version 216 and NEW MASTER ROTA version 102 are deployed in TEST.
Disabled invariant
CLOUDTMS_CANDIDATE_BRIDGE_ENABLED=false remains binding; legacy behaviour only and
zero bridge traffic.
Feature state
No Candidate flag, entitlement, source link, Candidate business data, provider effect or production
authority is enabled.
Next gate
Independent R12 review, then separately authorised one-cohort enabled TEST proving.
Installing and versioning the source while the bridge is false is not feature activation.
~~~


## Historical decision page 107

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R12 - 17 AUGUST 2026
Sections 95-97 - durable accepted subset and disabled Google deployment
R12 addendum page 2
95. Durable accepted-subset and ordering authority
The completed legacy write result is the sole factual authority for an Availability mirror. The original browser request is
never copied wholesale into CloudTMS.
Decision
Required result
Accepted row
Only applied=true and deferred!=true, with exact YYYY-MM-DD date and a closed legacy code.
All rejected
Identity, state, log and network no-op; return the exact existing legacy response.
Mixed result
Freeze and sign only the durable accepted subset; rejected and deferred dates are absent.
Busy/deferred
Do not mirror at enqueue time. Preserve the existing deferred response.
Queue flush
Perform existing value/background writes first; collect only successful rows; release the legacy write
lock; then mirror that exact subset.
Malformed result
Fail open toward legacy truth without inventing a CloudTMS command.
- Accepted dates are unique and exact; duplicate or contradictory accepted facts fail open.
 
- The persisted fingerprint and signed body are derived from the same normalized accepted subset.
 
- A failed legacy Sheet row cannot reach CloudTMS merely because it appeared in the browser request.
 
- No bridge network operation occurs while the legacy write lock is held.
 
AV-271 to AV-275 are later-controlling for the Availability write family.
~~~


## Historical decision page 108

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R12 - 17 AUGUST 2026
Sections 95-97 - durable accepted subset and disabled Google deployment
R12 addendum page 3
96. Closed recovery contract and self-contained evidence
HTTP status alone is not terminal authority. Every response is classified by the exact route-specific triple HTTP status,
error_code and retry_class.
Disposition
Operation ownership
2xx plus ok=true
Authoritative success; render/return durable result and clear the operation.
Approved terminal DO_NOT_RETRY or
REFRESH triple
Authoritative rejection; bounded log and clear only when the closed catalogue explicitly permits
it.
STATUS_CHECK
Non-terminal; retain the exact operation and probe status.
RETRY_SAME_KEY / RETRY_AFTER
/ transport uncertainty
Retain exact request, key, correlation, source HMAC and body; probe status first.
Malformed or unknown 4xx
Fail closed as uncertain; do not delete the recovery identity.
Status 404 / NOT_FOUND /
DO_NOT_RETRY
Only authoritative not-found; permits the one exact retry when not already consumed.
Not-found after retry consumed
Status-only forever until an authoritative result; never execute a second retry.
Evidence gate
R12 result
Focused Phase 1A/1B/2/3
54 passed, 0 failed, 0 skipped
Complete backend JavaScript
632 passed, 0 failed, 0 skipped
Phase 3 standalone inside archive
18 passed, 0 failed from extracted pack root with the packaged canonical vector.
R11 independent report
Included unmodified with the archive and standalone report identity recorded.
Local-path/secrets gate
No machine-local path or secret value is allowed in the archive.
AV-276 to AV-278 freeze the closed recovery catalogue, one-retry limit and self-contained audit
rule.
~~~


## Historical decision page 109

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R12 - 17 AUGUST 2026
Sections 95-97 - durable accepted subset and disabled Google deployment
R12 addendum page 4
97. Installed TEST authority, rollback and next phases
Surface
Installed R12 authority
Availability API
Complete corrected Code.gs plus CloudTMSCandidateBridge.gs; active web-app version 216;
version 215 retained.
NEW MASTER ROTA
One minimal existing-post seam plus CloudTMSCandidateBridge.gs; active web-app version
102; version 101 retained; operator configuration helper preserved.
Google properties
Operator-installed TEST property names; secret values neither read nor packaged; bridge false.
Candidate Workers
Public broker and private verifier retain the accepted Phase 1B route family; secret-change
versions are current; no Worker source change or deployment in R12.
Database
No schema, RPC or Candidate row change. R10 database/API GO remains controlling.
Legacy estate
No UI, login, msisdn, Sheet shape, cache, manifest, scope, trigger, Emergency or specialist
change.
Phase
Remaining full-product outcome
Phase 3 proving
Independent R12 acceptance; one approved TEST cohort; signed route, generation, recovery, projection,
quota/latency/outage soak.
Phase 4
Complete responsive Candidate web/iOS/Android journeys and retained specialist interfaces.
Phase 5
Controlled TEST cutover with identity, parity, soak, error-budget and rollback proof.
Phase 6
Emergency, cannot-attend, leave-early, running-late, DNA, messages/content, Past Shifts, DAILY signing
and EMAIL/PHONE acceptance.
Phase 7
Gradual entitled rollout, monitoring and separately authorised retirement of the temporary browser adapter.
AV-279 and AV-280 record disabled installation/deployment and preserve the full later-phase
scope. No production or feature enablement is authorised.
~~~


## Historical decision page 110

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R13 - 17 AUGUST 2026
Sections 98-100 - quota-safe generation recovery and TEST proving gate
R13 addendum page 1
Current Decisions
Candidate Daily Phase 3
R13 Master generation durability correction - Sections 98-100
Authority
Current fact
Decision date
17 August 2026
Base authority
Accepted Sections 1-97 and AV-001 through AV-280 remain controlling except where R13 is
explicitly later-controlling.
R12 review
Availability findings remain closed. Three executable Master durability/quota defects require
correction.
R13 correction
Freeze every generation command before POST in quota-safe verified chunks; recover pending
before new work; classify exact route results.
Population decision
The first enabled TEST exercise is population-wide. Kier Arthur is the first observational phone-app
journey only.
Google authority
Availability version 216 and Master version 102 remain active. The R13 Master helper is saved to
Head but deliberately undeployed pending independent GO.
Feature state
Both bridge flags, Candidate entitlements and public Candidate features remain disabled.
Source publication and an undeployed Apps Script Head do not activate the bridge.
~~~


## Historical decision page 111

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R13 - 17 AUGUST 2026
Sections 98-100 - quota-safe generation recovery and TEST proving gate
R13 addendum page 2
98. Quota-safe immutable Master generation authority
Every accepted legacy update-end event is owned by one ordered pending index. The complete immutable command
must be recoverable before any signed POST occurs.
Boundary
R13 requirement
Pending owner
CTMS_P3_ROTA_PENDING_INDEX lists ordered manifest identities for the complete event.
Manifest
Freezes body SHA-256, exact UTF-8 byte length, chunk keys, batch UUID, idempotency key,
correlation ID, item count and disposition.
Body chunks
UTF-8-safe numbered values, each no more than 7,000 bytes.
Property store
Preflight bridge-owned total below 480,000 bytes. Insufficient capacity means no POST.
Worker request
No more than 50 items and no more than 245,760 serialized UTF-8 bytes.
Reassembly
Verify ordered byte length and SHA-256 before every replay.
Corruption
Fail closed without deleting the owner or inventing a replacement command.
- All batches are persisted before the first request for the logical event.
 
- The seven-day automatic replacement rule is removed.
 
- A partial/orphan body is never treated as an executable command.
 
- The existing Availability publication remains first and authoritative for the temporary legacy path.
 
AV-281 to AV-286 are later-controlling for Master generation persistence.
~~~


## Historical decision page 112

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R13 - 17 AUGUST 2026
Sections 98-100 - quota-safe generation recovery and TEST proving gate
R13 addendum page 3
99. Exact recovery, disposition and population authority
Exact outcome
Ownership
2xx plus ok=true
That frozen batch succeeds and may be cleared.
409 BATCH_IN_PROGRESS /
STATUS_CHECK
Retain and exact-replay the same body/key/correlation; no completion log.
409 SOURCE_EVENT_CONFLICT /
DO_NOT_RETRY
Explicit terminal rejection; never completion.
422 GENERATION_INCOMPLETE /
DO_NOT_RETRY
Explicit terminal rejection; never completion.
429, 5xx, transport error
Retain exact command.
Malformed/unknown result
Retain exact command.
- Pending recovery always runs before a later accepted event can build a new generation.
 
- A new timestamp, UUID, key, correlation or body is forbidden while any prior batch remains unresolved.
 
- Overall completion is logged once only after every ordered batch succeeds.
 
- The product owner selected population-wide TEST enablement; no candidate-specific allowlist or hard-coded Kier identity
 exists.
- Every eligible Google source row must have one exact TEST source link before enablement. Bridge enablement never
 creates links.
- An existing global Candidate key proves the established Candidate-product mapping but does not replace the Daily
 Google-source HMAC catalogue.
- Controlled source-link bootstrap binds the HMAC to the existing Candidate UUID and never creates or replaces a
 Candidate record.
- A new-app-only candidate joins through the admin-entered global Candidate key and does not need to use the temporary
 legacy browser.
- Google generates the CID1 global key from normalized Credentially Public ID; the administrator enters that generated
 CID1 value in CloudTMS.
- Kier Arthur is the first observational phone-app journey, not a runtime scope boundary.
 
AV-287 to AV-298 freeze recovery ordering, population scope, Google-generated CID1
mapping, distinct source-HMAC authority and app-only onboarding to the same Candidate
UUID.
~~~


## Historical decision page 113

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R13 - 17 AUGUST 2026
Sections 98-100 - quota-safe generation recovery and TEST proving gate
R13 addendum page 4
100. Verification, deployment gate and remaining full
product
Evidence
R13 result
Focused Phase 1A/1B/2/3/R13
73 passed, 0 failed
R13 Master recovery/quota
19 passed, 0 failed.
Complete backend JavaScript
651 passed, 0 failed
Candidate Worker builds
Public broker and private Candidate dry builds pass; no Worker runtime changed.
Real TEST broker negative proof
Valid route/body with deliberately invalid signing authority fails closed at
SYSTEM_AUTH_FAILED; no source or business mutation.
Source-link readiness
Kier's existing global Candidate key is present and matches; that key is distinct from the Daily
Google-source HMAC catalogue, whose active Kier and overall TEST counts are zero.
App-only onboarding
Google generates CID1 from normalized Credentially Public ID; the administrator enters that
exact value; bootstrap binds the separate source HMAC to the same existing UUID.
Google deployment
R13 Master Head is saved but not deployed. Independent GO is required first.
Next phase/gate
Outcome
R13 GO/deploy
Deploy Master while false; prove source/trigger/legacy parity; bootstrap exact source links.
Phase 3 enabled proving
Population-wide TEST bridge with Kier observed first; signed generation, tiles, writes, recovery,
projection, quota/latency/outage and rollback.
Phase 4
Complete responsive Candidate web, iOS and Android plus retained specialist interfaces.
Phase 5
Controlled TEST cutover, parity, soak, error budget and rollback.
Phase 6
Emergency and attendance effects, messages/content, Past Shifts, DAILY signing and EMAIL/PHONE
acceptance.
Phase 7
Gradual rollout, monitoring and separately authorised retirement of the temporary browser adapter.
AV-293 to AV-298 bind observational scope, source-link readiness, Google-generated CID1
mapping, existing Candidate-row preservation, app-only onboarding and the
independent-GO-before-Google-deployment rule.
~~~


## Historical decision page 114

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R14 - 18 AUGUST 2026
Sections 101-103 - indexed aggregate-result authority
R14 addendum page 1
Current Decisions
Candidate Daily Phase 3
R14 aggregate-outcome authority correction - Sections 101-103
Authority
Current fact
Decision date
18 August 2026
Base authority
Accepted Sections 1-100 and AV-001 through AV-298 remain controlling except where R14 is
explicitly later-controlling.
R13 review
One bounded NO-GO: top-level HTTP 200 / ok=true could conceal indexed REJECTED generation
outcomes.
R14 correction
Validate one complete indexed aggregate; success requires every outcome to be COMMITTED or
REPLAYED.
Operational state
Availability 216 and Master 102 remain active; both bridges remain false; R14 is not installed or
deployed in Google.
Runtime boundary
No Worker, RPC, database, frontend, Candidate data, trigger, finance or production change.
Top-level ok=true is only a candidate aggregate result. It is never sufficient Master
batch-success authority.
~~~


## Historical decision page 115

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R14 - 18 AUGUST 2026
Sections 101-103 - indexed aggregate-result authority
R14 addendum page 2
101. Complete indexed generation-result authority
For the Master Rota generation route, the exact submitted item set and the durable server aggregate are jointly
authoritative. The client must validate the complete receipt before clearing any frozen Google operation.
Required fact
Closed rule
Receipt
result.batch_receipt_id is a structurally valid UUID.
Count
result.outcomes is an array whose length exactly equals submitted body.items.length.
Index
Every index is an integer, in range, unique and collectively complete.
Status
Every status is COMMITTED, REPLAYED or REJECTED.
Success
Every indexed outcome is COMMITTED or REPLAYED.
Malformed/unknown
Preserve the exact immutable owner; do not clear, complete or invent a replacement.
- AV-299: HTTP 2xx and ok=true is only a candidate aggregate result.
 
- AV-300: receipt, count and index topology must be exact before success.
 
- AV-301: only an all-COMMITTED/all-REPLAYED complete aggregate advances the frozen batch.
 
- AV-303: invalid receipts, counts, indexes, statuses or codes preserve the immutable operation.
 
AV-299 to AV-301 and AV-303 supersede every earlier statement that top-level 2xx/ok=true
alone completes a Master generation batch.
~~~


## Historical decision page 116

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R14 - 18 AUGUST 2026
Sections 101-103 - indexed aggregate-result authority
R14 addendum page 3
102. Rejected outcome and bounded logging authority
The installed TEST RPC legitimately completes one aggregate receipt with HTTP status 200 while recording item-level
failures as REJECTED. R14 consumes that server contract without redesigning the Worker or database.
Recognised rejected code
Required disposition
SOURCE_EVENT_CONFLICT
Terminal item rejection; never mirror completion.
GENERATION_INCOMPLETE
Terminal item rejection; never mirror completion.
IDENTITY_LINK_MISSING
Terminal item rejection under the current completed-receipt contract.
IDENTITY_LINK_AMBIGUOUS
Terminal item rejection under the current completed-receipt contract.
CANDIDATE_DAILY_NOT_READY
Terminal item rejection under the current completed-receipt contract.
Unknown/missing code
Preserve. Do not guess terminal authority.
- A known REJECTED outcome returns TERMINAL_REJECTION and cannot emit
 ROTA_GENERATION_MIRROR_COMPLETE.
- The rejection log may contain only bounded item indexes and safe closed-catalogue error codes.
 
- Raw Candidate identity, Public ID, phone, email, generation body and HMAC material remain prohibited in logs.
 
- Top-level 409/422 conflict/incomplete triples remain defensive compatibility paths.
 
- If the server later changes a rejection to status-recoverable, its durable receipt contract must change before this
 catalogue changes.
AV-302 and AV-304 freeze terminal rejection ownership, safe logging and the
no-false-completion rule.
~~~


## Historical decision page 117

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R14 - 18 AUGUST 2026
Sections 101-103 - indexed aggregate-result authority
R14 addendum page 4
103. R14 evidence, Google gate and unchanged
full-product plan
Evidence
R14 result
R13 recovery plus R14 aggregate
33 passed, 0 failed.
Complete Candidate Daily JavaScript
87 passed, 0 failed.
Complete backend JavaScript
665 passed, 0 failed.
Exact Worker envelope
Current success-body builder used; HTTP 200/ok=true REJECTED matrix passes.
Installed TEST RPC
Read-only proof of five REJECTED codes, completed receipt/status 200 and aggregate return.
Worker dry build
Not rerun: restricted desktop filesystem denied Wrangler traversal; no
Worker/config/dependency source changed.
Google deployment
None. Availability 216 and Master 102 remain active; both bridge switches remain false.
Next gate
Authority
Independent R14 review
Verify exact source/tests/contract and issue GO or one bounded NO-GO.
Disabled Google install
After GO, deploy only the exact Master helper while false and prove source/version/trigger/legacy
parity.
Enabled Phase 3 proving
Separately approve source-link readiness and population-wide TEST generation, tiles, Emergency
compatibility, recovery and rollback.
Later phases
Phase 4 apps; Phase 5 cutover/soak; Phase 6 effects/content/signing; Phase 7 monitored rollout and
separately authorised legacy-browser retirement.
- AV-305: R14 changes only the Master helper, exact tests and controlling documents; it grants no activation authority.
 
- Availability, Master legacy Code.gs, rollbacks, Worker/RPC, SQL, triggers, Candidate data, source links and Emergency
 remain unchanged.
- The temporary legacy experience remains fully operational while both bridge switches are false.
 
- A GO authorises only disabled Master installation qualification; it does not enable Candidate Daily or production.
 
The full Candidate App phase plan remains intact. R14 is one bounded Phase 3
compatibility-contract correction.
~~~


## Historical decision page 118

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R15 - 18 AUGUST 2026
Sections 104-107 - automatic first-generation identity authority
R15 addendum page 1
Current Decisions
Candidate Daily Phase 3
R15 automatic first-generation source-link authority - Sections 104-107
Authority
Current fact
Decision date
18 August 2026
Base authority
Accepted Sections 1-103 and AV-001 through AV-305 remain controlling except where R15 is
explicitly later-controlling.
Product correction
No bulk/manual source-link bootstrap. The first valid signed generation binds one existing Candidate
exactly.
Old enabled app
Reads the canonical generation by the new versioned source-HMAC link.
New app
Reads that same generation by Candidate UUID after the existing controlled gates.
Operational state
Availability 216 and Master 102 remain active; bridges remain false; R15 is not published, installed or
deployed.
One existing Candidate, one exact CID1 match, one source link, one authority scope and one
canonical rota generation.
~~~


## Historical decision page 119

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R15 - 18 AUGUST 2026
Sections 104-107 - automatic first-generation identity authority
R15 addendum page 2
104. Automatic first-generation identity binding
The trusted Master Rota server-side bridge supplies two separate identity facts: the certified CID1 global Candidate key
and a versioned HMAC of the Google public identity. The browser never nominates a Candidate UUID and never
receives CloudTMS credentials.
Fact
Closed rule
Global Candidate key
Derived by the existing certified Master function; exact CID1 format; sent only on the signed
server-to-server generation item.
Source identity
Separate SHA-256 HMAC under source key version 1; raw public identity is not transmitted or logged.
Candidate resolution
Exactly one active public.candidates.key_norm match after trim/case normalization.
First use
Create one GOOGLE_CREDENTIALLY_PUBLIC_ID PRIMARY link and a missing
GOOGLE_PRIMARY scope.
Candidate creation
Forbidden. An unmatched CID1 is a rejection, not an onboarding instruction.
Manual bootstrap
Not required and not a normal operating step.
- AV-306: no bulk/manual source-link bootstrap is required; first valid signed generation owns automatic exact binding.
 
- AV-307: only one active exact CID1 match is accepted; fuzzy/name/email/telephone matching and Candidate creation
 are prohibited.
- AV-313: the source HMAC version is explicit and closed at version 1; raw Google identity remains private to Google.
 
AV-306 supersedes earlier Phase 3 instructions to pre-populate source links for every
Candidate.
~~~


## Historical decision page 120

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R15 - 18 AUGUST 2026
Sections 104-107 - automatic first-generation identity authority
R15 addendum page 3
105. Atomicity, conflict and concurrency authority
Journey
Required outcome
Missing/inactive CID1
IDENTITY_LINK_MISSING; no link, scope or generation.
Duplicate active CID1
IDENTITY_LINK_AMBIGUOUS; no arbitrary winner.
Source HMAC owned elsewhere
IDENTITY_LINK_CONFLICT; no reassignment.
Different current PRIMARY on
Candidate
IDENTITY_LINK_CONFLICT; no silent rotation/replacement.
Generation invalid after proposed link
The item subtransaction rolls back its link and new scope.
Concurrent first executions
Deterministic identity locks; one COMMITTED and one REPLAYED winner; one
link/scope/generation.
- AV-308: link, initial scope and generation share one item subtransaction.
 
- AV-309: source ownership and different-current-PRIMARY conflicts fail closed as IDENTITY_LINK_CONFLICT.
 
- AV-310: concurrent first generation uses deterministic identity locks and converges to one durable winner.
 
- IDENTITY_LINK_CONFLICT is a recognised terminal indexed rejection under R14 aggregate authority.
 
- Exact replay and later factual generations reuse the established link; key/version rotation remains a separately controlled
 change.
A rejected item may leave an aggregate receipt, but it may not leave a partially created identity
mapping.
~~~


## Historical decision page 121

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R15 - 18 AUGUST 2026
Sections 104-107 - automatic first-generation identity authority
R15 addendum page 4
106. Enabled old app and new app share one generation
Phase 3 acceptance is the enabled compatibility path. Bridge-off behaviour is retained only as rollback proof and is not
evidence that the integration works.
Consumer
Identity and authority
Old Availability app, bridge enabled
Existing Apps Script handler derives the source HMAC; LEGACY_COMPAT resolves the private
link and reads the active canonical generation.
New Candidate app
Candidate session owns the Candidate UUID; after the existing authority transition, entitlement
and global feature gates, it reads the same generation.
Master Rota
Continues existing Availability publication first, then mirrors the accepted factual event to
CloudTMS.
Availability/Emergency
Remain retained operational consumers even after the temporary legacy phone UI retires.
- AV-311: old enabled and new apps never own separate rota generations for one factual event.
 
- AV-312: first generation does not create an entitlement, enable a feature flag or perform the Candidate authority cutover.
 
- AV-314: bridge-off behaviour is a rollback invariant only; Phase 3 acceptance is the enabled dual-consumer path.
 
- AV-315: Master preserves Availability-first publication, including after temporary legacy-browser retirement.
 
- The legacy browser receives no Candidate UUID, CloudTMS access token, Supabase credential or HMAC secret.
 
- Disabling the bridge restores the old-only path without deleting the canonical database facts already accepted during
 TEST proving.
The integration succeeds only when the old bridge-enabled app and the new app can consume
the same canonical generation under their separate identity policies.
~~~


## Historical decision page 122

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R15 - 18 AUGUST 2026
Sections 104-107 - automatic first-generation identity authority
R15 addendum page 5
107. R15 evidence, installation gate and residual authority
Evidence
R15 result
Candidate Daily JavaScript
95 passed, 0 failed.
Complete backend JavaScript
673 passed, 0 failed.
PostgreSQL 18.1
Exact ordered install and 44 Candidate runtime suites passed.
Real concurrency
Two connections converged to one Candidate link, scope and generation.
Worker dry builds
Normal, public Candidate and private Candidate all passed.
OpenAPI
Valid; 15 pre-existing warnings.
PostgreSQL 17.6
Workflow configured; independent/publication run required because unavailable locally.
Deployment
None. No Supabase, Worker, Google, frontend or production change.
Next gate
Authority
Independent R15 review
Verify exact Master/Worker/SQL contract, 17.6/18.1, concurrency, negative journeys and
dual-consumer proof.
Backend publication/install
Only after GO; reconcile source, installed definition and ACL; keep Candidate features off.
Disabled Google qualification
Install exact packaged source only with separate authorisation; prove
version/source/trigger/rollback parity while false.
Enabled TEST proof
Separately approve real bridge-enabled old-app plus new-app journey; no manual source-link
bootstrap.
R15 is an unpublished TEST-only review candidate. It grants no Google installation,
enablement, production or later-phase authority.
~~~


## Historical decision page 123

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R16 - 18 AUGUST 2026
Sections 108-113 - identity integrity and installation gate
R16 addendum page 1
Current Decisions
Candidate Daily Phase 3
R16 identity-integrity and installation-gate authority - Sections 108-113
Authority
Current decision
Decision date
18 August 2026
Retained model
First valid signed Master generation automatically binds exactly one existing Candidate; no manual
source-link bootstrap.
Identity invariant
One normalized active CID1 owner and one Candidate owner for a versioned source HMAC across
all history.
Installation invariant
Private mutating helpers are never committed with caller-role execute authority.
Migration invariant
PostgreSQL 17.6 and 18.1 exact-commit success precedes every TEST database mutation.
Operational state
Unpublished and uninstalled; Availability 216 and Master 102 active; both bridges false.
One existing Candidate. One normalized active CID1. One immutable source-HMAC owner
across history. One canonical generation.
- AV-316 through AV-324 are later-controlling over R15 for identity integrity, installation safety, transition proof and
 deployment sequencing.
- R16 creates no Candidate, automatic entitlement, feature enablement, authority cutover or manual bootstrap.
 
- No Google, frontend, finance, Banking Pay, provider or production authority is added.
~~~


## Historical decision page 124

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R16 - 18 AUGUST 2026
Sections 108-113 - identity integrity and installation gate
R16 addendum page 2
108. Normalized active CID1 database authority
Candidate resolution is not safe unless the same normalization used by first generation is also unique in PostgreSQL.
R16 makes that rule an invariant rather than a query-time observation.
Rule
Required database behaviour
Normalization
Use upper(btrim(key_norm)) for active valid CID1-shaped Candidate keys.
Preflight
Count duplicate normalized groups and fail generically without printing a controlled key.
Unique owner
A functional partial unique index prevents case/spacing-equivalent active owners.
Noncanonical existing row
A single lower-case or padded stored key remains resolvable without data rewrite.
Concurrent insert
First generation and a normalized-equivalent active Candidate insert cannot both commit.
Concurrent activation
An inactive duplicate cannot become active while another normalized owner exists.
- AV-316: normalized active CID1 uniqueness is owned by the database.
 
- The invariant is deliberately limited to active valid CID1-shaped keys; it does not redefine unrelated Candidate keys.
 
- Failure does not choose, merge, create, deactivate or edit a Candidate. Office identity review remains separate.
 
Generation may resolve a noncanonical unique stored CID1, but it may never arbitrate between
two normalized owners.
~~~


## Historical decision page 125

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R16 - 18 AUGUST 2026
Sections 108-113 - identity integrity and installation gate
R16 addendum page 3
109. One source-HMAC owner across all history
History condition
Automatic first-generation result
Current PRIMARY/OVERLAP, same
Candidate
Reuse the exact effective identity.
Current owner, different Candidate
IDENTITY_LINK_CONFLICT.
RETIRED or REJECTED owner
IDENTITY_LINK_CONFLICT, regardless of Candidate.
Expired or future-valid owner
IDENTITY_LINK_CONFLICT, regardless of Candidate.
Same-Candidate non-current history
IDENTITY_LINK_CONFLICT; no silent reactivation.
Identity-changing update
Rejected; immutable history owner retained.
- AV-317: environment, source system, HMAC version and identifier HMAC identify one Candidate across every state and
 validity period.
- AV-323: every rejected ownership journey leaves no partial scope, link or generation.
 
- One all-history unique index and one BEFORE trigger cover every insert and identity-changing update.
 
- The guard and first-generation binder use the same deterministic SOURCE advisory-lock namespace.
 
- Legitimate future rotation or repair requires a separately controlled authority; first generation is not a repair tool.
 
Retirement ends current use. It does not erase who historically owned the source identity.
~~~


## Historical decision page 126

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R16 - 18 AUGUST 2026
Sections 108-113 - identity integrity and installation gate
R16 addendum page 4
110. Transaction-safe privileged installation
The safe-migration runner applies repeatables with psql -f and does not wrap them. R16 therefore owns atomicity inside
the SQL file.
Installation stage
R16 authority
Repeatable begins
BEGIN opens one transaction.
Private binder created
Fixed search_path; SECURITY DEFINER.
Immediately afterwards
Revoke PUBLIC, anon, authenticated and service_role before defining the public RPC.
Public RPC installed
Final execute remains service_role only.
Repeatable completes
COMMIT makes the complete closed authority visible at once.
Failure
Transaction rollback exposes neither a partial helper nor a partial RPC replacement.
- AV-318: no committed interval may expose the private binder or history guard to caller roles.
 
- Runtime ACL verification asserts browser roles and service_role cannot execute either private helper.
 
- service_role retains execute only on the accepted public generation RPC boundary.
 
Correct final ACL is insufficient if installation transiently grants authority. R16 closes both
states.
~~~


## Historical decision page 127

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R16 - 18 AUGUST 2026
Sections 108-113 - identity integrity and installation gate
R16 addendum page 5
111. Complete conflict and dual-consumer contract
Contract journey
Required result
HTTP 200 indexed
IDENTITY_LINK_CONFLICT
Terminal item rejection; never mirror completion.
Top-level 409 /
IDENTITY_LINK_CONFLICT /
DO_NOT_RETRY
Terminal rejection; clear the exact frozen terminal operation.
Malformed or unknown aggregate
PRESERVE the exact frozen operation.
Legacy read
Read the canonical generation through the versioned source-HMAC link.
Candidate read before cutover
Rejected by existing gates.
Candidate read after real cutover
Same generation, only after reconciliation and durable transition/entitlement receipts.
- AV-319: Master recognises the exact top-level conflict triple and logs no mirror completion.
 
- AV-320: qualifying evidence calls candidate_daily_authority_transition_atomic_v1 with separate actor/approver, exact
 generation/version and exact cursors.
- Direct fixture updates to authority_mode or entitlements are not controlled-gate proof.
 
- R14 receipt/outcome completeness and uncertainty-preservation rules remain controlling.
 
The enabled old app and new app share database truth, but enter it through separate accepted
identities and gates.
~~~


## Historical decision page 128

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R16 - 18 AUGUST 2026
Sections 108-113 - identity integrity and installation gate
R16 addendum page 6
112. Exact-commit two-engine migration gate
Gate
Required sequencing
Candidate runtime workflow
Reusable workflow_call matrix for PostgreSQL 17.6 and 18.1.
Source chain
Exact ordered migrations, repeatables, 45 Candidate runtime suites and concurrency tests.
Safe migration
Explicitly needs the Candidate runtime job for the same commit.
Either engine pending/failed
Database mutation cannot start.
Both engines pass
A later authorised TEST migration may proceed; this does not enable Candidate Daily.
- AV-321: a separate concurrent test workflow is not an installation gate.
 
- The R16 migration installs before the R16 repeatable.
 
- The exact local 17.6 and 18.1 chains each pass 45 runtime suites and all authentication, authority-transition and identity
 concurrency suites.
- A changed rebase requires the exact matrices to be repeated before publication.
 
Compile first on TEST's own major/minor family and the next supported engine; mutate only
after both are green.
~~~


## Historical decision page 129

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R16 - 18 AUGUST 2026
Sections 108-113 - identity integrity and installation gate
R16 addendum page 7
113. R16 evidence, no-change boundary and next authority
Evidence
R16 result
Candidate Daily JavaScript
102 passed, 0 failed.
Complete backend JavaScript
680 passed, 0 failed.
PostgreSQL 17.6
45 Candidate suites plus 3/3 authentication, 7/7 mixed auth, 2/2 transition, 1/1 first-generation and
3/3 R16 concurrency.
PostgreSQL 18.1
The identical ordered matrix passed with the same suite counts.
Worker builds
Normal, public Candidate and private Candidate dry builds passed.
Deployment
None: no push, Supabase install, Worker deploy, Google install/version, frontend or production
action.
Next gate
Authority
Independent R16 review
Reproduce normalized identity, historical ownership, ACL, transition and two-engine workflow
findings.
Later TEST publication
Only after GO and a fresh collision/rebase check; complete coherent R16 change only.
Disabled Google qualification
Separate authorisation after installed database/Worker parity; both switches remain false.
Enabled Phase 3 proving
Separate controlled old-app/new-app journey; not granted by R16 GO.
R16 is an unpublished TEST-only review candidate. It grants no Google installation, feature
enablement, Phase 4 or production authority.
~~~


## Historical decision page 130

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R17 - 18 AUGUST 2026
Sections 114-118 - authority-transition source-identity integration
R17 addendum page 1
Current Decisions
Candidate Daily Phase 3
R17 authority-transition source-identity integration - Sections 114-118
Authority
Current decision
Decision date
18 August 2026
Retained R16 model
Normalized active CID1 and immutable all-history source-HMAC ownership remain unchanged.
Effective function
One later complete definition of the existing eight-argument authority-transition RPC.
Item authority
IDENTITY_LINK_CONFLICT rejects only its item and completes a durable aggregate result.
Lock authority
All sorted SOURCE locks precede every sorted Candidate authority-scope row lock.
Operational state
Unpublished and uninstalled; Availability 216 and Master 102 active; both bridges false.
One lock hierarchy. One durable batch owner. One rejected item cannot erase a valid sibling.
- AV-325 through AV-333 are later-controlling over R16 for transition item containment, lock order and two-engine
 qualification.
- R17 changes no Worker, HTTP route, Google source, frontend, finance, Banking Pay, provider or production owner.
 
- Exact PostgreSQL 17.6 and 18.1 local matrices pass; independent review and later exact-commit CI remain required.
~~~


## Historical decision page 131

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R17 - 18 AUGUST 2026
Sections 114-118 - authority-transition source-identity integration
R17 addendum page 2
114. Durable source-conflict item authority
The authority-transition RPC is a durable multi-item owner. A protected source-history conflict is an expected business
rejection, not an uncontrolled database abort.
Journey
Required terminal result
Single protected-history conflict
Completed batch; indexed REJECTED / IDENTITY_LINK_CONFLICT; no item mutation.
Valid then conflict
First item COMMITTED; second item REJECTED; valid sibling remains committed.
Conflict then valid
First item REJECTED; second item COMMITTED; response indexes retain caller order.
Exact same-key replay
Same stored terminal body and receipt with _idempotent_replay=true.
Changed-content key reuse
Existing idempotency conflict remains authoritative.
- AV-325: IDENTITY_LINK_CONFLICT is added to the closed item-error catalogue.
 
- AV-326: the item subtransaction rolls back only the rejected item and never erases a valid sibling.
 
- The aggregate receipt reaches COMPLETED and owns one terminal-response hash.
 
- The R16 trigger remains the source-history decision owner; R17 changes only containment by its supported caller.
 
Expected source conflict is durable product truth, not a reason to abandon the batch receipt.
~~~


## Historical decision page 132

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R17 - 18 AUGUST 2026
Sections 114-118 - authority-transition source-identity integration
R17 addendum page 3
115. Shared SOURCE-before-scope lock hierarchy
Order
R17 action
1
Validate the outer request and obtain the durable batch receipt owner.
2
Scan all syntactically safe source-link identities without casting malformed key versions.
3
Deduplicate and sort environment:SOURCE:key-version:HMAC lock identities.
4
Acquire pg_advisory_xact_lock(hashtextextended(identity, 0)) for every SOURCE identity.
5
Lock Candidate authority scopes in Candidate UUID order.
6
Process items under the existing per-item subtransactions; the R16 trigger reacquires the same xact lock.
- AV-327: generation and Office transition use SOURCE before scope.
 
- AV-328: multiple source identities use a request-order-independent global lock order.
 
- Response indexes remain caller order; lock order never reorders outcomes.
 
- Generation-versus-transition can no longer form SOURCE-to-scope versus scope-to-SOURCE deadlock ownership.
 
Every supported writer reaches source identity before Candidate scope. The table trigger is the
final invariant, not a second lock order.
~~~


## Historical decision page 133

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R17 - 18 AUGUST 2026
Sections 114-118 - authority-transition source-identity integration
R17 addendum page 4
116. Malformed input, security and unchanged contract
Boundary
R17 rule
Source HMAC
Exactly 64 lower-case hexadecimal characters before pre-lock eligibility.
Key version
Positive decimal text, no more than ten digits and no greater than 2147483647 by text checks.
Pre-lock cast
Forbidden; malformed facts bypass pre-lock and reach item validation.
Malformed result
Indexed REJECTED / VALIDATION_FAILED, without whole-batch abort.
Function boundary
Same eight arguments, JSONB result, SECURITY DEFINER and empty search_path.
ACL
PUBLIC/anon/authenticated denied; service_role execute retained.
- AV-329: malformed facts do not become lock identities and cannot cause a pre-lock integer exception.
 
- AV-331: no-source and same-mode transitions retain established NO_CHANGE behaviour.
 
- AV-332: transition, reconciliation, entitlement, independent-approver and replay contracts are unchanged.
 
- The later repeatable is transactional and restores the intended role grants.
 
Pre-lock filtering decides only which safe locks to acquire. Item validation still decides the
indexed business outcome.
~~~


## Historical decision page 134

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R17 - 18 AUGUST 2026
Sections 114-118 - authority-transition source-identity integration
R17 addendum page 5
117. Actual cross-writer and two-engine proof
Evidence
Required and observed result
Actual generation vs actual transition
Test-only delay widens the old inversion window; no 40P01, timeout control flow or partial
owner.
Opposite-order transitions
Same two source identities in reverse item order; deterministic completion without deadlock.
PostgreSQL 17.6
46 ordered Candidate suites; 18 concurrency/authentication tests; PASS.
PostgreSQL 18.1
The identical saved runner and counts; PASS.
JavaScript
72 focused Candidate Daily and 686 complete backend tests; PASS.
TEST migration
Still needs the exact-commit reusable 17.6/18.1 matrix before mutation.
- AV-330: the race uses both actual public functions, not a mock lock helper.
 
- AV-333: local two-engine success is pre-review evidence; exact-commit CI remains the later install gate.
 
- Docker engine access failure is not recorded as test evidence; each reported engine actually ran.
 
- No TEST Supabase or production database was used for local qualification.
 
A successful compile is not concurrency proof. R17 forces the old overlap and observes both
real transaction owners.
~~~


## Historical decision page 135

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R17 - 18 AUGUST 2026
Sections 114-118 - authority-transition source-identity integration
R17 addendum page 6
118. R17 disposition, no-change boundary and next gates
State
Authority
Source candidate
Implemented locally; exact 17.6/18.1 and JavaScript gates pass.
GitHub
No R17 commit or push.
Supabase/Workers
No R17 install or deployment; no Worker source change is required.
Google
No R17 source change, paste, version or deployment; bridges remain false.
Independent review
Required before any publication/install authority.
Enabled Phase 3 proving
Separate later authority after disabled installation qualification.
Explicitly unchanged
Boundary
Identity
R16 CID1 index, history index/trigger, first-generation binder and generation RPC.
Application
Candidate/Office frontend, Candidate HTTP/Worker contracts and Apps Script.
Operations
Google versions/triggers/properties, feature flags, entitlements and production.
Business
Emergency, finance, invoices, Banking Pay, payments, providers and Policy X.
R17 is an unpublished TEST-only review candidate. A technical GO is not final Phase 3
acceptance, bridge enablement, Phase 4 authority or production authority.
~~~


## Historical decision page 136

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R18 - 18 AUGUST 2026
Sections 119-124 - event-driven rota-publication investigation authority
R18 addendum page 1
Current Decisions
Candidate Daily Phase 3
R18 event-driven rota-publication investigation - Sections 119-124
Authority
Current decision
Decision date
18 August 2026
Retained authority
Installed R17 identity, generation, aggregate-result and transition authority remains unchanged.
Reproduced gap
Normal bookings, cancellations and amendments do not call the generation route.
Promptness
Routine changes must not wait for ai_dailyRefresh().
Efficiency
Publish only affected Candidates after settlement; each item remains a complete fourteen-day
snapshot.
R18 state
Investigation and implementation plan only; no R18 runtime source exists or is approved.
Prompt does not mean per cell. Complete does not mean the whole population.
~~~


## Historical decision page 137

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R18 - 18 AUGUST 2026
Sections 119-124 - event-driven rota-publication investigation authority
R18 addendum page 2
119. Reproduced routine-event trigger gap
The first controlled Master-enabled TEST booking completed through the established delayed legacy path but produced
no Candidate Daily generation request or row. The saved source explains that result.
Source journey
Current R17 result
Normal delayed booking batch
Emits EMAILHISTORY_UPDATED; Candidate bridge ignores it.
Cancellation or factual amendment
Uses the same routine family; Candidate bridge does not publish.
Direct Availability edit
Coalesces AVAILABILITY_PARTIAL_UPDATED; Candidate bridge ignores it.
ai_dailyRefresh()
Emits AVAILABILITY_UPDATE_END; this is the only action the bridge accepts.
Current generation builder
Scans and emits all otherwise eligible Candidates.
- AV-334: routine changes must publish promptly and may not wait for daily refresh.
 
- AV-340: daily refresh remains the full-population reconciliation and diagnostic route.
 
- The separate daily-refresh observation is useful end-to-end evidence but is not the routine trigger fix.
 
The accepted R17 backend is not disproved. The missing owner is the Master routine-event
seam.
~~~


## Historical decision page 138

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R18 - 18 AUGUST 2026
Sections 119-124 - event-driven rota-publication investigation authority
R18 addendum page 3
120. Mutation coverage and settled-state authority
Change family
R18 requirement
New booking
Publish the affected Candidate after EmailHistory and Availability facts settle.
Cancellation
Publish the complete current snapshot with the removed booking absent.
Time/reference/date/shift
Retain every old/new affected identity needed for a correct final snapshot.
Hospital/ward/client/job title
Publish when the field changes a canonical generation fact.
Direct Availability edit
Use the existing coalesced telephone/date cohort after its lock is released.
- AV-337: CloudTMS publication is after accepted legacy publication and after canonical facts settle.
 
- AV-338: the implementation plan must map every supported change type explicitly.
 
- EMAILHISTORY_UPDATED is not automatically the settled seam because downstream repaint/system-blocked work
 follows it.
- AV-339: direct-edit coalescing is retained; no bridge persistence or UrlFetch may execute under the existing
 partial-update ScriptLock.
Publish the final truth, not an intermediate EmailHistory signal.
~~~


## Historical decision page 139

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R18 - 18 AUGUST 2026
Sections 119-124 - event-driven rota-publication investigation authority
R18 addendum page 4
121. Affected-Candidate batching and performance
Scenario
Required shape
One changed Candidate
One complete Candidate item; no unrelated Candidate item.
Many edits to one Candidate
One coalesced dirty owner and one final complete item.
Several Candidates in one batch
One complete item per distinct affected Candidate.
More than fifty Candidates
Split by the established item/body limits with one immutable operation owner.
Daily reconciliation
The only normal full-population generation build.
- AV-335: preserve existing batching and debounce; never POST for each cell edit.
 
- AV-336: a routine batch is affected-only, while every affected item remains a complete fourteen-day snapshot.
 
- AV-343: qualification must measure Sheet reads, Apps Script property bytes, request bytes, HTTP calls and execution
 time.
- The current full-population builder cannot simply be called from every routine legacy event.
 
A cancellation is not a delete message. It is a complete new snapshot whose absence is
authoritative.
~~~


## Historical decision page 140

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R18 - 18 AUGUST 2026
Sections 119-124 - event-driven rota-publication investigation authority
R18 addendum page 5
122. Frozen-operation recovery and newer dirty facts
Authority
R18 rule
Existing frozen operation
Replay/recover the exact stored body, batch ID, key and correlation identity.
Newer rota change
Persist a separate dirty Candidate cohort until it can be frozen into its own factual operation.
Old operation resolves
Do not return in a way that silently discards the newer dirty cohort.
Old operation remains uncertain
Retain both owners; never overwrite or mutate the frozen request.
Terminal rejection
Clear only the operation/facts owned by the authoritative terminal result.
- AV-341: recovery-first remains binding, but later source facts require durable ownership.
 
- The plan must cover crashes before and after dirty persistence, freeze, POST, response and cleanup.
 
- One global ScriptLock order must cover dirty storage and frozen-operation publication without network work inside an
 unrelated legacy lock.
- Existing R17 malformed-response and aggregate-result protection remains unchanged.
 
An immutable old request and unfrozen new facts are different owners. Neither may erase the
other.
~~~


## Historical decision page 141

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R18 - 18 AUGUST 2026
Sections 119-124 - event-driven rota-publication investigation authority
R18 addendum page 6
123. Legacy compatibility, backend boundary and testing
Boundary
Decision
Bridge false
Hard no-op: zero CloudTMS network, persistence, bridge logs or trigger changes.
CloudTMS failure
Fail open to the legacy caller; return the existing legacy result unchanged.
Legacy application
No browser/login/UI/Sheets-response redesign.
Backend and database
No change presumed; first prove the installed R17 multi-item endpoint is sufficient.
Identity
Retain exact CID1/source-HMAC authority; no name, email or telephone database fuzzy matching.
- AV-342: disabled behaviour and legacy fail-open compatibility remain mandatory.
 
- AV-345: a Worker/SQL change requires independent contract proof; convenience is not sufficient.
 
- Mandatory tests cover every change family, coalescing, affected-only output, request splitting, pending-old-plus-dirty-new
 recovery and daily reconciliation.
- The Emergency and retained specialist systems continue to depend on the legacy Availability pathway.
 
Do not modernise the legacy app. Add one narrow, measurable, reversible publication seam.
~~~


## Historical decision page 142

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R18 - 18 AUGUST 2026
Sections 119-124 - event-driven rota-publication investigation authority
R18 addendum page 7
124. R18 disposition and independent planning gate
State
Authority
R17 database and Workers
Published, installed and retained.
Certified Google source
Installed by the operator; disabled Availability legacy test passed.
Routine Master publication
Incomplete; first enabled booking observation produced no generation.
R18 implementation
Not started and not authorised by this document.
Independent review
Must return a complete writer map, design, performance budget, recovery model, tests and rollout
plan.
Phase 3 completion
Not achieved until the later correction is implemented, independently accepted and controlled
TEST proving passes.
- AV-344: R18 is a plan gate. No R18 source may be pasted, published or deployed from this pack.
 
- Keep both bridges false outside the separately monitored daily-refresh diagnostic.
 
- Do not create manual source links, entitlements or feature flags.
 
- A later implementation requires its own review, publication authority, disabled regression, enabled affected-Candidate
 observation and rollback evidence.
R18 records the exact missing journey and the rules for planning it. It is not bridge-enable
authority.
~~~


## Historical decision page 143

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R19 - 18 AUGUST 2026
Sections 125-130 - full-reconciliation property-capacity authority
R19 addendum page 1
Current Decisions
Candidate Daily Phase 3
R19 full-reconciliation property-capacity investigation - Sections 125-130
Authority
Current decision
Decision date
18 August 2026
Retained authority
Published R17 database, Worker, identity and recovery contracts remain unchanged.
Reproduced failure
Enabled ai_dailyRefresh stopped at CTMS_ROTA_PROPERTY_STORE_CAPACITY before
Candidate POST.
Legacy result
All legacy stages completed; fail-open returned the established result unchanged.
TEST result
Feature false; zero enabled entitlements, source links, generations and generation receipts.
R19 state
Separate investigation and implementation-plan gate; no runtime correction is approved.
The safety wrapper worked. The full reconciliation did not publish.
~~~


## Historical decision page 144

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R19 - 18 AUGUST 2026
Sections 125-130 - full-reconciliation property-capacity authority
R19 addendum page 2
125. Live enabled full-reconciliation result
Observed fact
Result
Function
ai_dailyRefresh, run once with Master enabled and Availability disabled.
Population/window
220 legacy Availability rows and fourteen canonical days.
Legacy processing
Candidate sync, realignment, booked/system blocks, sort and date refresh completed.
Candidate stage
_emitAvailabilityUpdateEnd consumed 59,752 ms.
Safe bridge event
ROTA_GENERATION_FAIL_OPEN / LEGACY_UNCHANGED.
Exact error
CTMS_ROTA_PROPERTY_STORE_CAPACITY.
Whole execution
90,081 ms; Apps Script reported Execution completed.
- AV-346: full ai_dailyRefresh remains a required population reconciliation journey.
 
- AV-348: capacity rejection must precede partial bridge persistence and preserve the legacy result.
 
- Execution completed is legacy/fail-open success, not Candidate mirror completion.
 
Do not rerun the unchanged helper enabled. It performs expensive work and cannot publish.
~~~


## Historical decision page 145

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R19 - 18 AUGUST 2026
Sections 125-130 - full-reconciliation property-capacity authority
R19 addendum page 3
126. Exact capacity control flow and clean TEST result
Order
Current source authority
1
Build one complete fourteen-day item for every otherwise eligible Candidate.
2
Partition by maximum fifty items and 245,760 request bytes.
3
Build every request body, 7,000-byte property chunk, manifest and pending index in memory.
4
Project proposed values together with every existing Script Property.
5
Throw before setProperty when projected bytes exceed 480,000.
6
Only successfully persisted state can reach the signed Candidate POST recovery path.
Fresh TEST metric
Result
Candidate Daily feature
false
Enabled entitlements
0
Source links
0
Rota generations
0
Generation receipts
0
No partial Google operation and no Candidate database operation were created.
~~~


## Historical decision page 146

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R19 - 18 AUGUST 2026
Sections 125-130 - full-reconciliation property-capacity authority
R19 addendum page 4
127. R18 routine events and R19 capacity are separate
Workstream
Problem
Required end state
R18
Routine settled changes do not invoke Candidate
generation.
Prompt, coalesced affected-Candidate publication.
R19
Whole reconciliation cannot fit its current durable
owner.
Full-population exact recovery with quota headroom.
- AV-349: neither correction substitutes for the other.
 
- Routine R18 publication must not scan or POST the whole population per edit.
 
- R19 must not reduce ai_dailyRefresh to an affected-only journey.
 
- Both retain one complete canonical Candidate snapshot and legacy-first fail-open containment.
 
- Emergency and retained Availability consumers remain unchanged.
 
Prompt does not mean full population. Reconciliation does not mean routine trigger.
~~~


## Historical decision page 147

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R19 - 18 AUGUST 2026
Sections 125-130 - full-reconciliation property-capacity authority
R19 addendum page 5
128. Durable correction invariants and design alternatives
Invariant
Binding rule
Persistence before POST
Never send an operation that cannot be recovered exactly.
Immutable batch
Retain body, batch ID, key and correlation identity across uncertainty.
Ordered completion
A crash after any batch cannot lose or falsely complete the remaining population.
Newer facts
Later Sheet changes cannot silently rewrite an older frozen owner.
Legacy containment
Candidate failure never changes the established legacy result.
- AV-350 and AV-351 preserve exact replay and crash-safe multi-batch completion.
 
- Compression, one-batch-at-a-time ownership, a Google durable artifact and a server intake session are alternatives to
 compare, not approvals.
- Direct POST, deleting unrelated properties or simply raising the ceiling are not acceptable plans.
 
- No Worker or database change is presumed until a Master-only design is proven insufficient.
 
The design must reduce durable cost without reducing durable authority.
~~~


## Historical decision page 148

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R19 - 18 AUGUST 2026
Sections 125-130 - full-reconciliation property-capacity authority
R19 addendum page 6
129. Capacity, fault and performance qualification
Evidence family
Mandatory proof
Byte envelope
Existing property bytes, item/body/chunk/manifest bytes and projected total.
Scale
Current 220-row fixture plus larger population and property-pressure fixtures.
Limits
Below, at and above accepted ceilings; oversized item and more-than-fifty-item splitting.
Recovery
Crash before/after persistence, POST, response, result and cleanup for every batch.
Uncertainty
Lost response, one exact retry after not-found and then status-only recovery.
Performance
Sheet reads, UrlFetch calls, execution time and measurable safety headroom.
- AV-352 requires non-disclosing measurement of the real envelope.
 
- AV-353 prohibits treating a larger constant as capacity proof.
 
- Corruption, terminal rejection, newer Sheet facts and all-batch cleanup remain mandatory.
 
- Bridge false must remain a hard no-op after every correction.
 
A current-population pass without future headroom and crash proof is not qualification.
~~~


## Historical decision page 149

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R19 - 18 AUGUST 2026
Sections 125-130 - full-reconciliation property-capacity authority
R19 addendum page 7
130. R19 disposition and independent plan gate
State
Authority
Legacy refresh
PASS; unchanged legacy behaviour completed.
Current full Candidate reconciliation
NO-GO; capacity preflight prevents publication.
Candidate TEST data
Clean and disabled after the observed run.
R19 runtime correction
Not implemented and not authorised by this pack.
Independent reviewer
Must quantify, compare alternatives and return an exact implementation plan.
Operational state
Both bridges false outside a separately approved diagnostic; no unchanged rerun.
- AV-354: alternatives require security, quota, recovery, rollback and legacy-intrusion comparison.
 
- AV-355: the unchanged enabled helper must not be rerun.
 
- AV-356: preserve installed R17 Worker/RPC authority unless a separate backend need is proven.
 
- R18 routine event planning remains open and separately controlling.
 
GO to a separate design review. NO-GO to implementation or enabled rerun from this pack.
~~~


## Historical decision page 150

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R19C - 18 AUGUST 2026
Sections 131-139 - implemented full-reconciliation appData authority
R19C addendum page 1
Current Decisions
Candidate Daily Phase 3
R19C appData full-reconciliation implementation - Sections 131-139
Authority
Current decision
Decision date
18 August 2026
Problem closed in source
The 220-row full reconciliation no longer stores large immutable batch bodies in Script Properties.
Large owner
One gzip schema-3 artifact in Drive appDataFolder.
Small owner
One bounded V3 Script Property pointer with one active event and zero or one successor.
Network boundary
ai_dailyRefresh freezes and schedules only; a later continuation sends the exact stored body text.
Legacy boundary
Existing Availability publication and established return remain authoritative and unchanged.
Operational state
Implemented locally; not installed in Google; bridges remain disabled pending independent review.
R19C is implemented source, not installed Google runtime authority.
~~~


## Historical decision page 151

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R19C - 18 AUGUST 2026
Sections 131-139 - implemented full-reconciliation appData authority
R19C addendum page 2
131. Binding R19C architecture and separation
Workstream
Binding responsibility
Current state
R19C
Full-population reconciliation after accepted ai_dailyRefresh.
Implemented locally and independently reviewable.
R18E
Prompt affected-Candidate publication after settled routine
changes.
Separate and still outstanding.
Legacy
Existing Google browser, login, Sheets, Availability and
Emergency behaviour.
Preserved; no redesign.
- AV-357 stores large immutable reconciliation authority only in Drive appDataFolder.
 
- AV-358 and AV-359 keep the main bridge false gate first and restrict disabled operation to scope authorization only.
 
- AV-360 prohibits R19C from absorbing or replacing R18E.
 
- AV-361 permits only the execution-start/options seam and forbids those options from entering the legacy Availability
 body.
Reconciliation remains full population. Routine events remain affected-Candidate only.
~~~


## Historical decision page 152

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R19C - 18 AUGUST 2026
Sections 131-139 - implemented full-reconciliation appData authority
R19C addendum page 3
132. Immutable artifact and bounded pointer
Owner
Frozen authority
Schema-3 gzip artifact
Environment, source action/run, exact fourteen-day window, semantic event, items, batches, exact body
text and every transport identity.
V3 pointer
Revision, phase, one active descriptor, zero/one successor, lease, bounded hold and bounded audit
facts.
Every read
Compressed/raw size, hashes, exact keys, window digest/contiguity, batch sequence/count, body bytes
and total items.
- AV-362 requires semantic, payload, artifact and body hash verification on every read.
 
- AV-363 caps artifacts at 4 MiB compressed and 16 MiB raw and retains route limits of fifty items and 245,760 bytes.
 
- AV-364 caps the pointer at 4,096 bytes and the complete Script Property store at 480,000 bytes.
 
- Pointer changes use ScriptLock, exact raw expected value, revision advance, preflight and mandatory read-back
 classification.
No Candidate POST may occur until both artifact and pointer authority are durable and
revalidated.
~~~


## Historical decision page 153

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R19C - 18 AUGUST 2026
Sections 131-139 - implemented full-reconciliation appData authority
R19C addendum page 4
133. Designated Google principal authority
Stage
Required proof
Session principal
HMAC of the normalized effective Apps Script account email matches the enrolled non-disclosing
fingerprint.
Drive principal
Drive v3 About returns user.me=true and a permission ID whose HMAC matches the enrolled Drive
fingerprint.
Combined owner
Both fingerprints bind the pointer, artifact and release/adoption records.
Initial adoption
Allowed only with no V2/V3/file/trigger authority and no previous owner.
Later transfer
Requires a time-bounded release from the old principal while all authority is absent.
- AV-365 proves the effective account before Drive or authority work.
 
- AV-366 independently proves the Drive permission identity; a configured label is not authority.
 
- AV-367 prohibits silent overwrite during initial adoption.
 
- AV-368 prohibits later account change without old-principal release and zero authority.
 
- Raw email, permission ID and secrets never enter artifacts, requests or ordinary logs.
~~~


## Historical decision page 154

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R19C - 18 AUGUST 2026
Sections 131-139 - implemented full-reconciliation appData authority
R19C addendum page 5
134. Drive creation, duplicates, holds and cleanup
Condition
Binding response
Read uncertainty
At most three About/list/download attempts with 250 ms and 1,000 ms delays.
Create uncertainty
At most two create calls; the second only after complete exact-event listing proves zero files.
Identical duplicates
Select the lexicographically smallest ID after full metadata, byte, hash and schema equality proof.
Differing same-event file
Commit HOLD_CONFLICT, retain every file and make no Candidate POST.
Missing/corrupt referenced file
Commit HOLD_CORRUPT and never rebuild from newer Sheet facts.
Ordinary orphan
Delete only same-principal, fully valid, unreferenced files older than 72 hours; maximum ten per run.
Drive uncertainty is reconciled by exact evidence, never by blind create or destructive cleanup.
~~~


## Historical decision page 155

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R19C - 18 AUGUST 2026
Sections 131-139 - implemented full-reconciliation appData authority
R19C addendum page 6
135. Continuation, lease and Candidate result authority
Phase
Authority
Refresh
Freeze, verify, persist pointer, schedule; no Candidate POST.
Continuation
Reprove principal, revalidate pointer/artifact, acquire exact 420-second event/batch lease.
Transport
Sign and send the exact frozen body_text bytes; no parse/reserialize.
Uncertain/retryable
Clear lease, retain event/batch/index/IDs/body and schedule bounded exact recovery.
Terminal
Atomically promote/clear, log safe terminal facts only and never emit mirror completion.
Success
Advance or promote one successor before artifact deletion and trigger cleanup.
- AV-374 prohibits POST from ai_dailyRefresh.
 
- AV-375 preserves exact frozen transport bytes.
 
- AV-376 through AV-379 govern lease, uncertainty, terminal close and ordered success cleanup.
 
- Existing R14-R17 aggregate item-result and identity-conflict authority remains unchanged.
~~~


## Historical decision page 156

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R19C - 18 AUGUST 2026
Sections 131-139 - implemented full-reconciliation appData authority
R19C addendum page 7
136. Successor, trigger and V2 retirement rules
Boundary
Binding rule
Active event
Immutable until its exact terminal or successful transition.
Successor
Zero or one; only an unstarted and window-safe successor may be wholly replaced.
Trigger
At rest zero or one exact-handler future trigger; unrelated triggers are never changed.
Trigger capacity
Creation fails closed when twenty visible project/user triggers occupy capacity.
V2 state
Existing V2 authority blocks R19C admission; the R19C path never writes V2
pending/index/body/manifest keys.
- AV-380 preserves active authority and bounds successor replacement.
 
- AV-381 deduplicates only the exact continuation handler.
 
- AV-382 bounds principal-scoped orphan cleanup.
 
- AV-383 prevents mixed V2/V3 ownership.
~~~


## Historical decision page 157

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R19C - 18 AUGUST 2026
Sections 131-139 - implemented full-reconciliation appData authority
R19C addendum page 8
137. Local verification and unchanged boundaries
Gate
Recorded result
R19C focused suite
20 passed, 0 failed.
Selected R13-R19C suite
92 passed, 0 failed.
Complete backend JavaScript
706 passed, 0 failed.
Capacity fixtures
Current, twice-current and five-times-current synthetic populations.
Worker source
Unchanged; no Worker runtime proof is claimed by R19C.
External state
No Google install, GitHub publication, Supabase mutation, Worker deploy or production action.
- AV-384 freezes Candidate Worker/RPC/OpenAPI/SQL authority.
 
- AV-385 makes the packaged manifest fragment review guidance, not evidence of the effective Google manifest.
 
- AV-386 requires false main bridge and disabled R19C mode at installation start.
 
- AV-390 preserves finance, Banking Pay, invoices, payments, providers, Emergency and production.
~~~


## Historical decision page 158

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R19C - 18 AUGUST 2026
Sections 131-139 - implemented full-reconciliation appData authority
R19C addendum page 9
138. Google installation and qualification gates
Gate
Required evidence
Pre-install
Re-export saved/deployed source, effective manifest, Cloud project, scopes, triggers, owners and
false/empty authority.
Disabled install
Install complete source with bridge false and R19C DISABLED; prove the old app and legacy result
remain unchanged.
Enrollment
Authorize required scopes and prove same designated Session plus Drive principal with no business
artifact or Candidate POST.
Context canaries
Prove the same principal in manual, full-refresh and continuation contexts.
ACTIVE TEST
Separate mutation approval; current and twice-current timing/capacity plus exact recovery and
cleanup evidence.
Close
Return both bridge/mode gates to false/disabled unless wider authority is separately approved.
- AV-387 prohibits inferring live Google facts from repository source.
 
- AV-388 requires manual, full-refresh and continuation principal canaries.
 
- The complete effective Google manifest remains installation authority; the fragment is not copy/paste authority.
 
Independent source GO precedes disabled install; disabled install precedes ENROLLMENT;
ENROLLMENT precedes ACTIVE.
~~~


## Historical decision page 159

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R19C - 18 AUGUST 2026
Sections 131-139 - implemented full-reconciliation appData authority
R19C addendum page 10
139. R19C disposition and route to wider coexistence
Area
Disposition
R19C source implementation
COMPLETE LOCALLY; independent review requested.
R19C Google installation
NOT PERFORMED; requires independent GO and controlled disabled install.
R19C enabled qualification
NOT PERFORMED; requires separate TEST mutation authority and canary gates.
R18E routine event publication
SEPARATE OUTSTANDING implementation/qualification workstream.
Wider Candidate Daily coexistence
NO-GO until both R18E and R19C are independently qualified.
Production
NOT AUTHORISED.
- AV-389 keeps wider coexistence blocked until both workstreams pass.
 
- No manual source-link bootstrap, Candidate allowlist or new backend endpoint is introduced.
 
- A failed R19C qualification returns to the prior Google version and false/disabled properties; installed R17 backend
 authority remains unchanged.
- A new handover must record exact source/version identities, capacity/timing, principal evidence, safe logs, DB results
 and rollback state.
GO requested for source fitness only. No enabled rollout or production authority is implied.
~~~


## Historical decision page 160

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
R18E/R19C FINAL-PLAN AUTHORITY - 18 AUGUST 2026
Sections 140-147 - routine/full-reconciliation planning reconciliation
Planning addendum page 1
Current Decisions
Candidate Daily Phase 3
R18E/R19C final-plan reconciliation - Sections 140-147
140. Incoming R18E plan disposition
Area
Current authority
R18E direction
Retain minimal post-success taps, affected-Candidate publication and bounded Script Properties.
Routine Drive
Prohibited. Drive remains R19C full-population storage only.
Incoming plan
Technically strong but not implementation-ready against the implemented R19C interface.
Required output
One complete replacement R18E plan; not an addendum and not code.
Operational state
No R18E code/install/enablement; R19C source remains local and uninstalled.
- AV-391 retains separate routine R18E and full-reconciliation R19C responsibilities.
 
- AV-392 makes Script Properties, not Drive, the binding routine-body owner.
 
- AV-410 keeps implementation and Google qualification blocked until the replacement plan closes every integration gap.
 
The next independent task produces the final plan. It does not implement or deploy R18E.
~~~


## Historical decision page 161

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
R18E/R19C FINAL-PLAN AUTHORITY - 18 AUGUST 2026
Sections 140-147 - routine/full-reconciliation planning reconciliation
Planning addendum page 2
141. Exact implemented R19C source baseline
Interface
Binding current name
Mode
CLOUDTMS_CANDIDATE_R19C_MODE
Pointer
CTMS_R19C_OWNER_V3
Continuation
ctmsP3_masterContinueR19C
Configuration
ctmsP3_masterR19CConfigurationStatus_
Legacy mirror
ctmsP3_masterMirrorLegacyEvent_
- AV-393 makes the packaged complete R19C Master sources the plan baseline.
 
- AV-394 records the exact current names and rejects assumption from older handovers.
 
- AV-395 prohibits a second request/pointer/helper authority under obsolete proposed names.
 
- The current mirror accepts an accepted AVAILABILITY_UPDATE_END and owns full-population freeze/schedule.
 
Any interface replacement needs an exact migration and combined recovery design.
~~~


## Historical decision page 162

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
R18E/R19C FINAL-PLAN AUTHORITY - 18 AUGUST 2026
Sections 140-147 - routine/full-reconciliation planning reconciliation
Planning addendum page 3
142. Minimal legacy taps and timing authority
Legacy fact
Planning consequence
ai_dailyRefresh
Emits the accepted full-refresh end event before its Document Lock is released.
partialUpdate_flush
Uses its established Script Lock and remains unchanged absent a proved exception.
runDelayedUpdates
Early returns can skip a post-finally drain opportunity.
Other event sources
Candidate, AI Availability, hospital/ward and deletion paths do not guarantee a later drain.
- AV-396 protects the accepted full-refresh R19C seam from silent relocation.
 
- AV-403 requires an exact tap table with post-success condition, lock state, result and failure behavior.
 
- AV-404 protects partialUpdate_flush and legacy lock/return behavior.
 
- AV-408 prohibits redesign of booking, Sheets, Availability, messaging, triggers or errors.
~~~


## Historical decision page 163

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
R18E/R19C FINAL-PLAN AUTHORITY - 18 AUGUST 2026
Sections 140-147 - routine/full-reconciliation planning reconciliation
Planning addendum page 4
143. Reliable bounded routine recovery
Requirement
Binding decision
Recovery
Every accepted request and uncertain frozen operation receives a later bounded opportunity.
Not sufficient
A future successful runDelayedUpdates call cannot be the sole recovery guarantee.
Trigger bound
No per-edit trigger and no unbounded accumulation.
Combined ownership
Routine continuation must coexist with the R19C exact-handler trigger and trigger cap.
Uncertainty
Retain exact body, request ID, idempotency key, correlation ID, hashes and retry-not-before.
- AV-397 makes no-stranding recovery a final-plan blocker.
 
- AV-398 permits only a bounded/deduplicated sidecar continuation or a proved existing scheduler.
 
- AV-401 requires no starvation, cross-deletion, duplicate completion or omission between workstreams.
 
A single bounded sidecar trigger is not a legacy redesign; one trigger per edit is prohibited.
~~~


## Historical decision page 164

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
R18E/R19C FINAL-PLAN AUTHORITY - 18 AUGUST 2026
Sections 140-147 - routine/full-reconciliation planning reconciliation
Planning addendum page 5
144. Identity, coverage and schema authority
Boundary
Binding rule
Routine identity
Use (source_hmac_key_version, candidate_source_hmac) everywhere.
Rotation
The plan must define dirty/frozen behavior when the active source HMAC key version changes.
R19C schema
Current schema 3 rejects undeclared keys.
Coverage fact
Use explicit schema evolution or a separate bounded authority for R18E coverage.
V2
Historical V2 full-reconciliation authority must be distinguishable from new routine ownership.
- AV-399 makes composite identity mandatory for keying, sorting, dedupe and audit.
 
- AV-400 prohibits silently adding covered_r18e_request_version to the exact schema.
 
- AV-402 requires unambiguous V2/R18E ownership so R19C admission cannot block forever.
 
- AV-407 retains the exact routine item, body, chunk and property-store ceilings.
~~~


## Historical decision page 165

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
R18E/R19C FINAL-PLAN AUTHORITY - 18 AUGUST 2026
Sections 140-147 - routine/full-reconciliation planning reconciliation
Planning addendum page 6
145. Bidirectional R18E/R19C state transitions
Interleaving
Final plan must decide
R18E then R19C
Which routine version is covered, retained, adopted or replayed after full freeze.
R19C then R18E
Whether the newer routine request waits, becomes successor evidence or publishes independently.
R19C hold
Routine facts continue without deleting or falsely completing held full authority.
R18E uncertainty
Full reconciliation cannot erase the exact uncertain routine operation.
Hard deletion
Use an exact current R19C admission/scheduling interface, not an invented helper.
- AV-405 requires an exact hard-deletion path.
 
- AV-406 requires active, held and retrying states in both directions to be explicit.
 
- Coverage is determined at a specified freeze/adoption point and must prove no fourteen-day omission.
~~~


## Historical decision page 166

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
R18E/R19C FINAL-PLAN AUTHORITY - 18 AUGUST 2026
Sections 140-147 - routine/full-reconciliation planning reconciliation
Planning addendum page 7
146. Required replacement-plan verification
Test family
Required proof
Legacy
Every tap preserves exact legacy result, lock, Sheets, Availability, messages and errors.
Routine
Booking, cancellation, amendment, coalescing, limits, no Drive and exact lost-response recovery.
Scheduling
Recovery without later runDelayedUpdates, trigger dedupe/cap and unrelated-trigger preservation.
Combined
Both orderings, holds, successor replacement, V2, hard delete and all terminal/success interleavings.
Rollback
No authority, R18E only, R19C only and simultaneous durable authority.
- No test may substitute an obsolete function name or hand-written contract shape for current source behavior.
 
- The final plan must enumerate exact files, functions, taps, schemas, state transitions and expected assertions.
 
- AV-409 retains the no Worker, SQL, frontend and Availability API boundary absent a proved gap.
~~~


## Historical decision page 167

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
R18E/R19C FINAL-PLAN AUTHORITY - 18 AUGUST 2026
Sections 140-147 - routine/full-reconciliation planning reconciliation
Planning addendum page 8
147. Current disposition and next authority
Stage
Disposition
R19C local source
IMPLEMENTED; complete current source included as plan baseline.
Incoming R18E plan
NO-GO AS FINAL PLAN; direction retained, integration gaps open.
Replacement R18E plan
REQUIRED from independent reviewer.
R18E local code
NOT AUTHORISED until replacement plan receives GO.
Google install/enablement
NOT AUTHORISED.
Worker/SQL/frontend
NO CHANGE REQUIRED by current scope.
Production
NOT AUTHORISED.
- The replacement plan must close AV-395 through AV-406 without leaving implementation choices implicit.
 
- A GO authorises only a later local R18E implementation unless publication/install is separately approved.
 
- All existing R19C, Candidate broker/database and legacy safety decisions remain in force.
 
Current verdict: GO to independent final-plan production; NO-GO to R18E coding or Google
installation.
~~~


## Historical decision page 168

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R20 - 18 AUGUST 2026
Sections 148-154 - R19C installation-readiness correction
R20 addendum page 1
Current Decisions
Candidate Daily Phase 3
R20 R19C installation-readiness correction - Sections 148-154
Authority
Current decision
Decision date
18 August 2026
Incoming verdict
R19C NO-GO with five source blockers and adjacent hardening.
Corrected source
NEW MASTER ROTA CloudTMSCandidateBridge.gs only.
Legacy seam
Complete current Code.gs remains unchanged by R20.
External state
No Google install, bridge enablement, Worker, database or frontend change.
Review request
GO/NO-GO for controlled disabled Google installation only.
R20 corrects saved source. It does not authorise enablement or production.
~~~


## Historical decision page 169

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R20 - 18 AUGUST 2026
Sections 148-154 - R19C installation-readiness correction
R20 addendum page 2
148. Correction boundary and later-controlling status
Boundary
Decision
R20 runtime
Correct only the R19C Master helper and its tests.
Legacy
No redesign and no new legacy call site; existing return and primary publication remain authoritative.
Backend
R14-R17 Worker, SQL, RPC, identity and transition authority remains unchanged.
Google
Source is not installed; bridge false and R19C DISABLED remain mandatory.
R18E
Prompt routine affected-Candidate publication remains separate and must baseline on R20.
- AV-428 freezes the exact changed-file boundary.
 
- AV-429 leaves Google installation and enablement as open gates.
 
- AV-430 preserves the separate R18E workstream and wider-coexistence hold.
~~~


## Historical decision page 170

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R20 - 18 AUGUST 2026
Sections 148-154 - R19C installation-readiness correction
R20 addendum page 3
149. Drive appData and canary authority
Condition
Binding response
Create request
Use parents=['appDataFolder'].
Returned location
Require one control-free opaque parent ID and one space equal to appDataFolder.
Canary
Exact metadata, bytes, hash, permanent delete and exact-search absence.
Create uncertainty
Second create only after retryable/reconcile failure and complete zero-result search.
Drive errors
Closed missing/reconcile/retryable/configuration/authorisation/operator-hold classes.
Quota metadata
Finite, nonnegative integer validation before arithmetic or create.
- AV-411 replaces the invalid literal-parent response assumption.
 
- AV-415 binds canary reconciliation and residual-canary no-authority checks.
 
- AV-416 and AV-417 prohibit retrying permanent Drive failures or trusting malformed quota facts.
 
- Opaque Drive IDs and principal facts are never logged or returned by ordinary diagnostics.
~~~


## Historical decision page 171

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R20 - 18 AUGUST 2026
Sections 148-154 - R19C installation-readiness correction
R20 addendum page 4
150. Principal-pair adoption and pointer audit
Phase
Required proof
Pre-state
Re-read expected pair, staged pair and optional exact release under ScriptLock.
Write
One bulk setProperties pair write; no storage transaction assumption.
Read-back
Classify exact old, exact new, partial or unexpected different.
Partial
ADOPTION_PARTIAL_HOLD; ACTIVE is prohibited.
Cleanup
Delete/read back staged keys; delete/read back transfer release last.
Audit
Pointer supersession count, fingerprint and timestamp must be mutually consistent.
- AV-412 through AV-414 make complete-pair response loss idempotent and prevent one-key ownership.
 
- AV-418 closes pointer-audit structural ambiguity.
 
- Status returns bounded adoption labels only; it never exposes fingerprints or account identity.
~~~


## Historical decision page 172

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R20 - 18 AUGUST 2026
Sections 148-154 - R19C installation-readiness correction
R20 addendum page 5
151. Pre-armed continuation watchdog
Step
Authority
Gate
ACTIVE mode, designated principal and valid RUNNABLE pointer.
Pre-arm
Create and prove a distinct future exact-handler trigger excluding the current trigger.
Due
No earlier than lease/retry/existing-lease authority plus sixty seconds.
External work
Drive download, lease mutation and Candidate POST are forbidden before pre-arm proof.
Current trigger
Remove only after future proof or no-authority cleanup.
Repair
Legacy mirror and continuation entries repair missing authority before other R19C work.
- AV-419 makes the future watchdog a prerequisite, not end-of-run cleanup.
 
- AV-420 replaces only exact-handler futures and never touches unrelated triggers.
 
- AV-421 prevents an existing frozen operation from depending on a later successful refresh.
 
A hard execution stop after pre-arm leaves a future owner for the same durable operation.
~~~


## Historical decision page 173

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R20 - 18 AUGUST 2026
Sections 148-154 - R19C installation-readiness correction
R20 addendum page 6
152. Later-conflict isolation and orphan groups
State
Decision
No pointer
Initial differing same-event evidence may install explicit HOLD_CONFLICT.
Existing pointer
Preserve the complete raw pointer byte-for-byte.
Current work
Retain active descriptor, lease, progress, successor and continuation.
Attempted later event
Retain every conflict file; no Candidate POST; bounded aggregate log only.
Orphan cleanup
Group by event, validate all members before any delete, retain every differing group.
- AV-422 prevents later successor construction from suspending immutable current authority.
 
- AV-423 makes age insufficient deletion authority for conflict evidence.
 
- A group with missing identity metadata or any content/hash mismatch remains operator-owned evidence.
~~~


## Historical decision page 174

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R20 - 18 AUGUST 2026
Sections 148-154 - R19C installation-readiness correction
R20 addendum page 7
153. Capacity evidence and privacy
Evidence
Required output
Source scan
Candidate rows, eligible rows, categorised skips, fourteen dates and day counts.
Batching
Items per batch and item/body min, median, p95, max and total bytes.
Retired V2
Exact chunk/manifest/index projection, whole-store total and deficit.
R19C
Actual closed-envelope raw/gzip bytes, ratio, pointer bytes and headroom.
Calls/time
Exact Sheet call counts/timings, projected Drive/Candidate calls and elapsed budget position.
Privacy
No raw identity, key, source HMAC, property value, request body or secret.
- AV-424 uses the current generation builder rather than an aggregate approximation.
 
- AV-425 projects the exact retired property-store design without writing it.
 
- AV-426 builds the actual R19C schema-3 envelope and gzip without Drive.
 
- AV-427 prohibits Drive, Candidate, property, trigger and business mutation during measurement.
~~~


## Historical decision page 175

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R20 - 18 AUGUST 2026
Sections 148-154 - R19C installation-readiness correction
R20 addendum page 8
154. Verification, disposition and remaining gates
Area
Disposition
R20 focused source tests
27 passed, 0 failed.
Selected Candidate Daily R13-R20
99 passed, 0 failed.
Complete backend JavaScript
Recorded in the sealed R20 pack after final source.
Google source/install
Not performed by R20.
R20 independent source review
Required before disabled Google installation.
ACTIVE qualification
Separate TEST mutation authority after disabled/ENROLLMENT gates.
Wider coexistence
Blocked until R20 and R18E are both independently qualified.
Production
Not authorised.
- A source GO means fit for controlled disabled Google installation only.
 
- Disabled install must prove unchanged old-app behavior before ENROLLMENT or ACTIVE.
 
- No manual source-link bootstrap, Candidate allowlist or legacy redesign is introduced.
 
- The updated pack must include exact complete source, tests, incoming finding, decisions and manifests.
 
No enablement, Phase 4 or production authority follows automatically from an R20 GO.
~~~


## Historical decision page 176

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R21 - 18 AUGUST 2026
Sections 155-161 - R20 NO-GO correction authority
R21 addendum page 1
Current Decisions
Candidate Daily Phase 3
R21 R20-NO-GO correction authority - Sections 155-161
Authority
Current decision
Decision date
18 August 2026
Incoming verdict
R20 NO-GO with five reproducible helper defects and two assurance gaps.
Corrected runtime
NEW MASTER ROTA CloudTMSCandidateBridge.gs only.
Legacy seam
Complete Code.gs remains unchanged.
External state
No Google, Worker, database, frontend or production mutation.
Review request
GO/NO-GO for controlled disabled Google installation only.
R21 is a bounded authority correction. It does not authorise bridge enablement, R18E or
production.
~~~


## Historical decision page 177

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R21 - 18 AUGUST 2026
Sections 155-161 - R20 NO-GO correction authority
R21 addendum page 2
155. Disposition, scope and later-controlling status
Boundary
R21 decision
Principal
Reject every one-key staged state at initial, transfer and locked-commit entry points.
Watchdog
Retain one proved future at trigger capacity; fail before deletion without one.
Orphans
Actual compressed bytes and complete group evidence own deletion.
Drive
Unknown/no-code failures are operator holds; global canary inventory blocks adoption.
Capacity
Current, 2x and 5x release shapes plus exact route/store boundaries are mandatory.
No change
Code.gs, Availability, Workers, SQL, frontend, Google and production.
- AV-431 through AV-438 supersede only conflicting R20 implementation details.
 
- All earlier legacy-preservation, identity, transition and disabled-switch decisions remain binding.
 
- R21 is saved-source authority only and has not been pasted, versioned or deployed in Google.
~~~


## Historical decision page 178

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R21 - 18 AUGUST 2026
Sections 155-161 - R20 NO-GO correction authority
R21 addendum page 3
156. Complete staged-principal pair admission
Observed state
Binding disposition
Exactly one staged key
ADOPTION_PARTIAL_HOLD before canary or expected-pair mutation.
Complete staged pair
Both values must equal independently probed Session and Drive fingerprints.
Zero staged keys + old pair
Not adoption authority; remain pending/hold.
Zero staged keys + exact new pair
Only idempotent cleanup may continue.
Locked commit
Re-read and repeat the same invariant under ScriptLock.
Response loss
A completely committed new pair is safely recoverable; a partial pair never is.
- AV-431 applies to initial adoption, release-based transfer and direct locked commit.
 
- No entry point may infer a missing fingerprint from another property or store.
 
- Tests must exercise Session-only and Drive-only states through every entry point.
~~~


## Historical decision page 179

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R21 - 18 AUGUST 2026
Sections 155-161 - R20 NO-GO correction authority
R21 addendum page 4
157. Continuation watchdog at and below trigger capacity
Condition
Required sequence
At/above cap with future
Select and retain one distinct future, then prune only surplus exact handlers.
At/above cap without future
Raise R19C_TRIGGER_CAPACITY before deleting any trigger or doing external work.
Below cap
Create, inventory/read back and prove the replacement before pruning prior futures.
Create failure
Keep the previously proved future unchanged.
Committed create / lost response
Use pre/post inventory or created ID to find the committed future.
Never eligible
Current invocation and unrelated handlers.
- AV-432 prohibits deleting the last recovery owner merely to create trigger capacity.
 
- A reusable proved future is valid continuation authority; blind replacement is not.
 
- The trigger matrix covers 19, 20 and 21 visible-trigger states.
~~~


## Historical decision page 180

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R21 - 18 AUGUST 2026
Sections 155-161 - R20 NO-GO correction authority
R21 addendum page 5
158. Actual compressed-object orphan authority
Required proof
Closed rule
Metadata
Exact name, gzip MIME, closed nine-key appProperties and TEST/principal/event ownership.
Location
One opaque parent and exactly one appDataFolder space.
Size/time
Safe integer declared size and creation epoch satisfying retention.
Actual bytes
Downloaded length and SHA-256 equal declarations.
Whole group
Every member has the same actual compressed hash and length before first delete.
Artifact
Gzip decompression, artifact hash, event fingerprint and creation epoch all match.
- AV-433 makes claimed metadata insufficient deletion authority.
 
- Equal decompressed JSON with different gzip bytes retains the complete group.
 
- Any malformed, misplaced, unavailable or non-identical member retains every member.
~~~


## Historical decision page 181

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R21 - 18 AUGUST 2026
Sections 155-161 - R20 NO-GO correction authority
R21 addendum page 6
159. Closed Drive retry and environment-wide canaries
Evidence
Disposition
408, 425, 429, 5xx
Retryable.
Recognised rate-limit 403
Retryable.
Unknown/no-code exception
OPERATOR_HOLD; never authority for a second create.
Permanent
validation/configuration
Operator/configuration hold.
No-authority canary search
Filter only by owner, schema and TEST environment.
Any principal class
Current, old, unrelated or malformed residual canary blocks adoption.
- AV-434 closes the retry classifier rather than treating absence of a status as transient.
 
- AV-435 separates global no-authority inventory from exact current-canary reconciliation.
 
- Ordinary diagnostics expose only bounded error classes and aggregate counts.
~~~


## Historical decision page 182

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R21 - 18 AUGUST 2026
Sections 155-161 - R20 NO-GO correction authority
R21 addendum page 7
160. Capacity and self-contained review evidence
Matrix
Required proof
Population
Current, 2x and 5x Candidate populations.
Shapes
Compact, mixed and verbose route/job/identity text.
Characters
Quotes, backslashes, newlines, multibyte text and emoji.
Route boundary
Binary-searched largest accepted item and next-byte-class rejection.
Property store
Projected V3 total, headroom and deficit after add/replace.
Archive
Canonicalisation fixture, complete tests, source parity and dual manifests.
- AV-436 owns release-scale capacity and projected whole-store headroom.
 
- AV-437 requires a self-contained independently runnable archive.
 
- Measurement remains no-write and exposes no raw identity, secret or property value.
~~~


## Historical decision page 183

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R21 - 18 AUGUST 2026
Sections 155-161 - R20 NO-GO correction authority
R21 addendum page 8
161. Verification, downstream rebase and remaining gates
Gate
Disposition
Focused R21 adversarial
38 passed, 0 failed.
Selected Candidate Daily
98 passed, 0 failed.
Complete backend JavaScript
724 passed, 0 failed.
Google R21 install
Not performed; both bridges false and R19C disabled remain mandatory.
R18E
Separate workstream; plan must rebase on R21 and preserve AV-431 through AV-437.
Phase 3 / production
Not authorised by this source correction.
- Independent review must reproduce the five R20 failures and reassess the exact R21 bytes.
 
- A GO means fit only for controlled disabled Google installation and unchanged-old-app proof.
 
- ENROLLMENT, ACTIVE coexistence, R18E, final Phase 3 and production retain separate gates.
 
- AV-438 freezes the R21 no-change boundary and downstream rebase requirement.
 
Do not apply a parallel R18E plan verbatim to R20 source; rebase it on the approved R21
helper.
~~~


## Historical decision page 184

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R22 - 18 AUGUST 2026
Sections 162-164 - ACTIVE adoption-state execution gate
R22 addendum page 1
Current Decisions
Candidate Daily Phase 3
R22 ACTIVE adoption-state execution gate - Section 162
Authority
Current decision
Incoming verdict
R21 bounded NO-GO: ACTIVE entrypoints detected but did not enforce the R19C principal-adoption
state.
Reproduction
The exact reviewer matrix reached Drive or Candidate work for partial and pending adoption.
Correction
One property-only exact-ADOPTED guard in the NEW MASTER ROTA helper.
Runtime files
CloudTMSCandidateBridge.gs only; legacy Code.gs is byte-identical to R21.
External state
No Google, Worker, database, frontend, trigger or production action.
AV-439: ACTIVE is executable only when R19C principal adoption is exactly ADOPTED.
162. Binding finding and scope
R21 already classified ADOPTED, ADOPTION_PARTIAL_HOLD and ADOPTION_PENDING, but both ACTIVE
entrypoints proceeded without asserting the accepted state. R22 closes only that enforcement omission.
~~~


## Historical decision page 185

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R22 - 18 AUGUST 2026
Sections 162-164 - ACTIVE adoption-state execution gate
R22 addendum page 2
163. Exact gate and execution order
Entry point
Mandatory order
Full-refresh continuation
Bridge gate; ACTIVE mode; exact adoption guard; principal proof; recovery/Drive/pointer/Candidate
work.
Legacy-event mirror
Legacy result retained; bridge gate; ACTIVE mode; exact adoption guard; principal proof; bridge
publication.
Accepted state
Only ADOPTED: complete expected Session and Drive fingerprints with no staged pair.
Partial expected pair
ADOPTION_PARTIAL_HOLD; bounded diagnostic; zero business side effects.
Staged or incomplete
ADOPTION_PENDING/incomplete; bounded diagnostic; zero business side effects.
Disabled/off
Returns before adoption inspection; no property, Drive, trigger or Candidate side effect.
AV-440: a complete staged pair is evidence awaiting adoption, not ACTIVE execution authority. ACTIVE code must
never adopt, repair or infer ownership while executing business work.
The legacy workflow remains authoritative. A bridge adoption hold cannot change or replace
the legacy result.
~~~


## Historical decision page 186

~~~text
CLOUDTMS CANDIDATE APP - CURRENT DECISIONS
CANDIDATE DAILY PHASE 3 R22 - 18 AUGUST 2026
Sections 162-164 - ACTIVE adoption-state execution gate
R22 addendum page 3
164. Verification and operational gate
Gate
R22 evidence
Reviewer defect matrix
3/3 PASS after reproducing all three R21 failures.
Focused R19C/R22
41/41 PASS.
Selected Candidate Daily
113/113 PASS.
Independent/adversarial
42/42 PASS.
Complete backend JavaScript
727/727 PASS.
Source parity
Code.gs unchanged; exact R21-to-R22 helper patch included.
Decisions
AV-439 and AV-440 are later-controlling for ACTIVE adoption enforcement.
Installation
Not performed. Independent GO and a separate controlled Google window remain required.
R22 is saved-source assurance authority only. Keep both bridges false until the separately
authorised qualification sequence.
~~~


## Later-controlling decisions-file format rule

Every future Candidate App handover pack must include the latest complete cumulative Markdown decisions authority. Historical PDFs remain evidence only and are not required in future packs. The Markdown authority must retain the same full decision detail, identifiers, mappings, gates, no-change boundaries and traceability.

## Later-controlling R23 / R18E decisions

### AV-441 — Exact R22 rebase

R18E is implemented only on the complete R22 NEW MASTER ROTA source. The R22 helper SHA-256 before R18E is `0521861d633c70b7d6f256b9320bab6102e3003807c46f26757565ea61b65a6b`; the legacy `Code.gs` SHA-256 before R18E is `be88bb56a6f1cdab38cb2e6cdf41447d282964ab7e3e48a83e6fb95f93405147`. R18E must retain the R22 property-only `ADOPTED` precondition at both ACTIVE entrypoints. No R21 helper is implementation authority.

### AV-442 — Minimal legacy sidecar

Routine Candidate publication is supplementary to the existing legacy operations. It starts only after an accepted legacy mutation or at an accepted request-only seam, is fail-open relative to the legacy result and does not replace booking grouping, EmailHistory settlement, Availability calculation, email, WATI, legacy UI, login, identity, locking, errors or trigger cadence.

### AV-443 — Disabled equivalence

When `CLOUDTMS_CANDIDATE_BRIDGE_ENABLED` is false or absent, every R18E tap is inert before Candidate Sheet reads, Script Property reads or writes, trigger work, Drive work, network work and bridge logging. The legacy return value, error, Sheet state and message behaviour remain the pre-R18E behaviour.

### AV-444 — Routine versus full authority

R18E publishes only affected Candidate identities and stores its journal and frozen request bodies in bounded Script Properties. R18E never uses Drive. R19C remains the population-wide daily/full reconciliation authority using its existing schema-3 Drive appData artifact and `CTMS_R19C_OWNER_V3` pointer. Neither mechanism replaces the other.

### AV-445 — Exact routine identity

The routine identity is the tuple `(source_hmac_key_version, candidate_source_hmac)`, represented for local ordering and deduplication as a ten-digit zero-padded positive integer, a colon and a lowercase 64-hex HMAC. Raw Credentially public IDs, names, telephone numbers, email addresses, Candidate UUIDs and secrets are not persisted in R18E properties, sent in routine bodies beyond the already-approved signed contract or emitted in logs.

### AV-446 — Exact eight legacy taps

Only these effective `Code.gs` functions may differ from R22 for R18E: `changeHospitalAndWardFromPayload`, `changeWard`, `runDelayedUpdates`, `ai_processConversation`, `sendUpdatesToCandidates`, `createCandidate`, `saveCandidateEdit` and `deleteCandidateRow`. The first seven create bounded fail-open routine notice/pump work after the relevant legacy commit. `deleteCandidateRow` records only a blocked full-admission decision. No ninth legacy function may change.

### AV-447 — Candidate hard deletion remains blocked

Hard Candidate deletion records `CANDIDATE_REMOVAL` with state `BLOCKED_PRODUCT_DECISION`. It does not invent an empty or unavailable generation, source-link deletion, generation deletion, entitlement removal, Candidate deletion, tombstone or automatic full-admission clearance. Ordinary R18E and R19C success cannot clear it.

### AV-448 — Closed routine request authority

`CTMS_R18E_REQUEST_V1` is the bounded coalescing journal. It owns at most twenty distinct routine identities, factual fingerprints, monotonic request versions, readiness and reason masks. Identical notices do not advance the factual version. Later factual changes advance the same identity rather than creating parallel operations.

### AV-449 — Unlocked ingress

Where an accepted seam cannot safely wait for the Script Lock, it may write one bounded `CTMS_R18E_INGRESS_V1_<uuid>` record containing only hashed identities and bounded authority metadata. The continuation absorbs each ingress exactly once under the Script Lock. Ingress cannot directly publish to Candidate and cannot supersede frozen authority.

### AV-450 — Full admission and capacity

Routine overflow, item oversize, readiness not proved and property-capacity pressure create bounded `CTMS_R18E_FULL_ADMISSION_V1` reasons in `ADOPTABLE` state. A hard removal uses `BLOCKED_PRODUCT_DECISION`. The maximum twenty identities/items is a cardinality ceiling, not permission to exceed the 7,000-byte property-value ceiling: if the closed journal reaches that byte ceiling earlier, the previous valid journal is retained and affected identities are escalated to full admission. Routine storage remains under a 450,000-byte owner ceiling and the complete Script Property store remains under 480,000 bytes.

### AV-451 — Freeze-before-network

Before any routine Candidate POST, R18E persists the complete immutable request body in ordered `CTMS_R18E_BODY_V1_*` chunks and commits `CTMS_R18E_PENDING_V1` with the exact operation ID, batch request ID, idempotency key, correlation ID, body SHA-256, byte count, item count and claimed identity versions. A later event cannot replace or mutate this frozen operation.

### AV-452 — Route and body limits

Routine publication uses only the existing signed TEST route `/candidate-system/v1/google-availability/rota-generations`. A frozen routine body contains at most twenty items and at most 131,072 UTF-8 bytes. Each Script Property value is at most 7,000 UTF-8 bytes. A single oversize item is not partially sent and is escalated without network activity.

### AV-453 — Exact aggregate-result authority

The R14 result rule remains binding. HTTP 2xx plus `ok:true` is success only when the receipt and indexed outcome set are structurally valid, the outcome count exactly matches the frozen item count, every zero-based index occurs exactly once and every item is `COMMITTED` or `REPLAYED`. Recognised `REJECTED` outcomes are terminal rejection, never mirror completion. Malformed, incomplete or unknown responses preserve the frozen operation.

### AV-454 — Response-loss and retry authority

Network uncertainty, 429, 5xx and unclassified results retain the same frozen body and operation identities with bounded retry-not-before state. A live lease prevents duplicate POSTs. Response-loss recovery transfers only the exact claimed request versions; a newer dirty version survives and is published later.

### AV-455 — Completion and terminal closure

Successful response classification moves the frozen operation to durable `COMPLETION_REQUIRED`; terminal rejection moves it to `TERMINAL_CLOSURE_REQUIRED`. A later local unit emits exactly one bounded completion or rejection log and clears only that exact pending record and its exact body chunks. No completion log is emitted for terminal or preserved outcomes.

### AV-456 — Filtered builder contract

The existing generation builder accepts an optional closed object with exactly `allowed_source_identities` and `source_event_time_utc`. Validation occurs before any Sheet read. The identity filter is canonical, unique, version-supported and limited to twenty. Empty input returns no items without a Sheet read. Unfiltered R19C construction retains its existing population-wide behaviour and measurements.

### AV-457 — Combined continuation owner

`ctmsP3_masterContinueR19C` remains the sole automatic continuation handler. R18E and R19C share one classifier and repair owner which recognises both authorities, selects work deterministically and pre-arms a distinct future exact-handler trigger before external work. It must never create one trigger per edit or delete another authority's only future.

### AV-458 — Ordering and no-overtake rules

An already-frozen R18E operation runs before later full work. Existing R19C pointer authority prevents a new routine freeze, while routine dirty truth accumulates. Historical V2 authority blocks both. An R18E operator hold blocks automatic progress rather than allowing R19C or a new routine operation to overtake unresolved routine ownership.

### AV-459 — R22 adoption and session-only routine proof

Both ACTIVE entrypoints retain the R22 order: bridge gate, mode gate, exact `ADOPTED` property gate, then principal/business work. Routine work uses Session principal proof plus the already-adopted expected Drive fingerprint without calling Drive. Staged principals, release records or residual canaries block routine authority. Full R19C keeps its complete Session-and-Drive proof.

### AV-460 — R19C coverage watermark

Before a full build, R19C captures the exact routine request/admission versions it may cover. After the R19C pointer is durably committed it writes bounded `CTMS_R18E_R19C_COVERAGE_V1`. Full success may clear only represented identity versions no newer than the captured versions and may adopt only eligible `ADOPTABLE` reasons. Terminal full outcomes remove their coverage evidence but retain routine dirty truth. `BLOCKED_PRODUCT_DECISION` is never adopted.

### AV-461 — No-authority proof

R19C no-authority and continuation cleanup must treat every R18E request, ingress, pending record, body chunk, full-admission record and coverage record as authority. Absence or completion of one mechanism cannot delete the other mechanism's continuation or durable owner.

### AV-462 — Privacy and diagnostics

R18E diagnostics expose only schemas, phases, counts, byte measurements, bounded reason/error codes and booleans. They do not expose Script Property values, raw identities, HMACs, Candidate IDs, request bodies, secrets, transport headers or sensitive Sheet data. Status and capacity checks are mutation-free.

### AV-463 — Exact verification floor

The R23 implementation adds exactly eighty focused R18E tests: 16 source/tap, 20 journal/freeze, 14 publication/replay, 22 combined continuation and 8 capacity/privacy/archive tests. The required floors are 121 focused R19C/R22/R18E tests, 193 selected Candidate Daily tests and 807 complete backend JavaScript tests, with both Apps Script sources parsing and the changed-function inventory remaining exact.

### AV-464 — External no-change boundary

R23 changes only local NEW MASTER ROTA `Code.gs`, `CloudTMSCandidateBridge.gs`, Candidate Daily tests and assurance documents. Availability API, Worker runtime, SQL/RPC, OpenAPI, frontend, Google manifest/scopes/triggers/properties, Candidate data, feature flags, entitlements, finance, invoicing, Banking Pay, payments, provider, settlement, remittance, Emergency and production are unchanged.

### AV-465 — Installation and activation gates

The local R23 handover does not authorise GitHub publication, Google paste/save/version/deployment, trigger changes, R19C principal adoption, ACTIVE mode, either bridge switch, TEST mutation, feature enablement or production. After independent source GO, any Google qualification remains disabled-first with rollback versions and exact source re-export/hash proof, followed by separately authorised enabled TEST journeys.

### AV-466 — Decisions authority format

This cumulative Markdown file is the controlling Candidate App decisions authority. It preserves the complete historical R22 PDF text and adds AV-441 through AV-466. Future Candidate App packs must include the latest complete Markdown authority; generating a new Decisions PDF is unnecessary unless the user explicitly requests one.

---

# Later-controlling Phase 3 R24 / R18E assurance correction

## Status and precedence

This R24 section is later-controlling wherever it conflicts with the R23 routine affected-Candidate implementation. All earlier decisions remain in force unless this section expressly changes them. R24 does not authorise installation, versioning, deployment, enablement or TEST data mutation. It corrects the saved complete NEW MASTER ROTA source and its assurance boundary only.

## AV-467 — Explicit empty filter is service-free

After the complete options object has been validated and the result/measurement shape has been initialised, an explicitly present empty `allowed_source_identities` array must return zero generation items before Spreadsheet, Session, Script Properties, network or business logging work. Measurement mode must return the exact all-zero routine-filter measurement. An absent filter retains the population-wide path.

## AV-468 — Durable R18E schemas are closed

Every persisted R18E root and nested record must be validated against an exact key set and exact scalar/enumeration/form constraints before it is trusted. This includes request and dirty entries, freeze intent, compact ingress, full admission and reason, pending and identity, lease, last attempt, outcome, hold, body wrapper/root, and coverage root/entry. Unknown fields are corruption even when a hostile or obsolete writer recomputes a record hash.

## AV-469 — Persisted UTF-8 limits are authority

Every R18E property owner must prove its serialized UTF-8 byte size before publication. Compact ingress is limited to 2,048 bytes; route body roots are limited to 131,072 bytes; ordinary request, freeze, full-admission, pending, wrapper and coverage owners must remain within the established Script Property per-value ceiling. Oversize state must fail closed or create the exact durable overflow authority; it must never be truncated.

## AV-470 — Corrupt owners remain diagnostic holds

Invalid ingress, request, freeze, pending, body, full-admission or coverage state must classify as a non-destructive R18E hold. It must not be partly absorbed, normalised, deleted, superseded or bypassed by R19C. The combined continuation must retain R18E as the selected hold owner.

## AV-471 — Candidate edit readiness is identity-local

A uniquely resolved current post-commit public identity is READY even when a previous public ID is missing or requires verification. Previous public IDs are separate VERIFY identities. Old names and telephone numbers must not be treated as unrelated current aliases when the before/after Candidate row is known. One missing or ambiguous identity must not downgrade another independently READY identity.

## AV-472 — Duplicate public identities are ambiguous

Readiness is based on exact Candidate row cardinality, not merely distinct source-HMAC cardinality. Two Candidate rows carrying the same public identity are ambiguous and must remain VERIFY. No fuzzy name, email or telephone database matching is introduced.

## AV-473 — Current Candidate facts only at the edit seam

The Candidate edit seam supplies current public ID, current occupant key and current telephone as current authority. A genuinely changed prior public ID may be supplied separately. Obsolete prior name and telephone aliases must not poison the current identity.

## AV-474 — VERIFY is reachable automatic work

The existing combined continuation owns VERIFY work. Each continuation may select one due VERIFY identity and perform one read-only filtered resolution. It must not create a route body or make a Candidate generation POST while readiness is unproved.

## AV-475 — Three exact VERIFY attempts

A unique match changes the same request identity to READY. A failed check increments exactly once per continuation. After the third failed check the dirty identity remains owned and `READINESS_NOT_PROVEN` is durably admitted for population-wide reconciliation. An older verification result cannot overwrite a newer request version or factual fingerprint.

## AV-476 — Freeze intent owns every partial body

Before body chunks are published, the request must own an exact freeze intent identifying the operation and expected body. Every chunk is written, read back, parsed and validated. Pending is then validated, written and read back before request ownership transfers. A crash after any chunk, the final chunk, pending publication or request deletion must leave one exact recoverable owner.

## AV-477 — Expired freeze cleanup is exact

Only an expired freeze intent whose operation, prefix, counts and hashes exactly match its stored chunks may delete those chunks and clear that exact intent. Dirty truth remains. Unknown orphan chunks are `HOLD_CORRUPT`; competing valid owners are `HOLD_CONFLICT`.

## AV-478 — Compact ingress is the routine lock-contention owner

Unlocked notification ingress uses only the closed keys `s,o,id,at,e,m,f,ids,x,h`. Each identity is a readiness marker plus ten-digit source-HMAC key version and 64-hex source HMAC. Raw public ID, name, telephone, email, Candidate UUID and secret values are forbidden.

## AV-479 — Routine identity cardinality never silently drops

Up to twenty exact routine identities may be retained in compact ingress. A twenty-first identity, or any alias population that cannot be represented in the routine owner, must retain the bounded identities and durably add `ROUTINE_DIRTY_OVERFLOW` full-admission authority. Sixty or more aliases must not throw into the legacy fail-open void or disappear silently.

## AV-480 — Capacity fallback waits and merges

If compact ingress cannot be safely published because the lock is unavailable and property capacity rejects the value, the helper must wait for the Script Lock and durably merge/adopt the same already-accepted notice. It must not swallow the capacity error and report acceptance without durable authority.

## AV-481 — Exact reason catalogue

The only R18E reason bits are: `1` booking occupancy/create/cancel/reassignment; `2` booking reference/time/location metadata; `4` Candidate public-ID/name/telephone identity facts; `8` Availability preference facts; `16` Availability blocked/policy facts. Zero, unknown and event-incompatible masks are invalid. Compatible reasons may be ORed for the same settled operation.

## AV-482 — Booking notices use accepted changes only

Booking settlement notification must be derived from the operations that the legacy workflow actually accepted and changed, not from every Candidate present in a raw request. New/cancelled occupancy uses bit 1. Reference, time or location metadata changes use bit 2. The legacy result and `partialUpdate_flush` owner remain unchanged.

## AV-483 — Availability preference and policy are distinct

Availability preference facts use bit 8. Rule-blocked or other availability policy facts use bit 16. One accepted AI availability commit may carry both where both factual classes changed.

## AV-484 — Declared parser dependency and exact baselines

Acorn is a direct development dependency. Assurance must execute exact R22 Code/helper SHA-256 assertions and compare complete function inventories. R24 `Code.gs` may differ from R22 only in the eight declared seam functions: `ai_processConversation`, `changeHospitalAndWardFromPayload`, `changeWard`, `createCandidate`, `deleteCandidateRow`, `runDelayedUpdates`, `saveCandidateEdit`, and `sendUpdatesToCandidates`.

## AV-485 — Clean-extraction evidence is mandatory

The handover must include all source, declared dependencies, helpers, fixtures and exact test commands needed for a clean extraction. Its builder must perform an isolated dependency install and run the exact focused suite before manifest sealing. A test that succeeds only inside the developer worktree is insufficient.

## AV-486 — R24 external no-change boundary

R24 changes no Availability API source, Worker route or deployment, SQL/RPC/schema, OpenAPI, frontend, Google trigger, Candidate/entitlement/source-link/feature authority, finance, Banking Pay, payment/provider/settlement/remittance behavior or production resource. The saved Google source remains uninstalled and the bridge must not be enabled on the authority of this pack alone.

## R24 verification and next gate

The exact R24 package must prove the focused R18E/R24 suite, the selected Candidate Daily suite, the complete backend JavaScript suite, script-mode parsing, exact R22 hashes, the eight-function Code inventory, manifest byte/hash parity and a clean-extraction focused run. The next step is an independent source/package GO or NO-GO review. Google installation or bridge enablement remains a separate later operator gate.

---

# Later-controlling Phase 3 R25 / R18E assurance closure

## Status and precedence

This R25 section is later-controlling wherever it conflicts with R24. Every earlier Candidate App decision remains in force unless this section expressly changes it. R25 is a saved-source correction only. It does not authorise Google save, versioning or deployment; bridge enablement; GitHub publication; Worker or frontend deployment; database mutation; trigger changes; Candidate feature enablement; or production.

## AV-487 — Duplicate public identity is a hard ambiguity veto

Filtered routine identity resolution must count exact Candidate rows carrying each public identity before considering any secondary alias. If the same normalized public identity occurs on zero or more than one Candidate row, that identity is `UNMATCHED` or `AMBIGUOUS` respectively and remains `VERIFY`. A unique name, telephone number, email, occupant key or derived source HMAC must never override duplicate public-ID row cardinality. No fuzzy matching is introduced.

## AV-488 — Builder outcome authority is per requested identity

The filtered generation builder must return one closed `identity_results` entry for every requested routine identity, with the same `(source_hmac_key_version,candidate_source_hmac)` tuple and exactly one status from `MATCHED`, `UNMATCHED` or `AMBIGUOUS`. The result set must have exact cardinality, unique identities and no identities outside the request. Aggregate missing or ambiguous counts are measurements only; they are not authority to downgrade unrelated Candidates.

## AV-489 — Mixed filtered results are identity-local

When one filtered build contains both matched and unresolved identities, every matched identity may continue into the exact frozen generation body while only the corresponding unmatched or ambiguous identity advances to `VERIFY`. Nineteen matched identities and one failed identity must freeze nineteen items, not poison the complete request. Failed identities retain their own reason mask, factual fingerprint and monotonically advanced request version. No Candidate POST may include an unresolved identity.

## AV-490 — Supported source-HMAC versions are enforced at every durable boundary

Every request identity, compact-ingress identity, freeze identity, pending identity and filtered-builder identity must use a positive source-HMAC version present in the configured accepted-version catalogue. Unsupported versions are corruption or invalid input before publication. Recomputing a record hash does not make an unsupported identity authoritative.

## AV-491 — Freeze publication has two explicit stages

`CTMS_R18E_REQUEST_V1.freeze_intent` has an exact stage of `BUILDING` or `PREPARED`. `BUILDING` has empty/zero body metadata and cannot coexist with any body chunk. `PREPARED` owns the exact body SHA-256, UTF-8 byte count, chunk count and item count before the first chunk is written. A request may advance from `BUILDING` to `PREPARED` only after the complete route body has been constructed and measured locally and the updated request has been durably written and read back.

## AV-492 — Partial body ownership is closed and recoverable

Every body wrapper must have the exact closed key set, exact operation/prefix, canonical ordinal, declared chunk count, per-chunk bytes and SHA-256, and creation time. A valid interrupted PREPARED write may contain only the exact ordinal prefix `1..N` of the declared complete set and remains recoverable. Unknown wrapper fields, impossible ordinals, gaps, duplicate ordinals, owner mismatch, count mismatch or hash mismatch are `HOLD_CORRUPT`. A competing exact owner is `HOLD_CONFLICT`.

## AV-493 — Corrupt freeze evidence is never auto-recovered or deleted

Automatic continuation and expiry handling may clear only an expired freeze whose request, operation, stage, body metadata and every stored wrapper prove one exact owner. Corrupt, unknown or competing evidence remains a non-destructive operator hold. R19C, a later event and routine recovery must not overtake it or delete it merely because an expiry timestamp passed.

## AV-494 — Pending terminal and completion schemas are fully closed

Pending source-event identity and timestamp must equal the exact frozen operation and creation time; body bytes must be between one and 131,072 inclusive; and every claimed request version must not exceed the freeze claim version. `COMPLETION_REQUIRED` and `TERMINAL_CLOSURE_REQUIRED` require `lease:null`. Terminal item outcomes have exactly `index` and `error_code`, unique sorted zero-based in-range indices, and a recognised terminal Candidate Daily error. Unknown nested keys, duplicate/out-of-range indices, an active lease at closure, or a body above 131,072 bytes are corruption.

## AV-495 — Accepted routine notice always leaves durable authority

After an accepted post-commit routine notice returns, every affected exact identity must be represented by the current routine request or by exact/general full-admission authority. Property-value saturation, request-byte overflow or full-admission saturation must never silently discard the accepted identity. The write path must read back and verify representation before reporting acceptance.

## AV-496 — Full-admission compaction preserves meaning

When exact ordinary reasons cannot fit, the helper may compact equivalent non-removal reasons into one identity-neutral, population-wide reason of the same recognised code. This is full-reconciliation authority, not Candidate-specific publication. Candidate removal remains an exact `BLOCKED_PRODUCT_DECISION` and must not be generalized or silently adopted. Compaction must retain only closed recognised reason codes and remain inside the existing byte and owner ceilings.

## AV-497 — Booking reason masks remain Candidate-local

`sendUpdatesToCandidates` must not OR all booking settlement reasons and apply the union to every affected Candidate. While the existing settlement Script Lock remains held, it groups affected Candidates by their exact reason mask and invokes the existing locked notifier once per deterministic mask group. Candidates affected only by occupancy, metadata or both retain masks `1`, `2` or `3` respectively. The legacy EmailHistory, notification, repaint, Availability and return behavior remains unchanged.

## AV-498 — No broad legacy redesign

R25 changes no legacy booking, cancellation, amendment, EmailHistory, email, WATI, repaint, Availability, login, UI, trigger or locking architecture. The only legacy-source change is private logic inside the already-authorised `sendUpdatesToCandidates` seam, and the top-level function inventory remains exactly the same eight R18E-modified Code functions relative to R22. The helper continues to be supplementary and fail-open relative to the legacy result.

## AV-499 — R25 exact executable verification floor

R25 must execute: the 17 new adversarial closure tests; the complete 119-test R18E/R24/R25 focused suite; the complete 268-test Candidate Daily suite; and the complete 846-test backend JavaScript suite. It must additionally parse both complete Apps Script sources, prove the exact R22 baseline hashes and eight-function Code inventory, perform a clean extraction with `npm ci --ignore-scripts --no-audit --no-fund`, rerun the complete 119-test focused suite there, and verify both archive manifests and exact source byte parity.

## AV-500 — Review evidence must not claim an applyable patch unless it is exact

Cross-version source comparisons in an assurance archive are semantic review evidence unless the builder has independently proved they apply byte-for-byte to the packaged baseline. LF-normalized semantic diffs must be named and labelled as non-applyable. Complete source files, their SHA-256 identities and the manifest are the copy/paste and audit authority. Clean extraction must use the declared npm lockfile and a single documented install command.

## AV-501 — R25 no-change and next gate

R25 changes no Availability API source, Worker route or deployment, SQL/RPC/schema, OpenAPI, frontend, Google manifest/scopes/triggers/properties, Candidate/source-link/entitlement/feature data, finance, invoicing, Banking Pay, payment/provider/settlement/remittance behavior, Emergency system or production resource. The corrected saved Master source remains uninstalled. The next step is an independent R25 source/package GO or NO-GO. Any later Google qualification remains a distinct disabled-first operator action with exact rollback/version/source-export proof.

## R25 verification and next gate

The R25 archive is acceptable for independent review only if its regenerated provenance and evidence prove the exact current source hashes, all AV-487 through AV-501 invariants, the 119/119 focused gate, 268/268 Candidate Daily gate, 846/846 complete backend gate, clean-extraction 119/119 gate, script-mode parsing, R22 baseline inventory, no forbidden local references, manifest byte/hash parity and no external mutation. A source GO still does not itself authorise Google installation or bridge enablement.

---

# Later-controlling Phase 3 R26 / R18E R25-NO-GO correction

## Status and precedence

This R26 section is later-controlling wherever it conflicts with R25. Every earlier Candidate App decision remains in force unless this section expressly changes it. R26 is a saved-source and assurance correction only. It does not authorise Google save, versioning, deployment or enablement; GitHub publication; Worker or frontend deployment; Supabase mutation; trigger changes; Candidate feature enablement; or production.

## AV-502 — The independent R25 NO-GO is accepted in full

R26 accepts all seven independently reproduced R25 defects as real: post-eligibility duplicate counting, stale builder downgrade, pending/body identity mismatch, pending-first cross-owner mismatch, semantically impossible persisted outcomes, identity-neutral Candidate removal and exact-removal saturation. R26 must close the causes rather than weaken the controlling tests.

## AV-503 — Whole-list canonical public-identity cardinality is the first veto

The complete Candidate List is counted before telephone, Availability or booking eligibility. Public-ID comparison uses the same trim, whitespace-removal and uppercase canonical form as the certified CID1 authority. Zero rows is `UNMATCHED`; one row may proceed to eligibility; two or more rows is `AMBIGUOUS` with zero emitted items. The existing source-HMAC derivation is unchanged and secondary facts cannot override the duplicate veto.

## AV-504 — Outside-lock results are conditional on the exact claimed entry authority

For every matched or failed identity returned by an outside-lock build, later mutation is permitted only when the current dirty entry still equals the claimed composite identity, request version, factual fingerprint, reason mask and readiness. A newer or different entry remains untouched: its readiness, verification count, request version, reason mask and fingerprint cannot be changed by stale evidence.

## AV-505 — Pending identity and frozen route body are one closed authority

Every pending identity contains the exact supported source-HMAC version, HMAC, request version, factual fingerprint, reason mask and `READY` state. The reconstructed body has the exact root and item schemas, batch identity, item count, unique sorted item keys, supported identity versions, source event ID and source event time. Its sorted identity set must exactly equal `pending.identities`. Any difference is `HOLD_CORRUPT`, retains every owner and permits zero Candidate POSTs.

## AV-506 — Request, pending and body are jointly classified before work selection

Classification inventories request, freeze intent, pending, every body key, ingress, full admission, coverage, exact removal records and R19C/V2 authority before selecting automatic work. Request plus pending is legal only for exact PREPARED transfer recovery with complete equality of operation, request/claim, source event/time, batch, idempotency, correlation, notice-set, body metadata and claimed dirty entry authority. BUILDING or any mismatch is a non-destructive hold. Transfer enforces the same equality and cannot delete contradictory evidence.

## AV-507 — Persisted response outcomes encode exact semantic authority

Persisted `SUCCESS` requires HTTP 200–299 and `INDEXED_AGGREGATE`. Indexed terminal rejection requires HTTP 200–299, `INDEXED_REJECTION`, `GENERATION_ITEM_REJECTED`, empty retry class and at least one recognised unique indexed item. Defensive top-level terminal authority requires `TOP_LEVEL_DO_NOT_RETRY`, zero items and exactly one accepted triple: 409/SOURCE_EVENT_CONFLICT/DO_NOT_RETRY, 409/IDENTITY_LINK_CONFLICT/DO_NOT_RETRY or 422/GENERATION_INCOMPLETE/DO_NOT_RETRY. Every other combination is corruption/preservation. Body and outcome authority are revalidated immediately before logging or deletion.

## AV-508 — Candidate removal uses a separate exact HMAC-only journal

Candidate removal cannot use an identity-neutral full-admission reason. Each exact source identity has one deterministic Script-Property key under `CTMS_R18E_REMOVAL_V1_` and one closed record containing only schema, owner, state, reservation UUID, supported HMAC version, source HMAC and bounded timestamps. Raw public ID, Candidate name, telephone, email and Candidate UUID are forbidden. The journal supports at most 256 outstanding exact removal authorities and remains within the routine and total Script-Property budgets.

## AV-509 — Exact removal authority is reserved before hard legacy deletion

When both the Candidate bridge and R19C ACTIVE mode are enabled, `deleteCandidateRow` must establish and read back the exact HMAC-only reservation before `Candidate List.deleteRow`. Missing helper, blank/ambiguous public identity, lock/capacity failure or uncertain persistence blocks before legacy mutation. A row-delete failure rolls back only a newly created RESERVED record. After successful deletion, state promotion to COMMITTED is best-effort because RESERVED is already exact durable authority; failed promotion must never delete the only owner. With the bridge false or R19C not ACTIVE, the established legacy path remains unchanged in effect.

## AV-510 — Removal records are blocked authority, not reconciliation input

RESERVED and COMMITTED removal records are retained until a later expressly approved product authority defines clearance. R19C cannot adopt, generalise or clear them; they block the R19C no-authority proof and classify as `REMOVAL_BLOCKED`. At the declared maximum, the next enabled-bridge deletion fails before row deletion. This is an intentional safe ceiling, never silent loss.

## AV-511 — BUILDING and PREPARED crash rules remain distinct

A BUILDING intent has no body metadata and any body chunk beside it is corruption. PREPARED owns the complete expected body metadata before the first chunk write, so an interrupted prefix of PREPARED chunks remains exact recoverable authority. This clarification preserves AV-491 through AV-493 while preventing pending-first bypass.

## AV-512 — R26 route-body validation is closed

The frozen generation body accepts only `batch_request_id` and non-empty `items`. Each item has exactly the ten accepted Candidate Daily generation keys, one supported identity version, valid CID1, hashes, date/time, bounded strings, exactly fourteen closed day records and one unique item key. Unknown/raw identity fields, duplicate identities, wrong source event facts, wrong count or unsupported version are corruption before transfer or network activity.

## AV-513 — R26 executable assurance floor

R26 adds thirteen direct adversarial tests covering the seven R25 reproductions and the reserve-before-delete lifecycle. The focused R18E/R24/R25/R26 suite must pass 132/132. The selected Candidate Daily suite and complete backend JavaScript suite must include the R26 file and pass with zero failures. Both complete Apps Script sources must parse, the R22 baseline/eight-function legacy inventory must remain exact, the clean-extraction focused suite must pass 132/132, and archive hashes/sizes/source parity must verify.

## AV-514 — Complete source files remain the copy/paste authority

The complete R26 `Code.gs` and `CloudTMSCandidateBridge.gs` files, their SHA-256 values and the archive manifest are the only copy/paste authority. Semantic diffs are review aids only. The archive must contain the R25 independent review and reproduction evidence, complete current tests/helpers and sufficient unchanged repository source to independently rerun the declared suites.

## AV-515 — No broad legacy or platform redesign

R26 changes only the complete saved NEW MASTER ROTA `Code.gs`, supplementary bridge helper, focused tests and assurance documents. It adds no ninth legacy seam. Availability API, Worker route, SQL/RPC/schema, OpenAPI, frontend, Google manifest/scopes/triggers/properties, Candidate/source-link/entitlement/feature data, Emergency, finance, invoicing, Banking Pay, payments, provider, settlement, remittance and production are unchanged.

## AV-516 — External no-change boundary and next gate

R26 is not installed in Google and makes no GitHub, Supabase, Cloudflare, Worker, frontend, trigger, property, Candidate-data, TEST-business-data or production change. R22 remains the prior independently accepted deployed-source baseline unless separately proven otherwise. The next step is independent R26 source/package GO or NO-GO. Only a later explicit operator instruction after GO may begin disabled-first Google qualification with rollback/source-export proof.

## R26 verification and next gate

Independent review must reproduce every R25 machine-verifiable outcome against the complete R26 source, verify the exact removal maximum and pre-delete block, inspect all AV-502 through AV-516 decisions, rerun the declared suites from a clean extraction, and issue one definitive GO or NO-GO. A source GO does not itself enable or deploy either bridge.

---

# Later-controlling R27 visual-branding authority

## Status and precedence

This R27 section records the settled Office CloudTMS and future Candidate App branding decision. It changes no Candidate workflow, authentication authority, entitlement, feature flag, database, Worker, Google Apps Script or financial behavior. R26 remains the current Candidate Daily implementation awaiting independent review.

## AV-517 — Office CloudTMS logo authority

The supplied `TMS_black_background_2.png` artwork is the Office CloudTMS logo. Its exact repository authority is:

```text
TEST-Frontend/assets/branding/cloudtms-office-logo-black.png
SHA-256: 2904d5d483a1668a7a86a4cfee8c8b1893d251c264e84aa8f06031bc871868cd
Source dimensions: 1448 x 1086 pixels
```

Office CloudTMS must use this artwork in place of the previous top-left purple-dot plus `CloudTMS` text treatment. The header presentation may crop only the black source canvas around the artwork; it must not distort the logo proportions.

## AV-518 — Office login-background authority

The same Office CloudTMS artwork must appear as a large background watermark while the Office login screen is shown. The login form remains the foreground authority and must stay readable, focusable and operable. Responsive sizing, opacity and black-background blending may change by viewport, but the source artwork, proportions and identity must remain unchanged.

## AV-519 — Future My TMS app identity

The supplied `TMS_black_background_4.png` artwork is the settled future Candidate App / My TMS identity. Its exact stored repository authority is:

```text
TEST-Frontend/assets/branding/my-tms-app-logo-black.png
SHA-256: 0c0eb876096276f54363d651b1a1be3ae5943875d4b0a9e2e35fe5c878a473b2
Source dimensions: 1254 x 1254 pixels
```

When the Candidate App is built and rolled out, this artwork is to be used for the My TMS app thumbnail/installed-app identity and on the Candidate App loading/login surface. It must be copied or referenced from this exact authority without changing its identity or proportions.

## AV-520 — My TMS is stored, not prematurely activated

The My TMS artwork is deliberately stored now but must not be rendered by Office CloudTMS and must not be wired into the unfinished Candidate App yet. Candidate App favicon/PWA manifest icons, splash/loading presentation and login placement are later implementation work and require the normal Candidate App review and rollout gates.

## AV-521 — Responsive sizing and accessibility

Both brands must use responsive CSS or correctly generated icon derivatives appropriate to their target surface. Source images must not be stretched. The Office header retains an accessible `CloudTMS` name even though the visible text treatment is replaced by the image. The login watermark is decorative and must not enter the accessibility tree or intercept input.

## AV-522 — Branding no-change boundary

This Office implementation changes only frontend static branding assets, header/login markup and styling, branding tests and this decisions authority. It does not change Office login/session behavior, form fields, authentication requests, navigation behavior, Candidate App feature state, backend contracts, database authority, Google integration, Banking Pay, finance, payments, providers or production.
