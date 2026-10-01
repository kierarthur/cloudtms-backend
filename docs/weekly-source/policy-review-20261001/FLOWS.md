# Policy-derived journey flows

Read with README rules R01–R12 and the matching numbered screens in journeys.policy.json. These are proposed presentation flows over retained business authority, not evidence of runtime acceptance.

## NHSP — R02/R03/R04/R07/R08/R09

```mermaid
flowchart TD
 A[Imports: previously released export] --> B[Automatic checks and eligible contact]
 B --> Q[Queries: Office issues and hours questions]
 Q --> X[If source wrong: query booking inside NHSP]
 X --> A
 A --> C[Imports: actual final backing report]
 C --> D{Finalisation blockers?}
 D -->|Yes| E[Finalise: Blocked; resolve in Queries]
 E --> C
 D -->|No; cutoff valid| F[Review and confirm finalisation]
 F --> H[History: completed backing report]
 H --> P[Existing invoice and pay processes]
 Q -. Unresolved questions remain independently .-> Q
```

## Import-authoritative HealthRoster — R02/R03/R04/R07/R09

```mermaid
flowchart TD
 A[Imports: client file; Check hours] --> B[Queries: checks and automatic eligible contact]
 B --> C[Correct source or Office details; recheck]
 A --> D[Explicitly Prepare for finalisation]
 C --> D
 D --> E{Valid finalised rows; no genuine blockers?}
 E -->|No| B
 E -->|Yes| F[Review included rows, exclusions and reversals]
 F --> G[Confirm; early finalisation permitted]
 G --> H[History: completed report]
 H --> I[Existing invoice and pay processes]
```

Recognised non-finalised rows require explicit exclusion acknowledgement in a mixed report; invalid rows claiming finalisation cannot be bypassed. Unanswered hours questions alone are not genuine finalisation blockers. Protected pay remains separate from invoicing.

## Configured Magnit import-authoritative profile — R01/R02/R03/R07/R09/R12

```mermaid
flowchart TD
 A[Imports: client file; Check hours] --> B[Queries: resolve identity, contract, rates and hours]
 A --> C[Explicitly Prepare for finalisation]
 B --> C
 C --> D{Profile, coverage, cutoff and blockers pass?}
 D -->|No| E[Correct issues or wait for applicable cutoff]
 E --> D
 D -->|Yes| F[Review and confirm finalisation]
 F --> G[History: completed report]
 G --> H[Existing invoice and pay processes]
```

A brand name is not a source profile or early-finalisation grant. The depicted summary profile uses supplied total hours and booking evidence; no break interval is invented. Checking remains selectable after cutoff.

## Signed-Timesheet-authoritative Weekly roster — R01/R06/R10/R11

```mermaid
flowchart TD
 A[Imports: roster for selected client and coverage] --> B{Worker and manager signatures complete?}
 B -->|No| C[Existing Timesheet completion journey]
 C --> B
 B -->|Yes| D{Every shift matches one roster row?}
 D -->|No| E[Checks: manager correction or Office matching]
 E --> F[Corrected roster; re-import]
 F --> D
 D -->|Yes| G[Write references; clear resolved checks]
 G --> H{Eligible auto-authorisation enabled?}
 H -->|Yes| I[Existing whole-Timesheet auto-authorisation]
 H -->|No| J[Ready: existing Office authorisation]
 I --> K[Timesheets: normal pay and invoice journey]
 J --> K
```

No source finalisation or finalised-report History entry is created. Reference-required-before-pay, if enabled, remains an independent pay gate. Source mismatches never grant an Accept system hours action on this route. Ordinary Weekly without import and Daily retain existing journeys.
