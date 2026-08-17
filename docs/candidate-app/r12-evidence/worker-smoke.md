# Candidate Worker build and smoke evidence

Date: 17 August 2026

R12 changed no Candidate Worker source and performed no Worker source deployment.

```text
Candidate broker dry build: PASS
Private Candidate dry build: PASS
Public Candidate /healthz: HTTP 200, TEST environment
Public Candidate /readyz: HTTP 200
```

The private Candidate service is binding-owned; a guessed public hostname is not runtime authority and was not used as a failure signal.

No secret value was queried. Operator-provided Wrangler secret-name lists were used only to confirm ownership by name.
