# Real TEST Candidate broker negative runtime proof

Date: 17 August 2026

Target: public Candidate TEST broker only.

The probe sent a structurally valid 14-day generation request to the real signed Google generation route using deliberately invalid synthetic signing authority. It contained no real Candidate identity, Credentially public ID, source HMAC, secret or Candidate UUID.

```text
HTTP status: 401
error_code: SYSTEM_AUTH_FAILED
retry_class: DO_NOT_RETRY
database mutation: no
```

This proves the deployed TEST broker route is reachable, accepts the expected path/body/header structure and fails closed at signing authority before identity resolution or database mutation. It does not prove a positive signed publication. A positive journey remains gated on independent R13 GO, Google deployment while disabled, controlled source-link bootstrap and separate enabled TEST authorization.
