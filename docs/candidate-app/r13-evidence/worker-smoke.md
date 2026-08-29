# Candidate Worker smoke

- Public Candidate `/healthz`: HTTP 200; body `{"ok":true,"service":"candidate-broker","environment":"TEST"}`.
- Public Candidate `/readyz`: HTTP 200; body `{"ok":true,"service":"candidate-broker"}`.
- R13 changes no Worker runtime source and performs no Worker deployment.
- No secret, authorization header, token or candidate payload was captured.
