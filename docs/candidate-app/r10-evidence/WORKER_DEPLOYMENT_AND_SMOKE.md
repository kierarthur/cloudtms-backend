# Candidate Daily R10 Worker Deployment and Smoke Evidence

R10 changes database SQL only. It contains no Worker or frontend runtime change. The backend publication nevertheless produced the normal TEST Worker's repository-linked deployment, so the resulting TEST identity and smoke proof are recorded.

```text
Normal TEST Worker:       6ce0838a-862b-4abb-9fd1-d86e0202d5f4 / 100% traffic
Normal /healthz:          HTTP 200 / ok
Normal /readyz:           HTTP 200 / ready=true

Private Candidate Worker: 9d73bbff-5099-4f12-a58d-64cb9dbb4889 / 100% traffic
Private internet route:   intentionally absent

Public Candidate broker:  09ac826b-d7da-4932-b0ad-a5fe6e194779 / 100% traffic
Public /healthz:          HTTP 200 / candidate-broker / TEST
Public /readyz:           HTTP 200 / private service binding ready
```

No production Worker was inspected or deployed. No Candidate feature was enabled.
