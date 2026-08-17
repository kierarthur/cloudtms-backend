# R12 live Google installation evidence

Date: 17 August 2026

## Availability API

- spreadsheet: Availability API;
- active web-app version: 216;
- retained rollback version: 215;
- corrected `Code.gs` installed;
- corrected `CloudTMSCandidateBridge.gs` installed;
- bridge property remains false;
- no trigger, manifest or OAuth-scope change;
- no Google Sheet business-data mutation during installation verification.

## NEW MASTER ROTA

- spreadsheet: NEW MASTER ROTA System;
- active web-app version: 102;
- retained rollback version: 101;
- complete effective `Code.gs` installed, including the existing operator setup utility;
- `CloudTMSCandidateBridge.gs` installed;
- bridge property remains false;
- no stray `Untitled.gs` file remains;
- no trigger, manifest or OAuth-scope change;
- no Google Sheet business-data mutation during installation verification.

## Safety

Only property names and non-secret configuration facts were inspected. No signing or source-identity secret value was read, logged or packaged. No signed Google-to-Worker request was sent.
