#!/usr/bin/env python3
"""Harmless TEST broker reachability and fail-closed HMAC probe for R13."""

from __future__ import annotations

import hashlib
import json
import time
import urllib.error
import urllib.request
import uuid


URL = "https://test-cloudtms-candidate-broker.kier-88a.workers.dev/candidate-system/v1/google-availability/rota-generations"


def main() -> int:
    start = "2026-08-17"
    days = []
    for day in range(17, 31):
        days.append({
            "date": f"2026-08-{day:02d}",
            "booked": False,
            "system_blocked": False,
            "booking_id": None,
            "shift_starts_at": None,
            "shift_ends_at": None,
            "shift_info": None,
            "hospital": None,
            "ward": None,
            "job_title": None,
            "source_row_hash": "0" * 64,
        })
    body = {
        "batch_request_id": str(uuid.uuid4()),
        "items": [{
            "candidate_source_hmac": "0" * 64,
            "source_event_id": "r13-negative-runtime-proof",
            "source_revision": "r13-negative-runtime-proof",
            "source_hash": "0" * 64,
            "window_start": start,
            "days": days,
            "source_event_time": "2026-08-17T00:00:00.000Z",
            "item_key": "r13-negative-runtime-proof",
        }],
    }
    encoded = json.dumps(body, separators=(",", ":")).encode("utf-8")
    headers = {
        "user-agent": "CloudTMS-R13-evidence/1",
        "content-type": "application/json",
        "idempotency-key": str(uuid.uuid4()),
        "x-correlation-id": "01J5A000000000000000000000",
        "x-cloudtms-key-id": "r13-deliberately-invalid-key",
        "x-cloudtms-signature-version": "v1",
        "x-cloudtms-timestamp": str(int(time.time())),
        "x-cloudtms-nonce": "01J5A000000000000000000001",
        "x-cloudtms-content-sha256": hashlib.sha256(encoded).hexdigest(),
        "x-cloudtms-signature": "0" * 64,
    }
    request = urllib.request.Request(URL, data=encoded, headers=headers, method="POST")
    try:
        urllib.request.urlopen(request, timeout=30)
        raise RuntimeError("Deliberately invalid signing authority was unexpectedly accepted")
    except urllib.error.HTTPError as error:
        raw = error.read()
        try:
            response = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"Non-JSON broker response: status={error.code}, "
                f"content_type={error.headers.get('content-type', '')}, bytes={len(raw)}"
            ) from exc
        expected = (
            error.code == 401
            and response.get("ok") is False
            and response.get("error_code") == "SYSTEM_AUTH_FAILED"
            and response.get("retry_class") == "DO_NOT_RETRY"
        )
        if not expected:
            raise RuntimeError(
                f"Unexpected broker response: status={error.code}, code={response.get('error_code')}, "
                f"retry={response.get('retry_class')}"
            )
        print("R13_BROKER_NEGATIVE_RUNTIME_PASS|status=401|error_code=SYSTEM_AUTH_FAILED|retry_class=DO_NOT_RETRY")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
