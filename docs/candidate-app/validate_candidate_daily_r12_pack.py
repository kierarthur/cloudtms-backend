#!/usr/bin/env python3
"""Validate integrity, completeness, self-containment and disclosure safety of R12."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import subprocess
import tempfile
import zipfile
from pathlib import Path, PurePosixPath

from pypdf import PdfReader


LOCAL_PATH_PATTERNS = [
    re.compile(rb"[A-Za-z]:\\(?:Users|tmp|workspace)\\", re.I),
    re.compile(rb"OneDrive - Arthur Rai", re.I),
    re.compile(rb"sandbox:/mnt", re.I),
    re.compile(rb"/workspace/", re.I),
    re.compile(rb"\\\.codex\\", re.I),
    re.compile(rb"codex_outputs", re.I),
]

REQUIRED = {
    "00_HANDOVER.md",
    "01_INDEPENDENT_REVIEW_BRIEF.md",
    "02_CURRENT_STATE.md",
    "03_VERIFICATION_SUMMARY.md",
    "PROVENANCE.json",
    "MANIFEST.sha256",
    "MANIFEST.sizes",
    "CloudTMS_Candidate_App_Current_Decisions_20260817_Phase3_R12.pdf",
    "source/availability-api/Code.gs",
    "source/availability-api/CloudTMSCandidateBridge.gs",
    "source/availability-api/rollback/Code.gs",
    "source/master-rota/Code.gs",
    "source/master-rota/CloudTMSCandidateBridge.gs",
    "source/master-rota/rollback/Code.gs",
    "source/SCRIPT_PROPERTIES.md",
    "tests/candidate-daily-phase3-apps-script.test.js",
    "fixtures/candidate-daily-r5/canonicalization-v1-vectors.json",
    "package.json",
    "evidence/focused-candidate-daily.tap",
    "evidence/complete-backend.tap",
    "evidence/candidate-broker-dry-run.txt",
    "evidence/candidate-private-api-dry-run.txt",
    "evidence/r11-finding-closure.csv",
    "evidence/live-google-installation.md",
    "incoming_r11_review/CloudTMS_Candidate_App_Phase3_R11_Independent_Review_Artifacts_20260817.zip",
    "incoming_r11_review/CloudTMS_Candidate_App_Phase3_R11_Independent_Verification_20260817.md",
}

EXPECTED_HASHES = {
    "source/availability-api/Code.gs": "01a737620116b387776d1bcb24992abecb9cdde523f09a936c587b17a7b55309",
    "source/availability-api/CloudTMSCandidateBridge.gs": "2a44bce4dd72613178370c342e8dcfc45f13bdaedf16ef29b828b6a64c079bdf",
    "source/availability-api/rollback/Code.gs": "eacd187564ea9b0f00c1830f9240c6afcfe1a0d0611162c1bdf9b9fd6bbb3b3f",
    "source/master-rota/Code.gs": "e0d4ac48bffd4e9e79b1c0439cb952a298a2d11f90e3caf2a53df4b9d779d700",
    "source/master-rota/CloudTMSCandidateBridge.gs": "58e8da3948f2890b42abd802485776169ff500dedcf060d27b449a60597bcb2c",
    "source/master-rota/rollback/Code.gs": "c3ae9c480a97ad2771312f5f453adbe7049c07219f89624f75df543d319fa0a8",
    "CloudTMS_Candidate_App_Current_Decisions_20260817_Phase3_R12.pdf":
        "9dbebe2ccb5192c286cef7f7684d63eb5074080f7cb9675b1b1852836dc88052",
    "incoming_r11_review/CloudTMS_Candidate_App_Phase3_R11_Independent_Review_Artifacts_20260817.zip":
        "002ad39ca44354871a47ce89cc9d53426f63ee9c5644c87165e541b3302dada0",
    "incoming_r11_review/CloudTMS_Candidate_App_Phase3_R11_Independent_Verification_20260817.md":
        "15fc93ca055785361d1c2f515a30dc65fc2cc1f20d6b3ea74192fd3746c067b5",
}


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def parse_manifest(data: bytes) -> dict[str, str]:
    result = {}
    for line in data.decode("utf-8").splitlines():
        value, name = line.split("  ", 1)
        result[name] = value
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("archive")
    parser.add_argument("--node", default="node")
    args = parser.parse_args()

    errors: list[str] = []
    test_summary = "not run"
    with zipfile.ZipFile(args.archive, "r") as archive:
        names = archive.namelist()
        name_set = set(names)
        if len(names) != len(name_set):
            errors.append("duplicate archive entries")
        if any(name.lower().endswith((".png", ".jpg", ".jpeg", ".webp")) for name in names):
            errors.append("screenshots/images are forbidden in the compact R12 pack")
        for name in names:
            path = PurePosixPath(name)
            if path.is_absolute() or ".." in path.parts:
                errors.append(f"unsafe archive path: {name}")

        missing = sorted(REQUIRED - name_set)
        if missing:
            errors.append(f"missing required entries: {missing}")

        declared = parse_manifest(archive.read("MANIFEST.sha256"))
        declared_sizes = {
            name: int(size) for size, name in (
                line.split("  ", 1)
                for line in archive.read("MANIFEST.sizes").decode("utf-8").splitlines()
            )
        }
        payloads = name_set - {"MANIFEST.sha256", "MANIFEST.sizes"}
        if set(declared) != payloads:
            errors.append("SHA manifest declaration mismatch")
        if set(declared_sizes) != payloads:
            errors.append("size manifest declaration mismatch")

        secret_literal = re.compile(
            rb"CLOUDTMS_CANDIDATE_(?:GOOGLE_HMAC|SOURCE_HMAC)_SECRET\s*[:=]\s*['\"]?[A-Za-z0-9+/]{43}=",
            re.I,
        )
        for name in sorted(payloads):
            data = archive.read(name)
            if declared.get(name) != digest(data):
                errors.append(f"SHA mismatch: {name}")
            if declared_sizes.get(name) != len(data):
                errors.append(f"size mismatch: {name}")
            if name != "validate_candidate_daily_r12_pack.py" and name.endswith(
                (".md", ".json", ".yaml", ".yml", ".js", ".gs", ".py", ".csv", ".tap", ".txt", ".sha256")
            ):
                if any(pattern.search(data) for pattern in LOCAL_PATH_PATTERNS):
                    errors.append(f"machine-local path disclosure: {name}")
                if secret_literal.search(data):
                    errors.append(f"secret literal disclosure: {name}")

        for name, expected in EXPECTED_HASHES.items():
            if name in name_set and digest(archive.read(name)) != expected:
                errors.append(f"authority mismatch: {name}")

        for name in ("source/availability-api/Code.gs", "source/master-rota/Code.gs"):
            if name in name_set:
                text = archive.read(name).decode("utf-8")
                for placeholder in ("[REDACTED]", "<REDACTED>", "TODO: paste", "content omitted"):
                    if placeholder.lower() in text.lower():
                        errors.append(f"redaction/placeholder in copy-ready source: {name}")

        focused = archive.read("evidence/focused-candidate-daily.tap").decode("utf-8", "replace")
        if not all(item in focused for item in ("tests 54", "pass 54", "fail 0")):
            errors.append("focused TAP summary mismatch")
        complete = archive.read("evidence/complete-backend.tap").decode("utf-8", "replace")
        if not all(item in complete for item in ("tests 632", "pass 632", "fail 0")):
            errors.append("complete TAP summary mismatch")

        pdf_name = "CloudTMS_Candidate_App_Current_Decisions_20260817_Phase3_R12.pdf"
        reader = PdfReader(io.BytesIO(archive.read(pdf_name)))
        if len(reader.pages) != 109:
            errors.append(f"decisions PDF expected 109 pages, found {len(reader.pages)}")
        appended = " ".join((page.extract_text() or "") for page in reader.pages[105:])
        for required in (
            "95. Durable accepted-subset",
            "96. Closed recovery contract",
            "97. Installed TEST authority",
            "AV-271",
            "AV-280",
            "version 216",
            "version 102",
            "CLOUDTMS_CANDIDATE_BRIDGE_ENABLED=false",
            "Phase 7",
        ):
            if required not in appended:
                errors.append(f"decisions PDF missing: {required}")

        provenance = json.loads(archive.read("PROVENANCE.json"))
        if provenance.get("published") is not True:
            errors.append("provenance must record published source")
        if provenance.get("live_google_changed") is not True:
            errors.append("provenance must record live disabled Google installation")
        if provenance.get("feature_activated") is not False:
            errors.append("provenance must record feature disabled")
        if provenance.get("google", {}).get("bridge_enabled") is not False:
            errors.append("provenance bridge state mismatch")
        if provenance.get("safety", {}).get("signed_bridge_request") is not False:
            errors.append("provenance signed-route state mismatch")

        if not errors:
            with tempfile.TemporaryDirectory(prefix="candidate-r12-validate-") as temporary:
                archive.extractall(temporary)
                result = subprocess.run(
                    [args.node, "--test", "tests/candidate-daily-phase3-apps-script.test.js"],
                    cwd=temporary,
                    capture_output=True,
                    text=True,
                    timeout=120,
                    check=False,
                )
                combined = result.stdout + "\n" + result.stderr
                if result.returncode != 0 or not all(
                    item in combined for item in ("tests 18", "pass 18", "fail 0")
                ):
                    errors.append("extracted-root Phase 3 test did not pass 18/18")
                else:
                    test_summary = "18 passed, 0 failed"

    if errors:
        print(json.dumps({"ok": False, "errors": errors}, indent=2))
        return 1
    print(json.dumps({
        "ok": True,
        "archive_entries": len(names),
        "manifest_payloads": len(declared),
        "decisions_pages": 109,
        "local_path_disclosures": 0,
        "screenshots": 0,
        "extracted_phase3_tests": test_summary,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
