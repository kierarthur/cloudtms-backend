#!/usr/bin/env python3
"""Validate integrity, completeness, self-containment and disclosure safety of R13."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import subprocess
import tempfile
import zipfile
from pathlib import PurePosixPath

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
    "CloudTMS_Candidate_App_Current_Decisions_20260817_Phase3_R13.pdf",
    "source/availability-api/Code.gs",
    "source/availability-api/CloudTMSCandidateBridge.gs",
    "source/availability-api/rollback/Code.gs",
    "source/master-rota/Code.gs",
    "source/master-rota/CloudTMSCandidateBridge.gs",
    "source/master-rota/rollback/Code.gs",
    "source/SCRIPT_PROPERTIES.md",
    "runtime/candidate-broker/src/candidate-broker.js",
    "runtime/shared/candidate-daily-contract-v1.js",
    "runtime/shared/candidate-daily-hmac-v1.js",
    "runtime/shared/candidate-daily-phase1b.js",
    "runtime/database/17082026_0015_candidate_daily_phase2_rpcs_v1.sql",
    "tests/candidate-daily-phase3-apps-script.test.js",
    "tests/candidate-daily-phase3-r13-master-recovery.test.js",
    "fixtures/candidate-daily-r5/canonicalization-v1-vectors.json",
    "package.json",
    "evidence/focused-candidate-daily.tap",
    "evidence/complete-backend.tap",
    "evidence/candidate-broker-dry-run.txt",
    "evidence/candidate-private-api-dry-run.txt",
    "evidence/broker-negative-runtime.md",
    "evidence/test-database-safety.md",
    "evidence/pdf-visual-qa.md",
    "incoming_r12_review/CloudTMS_Candidate_App_Phase3_R12_Independent_Review_Artifacts_20260817.zip",
    "incoming_r12_review/CloudTMS_Candidate_App_Phase3_R12_Independent_Verification_20260817.md",
    "accepted_r12/CloudTMS_Candidate_App_Phase3_R12_Handover_20260817.zip",
}

EXPECTED_HASHES = {
    "source/availability-api/Code.gs": "01a737620116b387776d1bcb24992abecb9cdde523f09a936c587b17a7b55309",
    "source/availability-api/CloudTMSCandidateBridge.gs": "2a44bce4dd72613178370c342e8dcfc45f13bdaedf16ef29b828b6a64c079bdf",
    "source/availability-api/rollback/Code.gs": "eacd187564ea9b0f00c1830f9240c6afcfe1a0d0611162c1bdf9b9fd6bbb3b3f",
    "source/master-rota/Code.gs": "e0d4ac48bffd4e9e79b1c0439cb952a298a2d11f90e3caf2a53df4b9d779d700",
    "source/master-rota/CloudTMSCandidateBridge.gs": "141538b2c7d3b9719963484d057fafbfe733fd150712a50a5b9fb673fdc3783f",
    "source/master-rota/rollback/Code.gs": "c3ae9c480a97ad2771312f5f453adbe7049c07219f89624f75df543d319fa0a8",
    "CloudTMS_Candidate_App_Current_Decisions_20260817_Phase3_R13.pdf":
        "3a0e4066f2ccd11da3eb7afb31311dead06b05fef9ab9d6deefeccd6407eec38",
    "incoming_r12_review/CloudTMS_Candidate_App_Phase3_R12_Independent_Review_Artifacts_20260817.zip":
        "3e55281baf2eaf387f601850dfe91e0ff262d60cb861a421defa1d9f68680792",
    "incoming_r12_review/CloudTMS_Candidate_App_Phase3_R12_Independent_Verification_20260817.md":
        "14c8d4dd21ce1aff05cd00851119286b533df319149a96a53e0fc602523e45d6",
    "accepted_r12/CloudTMS_Candidate_App_Phase3_R12_Handover_20260817.zip":
        "9d0f511b6bc2e348fd1ab04cb1357ce905905f11183a821987164281ce1e85f5",
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
            errors.append("screenshots/images are forbidden in the compact R13 pack")
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
        textual = (".md", ".json", ".jsonc", ".yaml", ".yml", ".js", ".gs", ".py", ".csv", ".tap", ".txt", ".sql", ".sha256")
        for name in sorted(payloads):
            data = archive.read(name)
            if declared.get(name) != digest(data):
                errors.append(f"SHA mismatch: {name}")
            if declared_sizes.get(name) != len(data):
                errors.append(f"size mismatch: {name}")
            if name != "validate_candidate_daily_r13_pack.py" and name.endswith(textual):
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
        if not all(item in focused for item in ("tests 73", "pass 73", "fail 0")):
            errors.append("focused TAP summary mismatch")
        complete = archive.read("evidence/complete-backend.tap").decode("utf-8", "replace")
        if not all(item in complete for item in ("tests 651", "pass 651", "fail 0")):
            errors.append("complete TAP summary mismatch")

        pdf_name = "CloudTMS_Candidate_App_Current_Decisions_20260817_Phase3_R13.pdf"
        reader = PdfReader(io.BytesIO(archive.read(pdf_name)))
        if len(reader.pages) != 113:
            errors.append(f"decisions PDF expected 113 pages, found {len(reader.pages)}")
        appended = " ".join((page.extract_text() or "") for page in reader.pages[109:])
        for required in (
            "98. Quota-safe immutable Master",
            "99. Exact recovery",
            "100. Verification",
            "AV-281",
            "AV-297",
            "new-app-only candidate",
            "admin-entered global Candidate key",
            "version 102",
            "19 passed, 0 failed",
            "Phase 7",
        ):
            if required not in appended:
                errors.append(f"decisions PDF missing: {required}")

        provenance = json.loads(archive.read("PROVENANCE.json"))
        if provenance.get("published") is not True:
            errors.append("provenance must record published source")
        if provenance.get("feature_activated") is not False:
            errors.append("provenance must record feature disabled")
        if provenance.get("google", {}).get("master_r13_deployed") is not False:
            errors.append("provenance must record undeployed R13 Google Head")
        if provenance.get("google", {}).get("bridge_enabled") is not False:
            errors.append("provenance bridge state mismatch")
        identity = provenance.get("identity_readiness", {})
        if identity.get("existing_global_candidate_key_present") is not True:
            errors.append("provenance existing global key qualification missing")
        if identity.get("global_candidate_key_is_daily_source_link") is not False:
            errors.append("provenance must keep global key/source link distinct")
        if identity.get("new_app_only_candidate_requires_legacy_browser") is not False:
            errors.append("provenance app-only onboarding boundary mismatch")
        if identity.get("source_link_bootstrap_creates_candidate") is not False:
            errors.append("provenance duplicate-Candidate boundary mismatch")
        if provenance.get("safety", {}).get("secrets_printed_or_packaged") is not False:
            errors.append("provenance secret safety mismatch")

        if not errors:
            with tempfile.TemporaryDirectory(prefix="candidate-r13-validate-") as temporary:
                archive.extractall(temporary)
                result = subprocess.run(
                    [
                        args.node,
                        "--test",
                        "tests/candidate-daily-phase3-apps-script.test.js",
                        "tests/candidate-daily-phase3-r13-master-recovery.test.js",
                    ],
                    cwd=temporary,
                    capture_output=True,
                    text=True,
                    timeout=180,
                    check=False,
                )
                combined = result.stdout + "\n" + result.stderr
                if result.returncode != 0 or not all(
                    item in combined for item in ("tests 37", "pass 37", "fail 0")
                ):
                    errors.append("extracted-root Phase 3/R13 tests did not pass 37/37")
                else:
                    test_summary = "37 passed, 0 failed"

    if errors:
        print(json.dumps({"ok": False, "errors": errors}, indent=2))
        return 1
    print(json.dumps({
        "ok": True,
        "archive_entries": len(names),
        "manifest_payloads": len(declared),
        "decisions_pages": 113,
        "local_path_disclosures": 0,
        "screenshots": 0,
        "extracted_phase3_r13_tests": test_summary,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
