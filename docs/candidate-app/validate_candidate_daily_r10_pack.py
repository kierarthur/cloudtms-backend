#!/usr/bin/env python3
"""Validate the compact Candidate Daily R10 handover archive."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import tempfile
import zipfile
from pathlib import Path

from pypdf import PdfReader


FORBIDDEN_TEXT = [
    "C:" + "\\Users\\",
    "C:" + "/Users/",
    "OneDrive" + " - Arthur Rai",
    "_codex_" + "candidate_daily",
    "/mnt/" + "data/",
    "sandbox" + ":/",
    "{" * 2,
    "}" * 2,
]

REQUIRED_FILES = {
    "00_HANDOVER.md",
    "01_INDEPENDENT_REVIEW_BRIEF.md",
    "02_CURRENT_STATE.md",
    "03_VERIFICATION_SUMMARY.md",
    "04_FINDING_CLOSURE_MATRIX.md",
    "CloudTMS_Candidate_App_Current_Decisions_20260817_Phase2_Phase1B_R10.pdf",
    "decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_R10_ROLLBACK_AUTHORITY.md",
    "source/supabase/repeatable/17082026_0015_candidate_daily_phase2_rpcs_v1.sql",
    "tests/17082026_0955_candidate_daily_authority_transition_runtime_verification.sql",
    "tests/candidate-daily-authority-transition-concurrency.integration.js",
    "incoming_audit/CloudTMS_Candidate_App_Phase2_Phase1B_R9_Independent_Verification_20260817.md",
    "baseline_r9/CloudTMS_Candidate_App_Phase2_Phase1B_R9_Handover_20260817.zip",
    "MANIFEST.sha256",
    "MANIFEST.sizes",
    "PROVENANCE.json",
}


def digest(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def parse_manifest(path: Path) -> dict[str, str]:
    result = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        value, name = line.split("  ", 1)
        result[name] = value
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    args = parser.parse_args()
    archive_path = args.archive.resolve()

    with tempfile.TemporaryDirectory(prefix="candidate-r10-validate-") as temporary:
        root = Path(temporary)
        with zipfile.ZipFile(archive_path, "r") as archive:
            names = set(archive.namelist())
            missing = REQUIRED_FILES - names
            if missing:
                raise RuntimeError(f"Missing required files: {sorted(missing)}")
            if any(name.lower().endswith((".png", ".jpg", ".jpeg", ".webp")) for name in names):
                raise RuntimeError("Screenshots/images are not permitted in the compact R10 pack")
            archive.extractall(root)

        declared_sha = parse_manifest(root / "MANIFEST.sha256")
        declared_sizes = {name: int(size) for name, size in (
            (line.split("  ", 1)[1], line.split("  ", 1)[0])
            for line in (root / "MANIFEST.sizes").read_text(encoding="utf-8").splitlines()
        )}
        actual_payloads = {
            path.relative_to(root).as_posix(): path
            for path in root.rglob("*")
            if path.is_file() and path.name not in {"MANIFEST.sha256", "MANIFEST.sizes"}
        }
        if set(declared_sha) != set(actual_payloads) or set(declared_sizes) != set(actual_payloads):
            raise RuntimeError("Manifest membership differs from payload membership")
        for name, path in actual_payloads.items():
            if digest(path) != declared_sha[name]:
                raise RuntimeError(f"SHA-256 mismatch: {name}")
            if path.stat().st_size != declared_sizes[name]:
                raise RuntimeError(f"Size mismatch: {name}")

        for path in actual_payloads.values():
            if path.suffix.lower() not in {".md", ".txt", ".json", ".csv", ".sql", ".js", ".py", ".yml", ".yaml"}:
                continue
            text = path.read_text(encoding="utf-8", errors="strict")
            # GitHub Actions expressions are executable workflow syntax, not
            # unresolved handover placeholders. Remove only their complete,
            # bounded form before applying the generic double-brace check.
            scan_text = re.sub(r"\$\{\{[\s\S]*?\}\}", "", text)
            found = [value for value in FORBIDDEN_TEXT if value in scan_text]
            if found:
                raise RuntimeError(f"Forbidden local/placeholder text in {path.relative_to(root)}: {found}")

        repeatable = (root / "source/supabase/repeatable/17082026_0015_candidate_daily_phase2_rpcs_v1.sql").read_text(encoding="utf-8")
        if not re.search(r"v_prior_mode<>v_new_mode\s+and\s+v_actual_disposition='NONE'[\s\S]*?CANDIDATE_DAILY_NOT_READY", repeatable, re.I):
            raise RuntimeError("R10 runtime guard missing")
        runtime_suite = (root / "tests/17082026_0955_candidate_daily_authority_transition_runtime_verification.sql").read_text(encoding="utf-8")
        for token in ["PENDING", "CLAIMED", "RETRY", "TERMINAL", "rollback_unresolved", "CANDIDATE_DAILY_NOT_READY"]:
            if token not in runtime_suite:
                raise RuntimeError(f"R10 direct test token missing: {token}")
        concurrency = (root / "tests/candidate-daily-authority-transition-concurrency.integration.js").read_text(encoding="utf-8")
        if "parallel first rollback attempts cannot cross unresolved projection work" not in concurrency:
            raise RuntimeError("R10 concurrent rollback proof missing")

        pdf_path = root / "CloudTMS_Candidate_App_Current_Decisions_20260817_Phase2_Phase1B_R10.pdf"
        pdf = PdfReader(str(pdf_path))
        if len(pdf.pages) != 98:
            raise RuntimeError(f"Expected 98 decision pages, found {len(pdf.pages)}")
        appended = re.sub(r"\s+", " ", "\n".join((page.extract_text() or "") for page in pdf.pages[93:]))
        for token in ["85. R9 finding", "88. Verification", "AV-245", "does not activate Candidate Daily"]:
            if token not in appended:
                raise RuntimeError(f"R10 PDF decision missing: {token}")

        provenance = json.loads((root / "PROVENANCE.json").read_text(encoding="utf-8"))
        for field in ["runtime_commit", "backend_head", "candidate_db_run", "safe_migration_run", "repeatable_sha256", "function_sha256"]:
            if not str(provenance.get(field, "")).strip():
                raise RuntimeError(f"Provenance field missing: {field}")

    print(json.dumps({
        "result": "R10_PACK_VALID",
        "archive": archive_path.name,
        "sha256": digest(archive_path),
        "size": archive_path.stat().st_size,
        "files": len(names),
        "manifest_payloads": len(declared_sha),
        "decision_pages": 98,
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
