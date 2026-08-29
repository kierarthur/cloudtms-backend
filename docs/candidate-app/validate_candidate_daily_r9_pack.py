#!/usr/bin/env python3
"""Validate the self-contained Candidate Daily Phase 2/1B R9 handover."""

from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

from pypdf import PdfReader


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_hash_manifest(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if line.strip():
            digest, name = re.split(r"\s{2,}", line.strip(), maxsplit=1)
            result[name.replace("\\", "/")] = digest.lower()
    return result


def parse_size_manifest(path: Path) -> dict[str, int]:
    result: dict[str, int] = {}
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if line.strip():
            size, name = line.split("\t", 1)
            result[name.replace("\\", "/")] = int(size)
    return result


def validate_manifest(root: Path) -> int:
    hashes = parse_hash_manifest(root / "MANIFEST.sha256")
    sizes = parse_size_manifest(root / "MANIFEST.sizes")
    if set(hashes) != set(sizes):
        raise AssertionError("manifest key mismatch")
    actual = {
        p.relative_to(root).as_posix()
        for p in root.rglob("*")
        if p.is_file() and p.name not in {"MANIFEST.sha256", "MANIFEST.sizes"}
    }
    if actual != set(hashes):
        raise AssertionError(f"manifest inventory mismatch expected={len(hashes)} actual={len(actual)}")
    for name, expected in hashes.items():
        path = root / name
        if sha256(path) != expected or path.stat().st_size != sizes[name]:
            raise AssertionError(f"manifest payload mismatch: {name}")
    return len(hashes)


def require(root: Path, names: list[str]) -> None:
    for name in names:
        if not (root / name).is_file():
            raise AssertionError(f"required file absent: {name}")


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    require(root, [
        "00_HANDOVER.md",
        "01_INDEPENDENT_REVIEW_BRIEF.md",
        "02_CURRENT_STATE.md",
        "03_VERIFICATION_SUMMARY.md",
        "04_FINDING_CLOSURE_MATRIX.md",
        "PROVENANCE.json",
        "CloudTMS_Candidate_App_Current_Decisions_20260817_Phase2_Phase1B_R9.pdf",
        "baseline_r8/CloudTMS_Candidate_App_Phase2_Phase1B_R8_Handover_20260817.zip",
        "incoming_audit/CloudTMS_Candidate_App_Phase2_Phase1B_R8_Independent_Review_Artifacts_20260817.zip",
        "incoming_audit/INDEPENDENT_R8_REVIEW.md",
        "decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_DECISION_COMPLIANCE_MATRIX.md",
        "decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_IMPLEMENTATION_AUTHORITY.md",
        "decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_R9_CORRECTION_AUTHORITY.md",
        "source/supabase/repeatable/17082026_0015_candidate_daily_phase2_rpcs_v1.sql",
        "source/.github/workflows/candidate-db-runtime.yml",
        "tests/17082026_0955_candidate_daily_authority_transition_runtime_verification.sql",
        "tests/candidate-daily-authority-transition-concurrency.integration.js",
        "tests/candidate-daily-phase2-source-contract.test.js",
        "evidence/TEST_POSTINSTALL_SUMMARY.md",
        "evidence/WORKER_DEPLOYMENT_AND_SMOKE.md",
    ])
    payloads = validate_manifest(root)

    provenance = json.loads((root / "PROVENANCE.json").read_text(encoding="utf-8"))
    if provenance.get("package") != "CloudTMS Candidate Daily Phase 2/1B R9":
        raise AssertionError("unexpected provenance package identity")
    for entry in provenance.get("fixed_payloads", []):
        path = root / entry["path"]
        if not path.is_file() or sha256(path) != entry["sha256"]:
            raise AssertionError(f"fixed payload SHA mismatch: {entry['path']}")

    pdf = root / "CloudTMS_Candidate_App_Current_Decisions_20260817_Phase2_Phase1B_R9.pdf"
    reader = PdfReader(str(pdf))
    if len(reader.pages) != 93 or any(not (p.extract_text() or "").strip() for p in reader.pages):
        raise AssertionError("Decisions PDF must have 93 nonblank-text pages")

    base_pdf = root / "baseline_r8/CloudTMS_Candidate_App_Current_Decisions_20260817_Phase2_Phase1B_R8.pdf"
    if base_pdf.is_file():
        base = PdfReader(str(base_pdf))
        if len(base.pages) != 87:
            raise AssertionError("R8 baseline Decisions PDF must have 87 pages")
        for index in range(87):
            if (reader.pages[index].extract_text() or "") != (base.pages[index].extract_text() or ""):
                raise AssertionError(f"R8 Decisions PDF page {index + 1} changed")

    r5_ledger = (root / "baseline_r5/02_DECISION_LEDGER.md") if (root / "baseline_r5/02_DECISION_LEDGER.md").is_file() else None
    matrix = (root / "decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_DECISION_COMPLIANCE_MATRIX.md").read_text(encoding="utf-8")
    ids = {int(v) for v in re.findall(r"\| AV-(\d{3}) \|", matrix)}
    if not set(range(193, 245)).issubset(ids):
        raise AssertionError("R9 matrix does not contain every AV-193 through AV-244 decision")
    if r5_ledger:
        r5_ids = {int(v) for v in re.findall(r"\| AV-(\d{3}) \|", r5_ledger.read_text(encoding="utf-8"))}
        if r5_ids != set(range(1, 155)):
            raise AssertionError("R5 baseline decision IDs are incomplete")

    sql = (root / "source/supabase/repeatable/17082026_0015_candidate_daily_phase2_rpcs_v1.sql").read_text(encoding="utf-8")
    transition = re.search(
        r"create\s+or\s+replace\s+function\s+public\.candidate_daily_authority_transition_atomic_v1\([\s\S]*?\n\$function\$;",
        sql,
        re.I,
    )
    if not transition:
        raise AssertionError("corrected authority-transition owner missing")
    body = transition.group(0)
    required_patterns = [
        r"candidate_daily_authority_scopes[\s\S]*order by s\.candidate_id for update",
        r"expected_generation_id[\s\S]*expected_generation_version",
        r"expected_accepted_canonical_cursor[\s\S]*expected_effective_visible_cursor",
        r"candidate_daily_command_receipts[\s\S]*state='IN_PROGRESS'",
        r"candidate_daily_external_effect_receipts[\s\S]*state in \('IN_PROGRESS','UNKNOWN'\)",
        r"v_actual_disposition:=case[\s\S]*v_requested_disposition<>v_actual_disposition",
    ]
    for pattern in required_patterns:
        if not re.search(pattern, body, re.I):
            raise AssertionError(f"transition authority pattern missing: {pattern}")
    if re.search(r"insert into private\.candidate_daily_authority_scopes", body, re.I):
        raise AssertionError("transition owner must not create a missing scope")

    forbidden_names = ["unredacted", "playwright-test.env", "user.json", ".dev.vars", "screenshot"]
    local_patterns = [r"C:\\Users\\", r"C:/Users/", r"/mnt/data/", r"sandbox:/"]
    placeholder_patterns = [r"\{\{[A-Z0-9_]+\}\}", r"PENDING_PUBLICATION"]
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        lowered = path.name.lower()
        if any(token in lowered for token in forbidden_names):
            raise AssertionError(f"forbidden packaged filename: {path.relative_to(root)}")
        if path.suffix.lower() in {".md", ".txt", ".json", ".yaml", ".yml", ".csv"}:
            content = path.read_text(encoding="utf-8", errors="replace")
            if any(re.search(pattern, content, re.I) for pattern in local_patterns):
                raise AssertionError(f"local-path reference in: {path.relative_to(root)}")
            if any(re.search(pattern, content) for pattern in placeholder_patterns):
                raise AssertionError(f"unresolved publication placeholder in: {path.relative_to(root)}")

    print(f"CANDIDATE_DAILY_R9_PACK_PASS|payloads={payloads}|pdf_pages=93|decisions=244|tables=12|rpcs=13")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
