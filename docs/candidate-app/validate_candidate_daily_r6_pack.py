#!/usr/bin/env python3
"""Validate the self-contained Candidate Daily Phase 1A R6 handover."""

from __future__ import annotations

import hashlib
import re
import sys
from pathlib import Path

from pypdf import PdfReader


EXPECTED_OPENAPI_SHA = "1e4362f363e02eda34405f1f7edacdf7db0da8aad2a018cf75a5cd0993f765fa"
EXPECTED_HMAC_FIXTURE_SHA = "d82d943b44876466defcb38d324bf737829b1771e36a15a78b1ef6f93f1f0c22"
EXPECTED_PDF_SHA = "27f61425c8d320310729bffc6bd2c0909cabd5bb6771c854c5f4c2eab71e8ceb"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_hash_manifest(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if not line.strip():
            continue
        digest, name = re.split(r"\s{2,}", line.strip(), maxsplit=1)
        result[name.replace("\\", "/")] = digest.lower()
    return result


def parse_size_manifest(path: Path) -> dict[str, int]:
    result: dict[str, int] = {}
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if not line.strip():
            continue
        size, name = line.split("\t", 1)
        result[name.replace("\\", "/")] = int(size)
    return result


def validate_manifest(root: Path) -> int:
    hashes = parse_hash_manifest(root / "MANIFEST.sha256")
    sizes = parse_size_manifest(root / "MANIFEST.sizes")
    if set(hashes) != set(sizes):
        raise AssertionError("top-level manifest key mismatch")
    actual = {
        p.relative_to(root).as_posix()
        for p in root.rglob("*")
        if p.is_file() and p.name not in {"MANIFEST.sha256", "MANIFEST.sizes"}
    }
    if actual != set(hashes):
        raise AssertionError(f"top-level manifest inventory mismatch: expected={len(hashes)} actual={len(actual)}")
    for name, expected in hashes.items():
        path = root / name
        if sha256(path) != expected:
            raise AssertionError(f"SHA-256 mismatch: {name}")
        if path.stat().st_size != sizes[name]:
            raise AssertionError(f"size mismatch: {name}")
    return len(hashes)


def validate_nested_r5(root: Path) -> int:
    base = root / "baseline_r5"
    hashes = parse_hash_manifest(base / "MANIFEST.sha256")
    sizes = parse_size_manifest(base / "MANIFEST.sizes")
    if set(hashes) != set(sizes):
        raise AssertionError("R5 manifest key mismatch")
    for name, expected in hashes.items():
        path = base / name
        if not path.is_file() or sha256(path) != expected or path.stat().st_size != sizes[name]:
            raise AssertionError(f"R5 payload mismatch: {name}")
    return len(hashes)


def require(root: Path, names: list[str]) -> None:
    for name in names:
        if not (root / name).is_file():
            raise AssertionError(f"required file absent: {name}")


def main() -> int:
    root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    require(
        root,
        [
            "00_HANDOVER.md",
            "01_INDEPENDENT_REVIEW_BRIEF.md",
            "02_R6_CURRENT_STATE.md",
            "CloudTMS_Candidate_App_Current_Decisions_20260816_Phase1A_R6.pdf",
            "contracts/CANDIDATE_API_OPENAPI_V1_MERGED_R5.yaml",
            "r6_documents/CANDIDATE_DAILY_PHASE1A_DECISION_COMPLIANCE_MATRIX.md",
            "r6_documents/CANDIDATE_DAILY_PHASE1A_IMPLEMENTATION_AUTHORITY.md",
            "r6_documents/GOOGLE_EVIDENCE_GATE_20260816.md",
            "source/broker/src/candidate-daily-contract-v1.js",
            "source/broker/src/candidate-daily-hmac-v1.js",
            "source/broker/src/candidate-daily-phase1a.js",
            "tests/candidate-daily-phase1a-contract.test.js",
            "tests/fixtures/candidate-daily-r5/canonicalization-v1-vectors.json",
            "baseline_r5/02_DECISION_LEDGER.md",
            "baseline_r5/03_DECISION_COMPLIANCE_MATRIX.md",
        ],
    )
    payloads = validate_manifest(root)
    r5_payloads = validate_nested_r5(root)

    if sha256(root / "contracts/CANDIDATE_API_OPENAPI_V1_MERGED_R5.yaml") != EXPECTED_OPENAPI_SHA:
        raise AssertionError("merged OpenAPI SHA mismatch")
    if sha256(root / "tests/fixtures/candidate-daily-r5/canonicalization-v1-vectors.json") != EXPECTED_HMAC_FIXTURE_SHA:
        raise AssertionError("HMAC fixture SHA mismatch")

    pdf = root / "CloudTMS_Candidate_App_Current_Decisions_20260816_Phase1A_R6.pdf"
    if sha256(pdf) != EXPECTED_PDF_SHA:
        raise AssertionError("Decisions PDF SHA mismatch")
    reader = PdfReader(str(pdf))
    if len(reader.pages) != 70:
        raise AssertionError(f"Decisions PDF page count {len(reader.pages)} != 70")
    if any(not (page.extract_text() or "").strip() for page in reader.pages):
        raise AssertionError("Decisions PDF contains a blank-text page")

    source = (root / "source/broker/src/candidate-daily-contract-v1.js").read_text(encoding="utf-8")
    operations = set(
        re.findall(
            r"route\(\s*['\"][A-Z]+['\"]\s*,\s*['\"][^'\"]+['\"]\s*,\s*['\"]([^'\"]+)",
            source,
        )
    )
    if len(operations) != 24:
        raise AssertionError(f"runtime operation count {len(operations)} != 24")

    matrix = (root / "r6_documents/CANDIDATE_DAILY_PHASE1A_DECISION_COMPLIANCE_MATRIX.md").read_text(encoding="utf-8")
    decisions = {int(value) for value in re.findall(r"\| AV-(\d{3}) \|", matrix)}
    if decisions != set(range(155, 181)):
        raise AssertionError("R6 decision range is not exactly AV-155 through AV-180")

    forbidden_names = ["unredacted", "playwright-test.env", "user.json", ".dev.vars"]
    for path in root.rglob("*"):
        if path.is_file() and path.name.lower() not in {"redaction_manifest.md"}:
            lowered = path.name.lower()
            if any(token in lowered for token in forbidden_names):
                raise AssertionError(f"forbidden packaged filename: {path.relative_to(root)}")

    print(
        "CANDIDATE_DAILY_R6_PACK_PASS"
        f"|payloads={payloads}|nested_r5_payloads={r5_payloads}"
        f"|pdf_pages={len(reader.pages)}|runtime_operations={len(operations)}"
        f"|r6_decisions={len(decisions)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
