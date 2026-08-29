#!/usr/bin/env python3
"""Validate the self-contained Candidate Daily Phase 1A R7 handover."""

from __future__ import annotations

import hashlib
import re
import sys
from pathlib import Path

from pypdf import PdfReader


EXPECTED_OPENAPI_SHA = "1e4362f363e02eda34405f1f7edacdf7db0da8aad2a018cf75a5cd0993f765fa"
EXPECTED_HMAC_FIXTURE_SHA = "3aabb105d8d6d97bc0b916985e6791c5e356916ec62d07ef7fa6c90d0b805d30"
EXPECTED_PDF_SHA = "eefdec06d06306508ae8c67d842559ed8ab622b434515d5b7cefe1e062343f3c"


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
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file() and path.name not in {"MANIFEST.sha256", "MANIFEST.sizes"}
    }
    if actual != set(hashes):
        raise AssertionError(
            f"top-level manifest inventory mismatch: expected={len(hashes)} actual={len(actual)}"
        )
    for name, expected in hashes.items():
        path = root / name
        if sha256(path) != expected:
            raise AssertionError(f"SHA-256 mismatch: {name}")
        if path.stat().st_size != sizes[name]:
            raise AssertionError(f"size mismatch: {name}")
    return len(hashes)


def validate_nested_r6(root: Path) -> int:
    base = root / "baseline_r6"
    hashes = parse_hash_manifest(base / "MANIFEST.sha256")
    sizes = parse_size_manifest(base / "MANIFEST.sizes")
    if set(hashes) != set(sizes):
        raise AssertionError("R6 manifest key mismatch")
    for name, expected in hashes.items():
        path = base / name
        if not path.is_file() or sha256(path) != expected or path.stat().st_size != sizes[name]:
            raise AssertionError(f"R6 payload mismatch: {name}")
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
            "02_R7_CURRENT_STATE.md",
            "03_R7_VERIFICATION_SUMMARY.md",
            "04_INCOMING_R6_INDEPENDENT_AUDIT.md",
            "CloudTMS_Candidate_App_Current_Decisions_20260816_Phase1A_R7.pdf",
            "contracts/CANDIDATE_API_OPENAPI_V1_MERGED_R5.yaml",
            "r7_documents/CANDIDATE_DAILY_PHASE1A_DECISION_COMPLIANCE_MATRIX.md",
            "r7_documents/CANDIDATE_DAILY_PHASE1A_IMPLEMENTATION_AUTHORITY.md",
            "r7_documents/GOOGLE_EVIDENCE_GATE_20260816.md",
            "source/broker/src/candidate-daily-contract-v1.js",
            "source/broker/src/candidate-daily-hmac-v1.js",
            "source/broker/src/candidate-daily-phase1a.js",
            "source/candidate-broker/src/candidate-broker.js",
            "source/candidate-broker/wrangler.jsonc",
            "tests/candidate-daily-phase1a-contract.test.js",
            "tests/fixtures/candidate-daily-r5/canonicalization-v1-vectors.json",
            "baseline_r6/00_HANDOVER.md",
            "baseline_r6/baseline_r5/02_DECISION_LEDGER.md",
        ],
    )
    payloads = validate_manifest(root)
    r6_payloads = validate_nested_r6(root)

    if sha256(root / "contracts/CANDIDATE_API_OPENAPI_V1_MERGED_R5.yaml") != EXPECTED_OPENAPI_SHA:
        raise AssertionError("merged OpenAPI SHA mismatch")
    if sha256(root / "tests/fixtures/candidate-daily-r5/canonicalization-v1-vectors.json") != EXPECTED_HMAC_FIXTURE_SHA:
        raise AssertionError("R7 HMAC fixture SHA mismatch")

    pdf = root / "CloudTMS_Candidate_App_Current_Decisions_20260816_Phase1A_R7.pdf"
    if sha256(pdf) != EXPECTED_PDF_SHA:
        raise AssertionError("Decisions PDF SHA mismatch")
    reader = PdfReader(str(pdf))
    if len(reader.pages) != 76:
        raise AssertionError(f"Decisions PDF page count {len(reader.pages)} != 76")
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

    matrix = (
        root / "r7_documents/CANDIDATE_DAILY_PHASE1A_DECISION_COMPLIANCE_MATRIX.md"
    ).read_text(encoding="utf-8")
    decisions = {int(value) for value in re.findall(r"\| AV-(\d{3}) \|", matrix)}
    if decisions != set(range(155, 193)):
        raise AssertionError("R7 decision range is not exactly AV-155 through AV-192")

    forbidden_names = ["unredacted", "playwright-test.env", "user.json", ".dev.vars"]
    for path in root.rglob("*"):
        if path.is_file() and path.name.lower() not in {"redaction_manifest.md"}:
            lowered = path.name.lower()
            if any(token in lowered for token in forbidden_names):
                raise AssertionError(f"forbidden packaged filename: {path.relative_to(root)}")

    print(
        "CANDIDATE_DAILY_R7_PACK_PASS"
        f"|payloads={payloads}|nested_r6_payloads={r6_payloads}"
        f"|pdf_pages={len(reader.pages)}|runtime_operations={len(operations)}"
        f"|r7_decisions={len(decisions)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
