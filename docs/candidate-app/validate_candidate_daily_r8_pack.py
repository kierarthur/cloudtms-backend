#!/usr/bin/env python3
"""Validate the self-contained Candidate Daily Phase 2/Phase 1B R8 handover."""

from __future__ import annotations

import hashlib
import re
import sys
from pathlib import Path

from pypdf import PdfReader


EXPECTED_OPENAPI_SHA = "d3a9b31be40a9ae9cc277e9517865a51ba7e2b662556b6ba4be4a248d1bdaf93"
EXPECTED_PDF_SHA = "77870b5004d4582373376ffae83ef3de66bc4140c9df77997d49f9eb70ebde90"
EXPECTED_SCHEMA_SHA = "cdd8446c0a390b89ed324ffa89f1fda11e85f3ffbccfdf1dcf54bbc4e764b226"
EXPECTED_RPC_SHA = "d9297dd73058e71ad01fb96e9460077be2ffc2649acb1b0fadeee615302f668c"


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
        "00_HANDOVER.md", "01_INDEPENDENT_REVIEW_BRIEF.md", "02_CURRENT_STATE.md",
        "03_VERIFICATION_SUMMARY.md", "PROVENANCE.md",
        "CloudTMS_Candidate_App_Current_Decisions_20260817_Phase2_Phase1B_R8.pdf",
        "contracts/CANDIDATE_API_OPENAPI_V1_MERGED_R8.yaml",
        "decisions/02_DECISION_LEDGER_R5.md",
        "decisions/03_DECISION_COMPLIANCE_MATRIX_R5.md",
        "decisions/CANDIDATE_DAILY_PHASE1A_DECISION_COMPLIANCE_MATRIX.md",
        "decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_DECISION_COMPLIANCE_MATRIX.md",
        "source/supabase/migrations/17082026_0010_candidate_daily_phase2_authority_schema.sql",
        "source/supabase/repeatable/17082026_0015_candidate_daily_phase2_rpcs_v1.sql",
        "source/broker/src/candidate-daily-phase1b.js",
        "tests/17082026_0053_candidate_daily_phase2_runtime_verification.sql",
        "evidence/TEST_POSTINSTALL_SUMMARY.md",
    ])
    payloads = validate_manifest(root)

    openapi = root / "contracts/CANDIDATE_API_OPENAPI_V1_MERGED_R8.yaml"
    pdf = root / "CloudTMS_Candidate_App_Current_Decisions_20260817_Phase2_Phase1B_R8.pdf"
    schema = root / "source/supabase/migrations/17082026_0010_candidate_daily_phase2_authority_schema.sql"
    rpc = root / "source/supabase/repeatable/17082026_0015_candidate_daily_phase2_rpcs_v1.sql"
    expected = [(openapi, EXPECTED_OPENAPI_SHA), (pdf, EXPECTED_PDF_SHA), (schema, EXPECTED_SCHEMA_SHA), (rpc, EXPECTED_RPC_SHA)]
    for path, digest in expected:
        if sha256(path) != digest:
            raise AssertionError(f"fixed authority SHA mismatch: {path.relative_to(root)}")

    reader = PdfReader(str(pdf))
    if len(reader.pages) != 87 or any(not (p.extract_text() or "").strip() for p in reader.pages):
        raise AssertionError("Decisions PDF must have 87 nonblank-text pages")

    r5 = (root / "decisions/02_DECISION_LEDGER_R5.md").read_text(encoding="utf-8")
    phase1a = (root / "decisions/CANDIDATE_DAILY_PHASE1A_DECISION_COMPLIANCE_MATRIX.md").read_text(encoding="utf-8")
    r8 = (root / "decisions/CANDIDATE_DAILY_PHASE2_PHASE1B_DECISION_COMPLIANCE_MATRIX.md").read_text(encoding="utf-8")
    ids_r5 = {int(v) for v in re.findall(r"\| AV-(\d{3}) \|", r5)}
    ids_1a = {int(v) for v in re.findall(r"\| AV-(\d{3}) \|", phase1a)}
    ids_r8 = {int(v) for v in re.findall(r"\| AV-(\d{3}) \|", r8)}
    if ids_r5 != set(range(1, 155)) or ids_1a != set(range(155, 193)) or ids_r8 != set(range(193, 229)):
        raise AssertionError("Decision IDs are not exactly consecutive AV-001 through AV-228")

    sql = rpc.read_text(encoding="utf-8")
    public_functions = set(re.findall(r"create\s+or\s+replace\s+function\s+public\.(candidate_daily_[a-z0-9_]+)\s*\(", sql, re.I))
    if len(public_functions) != 13:
        raise AssertionError(f"Daily public RPC owner count {len(public_functions)} != 13")

    schema_text = schema.read_text(encoding="utf-8")
    daily_tables = set(re.findall(r"create\s+table\s+if\s+not\s+exists\s+(?:public|private)\.(candidate_daily_[a-z0-9_]+)", schema_text, re.I))
    if len(daily_tables) != 12:
        raise AssertionError(f"Daily table owner count {len(daily_tables)} != 12")

    forbidden = ["unredacted", "playwright-test.env", "user.json", ".dev.vars", "screenshot"]
    local_patterns = [r"C:\\Users\\", r"C:/Users/", r"/mnt/data/", r"sandbox:/"]
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        lowered = path.name.lower()
        if any(token in lowered for token in forbidden):
            raise AssertionError(f"forbidden packaged filename: {path.relative_to(root)}")
        if path.suffix.lower() in {".md", ".txt", ".json", ".yaml", ".yml", ".csv"}:
            text = path.read_text(encoding="utf-8", errors="replace")
            if any(re.search(pattern, text, re.I) for pattern in local_patterns):
                raise AssertionError(f"local-path reference in: {path.relative_to(root)}")

    print(f"CANDIDATE_DAILY_R8_PACK_PASS|payloads={payloads}|pdf_pages=87|decisions=228|tables=12|rpcs=13")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
