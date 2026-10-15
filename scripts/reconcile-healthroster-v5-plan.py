"""Capture the reviewed V5 specification and reopen affected build gates.

Read-only against the supplied authority folder and all product source. Writes only
the additive build-plan directory; never edits the historical approved plan.
"""
import argparse
import csv
import hashlib
import json
import re
import subprocess
from collections import Counter
from pathlib import Path


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pack", type=Path, required=True)
    ap.add_argument("--workspace", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()
    pack, workspace, output = args.pack.resolve(), args.workspace.resolve(), args.output.resolve()
    repo = Path(__file__).resolve().parents[1]
    output.relative_to(repo / "docs" / "healthroster-build")
    if not (pack / "HEALTHROSTER_BUILDER_INSTRUCTIONS.md").is_file():
        raise SystemExit("Missing exact reviewed authority")
    evidence = json.loads((pack / "AUTHORITY_FILES.json").read_text(encoding="utf-8-sig"))
    for entry in evidence:
        data = Path(entry["Path"]).read_bytes()
        if len(data) != entry["Bytes"] or sha(data) != entry["SHA256"].lower():
            raise SystemExit("Authority changed: " + entry["Path"])
    # The snapshot is deliberate: rerunning may verify existing bytes, never
    # silently replace an earlier reviewed authority or current-source capture.
    def save(relative, data):
        target = output / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists() and target.read_bytes() != data:
            raise SystemExit("Refusing changed existing capture: " + str(target))
        target.write_bytes(data)

    authority_names = [
        "HEALTHROSTER_BUILDER_INSTRUCTIONS.md", "TASK_BRIEF.md", "INVARIANTS.md",
        "Health_CODE_AUDIT.md", "HEALTHROSTER_CHANGE_MAP.csv",
        "HEALTHROSTER_ACCEPTANCE_SPEC.md", "SOURCE_NATIVE_ACCEPTANCE_CONTRACT.md",
        "CONTACT_PAUSE_SPEC.md", "UI_POLICY_AND_COPY.md",
        "HISTORY_RECONCILIATION_CONTRACT.md", "BANKING_BUILDER_INSTRUCTIONS.md",
        "BANKING_CODE_AUDIT.md", "BANKING_CHANGE_MAP.csv", "FINANCE_ACCEPTANCE_SPEC.md",
        "AUTHORITY_FILES.json", "CONTENT_MANIFEST.json",
        "REVIEWS/SOL_FINAL_ACCEPTANCE_V5.md", "REVIEWS/ASTRA_FINAL_ACCEPTANCE_V5.md",
    ]
    authorities = []
    for name in authority_names:
        data = (pack / name).read_bytes()
        save("authority/" + name, data)
        authorities.append({"path": name, "source": str(pack / name), "bytes": len(data), "sha256": sha(data)})
    rows = list(csv.DictReader((pack / "HEALTHROSTER_CHANGE_MAP.csv").read_text(encoding="utf-8-sig").splitlines()))
    bank_rows = list(csv.DictReader((pack / "BANKING_CHANGE_MAP.csv").read_text(encoding="utf-8-sig").splitlines()))
    if len(rows) != 326 or len(bank_rows) != 100 or len(evidence) != 99:
        raise SystemExit("Unexpected reviewed map/authority inventory")
    roots = {
        "backend": repo,
        "office": workspace / ".codex-worktrees/healthroster-v2-office-13102026",
        "mytms": workspace / ".codex-worktrees/healthroster-v2-mytms-13102026",
    }
    commits = {key: subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=value, text=True).strip() for key, value in roots.items()}
    def resolve_source(row):
        value = row["physical_file"] or row["source_root"]
        if not value or value in ("NEW", "NEW_REQUIRED", "N/A", "NOT_YET_IMPLEMENTED"):
            return None
        value = value.replace("\\", "/")
        if re.match(r"^[A-Za-z]:/", value):
            target = Path(value)
        else:
            for prefix in ("MyTMS::", "Office::", "Backend::"):
                if value.startswith(prefix):
                    value = value[len(prefix):].split("::", 1)[0]
            if value.startswith(("js/", "css/", "html/")) or value == "index.html":
                target = roots["office"] / value
            elif value.startswith(("apps/", "packages/", "google-apps-script/")):
                target = roots["mytms"] / value
            else:
                target = repo / value
        return target if target.is_file() else None
    trace = []
    for n, row in enumerate(rows, 1):
        source = resolve_source(row)
        trace.append({
            "map_row": n, "authority": row,
            "current_saved_source": str(source) if source else None,
            "current_saved_sha256": sha(source.read_bytes()) if source else None,
            "current_source_resolution": "SAVED_FILE_CAPTURED_NOT_FINAL_OWNER_PROOF" if source else "REQUIRES_EXACT_OWNER_RESOLUTION_OR_NEW_OBJECT",
            "implementation_status": "V5_REQUIRED_NOT_RUN",
            "implemented_root_signature": None, "implemented_schema_api_acl": None,
            "source_test_receipts": [], "native_test_receipts": [],
            "producer_consumer_acknowledgements": [], "remaining_gates": ["EXACT_CURRENT_OWNER", "IMPLEMENTATION", "SOURCE_TEST", "NATIVE_API_UI_AS_APPLICABLE"],
        })
    # All new map entries, including DTOs and external APIs, remain distinct.
    # Do not infer that every NEW_REQUIRED row is a table or that the map alone
    # includes every companion schema object.
    interfaces = [{"object": r["object"], "work_package": r["work_package"], "classification": r["classification"],
                   "kind": "REQUIRES_EXACT_CONTRACT_CLASSIFICATION", "authority": "HEALTHROSTER_CHANGE_MAP.csv row " + str(i + 1),
                   "owner": "BANKING_DEPENDENCY" if r["object"].startswith("private.bpay_next_") else "HEALTH_IMPLEMENTATION",
                   "ddl_status": "REQUIRED_NOT_IMPLEMENTED_BY_THIS_REGISTER", "native_status": "REQUIRED_NOT_RUN"}
                  for i, r in enumerate(rows) if r["classification"].startswith("NEW")]
    companions = [
        ("public.weekly_contact_pause_scopes", "TABLE", "CONTACT_PAUSE_SPEC.md:17"),
        ("public.weekly_contact_pause_commands", "TABLE", "CONTACT_PAUSE_SPEC.md:18"),
        ("public.weekly_contact_pause_events", "TABLE", "CONTACT_PAUSE_SPEC.md:19"),
        ("public.weekly_contact_delivery_memberships", "TABLE", "CONTACT_PAUSE_SPEC.md:21"),
        ("public.weekly_source_contact_pause_get_v1(jsonb)", "SERVICE_RPC", "CONTACT_PAUSE_SPEC.md:35"),
        ("public.weekly_source_contact_pause_atomic_v1(jsonb)", "SERVICE_RPC", "CONTACT_PAUSE_SPEC.md:36"),
        ("public.weekly_source_contact_resume_atomic_v1(jsonb)", "SERVICE_RPC", "CONTACT_PAUSE_SPEC.md:37"),
        ("public.weekly_source_contact_preview_v1(jsonb)", "SERVICE_RPC", "CONTACT_PAUSE_SPEC.md:38"),
        ("private.weekly_contact_scope_guard_v1(uuid,uuid,text,timestamptz)", "PRIVATE_HELPER", "CONTACT_PAUSE_SPEC.md:42"),
        ("weekly_source_global_settings timing columns and exact request journal", "EXISTING_OWNER_EXTENSION", "CONTACT_PAUSE_SPEC.md:104-112"),
        ("public.weekly_source_push_calendar_decision_v1(jsonb)", "SERVICE_CALENDAR_ADAPTER", "CONTACT_PAUSE_SPEC.md:118"),
        ("SOURCE_NATIVE_COMPOSED_RESULT_V1 / WEEKLY_PROTECTED_LOCAL_RESULT_V1 / SOURCE_NATIVE_LOCAL_RECEIPT_ROW_V1", "DISTINCT_HASH_DOMAINS", "SOURCE_NATIVE_ACCEPTANCE_CONTRACT.md:17-19"),
    ]
    for name, kind, authority in companions:
        interfaces.append({"object": name, "work_package": "HRV5-RC/QT" if "contact" in name or "settings" in name or "calendar" in name else "HRV5-NP",
                           "classification": "COMPANION_REQUIRED", "kind": kind, "authority": authority,
                           "owner": "HEALTH_IMPLEMENTATION", "ddl_status": "REQUIRED_NOT_IMPLEMENTED_BY_THIS_REGISTER", "native_status": "REQUIRED_NOT_RUN"})
    header = {"version": "HEALTHROSTER_V5_RECONCILIATION_V1", "authority_folder": str(pack),
              "content_manifest_sha256": sha((pack / "CONTENT_MANIFEST.json").read_bytes()),
              "reviewed_content_set_sha256": "74b8ab9d8c302be9152334d0191b6b5b0dd7dc1b5c067dfa7ff701cfc1f40827",
              "capture_commits": commits, "authority_files_verified": len(evidence),
              "health_map_rows": len(rows), "banking_map_rows": len(bank_rows),
              "health_classification_counts": dict(Counter(r["classification"] for r in rows)),
              "historical_status_preserved": True, "v5_native_acceptance": "REQUIRED_NOT_RUN"}
    for filename, body in [("AUTHORITY_CAPTURE.json", {**header, "files": authorities}),
                           ("CHANGE_TRACEABILITY.json", {**header, "rows": trace}),
                           ("SCHEMA_INTERFACE_REGISTER.json", {**header, "objects": interfaces})]:
        save(filename, (json.dumps(body, indent=2, ensure_ascii=False) + "\n").encode())
    print(json.dumps({**header, "output": str(output), "authority_copies": len(authorities),
                      "resolved_current_map_files": sum(r["current_saved_source"] is not None for r in trace),
                      "new_or_extended_interface_entries": len(interfaces)}, indent=2))


if __name__ == "__main__":
    main()
