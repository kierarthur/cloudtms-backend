"""Verify actual Health source assembly. Unpublished owner joins never emit SQL."""
from pathlib import Path
import hashlib
import json
import re
import subprocess
import sys

HERE = Path("D:/Development/GitHub/.codex-worktrees/healthroster-v2-money-build-13102026/docs/healthroster-build/proposals/contract-preview-current-independent-peer-20261017/reference")
OWN = Path("D:/Development/GitHub/.codex-worktrees/healthroster-v2-money-build-13102026")
ROOT = Path("D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026")
POLICY = Path('D:/Development/Legacy-Work/C-tmp/healthroster-handover/HealthRoster_Builder_Pack_16102026/NHSP_AUTHORISATION_AUDIT_15102026_HANDOFF_V6')
sha = lambda value: hashlib.sha256(value).hexdigest()

def verify():
    for path in [OWN, ROOT, POLICY]:
        assert str(path.resolve()).replace('\\', '/').startswith('D:/Development/'), 'PHYSICAL_D_REQUIRED'
    pins = json.loads((HERE / 'SOURCE_PINS.json').read_bytes())
    for row in pins['files']:
        root = ROOT if row['workspace'] == 'integration' else POLICY
        raw = (root / row['path']).read_bytes()
        assert sha(raw) == row['sha256'] and len(raw) == row['bytes'], row['path']
        if row.get('commit'):
            git = subprocess.check_output(['git', '-c', 'gc.auto=0', '-c', 'maintenance.auto=false', 'show', row['commit'] + ':' + row['path']], cwd=root)
            assert raw == git, row['path']
    registry = json.loads((ROOT / 'docs/bpay-next/final-definer-closures.json').read_bytes())
    for row in pins['finalOwners']:
        actual = next(x for x in registry['chains'] if x['signature'] == row['signature'])
        assert all(actual[key] == value for key, value in row.items()), row['signature']
    for row in pins['completeDeclarations']:
        assert sha((HERE / row['output']).read_bytes()) == row['savedCompleteDeclarationSha256']
    derivation = json.loads((HERE / 'VALIDATOR_DERIVATION.json').read_bytes())
    original = (HERE / derivation['parent']['output']).read_text(encoding='utf-8')
    output = (HERE / 'health/00_public_assert.sqlinc').read_text(encoding='utf-8').split('\nrevoke all on function')[0] + '\n'
    assert sha(output.encode()) == derivation['completeOutputDeclarationSha256']
    for edit in reversed(derivation['finiteReplacements']):
        assert output.count(edit['after']) == edit['count']
        output = output.replace(edit['after'], edit['before'])
    assert output == original, 'COMPLETE_VALIDATOR_REVERSE_REQUIRED'
    blocks = json.loads((HERE / 'OWNER_BLOCKS.json').read_bytes())['bindings']
    texts = {file.relative_to(HERE).as_posix(): file.read_text(encoding='utf-8')
        for directory in ['health', 'templates'] for file in sorted((HERE / directory).iterdir())}
    for token, row in blocks.items():
        assert texts[row['file']].count('@@' + token + '@@') == 1
        assert row['status'] == 'UNPROVIDED' and row['publishedContract'] is None and row['publishedCallBlock'] is None
    joined = '\n'.join(texts.values())
    assert len(re.findall(r'create or replace function ', joined)) == 7
    assert not re.search(r'create(?: or replace)? function private\.bpay_next_|grant .*service_role|insert into public\.|update public\.|delete from public\.|create table|alter table', joined, re.I)
    for file in ['templates/10_preview_read.sql.tmpl', 'templates/30_current_result.sql.tmpl']:
        assert not re.search(r'\b(?:insert into|update|delete from|for update|for share|pg_advisory)\b', texts[file], re.I), file
    preview = texts['templates/20_preview.sql.tmpl']
    expected = ['request_assert_v1(p_args,\'PREVIEW\')', 'preview_replay_v1(p_args)', "source.sheet_scope::text='DAILY'", 'timesheet_correction_entry_subject_v1(',
        'insert into private.timesheet_contract_move_operations', 'on conflict(id) do nothing', 'preview_replay_v1(p_args)',
        'timesheet_contract_move_scope_v1(o.id,false)', 'timesheet_contract_move_review_members_v1(', '@@BANK_PREVIEW@@',
        'timesheet_contract_move_scope_v1(o.id,true)', 'invoice_contract_move_inspect_v1(physical,actor,true)',
        "'timesheet-contract-move-request:'", 'insert into private.timesheet_contract_move_requests',
        'insert into private.timesheet_contract_move_reviews', 'timesheet_contract_move_preview_read_v1(request,actor)', 'set receipt_json=result']
    at = -1
    for needle in expected:
        at = preview.index(needle, at + 1)
    assert not re.search(r'bank_(?:ready|financial_visible|rechecked)\s*:=\s*true', joined, re.I)
    assert 'to_regprocedure' not in joined and 'can_open_review' not in re.sub(r'--[^\n]*', '', joined)
    return {'pinnedCurrentFiles': len(pins['files']), 'completeHealthDeclarations': len(pins['completeDeclarations']),
        'actualFinalOwners': len(pins['finalOwners']), 'newHealthDefinitions': 7,
        'completeOriginalOwnersChanged': 0, 'unprovidedBankOwnerBlocks': len(blocks), 'runtimeSQLFilesEmitted': 0,
        'native': 'NOT_RUN', 'API': 'NOT_RUN', 'sourceAssembly': 'HELD_UNPROVIDED_GENUINE_OWNER_JOINS'}

if __name__ == '__main__':
    state = verify()
    if '--emit' in sys.argv:
        # Deliberately no successful emission path in this frozen source. A
        # future owner interface must be independently reviewed with a successor.
        print(json.dumps(state))
        sys.exit(2)
    print(json.dumps(state))
