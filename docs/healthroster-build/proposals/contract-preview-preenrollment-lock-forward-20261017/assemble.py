"""Held source assembly; no runtime emitter or proposed Banking callable ABI."""
from pathlib import Path
import json, hashlib, importlib.util, re, subprocess, sys
sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
BASE = HERE.parent / 'contract-preview-current-health-20261017'
ROOT = REPO.parent / 'healthroster-install2-14102026'
sha = lambda b: hashlib.sha256(b).hexdigest()

def text(path):
    return path.read_bytes().decode('utf8')

def verify():
    assert str(REPO.resolve()).replace('\\', '/').startswith('D:/Development/')
    manifest = json.loads((BASE/'MANIFEST.json').read_bytes())
    for row in manifest['files']:
        data = (BASE/row['path']).read_bytes()
        assert sha(data) == row['savedSha256'] and len(data) == row['savedBytes']
    spec = importlib.util.spec_from_file_location('original_contract_source_only', BASE/'assemble.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.verify()  # Immutable baseline/actual inputs only; not approval of old order.

    parent = text(BASE/'templates/20_preview.sql.tmpl')
    assert parent == text(HERE/'reference/20_preview.before.sql.tmpl')
    edits = json.loads((HERE/'TEMPLATE_PARENT_EDITS.json').read_bytes())
    preview = text(HERE/'templates/20_preview.sql.tmpl')
    assert sha(parent.encode()) == edits['inputSha256'] and sha(preview.encode()) == edits['outputSha256']
    reversed_preview = preview
    for hunk in reversed(edits['hunks']):
        assert reversed_preview.count(hunk['after']) == 1
        reversed_preview = reversed_preview.replace(hunk['after'], hunk['before'])
    assert reversed_preview == parent

    derivation = json.loads((HERE/'SCOPE_BLOCK_DERIVATION.json').read_bytes())
    native_scope = text(HERE/'reference/private__timesheet_contract_move_scope_v1.sqlinc')
    assert native_scope == text(BASE/'inputs/private__timesheet_contract_move_scope_v1.sqlinc')
    locked = text(HERE/'fragments/21_scope_locked_from_native_owner.plpgsql')
    assert sha(native_scope.encode()) == derivation['parentSha256']
    assert sha(locked.encode()) == derivation['outputSha256']
    reverse_scope = locked
    for hunk in reversed(derivation['finiteReplacements']):
        at = hunk['afterOffset']
        assert reverse_scope[at:at+len(hunk['after'])] == hunk['after']
        reverse_scope = reverse_scope[:at] + hunk['before'] + reverse_scope[at+len(hunk['after']):]
    assert reverse_scope == native_scope, 'COMPLETE_NATIVE_SCOPE_REVERSE_REQUIRED'
    assert 'p_lock constant boolean:=true' in locked
    assert not re.search(r'\b(?:insert\s+into|update\s+(?:public|private)|delete\s+from)\b', locked, re.I)

    blocks = json.loads((HERE/'OWNER_BLOCKS.json').read_bytes())['bindings']
    original_blocks = json.loads((BASE/'OWNER_BLOCKS.json').read_bytes())['bindings']
    assert set(blocks) == set(original_blocks) and len(blocks) == 4
    for key, block in blocks.items():
        assert block['status'] == 'UNPROVIDED' and block['publishedContract'] is None and block['publishedCallBlock'] is None
        if key != 'BANK_PREVIEW':
            assert block == original_blocks[key]
    splice = text(HERE/'fragments/21_locked_preview_enrollment.plpgsql')
    requirement = blocks['BANK_PREVIEW']['fixedHealthSourceSplice']
    assert requirement['occurrences'] == 1 and sha(splice.encode()) == requirement['sha256']
    assert requirement['position'] == 'AFTER_GENUINE_NATIVE_LOCK_PREFIX_BEFORE_GENUINE_REVIEWED_CUT_SUFFIX'
    assert requirement['noFifthCallableOrResultABI'] is True
    assert splice.count(locked) == 1
    assert splice.index(locked) < splice.index('if o.id is distinct') < splice.index('insert into private.timesheet_contract_move_operations')
    assert splice.index('insert into private.timesheet_contract_move_operations') < splice.index('timesheet_contract_move_review_members_v1(')
    assert 'timesheet_contract_move_scope_v1(o.id,false)' not in splice
    before_bank = preview.split('@@BANK_PREVIEW@@')[0]
    assert preview.count('@@BANK_PREVIEW@@') == 1
    assert not re.search(r'\b(?:insert\s+into|update\s+(?:public|private)|delete\s+from|for\s+update|for\s+share|pg_advisory)\b', before_bank, re.I)
    assert 'jsonb_populate_record(null::private.timesheet_contract_move_operations' in before_bank
    assert preview.index('preview_replay_v1(p_args)') < preview.index('@@BANK_PREVIEW@@')
    assert preview.index('@@BANK_PREVIEW@@') < preview.index('timesheet_contract_move_scope_v1(o.id,true)') < preview.index("'timesheet-contract-move-request:'")

    finding = json.loads((HERE/'FINDING_SOURCE_PIN.json').read_bytes())
    family = text(HERE/'reference/private__timesheet_contract_move_family_lock_v1.sqlinc')
    assert sha(family.encode()) == finding['familyLockSha256'] == finding['familyLock']['sha256']
    assert family.strip() in text(ROOT/finding['familyLock']['physical'])
    assert 'perform 1 from public.candidates c where c.id=candidate for update;' in family
    registry = json.loads((ROOT/'docs/bpay-next/final-definer-closures.json').read_bytes())
    assert next(row for row in registry['chains'] if row['signature'] == finding['familyLock']['registry']['signature']) == finding['familyLock']['registry']
    return dict(source='HEALTH_F1_FK_FIRST_CHOREOGRAPHY_REMOVED', original43FilesUnchanged=True,
                fixedHealthScope='COMPLETE_ACTUAL_OWNER_LOCKED_TRUE', fixedHealthEnrollment='AFTER_NATIVE_PREFIX_AND_COMPLETE_HEALTH_SCOPE',
                existingOwnerJoins=4, unprovidedOwnerJoins=4, newBankCallableABI=0,
                originalNativeOwnersChanged=0, runtimeSQLFilesEmitted=0,
                financialPrefixClosure='NOT_PROVIDED', native='NOT_RUN', races='NOT_RUN')

if __name__ == '__main__':
    state = verify()
    print(json.dumps(state))
    if '--emit' in sys.argv:
        sys.exit(2)  # A genuine reviewed owner successor remains necessary.
