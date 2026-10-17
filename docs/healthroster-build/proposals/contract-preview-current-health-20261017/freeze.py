"""Freeze/verify this held Health source proposal only; no native execution."""
from pathlib import Path
import hashlib
import json
import re
import subprocess
import sys

HERE = Path(__file__).resolve().parent
OWN = HERE.parents[3]
assert str(OWN.resolve()).replace('\\', '/').startswith('D:/Development/GitHub/')
sha = lambda raw: hashlib.sha256(raw).hexdigest()
check = subprocess.run([sys.executable, str(HERE / 'assemble.py'), '--check'], cwd=OWN, capture_output=True, text=True)
assert check.returncode == 0, check.stderr
assembly = json.loads(check.stdout)
results = json.loads((HERE / 'OFFLINE_RESULTS.json').read_bytes())
assert results['assembly'] == assembly and results['actualProtocolVectors']['assertions'] == 109
assert results['actualExistingContractTestSummary'] == ['# tests 12', '# pass 12', '# fail 0']
assert results['invoiceAssignmentASTVectors'] == 8 and results['resultAssignmentASTVectors'] == 8
assert results['runtimeSQLFilesEmitted'] == 0
files = sorted(file for file in HERE.rglob('*') if file.is_file() and file.name != 'MANIFEST.json')
assert not any('__pycache__' in str(file) or file.name == 'forbidden-runtime.sql' for file in files)
patterns = {
    'PRIVATE_KEY': rb'-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----',
    'GITHUB_TOKEN': rb'(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{50,})',
    'AWS_ACCESS_KEY': rb'AKIA[0-9A-Z]{16}',
    'JWT': rb'eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{15,}',
}
entries = []
for file in files:
    raw = file.read_bytes()
    for kind, pattern in patterns.items():
        assert not re.search(pattern, raw), file.name + ':' + kind  # No matched value is printed.
    lf = raw.replace(b'\r\n', b'\n')
    entries.append({'path': file.relative_to(HERE).as_posix(), 'savedSha256': sha(raw), 'savedBytes': len(raw),
        'LF': {'sha256': sha(lf), 'bytes': len(lf)},
        'completeCRLF': {'sha256': sha(lf.replace(b'\n', b'\r\n')), 'bytes': len(lf.replace(b'\n', b'\r\n'))}})
for file in [HERE / 'assemble.py', HERE / 'offline-checks.py', HERE / 'freeze.py', HERE / 'protocol-vectors.mjs']:
    assert not re.search(rb'C:[/\\](?:tmp[/\\]|Users[/\\][^\r\n]*?(?:OneDrive|\.codex))', file.read_bytes(), re.I), file.name
manifest = {
    'status': 'HEALTH_SOURCE_AUTHORED_COMPLETE_RUNTIME_ASSEMBLY_HELD',
    'ReportInput': '09279bfeeff28e1a52f321edb2ec3760478c3729',
    'integrationInput': '84738fe4057d9d693ccd723eb1835106f1091f57',
    'OfficeUnchanged': 'ac112422e529aefbcba0fc0147d2b28123751823',
    'model': 'gpt-6.1-sol/xhigh', 'assembly': assembly,
    'validation': {'PG17Grammar': True, 'actualAssignmentASTVectors': 16, 'actualClosedProtocolAssertions': 109,
        'originalCurrentContractTests': 12, 'credentialPatterns': list(patterns), 'credentialFindings': 0},
    'currentOwnerEdits': 0, 'newHealthDefinitions': 7, 'newPrivate': 6, 'publicServiceExecuteGranted': 0,
    'unprovidedBankOwnerBlocks': 4, 'runtimeSQLEmitted': 0, 'BankOwnedBodiesRead': 0,
    'native': 'NOT_RUN', 'API': 'NOT_RUN', 'browser': 'NOT_RUN', 'installation': 'NOT_RUN', 'publication': 'NOT_RUN',
    'lineEndingPolicy': 'Only each recorded exact saved, LF or complete CRLF form; no arbitrary drift.', 'files': entries,
}
destination = HERE / 'MANIFEST.json'
if '--verify' in sys.argv:
    frozen = json.loads(destination.read_bytes())
    assert {row['path'] for row in frozen['files']} == {row['path'] for row in entries}
    for row in frozen['files']:
        raw = (HERE / row['path']).read_bytes()
        assert (sha(raw), len(raw)) in [(row['savedSha256'], row['savedBytes']),
            (row['LF']['sha256'], row['LF']['bytes']), (row['completeCRLF']['sha256'], row['completeCRLF']['bytes'])], row['path']
    for key in ['assembly', 'validation', 'status', 'integrationInput', 'newHealthDefinitions', 'unprovidedBankOwnerBlocks']:
        assert frozen[key] == manifest[key], key
else:
    destination.write_bytes((json.dumps(manifest, indent=2) + '\n').encode('utf-8'))
print(json.dumps({'files': len(files), 'manifestSha256': sha(destination.read_bytes()), 'assembly': assembly['sourceAssembly'],
    'native': 'NOT_RUN', 'credentialFindings': 0}))
