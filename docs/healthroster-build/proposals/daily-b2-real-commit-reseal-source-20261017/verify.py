"""Read-only source capture/inverse verifier; never calls real materialization/admission."""
import ast,json,subprocess
from reseal import *
a=Authority();c=a.composer;file=c['file'];s=a.functional['root',file].decode()
for h in reversed(c['edits']):
 assert s.count(h['after'])==h['occurrences'];s=s.replace(h['after'],h['before'])
assert s.encode()==a.parents['root',file]
raw=(HERE/'acl/b2-plus-accepted-acl-phase-parent.mjs').read_text()
for h in reversed(a.acl['edits']):assert raw.count(h['after'])==1;raw=raw.replace(h['after'],h['before'],1)
assert raw.encode()==a.functional['report',a.acl['file']]
accepted=(HERE/'acl/accepted-phase-parent.mjs').read_text()
for h in reversed(a.acl['edits']):assert accepted.count(h['after'])==1;accepted=accepted.replace(h['after'],h['before'],1)
assert accepted.encode()==a.parents['report',a.acl['file']]
for f in ['reseal.py','git_values.py','test_reseal.py','verify.py']:ast.parse((HERE/f).read_text())
for f in ['admit-public.mjs','test.mjs','payload/root/'+file]:subprocess.run(['node','--check',str(HERE/f).removeprefix('\\\\?\\')],check=True,capture_output=True)
manifest=HERE/'MANIFEST.json'
if manifest.exists():
 rows=json.loads(manifest.read_text())['files'];actual={p.relative_to(HERE).as_posix() for p in HERE.rglob('*') if p.is_file() and '__pycache__' not in p.parts and p!=manifest}
 assert {r['file'] for r in rows}==actual
 for r in rows:
  b=(HERE/r['file']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256']
print(json.dumps(dict(status='SOURCE_ONLY_PASS',completeB2Authority='29fc5acee6a0dd454516c708edf0b473e0ca27afc8e91dffaee77ec3c4555af8',originalParents=55,composerCaptures=3,functionalImages=19,metadataOwners=14,originalRuntimeRows=12,remaps=5,unchangedRuntimeRows=7,actualMaterialization='NOT_RUN',actualPublicSourceAdmission='NOT_RUN',native='NOT_RUN')))
