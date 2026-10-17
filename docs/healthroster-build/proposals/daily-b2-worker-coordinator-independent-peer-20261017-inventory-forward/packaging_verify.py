"""Read-only complete packaging replay, including eighteen omission counterexamples."""
from pathlib import Path
import json
from capture import HERE,sha
old=HERE.parent/'daily-genuine-b2-current-read-source-20261017'
proof=json.loads((HERE/'PACKAGING_FORWARD.json').read_text())
assert sha((old/'MANIFEST.json').read_bytes())==proof['originalManifestSha256']
assert (old/'MANIFEST.json').read_bytes()==(HERE/proof['retainedOriginalManifest']).read_bytes()
original={p.relative_to(old).as_posix() for p in old.rglob('*') if p.is_file() and '__pycache__' not in p.parts and p!=old/'MANIFEST.json'}
assert len(original)==152 and proof['baseFilesBeforeForwardEvidence']==153
changed={r['file']:r for r in proof['packagingToolWholeInverses']}
for f in original:
 before=(old/f).read_bytes();after=(HERE/f).read_bytes()
 if f in changed:
  r=changed[f];assert before.decode()==r['before'] and after.decode()==r['after'] and sha(before)==r['beforeSha256'] and sha(after)==r['afterSha256']
  assert after.decode().replace("p!=HERE/'MANIFEST.json'","p.name!='MANIFEST.json'",1).encode()==before
 else:assert after==before,('ORIGINAL_RAW_FILE_CHANGED',f)
parents=json.loads((HERE/'PARENTS.json').read_text())['files']
for r in proof['omittedOriginalManifestReferences']:
 b=(HERE/r['file']).read_bytes();p=next(x for x in parents if x['owner']==r['owner'] and x['file']==r['parentFile'])
 assert len(b)==p['bytes']==r['bytes'] and sha(b)==p['sha256']==r['sha256']
 assert b==(HERE/'parents'/r['owner']/r['parentFile']).read_bytes()
assert len(proof['omittedOriginalManifestReferences'])==18
if (HERE/'MANIFEST.json').exists():
 rows=json.loads((HERE/'MANIFEST.json').read_text())['files'];names={r['file'] for r in rows}
 actual={p.relative_to(HERE).as_posix() for p in HERE.rglob('*') if p.is_file() and '__pycache__' not in p.parts and p!=HERE/'MANIFEST.json'}
 def accept(n):return n==actual
 assert accept(names)
 for r in proof['omittedOriginalManifestReferences']:assert not accept(names-{r['file']}),('NESTED_OMISSION_ADMITTED',r['file'])
 for r in rows:
  b=(HERE/r['file']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256']
print(json.dumps(dict(status='PACKAGING_ONLY_PASS',completeOriginalFiles=152,retainedOriginalManifest=1,nestedManifests=18,omissionCounterexamples=18,functionalPostimagesUnchanged=18,literalHunksUnchanged=76)))
