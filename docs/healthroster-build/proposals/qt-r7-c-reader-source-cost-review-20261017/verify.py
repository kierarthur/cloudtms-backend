from pathlib import Path
import json,hashlib,subprocess
H=Path(__file__).resolve().parent;S=json.loads((H/'SOURCES.json').read_bytes());R=Path(S['root'])
sha=lambda b:hashlib.sha256(b).hexdigest()
git=lambda *a:subprocess.check_output(['git','-C',str(R),*a])
head=git('rev-parse','HEAD').decode().strip();git('merge-base','--is-ancestor',S['parentCommit'],head)
for r in S['rows']:
 b=(R/r['file']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256']
 for ref in [':',head+':',S['parentCommit']+':']:assert b==git('show',ref+r['file'])
assert git('rev-parse','HEAD').decode().strip()==head
if (H/'MANIFEST.json').exists():
 m=json.loads((H/'MANIFEST.json').read_bytes());assert sorted(x['file'] for x in m['files'])==sorted(str(x.relative_to(H)).replace('\\','/') for x in H.rglob('*') if x.is_file() and x.name!='MANIFEST.json')
 for r in m['files']:b=(H/r['file']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256']
print(json.dumps(dict(sourceOnly=True,actualSourceAndClosedReceiptImages=8,savedIndexHeadHistorical=True,runtimeDelta=0,nativeCalls=0,newCandidate='NOT_JUSTIFIED')))
