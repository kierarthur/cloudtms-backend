"""Peer read-only raw source/object proof. Not an auth/source admission gate."""
from pathlib import Path
import hashlib,json,re,subprocess,os
SOURCE=Path('\\\\?\\'+'D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026/.codex-tmp/daily-auth-r11-b2-source-recipe-20261017'.replace('/','\\'))
ROOTS={'root':'D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026','report':'D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026'}
H=Path(__file__).resolve().parent
def sha(b):return hashlib.sha256(b).hexdigest()
mf=(SOURCE/'MANIFEST.json').read_bytes()
assert sha(mf)=='2dfd4d6ffc8859456cd16aa102c125cd7e45035322724d743453f8871fc634e1','FROZEN_AUTHOR_SELECTION_CHANGED'
m=json.loads(mf);listed={r['file'] for r in m['files']}
actual={p.relative_to(SOURCE).as_posix() for p in SOURCE.rglob('*') if p.is_file() and '__pycache__' not in p.parts and p!=SOURCE/'MANIFEST.json'}
assert listed==actual
for r in m['files']:
 b=(SOURCE/r['file']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256']
fixture=json.loads((SOURCE/'HISTORICAL_SOURCE_FIXTURE.json').read_text());count=0
for owner,root in ROOTS.items():
 selected={}
 for r in fixture['blobs']:
  if r['owner']!=owner:continue
  assert re.fullmatch('[a-f0-9]{40}',r['ref'])
  assert re.fullmatch(r'(?:docs/healthroster-build/|scripts/healthroster-)[A-Za-z0-9_./-]+',r['file']) and '..' not in r['file'].split('/')
  key=r['ref']+':'+r['file'];value=r['text'].encode()
  assert key not in selected or selected[key]==value
  selected[key]=value
 requests=list(selected)
 result=subprocess.run(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',root,'cat-file','--batch'],
  input=('\n'.join(requests)+'\n').encode(),stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,check=True,
  env={**os.environ,'GIT_OPTIONAL_LOCKS':'0','GIT_NO_LAZY_FETCH':'1'},timeout=30)
 raw=result.stdout;assert len(raw)<=32_000_000;cursor=0
 for key in requests:
  end=raw.index(b'\n',cursor);header=raw[cursor:end].split();assert len(header)==3 and header[1]==b'blob' and re.fullmatch(b'[a-f0-9]{40}',header[0])
  n=int(header[2]);assert 0<=n<=2_000_000;cursor=end+1
  assert raw[cursor:cursor+n]==selected[key] and raw[cursor+n:cursor+n+1]==b'\n',key
  cursor+=n+1;count+=1
 assert cursor==len(raw),'EXTRA_OR_TRUNCATED_SOURCE_OBJECT_FRAME'
assert (SOURCE/'MANIFEST.json').read_bytes()==mf
print(json.dumps(dict(status='FROZEN_PUBLIC_SOURCE_AND_REAL_HISTORICAL_OBJECTS_PASS',authorFiles=len(listed)+1,actualGitBlobs=count,
 native='NOT_RUN',sourceAdmission='NOT_RUN',auth='NOT_RUN',privateTargetAccess=False)))
