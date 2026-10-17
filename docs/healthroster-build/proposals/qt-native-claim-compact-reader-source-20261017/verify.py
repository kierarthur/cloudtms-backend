from pathlib import Path
import hashlib,json,subprocess,os,sys
P=Path(__file__).resolve().parent;ROOT=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
def sha(b):return hashlib.sha256(b).hexdigest()
def read(p):return (P/p).read_bytes()
def doc(p):return json.loads(read(p))
def git(*a):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false',*a],cwd=ROOT,env={**os.environ,'GIT_OPTIONAL_LOCKS':'0','GIT_NO_LAZY_FETCH':'1'},stderr=subprocess.PIPE)
f=doc('FORWARD.json');parents=doc('PARENTS.json');head=git('rev-parse','HEAD').decode().strip()
assert len(f['targets'])==4 and len(f['newTargets'])==1
for r in f['targets']:
 old=read(r['parent']);new=read(r['payload']);assert len(old)==r['beforeBytes'] and sha(old)==r['beforeSha256'];assert len(new)==r['bytes'] and sha(new)==r['sha256']
 assert old.decode()==r['wholeInverse']['before'] and new.decode()==r['wholeInverse']['after']
 captured=next(x for x in parents['currentImages'] if x['file']==r['beforeFile']);assert captured['sha256']==sha(old) and read(captured['capture'])==old
 s=new.decode()
 for h in reversed(r['hunks']):
  if h.get('kind')=='APPEND':assert s.endswith(h['after']);s=s[:-len(h['after'])]
  else:assert s.count(h['after'])==1;s=s.replace(h['after'],h['before'])
 assert s.encode()==old
 if r['target']!=r['beforeFile']:assert not (ROOT/r['target']).exists()
for r in f['newTargets']:
 b=read(r['payload']);assert len(b)==r['bytes'] and sha(b)==r['sha256'];assert not (ROOT/r['target']).exists()
for r in parents['currentImages']:
 b=read(r['capture']);assert len(b)==r['bytes'] and sha(b)==r['sha256'];assert b==git('show',r['historicalCommit']+':'+r['file'])
 if '--current' in sys.argv:assert b==(ROOT/r['file']).read_bytes()==git('show',':'+r['file'])==git('show',head+':'+r['file'])
old=doc('parent/MANIFEST.json')['files'];new=doc('payload/MANIFEST.json')['files'];changed={'fixed-metadata-reader.mjs','result-validator.mjs'}
for r in old:
 if r['file'] not in changed:assert next(x for x in new if x['file']==r['file'])==r
assert len(new)==len(old)+2
for r in doc('SHARED_SOURCE_PINS.json')['rows']:
 b=read(r['capture']);assert sha(b)==r['sha256'];assert b==git('show',r['commit']+':'+r['file'])
 if '--current' in sys.argv:assert b==(ROOT/r['file']).read_bytes()==git('show',':'+r['file'])==git('show',head+':'+r['file'])
assert head==git('rev-parse','HEAD').decode().strip()
if (P/'MANIFEST.json').exists():
 m=doc('MANIFEST.json');actual={str(p.relative_to(P)).replace('\\','/') for p in P.rglob('*') if p.is_file() and p.name!='MANIFEST.json' and '__pycache__' not in p.parts}
 # Nested complete manifests are deliberately included.
 actual={str(p.relative_to(P)).replace('\\','/') for p in P.rglob('*') if p.is_file() and p!=P/'MANIFEST.json' and '__pycache__' not in p.parts}
 assert actual=={r['file'] for r in m['files']}
 for r in m['files']:b=read(r['file']);assert len(b)==r['bytes'] and sha(b)==r['sha256']
print(json.dumps({'status':'PASS_SOURCE_ONLY','completeParentFiles':len(parents['currentImages']),'completeRawInverses':4,'selectedTargets':5,'oldManifestRows':len(old),'retainedRows':len(old)-2,'sharedSources':len(doc('SHARED_SOURCE_PINS.json')['rows']),'native':'NOT_RUN'}))
