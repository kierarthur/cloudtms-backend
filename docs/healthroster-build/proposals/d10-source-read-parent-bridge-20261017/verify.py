"""Read-only inventory, complete inverse and actual public Git-image checks."""
from pathlib import Path
import hashlib,json,subprocess,os,sys
HERE=Path(__file__).resolve().parent
ROOT=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
def lp(p):return Path('\\\\?\\'+str(Path(p).resolve()))
def read(p):return lp(HERE/p).read_bytes()
def doc(p):return json.loads(read(p))
def sha(b):return hashlib.sha256(b).hexdigest()
def git(*a):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-c','core.longpaths=true',*a],cwd=ROOT,env={**os.environ,'GIT_OPTIONAL_LOCKS':'0','GIT_NO_LAZY_FETCH':'1'})
selection=doc('PAYLOAD_SELECTION.json');parents=doc('PARENTS.json')
assert len(selection['targets'])==2 and selection['SQLChanges']==0 and selection['oldSourceReadModuleChanged'] is False
for row in selection['targets']:
 b=read(row['source']);assert len(b)==row['bytes'] and sha(b)==row['sha256']
 parent=read('parents/'+row['target']);assert sha(parent)==row['beforeSha256'] and len(parent)==row['beforeBytes']
 f=next(x for x in doc('FULL_PARENT_FORWARD.json')['rows'] if x['target']==row['target'])
 assert parent==f['before'].encode() and b==f['after'].encode()
 reversed=b.decode()
 for h in f['hunks'][::-1]:
  if h.get('kind')=='APPEND':assert reversed.endswith(h['after']);reversed=reversed[:-len(h['after'])]
  else:assert reversed.count(h['after'])==1;reversed=reversed.replace(h['after'],h['before'])
 assert reversed.encode()==parent
 if '--current' in sys.argv:
  assert lp(ROOT/row['target']).read_bytes()==git('show',':'+row['target'])==parent,('ACTUAL_STAGED_PARENT_CHANGED',row['target'])
  p=next(x for x in parents['rows'] if x['target']==row['target'])
  if p['historicalSha256']:
   assert sha(git('show',p['HEAD']+':'+row['target']))==p['historicalSha256']
  else:
   exists=subprocess.run(['git','-c','gc.auto=0','-c','maintenance.auto=false','cat-file','-e',p['HEAD']+':'+row['target']],cwd=ROOT,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode
   assert exists!=0,('NEW_MODULE_WAS_NOT_NEW',row['target'])
unchanged=parents['unchangedSourceRead'];b=read(unchanged['source'])
assert sha(b)==unchanged['sha256'] and len(b)==unchanged['bytes']
if '--current' in sys.argv:assert b==lp(ROOT/unchanged['target']).read_bytes()==git('show',':'+unchanged['target'])==git('show','HEAD:'+unchanged['target'])
frozen=doc('FROZEN_BASELINE.json');b=read(frozen['source']);assert sha(b)==frozen['sha256'] and len(b)==frozen['bytes']
assert b==git('show',frozen['commit']+':'+frozen['target'])
for row in doc('ORIGINAL_PROOF_INPUTS.json')['rows']:
 b=read('original-proof/'+row['file']);assert sha(b)==row['sha256'] and len(b)==row['bytes']
 if '--current' in sys.argv:assert b==lp(ROOT/'docs/healthroster-build/proposals/d10-reader-current-source-20261017'/row['file']).read_bytes()
for row in doc('RETAINED_REAL_PROOF_INPUTS.json')['rows']:
 b=read(row['capture']);assert sha(b)==row['sha256'] and len(b)==row['bytes']
 assert b==git('show',row['commit']+':'+row['file'])
 if '--current' in sys.argv:assert b==lp(ROOT/row['file']).read_bytes()==git('show',':'+row['file'])
old=HERE.parent/'d10-reader-current-source-20261017';oldmf=json.loads(lp(old/'MANIFEST.json').read_bytes())
assert sha(lp(old/'MANIFEST.json').read_bytes())==doc('ORIGINAL_PROOF_INPUTS.json')['originalManifestSha256']
entries=oldmf.get('files',oldmf.get('entries'))
if isinstance(entries,dict):entries=[dict(file=k,**v) for k,v in entries.items()]
for row in entries:
 name=row.get('file',row.get('path'));b=lp(old/name).read_bytes();assert sha(b)==row['sha256']
if (HERE/'MANIFEST.json').exists():
 mf=doc('MANIFEST.json');actual={str(p.relative_to(lp(HERE))).replace('\\','/') for p in lp(HERE).rglob('*') if p.is_file() and p!=lp(HERE/'MANIFEST.json') and '__pycache__' not in p.parts}
 assert actual=={x['file'] for x in mf['files']}
 for row in mf['files']:b=read(row['file']);assert len(b)==row['bytes'] and sha(b)==row['sha256']
print(json.dumps({'status':'PASS','payloads':2,'fullRawInverses':2,'literalHunks':3,'oldSourceReadModule':'UNCHANGED','retainedActualProofFiles':len(doc('RETAINED_REAL_PROOF_INPUTS.json')['rows']),'native':'NOT_RUN','joinedFullGate':'ROOT_REQUIRED'}))
