from pathlib import Path
import json,subprocess,hashlib
from capture import HERE,REPORT,ROOT,PIN,OP,S0,ONE,BOOK,HOLDER,P,sha,git,dump
def verify(current=False):
 manifest=HERE/'MANIFEST.json'
 if manifest.exists():
  rows=json.loads(manifest.read_text())['files'];expected={r['file'] for r in rows}
  actual={p.relative_to(HERE).as_posix() for p in HERE.rglob('*') if p.is_file() and '__pycache__' not in p.parts and p.name!='MANIFEST.json'}
  assert expected==actual,'FROZEN_FILE_SET_CHANGED'
  for r in rows:
   b=(HERE/r['file']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],('FROZEN_BYTES_CHANGED',r['file'])
 parents=json.loads((HERE/'PARENTS.json').read_text());selection=json.loads((HERE/'SELECTION.json').read_text())['targets'];edits=json.loads((HERE/'EDITS.json').read_text())['hunks'];n=0
 for r in parents['files']:
  b=(HERE/'parents'/r['owner']/r['file']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r
  if current:
   root=REPORT if r['owner']=='report' else ROOT
   assert b==git(root,'show',r['commit']+':'+r['file'])==git(root,'show',':'+r['file'])==(root/r['file']).read_bytes(),('CURRENT_PARENT_DRIFT',r['file'])
 for r in selection:
  b=(HERE/'payload'/r['owner']/r['file']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256']
  if r['file'].endswith('.mjs'):
   node_path=str(HERE/'payload'/r['owner']/r['file']).removeprefix('\\\\?\\')
   subprocess.run(['node','--check',node_path],check=True,capture_output=True)
  if r.get('parent')=='ABSENT':
   if current:
    root=REPORT if r['owner']=='report' else ROOT
    assert not (root/r['file']).exists(),('ADDITIVE_COLLISION',r['file'])
   continue
  s=b.decode()
  for h in reversed([e for e in edits if e['owner']==r['owner'] and e['file']==r['file']]):
   assert s.count(h['after'])==1,('REVERSE_ANCHOR',r['file']);s=s.replace(h['after'],h['before'],1);n+=1
  assert s.encode()==(HERE/'parents'/r['owner']/r['file']).read_bytes(),('FULL_PARENT_INVERSE',r['file'])
 for f in [OP+'native-read.mjs',OP+'contract.mjs',OP+'network.mjs',OP+'source.mjs',OP+'git-image-batch.mjs',S0+'coordinate.mjs',S0+'sequence.mjs',ONE+'sequence.mjs',HOLDER+'source.mjs','scripts/healthroster-turn-control.mjs','scripts/healthroster-rehearsal-turn.mjs']:
  owner='root' if f.startswith('scripts/') else 'report';assert (HERE/'parents'/owner/f).read_bytes()==(HERE/'payload'/owner/f).read_bytes(),('ORIGINAL_CHANGED',f)
 assert n==len(edits)
 print(json.dumps(dict(status='SOURCE_ONLY_PASS',parents=len(parents['files']),targets=len(selection),changed=sum(r.get('parent')!='ABSENT' for r in selection),new=sum(r.get('parent')=='ABSENT' for r in selection),literalHunks=n,currentRawSavedIndexHistoricalChecked=current,admission='NOT_RUN_REQUIRES_REAL_SUCCESSOR',native='NOT_RUN')))
if __name__=='__main__':
 import sys
 assert set(sys.argv[1:])<=set(['--current'])
 verify('--current' in sys.argv)
