"""Non-writing source/Git/inverse/manifest/syntax check. No helper execution."""
import hashlib,json,pathlib,subprocess,sys
HERE=pathlib.Path(__file__).resolve().parent
def sha(b):return hashlib.sha256(b).hexdigest()
def load(p):return json.loads((HERE/p).read_text())
def git(root,*args):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false',*args],cwd=root)
def files():return {p.relative_to(HERE).as_posix():p for p in HERE.rglob('*') if p.is_file() and '__pycache__' not in p.parts and p!=HERE/'MANIFEST.json'}
assert sys.argv[1:]==['--verify'],'READ_ONLY_VERIFY_MODE_REQUIRED'
manifest=load('MANIFEST.json');actual=files();assert set(actual)=={r['path'] for r in manifest['rows']},'COMPLETE_INVENTORY_REQUIRED'
for r in manifest['rows']:
 b=actual[r['path']].read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['path']
parents=load('PARENTS.json');root=pathlib.Path(parents['root']);head=git(root,'rev-parse','HEAD').strip()
for r in parents['rows']:
 b=(HERE/'parents'/r['path']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256']
 assert (root/r['path']).read_bytes()==git(root,'show',':'+r['path'])==git(root,'show','HEAD:'+r['path'])==git(root,'show',parents['commit']+':'+r['path'])==b,r['path']
forward=load('FORWARD.json')
for r in forward['rows']:
 b=(HERE/'payload'/r['target']).read_bytes();assert sha(b)==r['payloadSha256'];text=b.decode()
 for e in reversed(r['hunks']):
  start=e['start']+sum(len(x['after'])-len(x['before']) for x in r['hunks'] if x['start']<e['start'])
  assert text[start:start+len(e['after'])]==e['after'],'RAW_HUNK_MISMATCH'
  text=text[:start]+e['before']+text[start+len(e['after']):]
 assert text.encode()==(HERE/'parents'/r['parent']).read_bytes(),'COMPLETE_RAW_INVERSE'
selection=load('SELECTION.json')['rows']
for r in selection:
 b=(HERE/'payload'/r['target']).read_bytes();assert sha(b)==r['sha256']
 assert not (root/r['target']).exists(),'ADDITIVE_TARGET_COLLISION'
 tracked=subprocess.run(['git','-c','gc.auto=0','-c','maintenance.auto=false','ls-files','--error-unmatch',r['target']],cwd=root,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
 assert tracked.returncode!=0,'TRACKED_TARGET_COLLISION'
 subprocess.run(['node','--check',str(HERE/'payload'/r['target'])],check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
assert git(root,'rev-parse','HEAD').strip()==head,'HEAD_CHANGED_DURING_REVIEW'
print(json.dumps(dict(status='BOUNDED_SOURCE_ONLY_PASS',files=len(actual)+1,parents=len(parents['rows']),targets=len(selection),
 rawInverses=len(forward['rows']),hunks=sum(len(x['hunks']) for x in forward['rows']),native='NOT_RUN',docker='NOT_RUN')))
