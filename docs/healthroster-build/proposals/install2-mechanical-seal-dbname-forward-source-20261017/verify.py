import pathlib,json,hashlib,subprocess,sys
P=pathlib.Path(__file__).resolve().parent
def sha(b):return hashlib.sha256(b).hexdigest()
def load(p):return json.loads((P/p).read_text())
def git(r,*a):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false',*a],cwd=r)
assert sys.argv[1:]==['--verify']
mf=load('MANIFEST.json');files={f.relative_to(P).as_posix():f for f in P.rglob('*') if f.is_file() and '__pycache__' not in f.parts and f!=P/'MANIFEST.json'}
assert set(files)=={r['path'] for r in mf['rows']}
for r in mf['rows']:
 b=files[r['path']].read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256']
parents=load('PARENTS.json');root=pathlib.Path(parents['root']);head=git(root,'rev-parse','HEAD')
for r in parents['rows']:
 b=(P/'parents'/r['path']).read_bytes();assert sha(b)==r['sha256']
 assert (root/r['path']).read_bytes()==git(root,'show',':'+r['path'])==git(root,'show','HEAD:'+r['path'])==git(root,'show',parents['commit']+':'+r['path'])==b
old=load('ORIGINAL45_MANIFEST.json');prefix=parents['originalPrefix']
assert sha((P/'ORIGINAL45_MANIFEST.json').read_bytes())==parents['originalManifestSha256']
for r in old['rows']:
 p=prefix+r['path'];b=(root/p).read_bytes();assert sha(b)==r['sha256'] and len(b)==r['bytes']
 assert b==git(root,'show',':'+p)==git(root,'show','HEAD:'+p)==git(root,'show',parents['commit']+':'+p)
p=prefix+'MANIFEST.json';b=(P/'ORIGINAL45_MANIFEST.json').read_bytes()
assert (root/p).read_bytes()==git(root,'show',':'+p)==git(root,'show','HEAD:'+p)==git(root,'show',parents['commit']+':'+p)==b
for r in load('FORWARD.json')['rows']:
 a=(P/'parents'/r['target']).read_bytes();b=(P/'payload'/r['target']).read_bytes()
 assert sha(a)==r['beforeSha256'] and sha(b)==r['afterSha256']
 assert b==a.replace(b'healthroster_install2_seal',b'banking_modal_v2_test')
 assert a==b.replace(b'banking_modal_v2_test',b'healthroster_install2_seal')
 subprocess.run(['node','--check',str(P/'payload'/r['target'])],check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
assert git(root,'rev-parse','HEAD')==head
print(json.dumps(dict(status='SOURCE_ONLY_VALUE_FORWARD_PASS',files=len(files)+1,original45Unchanged=True,changed=2,unchanged=1,native='NOT_RUN')))
