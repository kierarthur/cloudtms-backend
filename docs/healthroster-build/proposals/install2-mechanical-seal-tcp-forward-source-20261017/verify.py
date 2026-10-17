import pathlib,hashlib,json,subprocess,sys
P=pathlib.Path(__file__).resolve().parent
def h(b):return hashlib.sha256(b).hexdigest()
def j(p):return json.loads((P/p).read_text())
def g(root,*a):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false',*a],cwd=root)
assert sys.argv[1:]==['--verify'];m=j('MANIFEST.json');files={f.relative_to(P).as_posix():f for f in P.rglob('*') if f.is_file() and '__pycache__' not in f.parts and f!=P/'MANIFEST.json'}
assert set(files)=={r['path'] for r in m['rows']}
for r in m['rows']:b=files[r['path']].read_bytes();assert h(b)==r['sha256'] and len(b)==r['bytes']
p=j('PARENTS.json');root=pathlib.Path(p['root']);head=g(root,'rev-parse','HEAD')
for r in p['rows']:
 b=(P/'parents'/r['path']).read_bytes();assert h(b)==r['sha256']
 assert b==(root/r['path']).read_bytes()==g(root,'show',':'+r['path'])==g(root,'show','HEAD:'+r['path'])==g(root,'show',p['commit']+':'+r['path'])
f=j('FORWARD.json');b=(P/'payload'/f['target']).read_bytes();assert h(b)==f['afterSha256'];text=b.decode()
for a in reversed(f['hunks']):assert text.count(a['after'])==1;text=text.replace(a['after'],a['before'],1)
assert text.encode()==(P/'parents'/f['target']).read_bytes()
c=f['commandForward'];b=(P/'COMMANDS.ps1.txt').read_bytes();old=(P/'parents/COMMANDS.ps1.txt').read_bytes()
assert h(b)==c['afterSha256'] and h(old)==c['beforeSha256'] and b==old.replace(c['before'].encode(),c['after'].encode())
for file in ['scripts/healthroster-install2-local-provider.mjs','scripts/healthroster-install2-local-seal-profile.mjs']:
 subprocess.run(['node','--check',str(P/'payload'/file)],check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
assert g(root,'rev-parse','HEAD')==head
print(json.dumps(dict(status='TCP_ADMIN_ENV_SOURCE_ONLY_PASS',files=len(files)+1,parents=len(p['rows']),runtimeTargets=1,hunks=3,launcherHunks=1,docker='NOT_RUN',native='NOT_RUN')))
