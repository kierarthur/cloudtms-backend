"""Frozen public-source capture only. Never follows a state-path string."""
import hashlib,json,pathlib,subprocess,sys
HERE=pathlib.Path(__file__).resolve().parent
AUTHOR=pathlib.Path('D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026')
REL='docs/healthroster-build/proposals/daily-health-genuine-auth-r9-source-recipe-20261017'
COMMIT='0576cc8ac64f295fde418c6b3c94407b28f77fbe'
def sha(b):return hashlib.sha256(b).hexdigest()
def read(p):return pathlib.Path('\\\\?\\'+str(p)).read_bytes()
def git(root,*args):return subprocess.check_output(['git','-c','core.longpaths=true','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(root),*args])
def save(p,b):
    p=pathlib.Path('\\\\?\\'+str(p));p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b)
mf=read(AUTHOR/REL/'MANIFEST.json');assert sha(mf)=='5b1d2acc46b5741c74c624888c41b131a3043a3f8fef499d210b1263326b8359'
rows=json.loads(mf)['files']+[{'file':'MANIFEST.json','bytes':len(mf),'sha256':sha(mf)}]
for r in rows:
    b=read(AUTHOR/REL/r['file']);assert len(b)==r['bytes'] and sha(b)==r['sha256']
    assert b==git(AUTHOR,'show',COMMIT+':'+REL+'/'+r['file'])==git(AUTHOR,'show','HEAD:'+REL+'/'+r['file'])==git(AUTHOR,'show',':'+REL+'/'+r['file'])
    if '--capture' in sys.argv:save(HERE/'reference'/r['file'],b)
    else:assert read(HERE/'reference'/r['file'])==b
parents=json.loads(read(AUTHOR/REL/'PARENTS.json'))
assert len(parents['rows'])==23 and parents['baseCommit']=='00548ecadba24f9d088182f427f622a307abddd6'
for r in parents['rows']:
    assert r['root'].startswith('D:/Development/GitHub/.codex-worktrees/') and r['file'].startswith('docs/healthroster-build/') and '.codex-tmp' not in r['file']
    b=read(pathlib.Path(r['root'])/r['file']);assert len(b)==r['bytes'] and sha(b)==r['sha256']
    assert b==read(AUTHOR/REL/r['captured'])==git(r['root'],'show',r['commit']+':'+r['file'])==git(r['root'],'show','HEAD:'+r['file'])==git(r['root'],'show',':'+r['file'])
print(json.dumps({'frozenFiles':len(rows),'completeCurrentHistoricalParents':len(parents['rows']),'savedIndexHeadFrozenGit':'PASS','native':'NOT_RUN','privateTargetAccess':'NONE'}))
