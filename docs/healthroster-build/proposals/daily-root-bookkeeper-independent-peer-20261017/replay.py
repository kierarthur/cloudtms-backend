"""Read-only public source/Git verification. Never imports/runs bookkeeper.run."""
import pathlib,json,hashlib,subprocess
A=pathlib.Path('D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026')
REL='docs/healthroster-build/proposals/daily-genuine-stage-root-bookkeeper-20261017'
C='bee869c3a333be050fe345612318da95ee36a02b'
def read(p):return pathlib.Path('\\\\?\\'+str(p)).read_bytes()
def sha(b):return hashlib.sha256(b).hexdigest()
def git(r,*a):return subprocess.check_output(['git','-c','core.longpaths=true','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(r),*a])
b=read(A/REL/'MANIFEST.json');assert sha(b)=='b737600654f64127a47eb0fd6af759e16de8c30625ad465e86bbc960c1ba5c0e'
rows=json.loads(b)['files']+[{'file':'MANIFEST.json','bytes':len(b),'sha256':sha(b)}]
for r in rows:
    b=read(A/REL/r['file']);assert len(b)==r['bytes'] and sha(b)==r['sha256']
    assert b==git(A,'show',C+':'+REL+'/'+r['file'])==git(A,'show','HEAD:'+REL+'/'+r['file'])==git(A,'show',':'+REL+'/'+r['file'])
pins=json.loads(read(A/REL/'PARENTS.json'))
for r in pins['rows']:
    assert '.codex-tmp' not in r['file'] and (r['file'].startswith('docs/healthroster-build/') or r['file'].startswith('scripts/'))
    b=read(pathlib.Path(r['root'])/r['file']);assert len(b)==r['bytes'] and sha(b)==r['sha256']
    assert b==read(A/REL/r['captured'])==git(r['root'],'show',r['commit']+':'+r['file'])==git(r['root'],'show','HEAD:'+r['file'])==git(r['root'],'show',':'+r['file'])
assert len(pins['fixedRuntime'])==12
print(json.dumps({'frozenFiles':len(rows),'completeCurrentHistoricalParents':len(pins['rows']),'fixedRuntimeBodies':12,'savedIndexHeadFrozenGit':'PASS','actualRun':'NOT_RUN','privateTargetAccess':'NONE'}))
