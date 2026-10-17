"""Finite source-only proof; no observer input execution/private target access."""
import pathlib,json,hashlib,subprocess
A=pathlib.Path('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026')
REL='docs/healthroster-build/proposals/daily-health-observer-seven-field-turn-forward-20261017'
C='218dec4b93632b167d38434ed03530fa0e86a314'
def read(p):return pathlib.Path('\\\\?\\'+str(p)).read_bytes()
def sha(b):return hashlib.sha256(b).hexdigest()
def git(r,*a):return subprocess.check_output(['git','-c','core.longpaths=true','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(r),*a])
b=read(A/REL/'MANIFEST.json');assert sha(b)=='51ce676b14e714dfd7b58aea1852f2f9e8f16feaf8d632699114c2a36e21c29b'
rows=json.loads(b)['files']+[{'file':'MANIFEST.json','bytes':len(b),'sha256':sha(b)}]
for r in rows:
    b=read(A/REL/r['file']);assert len(b)==r['bytes'] and sha(b)==r['sha256']
    assert b==git(A,'show',C+':'+REL+'/'+r['file'])==git(A,'show','HEAD:'+REL+'/'+r['file'])==git(A,'show',':'+REL+'/'+r['file'])
for r in json.loads(read(A/REL/'FULL_PARENT_INVERSES.json'))['inverses']:
    b=read(A/r['parentFile']);assert len(b)==r['beforeBytes'] and sha(b)==r['beforeSha256']
    assert b==git(A,'show',r['parentCommit']+':'+r['parentFile'])==git(A,'show','HEAD:'+r['parentFile'])==git(A,'show',':'+r['parentFile'])
for r in json.loads(read(A/REL/'SOURCE_SELECTION.json'))['sourcePins']:
    assert '.codex-tmp' not in r['file']
    b=read(pathlib.Path(r['root'])/r['file']);assert len(b)==r['bytes'] and sha(b)==r['sha256']
    assert b==git(r['root'],'show','HEAD:'+r['file'])==git(r['root'],'show',':'+r['file'])
print(json.dumps({'frozenFiles':len(rows),'complete6389Parents':3,'exactCurrentSourcePins':3,'savedIndexHeadFrozenGit':'PASS','native':'NOT_RUN'}))
