"""Finite public-source capture verification only; never executes programme SQL."""
import hashlib,json,pathlib,subprocess
HERE=pathlib.Path(__file__).resolve().parent
AUTHOR=pathlib.Path('D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026')
REL='docs/healthroster-build/proposals/qt-audit-select-into-forward-20261017'
COMMIT='ef3fbd69c5540a8b7b4157da0b72a537a3a033a1'
def sha(b):return hashlib.sha256(b).hexdigest()
def read(p):return pathlib.Path('\\\\?\\'+str(p)).read_bytes()
def git(root,*args):return subprocess.check_output(['git','-c','core.longpaths=true','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(root),*args])
mf=read(AUTHOR/REL/'MANIFEST.json');assert sha(mf)=='db2a391ad6a3ea390bd3757cd63f54b6cc1a99d272672fdaba4b3ab857dece41'
rows=json.loads(mf)['files']+[{'file':'MANIFEST.json','bytes':len(mf),'sha256':sha(mf)}]
for r in rows:
    b=read(AUTHOR/REL/r['file']);assert len(b)==r['bytes'] and sha(b)==r['sha256']
    assert b==git(AUTHOR,'show',COMMIT+':'+REL+'/'+r['file'])==git(AUTHOR,'show','HEAD:'+REL+'/'+r['file'])==git(AUTHOR,'show',':'+REL+'/'+r['file'])
parents=json.loads(read(AUTHOR/REL/'PARENTS.json'))
for r in parents['rows']:
    assert r['file'].startswith('docs/healthroster-build/') or r['file'].startswith('supabase/')
    assert '.codex-tmp' not in r['file']
    b=read(pathlib.Path(r['root'])/r['file']);assert len(b)==r['bytes'] and sha(b)==r['sha256']
    assert b==read(AUTHOR/REL/r['captured'])==git(r['root'],'show',r['commit']+':'+r['file'])==git(r['root'],'show','HEAD:'+r['file'])==git(r['root'],'show',':'+r['file'])
root=pathlib.Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
adapter='docs/healthroster-build/proposals/contact-qt-positive-native-proposal-20261017/'
old_manifest=json.loads(read(AUTHOR/REL/'reference/adapter/MANIFEST.json'))
for r in old_manifest['files']:
    assert not r['file'].startswith('/') and '..' not in pathlib.PurePosixPath(r['file']).parts and '.codex-tmp' not in r['file']
    file=adapter+r['file'];b=read(root/file);assert len(b)==r['bytes'] and sha(b)==r['sha256'],file
    assert b==git(root,'show','148fc0c36ea82867532368b9578d727713ef993e:'+file)==git(root,'show','HEAD:'+file)==git(root,'show',':'+file)
print(json.dumps({'frozenFiles':len(rows),'completeCurrentHistoricalParents':len(parents['rows']),'completeCurrentAdapterImages':len(old_manifest['files']),'savedIndexHeadFrozenGit':'PASS','native':'NOT_RUN','performance':'NOT_PROVED'}))
