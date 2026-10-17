"""Finite source-only peer. No SQL/observer execution or private target lookup."""
import hashlib,json,pathlib,subprocess,sys
HERE=pathlib.Path(__file__).resolve().parent
AUTHOR=pathlib.Path('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026')
ROOT=pathlib.Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
REL='docs/healthroster-build/proposals/daily-health-s1-scoped-effects-observer-20261017'
COMMIT='30ecfec7f7ca813ccc417ec1fac98d8059088cf6'
def sha(b):return hashlib.sha256(b).hexdigest()
def read(p):return pathlib.Path('\\\\?\\'+str(p)).read_bytes()
def git(root,*args):return subprocess.check_output(['git','-c','core.longpaths=true','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(root),*args])
def save(p,b):
    p=pathlib.Path('\\\\?\\'+str(p));p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b)
mf=read(AUTHOR/REL/'MANIFEST.json');assert sha(mf)=='c1e9570187c001a49932d5d8d003ed9c289a4193e7fdf19e92459a650737dc6a'
rows=json.loads(mf)['files']+[{'file':'MANIFEST.json','bytes':len(mf),'sha256':sha(mf)}]
for r in rows:
    b=read(AUTHOR/REL/r['file']);assert len(b)==r['bytes'] and sha(b)==r['sha256']
    assert b==git(AUTHOR,'show',COMMIT+':'+REL+'/'+r['file'])==git(AUTHOR,'show','HEAD:'+REL+'/'+r['file'])==git(AUTHOR,'show',':'+REL+'/'+r['file'])
    p=HERE/'reference'/r['file']
    if '--capture' in sys.argv:save(p,b)
    else:assert read(p)==b
selection=json.loads(read(AUTHOR/REL/'SOURCE_SELECTION.json'))
for r in selection['parents']:
    b=read(ROOT/r['file']);assert len(b)==r['bytes'] and sha(b)==r['sha256']
    assert b==git(ROOT,'show',':'+r['file'])==git(ROOT,'show','HEAD:'+r['file'])==git(ROOT,'show',selection['rootCommit']+':'+r['file'])
for r in selection['captures']:
    # Author scanner offsets are UTF-16 JavaScript string positions, not UTF-8 bytes.
    raw=read(ROOT/r['file']);encoded=raw.decode('utf-8').encode('utf-16-le')
    b=encoded[2*r['start']:2*r['end']].decode('utf-16-le').encode('utf-8');assert b==read(AUTHOR/REL/r['capture']),r['signature']
    assert sha(b)==r['sha256'] and len(b)==r['bytes']
assert selection['semanticBankBodies']==[] and selection['runtimeTargets']==[]
print(json.dumps({'frozenFiles':len(rows),'completeCurrentHistoricalParents':len(selection['parents']),'completeStatements':len(selection['captures']),'savedIndexHeadFrozenGit':'PASS','rootHead':git(ROOT,'rev-parse','HEAD').decode().strip(),'native':'NOT_RUN'}))
