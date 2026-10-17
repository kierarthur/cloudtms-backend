"""Public source/Git verification only; no native/business/private reads."""
import hashlib,json,pathlib,subprocess
AUTHOR=pathlib.Path('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026')
ROOT=pathlib.Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
REL='docs/healthroster-build/proposals/daily-health-s2-s3-scoped-effects-observer-20261017'
COMMIT='92ec880ade00bc1119a7e6a900dbe32467ee0ad6'
def sha(b):return hashlib.sha256(b).hexdigest()
def read(p):return pathlib.Path('\\\\?\\'+str(p)).read_bytes()
def git(root,*args):return subprocess.check_output(['git','-c','core.longpaths=true','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(root),*args])
mf=read(AUTHOR/REL/'MANIFEST.json');assert sha(mf)=='26cfe35e7193fffcf4d31bf4d3cd8d2a6a8192eb93d513707ab5b938458f4a9f'
rows=json.loads(mf)['files']+[{'file':'MANIFEST.json','bytes':len(mf),'sha256':sha(mf)}]
for r in rows:
    b=read(AUTHOR/REL/r['file']);assert len(b)==r['bytes'] and sha(b)==r['sha256']
    assert b==git(AUTHOR,'show',COMMIT+':'+REL+'/'+r['file'])==git(AUTHOR,'show','HEAD:'+REL+'/'+r['file'])==git(AUTHOR,'show',':'+REL+'/'+r['file'])
s=json.loads(read(AUTHOR/REL/'SOURCE_SELECTION.json'))
for r in s['parents']:
    b=read(ROOT/r['file']);assert len(b)==r['bytes'] and sha(b)==r['sha256']
    assert b==git(ROOT,'show',s['rootCommit']+':'+r['file'])==git(ROOT,'show','HEAD:'+r['file'])==git(ROOT,'show',':'+r['file'])
def js_slice(raw,a,b):return raw.decode().encode('utf-16-le')[2*a:2*b].decode('utf-16-le').encode()
published=git(ROOT,'show',s['published']+':broker/src/index.js').decode()
for r in s['functions']+s['nativeStatements']:
    b=js_slice(read(ROOT/r['file']),r['start'],r['end']);assert b==read(AUTHOR/REL/r['capture']) and sha(b)==r['sha256'] and len(b)==r['bytes']
    if 'name' in r:assert b.decode() in published
assert len(s['existingOperatorScopes'])==11 and not s['semanticBankBodies'] and not s['productTargets']
drel='docs/healthroster-build/proposals/daily-health-s2-s3-settings-effect-disposition-20261017'
dmf=read(AUTHOR/drel/'MANIFEST.json');assert sha(dmf)=='df606dc03d4e1dfcfdc5d2bad8833725465cdb5cbfe2487c3df83dfeb3fb9670'
drows=json.loads(dmf)['files']+[{'file':'MANIFEST.json','bytes':len(dmf),'sha256':sha(dmf)}]
for r in drows:
    b=read(AUTHOR/drel/r['file']);assert sha(b)==r['sha256'] and len(b)==r['bytes']
    assert b==git(AUTHOR,'show','a72d3387a3489d0f483da2d6066218aee70b971d:'+drel+'/'+r['file'])==git(AUTHOR,'show','HEAD:'+drel+'/'+r['file'])==git(AUTHOR,'show',':'+drel+'/'+r['file'])
fmf=read(AUTHOR/drel/'COMMON_HELPER_FORWARD_MANIFEST.json');assert sha(fmf)=='04fcd81e72f40ad360e087df71023a69b744e6b6e6101db532c6c1036337fa38'
frows=json.loads(fmf)['files']+[{'file':'COMMON_HELPER_FORWARD_MANIFEST.json','bytes':len(fmf),'sha256':sha(fmf)}]
for r in frows:
    b=read(AUTHOR/drel/r['file']);assert sha(b)==r['sha256'] and len(b)==r['bytes']
    assert b==git(AUTHOR,'show','fa55ae0c9c701cad3419d143732e0c46711020ab:'+drel+'/'+r['file'])==git(AUTHOR,'show','HEAD:'+drel+'/'+r['file'])==git(AUTHOR,'show',':'+drel+'/'+r['file'])
print(json.dumps({'frozenFiles':len(rows),'dispositionFiles':len(drows)+len(frows),'completeCurrentHistoricalParents':len(s['parents']),'publishedEqualCompleteWorkerFunctions':len(s['functions']),'completeHealthStatements':len(s['nativeStatements']),'savedIndexHeadFrozenGit':'PASS','native':'NOT_RUN'}))
