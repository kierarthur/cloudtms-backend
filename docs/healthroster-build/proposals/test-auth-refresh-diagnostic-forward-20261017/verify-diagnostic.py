"""Public source only. Does not execute auth/browser/network or inspect private data."""
from pathlib import Path
import json,hashlib,subprocess,os
HERE=Path(__file__).resolve().parent;OLD=HERE.parent/'test-auth-refresh-ps5-forward-20261017';ROOT=HERE.parents[3]
checks=0
def require(value,label):
    global checks
    if not value:raise AssertionError(label)
    checks+=1
def sha(b):return hashlib.sha256(b).hexdigest()
def git(root,*args):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(root),*args],stderr=subprocess.DEVNULL,
    env={**os.environ,'GIT_OPTIONAL_LOCKS':'0','GIT_NO_LAZY_FETCH':'1'})
subprocess.run(['python',str(OLD/'verify-forward.py')],check=True)
old_manifest=json.loads((OLD/'MANIFEST.json').read_bytes())
for name in [r['file'] for r in old_manifest['files']]+['MANIFEST.json']:
    b=(OLD/name).read_bytes();relative=(OLD/name).relative_to(ROOT).as_posix()
    require(b==git(ROOT,'show','da869d45d2ff88a0d7ae096838e060ea118dedee:'+relative),'all24 frozen R2 files unchanged')
transfers=json.loads((HERE/'DIAGNOSTIC_FULL_PARENT_TRANSFERS.json').read_bytes())
require(len(transfers['files'])==5,'four runtime +one test full parent forwards')
for row in transfers['files']:
    before=(HERE/row['reference']).read_bytes();after=(HERE/row['file']).read_bytes()
    require(before==(OLD/row['file']).read_bytes(),'actual complete R2 parent')
    require(sha(before)==row['beforeSha256'] and sha(after)==row['afterSha256'],'full source hashes')
    value=after.decode()
    for hunk in reversed(row['hunks']):
        require(value.count(hunk['after'])==1,'one exact diagnostic hunk')
        value=value.replace(hunk['after'],hunk['before'],1)
    require(value.encode()==before,'complete raw R2 inverse')
for name in ['credentials-child.mjs','admission.mjs','secure-owned.ps1','forward/config.mjs','SOURCE_PINS.json','reference/health-query-workspace-schema.js.txt']:
    require((HERE/name).read_bytes()==(OLD/name).read_bytes(),'unchanged credential/admission/PS5/runtime pin authority')
def function(source,name):
    start=source.index('export '+('async ' if 'export async function '+name in source else '')+'function '+name+'(')
    return source[start:source.index('\n}\n',start)+3]
policy=(HERE/'policy.mjs').read_text();old_policy=(OLD/'policy.mjs').read_text()
worker=(HERE/'worker.mjs').read_text();old_worker=(OLD/'worker.mjs').read_text()
launcher=(HERE/'run-auth-refresh.mjs').read_text();old_launcher=(OLD/'run-auth-refresh.mjs').read_text()
require(function(policy,'requestKind')==function(old_policy,'requestKind'),'exact route acceptance function unchanged')
require(function(worker,'guardedRoute')==function(old_worker,'guardedRoute'),'exact redirect/transport refusal unchanged')
for name in ['childEnvironment','aclChildEnvironment']:
    require(function(launcher,name)==function(old_launcher,name),'exact child environment boundaries unchanged')
for literal in ["const cap={LOGIN:1,REFRESH:1,READY:1,ME:8,WORKSPACE:2,HOME:1,STATIC:400,PREFLIGHT:24}[kind];",
    "if(++total>512){violation=true;return false;}","assertAdmission(){check(!violation&&counts.LOGIN===1,'AUTH_NETWORK_BOUNDARY_HELD');}"]:
    require(literal in policy and literal in old_policy,'caps/admission remain identical')
callback=(HERE/'forward/authenticate.mjs').read_text()
require(callback.index('const admitted=await admission(page);')<callback.index('network.recordAdmission(admitted);')<callback.index("check(admitted.meStatus===200"),'observed values recorded before admission checks')
require(callback.index('network.assertAdmission();')<callback.index('storageState({ path: authFile })'),'state remains after strict network admission')
require('diagnostic:result?.diagnostic??null' in launcher and 'diagnostic:result.diagnostic' in launcher,'parent forwards actual closed diagnostics only')
require("diagnostic:network.diagnostics()" in worker and 'TEST_AUTH_REFRESH_PRIVATE_RESULT_V2' in worker,'explicit local closed diagnostic version')
routes=json.loads((HERE/'STATIC_ROUTE_SOURCE.json').read_bytes());main=(Path(routes['root'])/routes['file']).read_bytes()
require(len(main)==routes['savedBytes'] and sha(main)==routes['savedSha256'],'whole public main source exact')
require(main==git(routes['root'],'show',routes['commit']+':'+routes['file'])==git(routes['root'],'show',':'+routes['file']),'raw saved/index/pinned main equality')
require(git(routes['root'],'rev-parse',routes['commit']+':'+routes['file']).decode().strip()==routes['gitBlob'],'pinned Git blob authority')
require([(r['literal'],r['method'],r['label']) for r in routes['routes']]==[('/api/changes/ping','POST','STATIC_CHANGES_PING'),('/api/users/me/grid-prefs','PATCH','STATIC_USER_GRID_PREFS')],'only two source-derived static labels')
for row in routes['routes']:
    for occurrence in row['occurrences']:
        require(occurrence['source'] in main.decode(),'exact public literal/method source capture')
for p in HERE.rglob('*.mjs'):
    subprocess.run(['node','--check',str(p)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    require(True,'Node syntax only')
manifest=HERE/'MANIFEST.json'
if manifest.exists():
    rows=json.loads(manifest.read_bytes())['files']
    require({r['file'] for r in rows}=={p.relative_to(HERE).as_posix() for p in HERE.rglob('*') if p.is_file() and p.name!='MANIFEST.json'},'complete diagnostic freeze membership')
    for row in rows:
        b=(HERE/row['file']).read_bytes();require(len(b)==row['bytes'] and sha(b)==row['sha256'],'frozen diagnostic bytes')
print(json.dumps({'status':'PASS','diagnosticChecks':checks,'auth':'NOT_RUN','browser':'NOT_RUN','network':'NOT_RUN','privateContentsRead':False}))
