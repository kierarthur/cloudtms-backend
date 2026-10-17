"""Public-only finite suppression proof; no auth/browser/network/private data."""
from pathlib import Path
import json,hashlib,subprocess,os
HERE=Path(__file__).resolve().parent;OLD=HERE.parent/'test-auth-refresh-diagnostic-forward-20261017';ROOT=HERE.parents[3]
checks=0
def require(value,label):
    global checks
    if not value:raise AssertionError(label)
    checks+=1
def sha(b):return hashlib.sha256(b).hexdigest()
def git(*args):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(ROOT),*args],stderr=subprocess.DEVNULL,
    env={**os.environ,'GIT_OPTIONAL_LOCKS':'0','GIT_NO_LAZY_FETCH':'1'})
subprocess.run(['python',str(OLD/'verify-diagnostic.py')],check=True)
old_manifest=json.loads((OLD/'MANIFEST.json').read_bytes())
for name in [r['file'] for r in old_manifest['files']]+['MANIFEST.json']:
    b=(OLD/name).read_bytes();relative=(OLD/name).relative_to(ROOT).as_posix()
    require(b==git('show','aaf6e878dff85a1612a98e32910dffe34e5e1e6b:'+relative),'all23 frozen R3 files unchanged')
transfers=json.loads((HERE/'SUPPRESSION_FULL_PARENT_TRANSFERS.json').read_bytes())
require(len(transfers['files'])==5 and transfers['runtimeFiles']==3,'three runtime +two test full parents')
for row in transfers['files']:
    before=(HERE/row['reference']).read_bytes();after=(HERE/row['file']).read_bytes()
    require(before==(OLD/row['file']).read_bytes(),'complete actual R3 parent')
    require(sha(before)==row['beforeSha256'] and sha(after)==row['afterSha256'],'whole forward hashes')
    value=after.decode()
    for h in reversed(row['hunks']):
        require(value.count(h['after'])==1,'one exact suppression hunk')
        value=value.replace(h['after'],h['before'],1)
    require(value.encode()==before,'full raw R3 inverse')
for name in ['credentials-child.mjs','admission.mjs','secure-owned.ps1','forward/authenticate.mjs','forward/config.mjs','SOURCE_PINS.json','STATIC_ROUTE_SOURCE.json','reference/health-query-workspace-schema.js.txt']:
    require((HERE/name).read_bytes()==(OLD/name).read_bytes(),'genuine admission/env/PS5/source pins unchanged')
def function(source,name):
    start=source.index('export '+('async ' if 'export async function '+name in source else '')+'function '+name+'(')
    return source[start:source.index('\n}\n',start)+3]
policy=(HERE/'policy.mjs').read_text();old_policy=(OLD/'policy.mjs').read_text()
only="  if(method==='POST'&&url===BACK+'/api/changes/ping')return 'SUPPRESSED_STARTUP';\n"
require(function(policy,'requestKind').replace(only,'')==function(old_policy,'requestKind'),'only exact raw queryless POST classification added')
worker=(HERE/'worker.mjs').read_text();old_worker=(OLD/'worker.mjs').read_text()
require(function(worker,'guardedRoute')==function(old_worker,'guardedRoute'),'transport still aborts suppressed request; no fake response')
require("if(kind==='SUPPRESSED_STARTUP'){\n    if(counts[kind]>cap)violation=true;\n    return false;" in policy,'max1 suppression always false, over-cap violation')
require('SUPPRESSED_STARTUP:1' in policy and 'suppressedStartupRequests:counts.SUPPRESSED_STARTUP??0' in policy,'separate actual bounded counter')
launcher=(HERE/'run-auth-refresh.mjs').read_text();old_launcher=(OLD/'run-auth-refresh.mjs').read_text()
for name in ['childEnvironment','aclChildEnvironment']:
    require(function(launcher,name)==function(old_launcher,name),'child environments unchanged')
require('result.forbiddenWriteRequests===0&&result.refusedRequests===0&&result.suppressedStartupRequests<=1' in launcher,'strict parent export check retained plus suppression bound')
require(worker.index('await Promise.race([browser?.close()')<worker.index('if(success){try{network.assertAdmission();}')<worker.index('if(!success&&fs.existsSync(state))'),'late close callbacks rechecked before retaining state')
for literal in ["if(++total>512){violation=true;return false;}","assertAdmission(){check(!violation&&counts.LOGIN===1,'AUTH_NETWORK_BOUNDARY_HELD');}"]:
    require(literal in policy and literal in old_policy,'original request/admission budgets unchanged')
for p in HERE.rglob('*.mjs'):
    subprocess.run(['node','--check',str(p)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);require(True,'Node syntax')
manifest=HERE/'MANIFEST.json'
if manifest.exists():
    rows=json.loads(manifest.read_bytes())['files']
    require({r['file'] for r in rows}=={p.relative_to(HERE).as_posix() for p in HERE.rglob('*') if p.is_file() and p.name!='MANIFEST.json'},'complete freeze membership')
    for row in rows:
        b=(HERE/row['file']).read_bytes();require(len(b)==row['bytes'] and sha(b)==row['sha256'],'frozen public bytes')
print(json.dumps({'status':'PASS','suppressionChecks':checks,'auth':'NOT_RUN','browser':'NOT_RUN','network':'NOT_RUN','privateContentsRead':False}))
