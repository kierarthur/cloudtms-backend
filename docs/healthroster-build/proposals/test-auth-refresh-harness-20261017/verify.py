"""Offline public-source verifier. Never invokes auth, browser or private files."""
from pathlib import Path
import hashlib, json, os, re, subprocess

HERE = Path(__file__).resolve().parent
checks = 0
def require(value, label):
    global checks
    if not value:
        raise AssertionError(label)
    checks += 1
def sha(value):
    return hashlib.sha256(value).hexdigest()
def git(root, *args):
    return subprocess.check_output(['git', '-c', 'gc.auto=0', '-c', 'maintenance.auto=false', '-C', str(root), *args], stderr=subprocess.DEVNULL,
        env={**os.environ, 'GIT_OPTIONAL_LOCKS':'0', 'GIT_NO_LAZY_FETCH':'1'})

require(str(Path.cwd().resolve()).replace('\\','/').startswith('D:/Development/GitHub'), 'actual D cwd')
pins = json.loads((HERE/'SOURCE_PINS.json').read_bytes())
for row in pins['sources']:
    data = (Path(row['root'])/row['source']).read_bytes()
    require(len(data)==row['bytes'] and sha(data)==row['sha256'], 'pinned public source')
    comparable = data.replace(b'\r\n',b'\n') if row.get('gitLineEndingRule') else data
    require(comparable==git(row['root'], 'show', row['commit']+':'+row['source']), 'public Git source')
    if row.get('captured'):
        require(data==(HERE/row['captured']).read_bytes(), 'complete raw public capture')

for row in pins['localRuntime']:
    data=Path(row['file']).read_bytes()
    require(len(data)==row['bytes'] and sha(data)==row['sha256'], 'installed public runtime pin')
require(json.loads(Path(pins['localRuntime'][0]['file']).read_bytes())['version']=='1.60.0', 'actual Playwright version')
require(subprocess.check_output(['node','--version'], text=True).strip()=='v24.15.0', 'actual Node version')
# Import only the installed public library/config; no browser, worker or env loader runs.
subprocess.run(['node','--input-type=module','-e',"const p=await import('file:///D:/Development/GitHub/TEST-Frontend/node_modules/@playwright/test/index.mjs'); if(typeof p.chromium.launch!=='function'||typeof p.expect!=='function')process.exit(1)"],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
require(True,'actual Windows ESM SDK import')
subprocess.run(['node', '--input-type=module', '-e', "const {default:c}=await import("+json.dumps((HERE/'forward/config.mjs').as_uri())+"); if(c.use.baseURL!=='https://testmode.arthur-rai.co.uk'||c.use.trace!=='off'||c.use.screenshot!=='off'||c.use.video!=='off')process.exit(1)"],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
require(True,'actual public forward config import')

schema=pins['querySchemaSource']
actual=(Path(schema['root'])/schema['file']).read_bytes()
capture=(HERE/schema['captured']).read_bytes()
require(sha(actual)==schema['sha256'] and sha(capture)==schema['capturedSha256'], 'Health schema source pins')
require(actual==git(schema['root'],'show',schema['commit']+':'+schema['file']), 'whole Health schema Git source')
for name in schema['selectedCompleteFunctions']:
    pattern=rb'^  function '+name.encode()+rb'\(.*?^  }(?:\r?\n)?'
    left=re.findall(pattern, actual, re.M|re.S)
    right=re.findall(pattern, capture, re.M|re.S)
    require(len(left)==len(right)==1 and left==right, 'complete genuine Health '+name)

for row in json.loads((HERE/'FULL_PARENT_TRANSFERS.json').read_bytes()):
    before=(HERE/row['reference']).read_bytes(); after=(HERE/row['file']).read_bytes()
    require(sha(before)==row['beforeSha256'] and sha(after)==row['afterSha256'], 'full parent hashes')
    current=after.decode()
    for hunk in reversed(row['hunks']):
        require(current.count(hunk['after'])==1, 'unique complete after')
        current=current.replace(hunk['after'],hunk['before'],1)
    require(current.encode()==before, 'raw complete parent inverse')

sources={p.relative_to(HERE).as_posix():p.read_text() for p in HERE.rglob('*.mjs')}
for name,source in sources.items():
    subprocess.run(['node','--check',str(HERE/name)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    require(True, 'Node syntax '+name)
    if name!='test.mjs':
        require(not re.search(r'C:[/\\](?:tmp|Users)',source,re.I), 'no former C operational path')
        require(not re.search(r'console\.(?:log|error|warn)|screenshot\(|tracing\.|recordVideo|page\.on\(',source)
            or name=='run-auth-refresh.mjs', 'no captures or page log')
parent=sources['run-auth-refresh.mjs']; worker=sources['worker.mjs']; policy=sources['policy.mjs']
require('process.loadEnvFile' not in parent and 'process.loadEnvFile(ENV_FILE)' in sources['credentials-child.mjs'], 'only child env loader')
require('readFileSync(ENV_FILE' not in parent and 'readFileSync(SHARED_STATE' not in parent, 'parent no private content read')
require("stdio:['ignore','ignore','ignore','ipc']" in parent and "stdio:'ignore'" in parent, 'no child/ACL output capture')
require("await chromium.launch" in worker and worker.index("await chromium.launch")<worker.index("await import('./authenticate.mjs')"), 'browser before credential-capable callback')
require('maxRedirects:0' in worker and 'route.continue' not in worker and 'network.refuseTransport()' in worker, 'redirect closure')
require("serviceWorkers:'block'" in worker and 'routeWebSocket' in worker, 'worker/websocket closure')
require("'--outside-native-interval'" in parent and '90000' in parent and '80000' in worker and '8000' in worker, 'authority and bounded close')
require("'--owned-auth-worker'" in sources['credentials-child.mjs'] and 'typeof process.send' in worker, 'IPC worker boundary')
require("'trace: 'off'" not in worker and 'testDir:' not in sources['forward/config.mjs'] and 'reporter:[]' in sources['forward/config.mjs'], 'no test runner')
require("auth401Mode:'return'" in sources['admission.mjs'] and 'Array.isArray(ws.queries.rows)' in sources['admission.mjs'], 'real explicit admission contract')
require('test-auth-refresh-20261017-' in policy and 'SetAccessRuleProtection($true,$false)' in (HERE/'secure-owned.ps1').read_text(), 'fresh owned private output')
ps = "$tokens=$null; $errors=$null; [void][Management.Automation.Language.Parser]::ParseFile('"+str(HERE/'secure-owned.ps1').replace("'","''")+"',[ref]$tokens,[ref]$errors); if($errors.Count){exit 1}"
# Parser only; the ACL source is never invoked.
subprocess.run(['powershell','-NoProfile','-NonInteractive','-Command',ps],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
require(True,'PowerShell syntax only')

manifest=HERE/'MANIFEST.json'
if manifest.exists():
    rows=json.loads(manifest.read_bytes())['files']
    listed={r['file'] for r in rows}
    actual={p.relative_to(HERE).as_posix() for p in HERE.rglob('*') if p.is_file() and p.name!='MANIFEST.json'}
    require(listed==actual,'complete public manifest membership')
    for row in rows:
        data=(HERE/row['file']).read_bytes()
        require(len(data)==row['bytes'] and sha(data)==row['sha256'],'frozen public manifest bytes')
print(json.dumps({'status':'PASS','checks':checks,'auth':'NOT_RUN','browser':'NOT_RUN','network':'NOT_RUN','privateContentsRead':False}))
