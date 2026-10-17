"""Public source/whole-parent verifier. No authentication or ACL mutation."""
from pathlib import Path
import hashlib,json,subprocess,os
HERE=Path(__file__).resolve().parent
OLD=HERE.parent/'test-auth-refresh-harness-20261017'
ROOT=HERE.parents[3]
checks=0
def require(value,label):
    global checks
    if not value:raise AssertionError(label)
    checks+=1
def sha(value):return hashlib.sha256(value).hexdigest()
def git(ref,file):
    return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(ROOT),'show',ref+file],stderr=subprocess.DEVNULL,
        env={**os.environ,'GIT_OPTIONAL_LOCKS':'0','GIT_NO_LAZY_FETCH':'1'})
manifest=json.loads((OLD/'MANIFEST.json').read_bytes())
for name in [r['file'] for r in manifest['files']]+['MANIFEST.json']:
    original=(OLD/name).read_bytes();relative=(OLD/name).relative_to(ROOT).as_posix()
    for ref in ['ba01714404c0ff28adf02ceefb04bd6723206823:','HEAD:',':']:
        require(original==git(ref,relative),'all19 original saved/index/HEAD/ba017 files unchanged')
    if name not in ['README.md','MANIFEST.json','run-auth-refresh.mjs','OFFLINE_RESULTS.json']:
        require(original==(HERE/name).read_bytes(),'unchanged inherited auth/runtime/source evidence')
row=json.loads((HERE/'LAUNCHER_FULL_PARENT_TRANSFER.json').read_bytes())
before=(HERE/row['reference']).read_bytes();after=(HERE/row['file']).read_bytes()
require(before==(OLD/'run-auth-refresh.mjs').read_bytes(),'complete actual R1 launcher capture')
require(sha(before)==row['beforeSha256'] and sha(after)==row['afterSha256'],'whole launcher hashes')
require(len(row['hunks'])==3,'exact three finite launcher hunks')
current=after.decode()
for hunk in reversed(row['hunks']):
    require(current.count(hunk['after'])==1,'unique exact current launcher hunk')
    current=current.replace(hunk['after'],hunk['before'],1)
require(current.encode()==before,'complete raw R1 launcher inverse')
source=after.decode()
require(source.count('env:aclChildEnvironment(aclShell)')==1,'only exact ACL child env call')
require("env:childEnvironment(out),stdio:['ignore','ignore','ignore','ipc']" in source,'Playwright child environment/output unchanged')
require('process.env.PSModulePath=' not in source and 'SetEnvironmentVariable' not in source and 'ExecutionPolicy' not in source,'no global/machine/policy change')
require(source.index("phase:'PRIVATE_DIRECTORY_ACL'")<source.index("fs.mkdirSync(path.join(out,'tmp')"),'closed ACL refusal before copies/browser/credential loader')
require('credentialLoaderStarted:false' in source and 'browserStarted:false' in source,'truthful closed early phase')
probe=(HERE/'ps5-module-preflight.ps1').read_text()
require('Import-Module Microsoft.PowerShell.Security' in probe and '$acl=Get-Acl -LiteralPath $PublicDirectory' in probe,'real module and public metadata probe')
require('\nSet-Acl ' not in probe and '$setter=Get-Command Set-Acl ' in probe,'Set-Acl resolved only, never invoked')
subprocess.run(['python',str(HERE/'verify.py')],check=True)
print(json.dumps({'status':'PASS','forwardChecks':checks,'originalPackageFiles':19,'launcherHunks':3,'auth':'NOT_RUN','aclMutation':'NOT_RUN'}))
