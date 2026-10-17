from pathlib import Path
import hashlib,json,subprocess,sys
HERE=Path(__file__).resolve().parent
manifest=json.loads((HERE/'FREEZE_MANIFEST.json').read_bytes())
sha=lambda b:hashlib.sha256(b).hexdigest()
committed=sys.argv[1:]==['--committed']
assert not sys.argv[1:] or committed
git=lambda *a:subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(HERE),*a])
prefix='docs/healthroster-build/proposals/source-status-child-pure-rollback-adapter-20261017/'
for r in manifest['files']:
 b=(HERE/r['file']).read_bytes()
 assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['file']
 if committed:assert b==git('show','HEAD:'+prefix+r['file'])==git('show',':'+prefix+r['file']),r['file']
if committed:
 b=(HERE/'FREEZE_MANIFEST.json').read_bytes()
 assert b==git('show','HEAD:'+prefix+'FREEZE_MANIFEST.json')==git('show',':'+prefix+'FREEZE_MANIFEST.json')
print(json.dumps(dict(files=len(manifest['files'])+1,manifestSha256=sha((HERE/'FREEZE_MANIFEST.json').read_bytes()),
 savedIndexHEAD='EQUAL' if committed else 'NOT_CHECKED_BEFORE_COMMIT',native='NOT_RUN',restoration='NOT_RUN')))
