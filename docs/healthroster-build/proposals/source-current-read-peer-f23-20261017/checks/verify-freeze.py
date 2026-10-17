from pathlib import Path
import sys, json, hashlib, os, subprocess
here = Path(__file__).resolve().parent.parent
repo = here.parents[3]
def raw(path):
    return Path(('\\\\?\\'+str(path)) if os.name == 'nt' else path).read_bytes()
manifest = json.loads(raw(here/'MANIFEST.json'))
actual = {p.relative_to(here).as_posix() for p in here.rglob('*') if p.is_file() and p.name != 'MANIFEST.json'}
assert actual == {row['path'] for row in manifest['files']}
for row in manifest['files']:
    data = raw(here/row['path'])
    assert len(data) == row['bytes'] and hashlib.sha256(data).hexdigest() == row['sha256'], row['path']
    if '--git' in sys.argv:
        relative = (here/row['path']).relative_to(repo).as_posix()
        saved = subprocess.check_output(['git', '-c', 'gc.auto=0', '-c', 'maintenance.auto=false', 'show', 'HEAD:'+relative], cwd=repo)
        assert saved == data, relative
print(json.dumps({'files':len(manifest['files']), 'savedExact':True, 'GitExact':'--git' in sys.argv}))
