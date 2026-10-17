"""Author-only offline manifest; never imports/runs runtime helpers."""
import hashlib,json,pathlib
p=pathlib.Path(__file__).resolve().parent
rows=[]
for f in sorted(p.rglob('*')):
 if not f.is_file() or '__pycache__' in f.parts or f==p/'MANIFEST.json':continue
 b=f.read_bytes();rows.append(dict(path=f.relative_to(p).as_posix(),bytes=len(b),sha256=hashlib.sha256(b).hexdigest()))
(p/'MANIFEST.json').write_text(json.dumps(dict(version='INSTALL2_MECHANICAL_FRESH_SOURCE_MANIFEST_V1',rows=rows),indent=2)+'\n',newline='\n')
print(json.dumps(dict(files=len(rows)+1,manifestSha256=hashlib.sha256((p/'MANIFEST.json').read_bytes()).hexdigest())))
