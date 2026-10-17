"""AUTHOR ONLY: seal source recipe, never materialize runtime metadata."""
from reseal import HERE,sha,encoded
rows=[]
for p in sorted(HERE.rglob('*')):
 if p.is_file() and '__pycache__' not in p.parts and p!=HERE/'MANIFEST.json':
  b=p.read_bytes();rows.append(dict(file=p.relative_to(HERE).as_posix(),bytes=len(b),sha256=sha(b)))
(HERE/'MANIFEST.json').write_bytes(encoded(dict(version='B2_REAL_COMMIT_RESEAL_SOURCE_FREEZE_V1',sourceOnly=True,actualMaterialization='NOT_RUN',files=rows)))
print(dict(files=len(rows),manifestSha256=sha((HERE/'MANIFEST.json').read_bytes())))
