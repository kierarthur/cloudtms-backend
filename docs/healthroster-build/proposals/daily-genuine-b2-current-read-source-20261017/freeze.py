"""AUTHOR ONLY: seal this ignored source packet after all replay completes."""
from capture import HERE,sha,dump
files=[]
for p in sorted(HERE.rglob('*')):
 if p.is_file() and '__pycache__' not in p.parts and p.name!='MANIFEST.json':
  b=p.read_bytes();files.append(dict(file=p.relative_to(HERE).as_posix(),bytes=len(b),sha256=sha(b)))
dump('MANIFEST.json',dict(version='B2_IGNORED_SOURCE_FREEZE_V1',sourceOnly=True,commit=None,files=files))
print(dict(files=len(files),manifestSha256=sha((HERE/'MANIFEST.json').read_bytes())))
