import pathlib,json,hashlib
P=pathlib.Path(__file__).resolve().parent;r=[]
for f in sorted(P.rglob('*')):
 if f.is_file() and '__pycache__' not in f.parts and f!=P/'MANIFEST.json':
  b=f.read_bytes();r.append(dict(path=f.relative_to(P).as_posix(),bytes=len(b),sha256=hashlib.sha256(b).hexdigest()))
(P/'MANIFEST.json').write_text(json.dumps(dict(rows=r),indent=2)+'\n',newline='\n')
print(json.dumps(dict(files=len(r)+1,manifestSha256=hashlib.sha256((P/'MANIFEST.json').read_bytes()).hexdigest())))
