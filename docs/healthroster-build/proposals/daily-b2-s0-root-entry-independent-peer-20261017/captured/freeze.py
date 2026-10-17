"""AUTHOR ONLY: package source; never invoke a root entry or materializer."""
from pathlib import Path
import json,hashlib
H=Path('\\\\?\\'+str(Path(__file__).resolve().parent));rows=[]
for p in sorted(H.rglob('*')):
 if p.is_file() and '__pycache__' not in p.parts and p!=H/'MANIFEST.json':
  b=p.read_bytes();rows.append(dict(file=p.relative_to(H).as_posix(),bytes=len(b),sha256=hashlib.sha256(b).hexdigest()))
b=(json.dumps(dict(version='B2_S0_THREE_ENTRY_SOURCE_FREEZE_V1',files=rows,native='NOT_RUN',actualSourceAdmission='NOT_RUN'),indent=2)+'\n').encode();(H/'MANIFEST.json').write_bytes(b);print(dict(entries=len(rows),manifestSha256=hashlib.sha256(b).hexdigest()))
