import hashlib,json,pathlib,subprocess
H=pathlib.Path(__file__).resolve().parent;sha=lambda b:hashlib.sha256(b).hexdigest()
F=json.loads((H/'FORWARD.json').read_text());S=dict(version='HEALTH_SOURCE_EXACT_ONCE_SELECTION_V1',targets=[dict(target=t['file'],source='payload/'+t['file'],sha256=t['afterSha256'],bytes=t['afterBytes'],parentSha256=t['beforeSha256']) for t in F['targets']],immutableMigrationsChanged=False,migrationLockChanged=False,coverageOnly=['16102026_1100','16102026_1101','16102026_1104'],native='NOT_RUN')
(H/'SELECTION.json').write_text(json.dumps(S,indent=2)+'\n',encoding='utf8')
rows=[]
for p in sorted(H.rglob('*')):
 if p.is_file() and p!=H/'MANIFEST.json' and '__pycache__' not in p.parts:
  b=p.read_bytes();rows.append(dict(file=str(p.relative_to(H)).replace('\\','/'),bytes=len(b),sha256=sha(b)))
(H/'MANIFEST.json').write_text(json.dumps(dict(version='HEALTH_SOURCE_EXACT_ONCE_PACKET_V1',files=rows,native='NOT_RUN'),indent=2)+'\n',encoding='utf8')
print(json.dumps(dict(files=len(rows)+1,manifestSha256=sha((H/'MANIFEST.json').read_bytes()),selectionSha256=sha((H/'SELECTION.json').read_bytes()),forwardSha256=sha((H/'FORWARD.json').read_bytes()))))
