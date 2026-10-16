"""Offline finite pure helper preparation; no database or network access."""
from pathlib import Path
import hashlib,json,sys
base=Path(__file__).resolve().parent
recipe=json.loads((base/'PURE_FACTS_RECIPE.json').read_text(encoding='utf-8'))
root=Path(recipe['readSourceRoot'])
source=recipe['sourceInput']
assert hashlib.sha256((root/source['path']).read_bytes()).hexdigest()==source['sha256'],'ACTUAL_NATIVE_DEPENDENCY_SOURCE_CHANGED'
paths=[leaf['path'] for leaf in recipe['orderedLeaves']]
body='-- Complete source-only pure facts subset; root owns SEND7 rollback execution.\n'
for name in paths:
 raw=(base/name).read_bytes()
 assert b'\r' not in raw and not raw.startswith(b'\xef\xbb\xbf'),'NONCANONICAL_LEAF'
 body+=raw.decode('utf-8').strip()+'\n\n'
body=body.rstrip()+'\n'
(base/'prepared/pure-facts.sql').write_text(body,encoding='utf-8',newline='\n')
sys.path.insert(0,'C:/tmp/healthroster-v6-physical-parser')
from pglast import parse_sql
statements=parse_sql(body)
assert len(statements)==11,'UNREVIEWED_PURE_ENVELOPE'
for leaf in recipe['orderedLeaves']:
 raw=(base/leaf['path']).read_bytes();leaf['sha256']=hashlib.sha256(raw).hexdigest();leaf['bytes']=len(raw)
raw=(base/'prepared/pure-facts.sql').read_bytes()
recipe['preparedSql'].update(sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw),parsedTopLevelStatements=len(statements))
(base/'PURE_FACTS_RECIPE.json').write_text(json.dumps(recipe,indent=2)+'\n',encoding='utf-8',newline='\n')
print(json.dumps(recipe['preparedSql']))
