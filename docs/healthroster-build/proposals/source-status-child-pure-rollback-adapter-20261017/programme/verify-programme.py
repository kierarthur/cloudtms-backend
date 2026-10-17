from pathlib import Path
import hashlib,json,re,subprocess,sys

HERE=Path(__file__).resolve().parent
ROOT=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
sha=lambda x:hashlib.sha256(x).hexdigest()
git=lambda *args:subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(ROOT),*args])
m=json.loads((HERE/'SOURCE_MANIFEST.json').read_bytes())
checks=0
def check(ok):
 global checks
 assert ok
 checks+=1

for row in m['rootInputs']:
 b=(ROOT/row['file']).read_bytes()
 check(sha(b)==row['fileSha256'])
 check(b==git('show',m['rootCommit']+':'+row['file']))
 prefix='create or replace function '+row['name']+'('
 text=b.decode();start=text.index(prefix);end=text.index('$function$;',text.index('as $function$',start))+len('$function$;')
 declaration=text[start:end].encode()
 check(len(declaration)==row['definitionBytes'] and sha(declaration)==row['definitionSha256'])
 body=declaration.decode().split('as $function$')[1].split('$function$;')[0]
 check(not re.search(r'\b(insert|update|delete|merge|execute|nextval|setval|pg_advisory|current_setting|set_config|pg_notify|dblink)\b',body,re.I))
 check(not re.search(r'\b(?:from|join)\s+(?:private|public)\.',body,re.I))
 calls=sorted(set(re.findall(r'\b((?:private|public)\.[a-z_0-9]+)\s*\(',body)))
 check(calls==(['private.weekly_exceptional_json_keys_exact_v1'] if 'status_child' in row['name'] else []))
for row in m['metadataInputs']:
 b=(ROOT/row['file']).read_bytes()
 check(len(b)==row['bytes'] and sha(b)==row['sha256'])
 check(b==git('show',m['rootCommit']+':'+row['file']))
for row in m['localInputs']:
 b=(HERE/row['file']).read_bytes();check(len(b)==row['bytes'] and sha(b)==row['sha256'])
sql=(HERE/'PROGRAMME.sql').read_bytes()
check(len(sql)==20303 and sha(sql)==m['sqlSha256'])
check(len(pglast.parse_sql(sql.decode()))==6)
check(len(pglast.parse_plpgsql(sql.decode()))==4)
check(sql.count(b'create function ')==1 and b'create or replace function' not in sql)
check(b'create function private.weekly_exceptional_json_keys_exact_v1' not in sql)
check(sql.index(b'SOURCE_PURE_NEW_NAME_ALREADY_PRESENT')<sql.index(b'create function '))
check(sql.index(b'revoke all on function')<sql.index(b'SOURCE_PURE_NEW_DEFINITION_OR_ACL_REFUSED')<sql.index(b'-- SYNTHETIC JSON'))
check(b"where s.nspname='private' and p.proname='weekly_source_pay_decision_native_status_child_v1'" in sql)
check(b"pg_catalog.pg_get_functiondef(p.oid)" in sql and b'bb36d7e2d71abd7b9c0dfba29936d85edca54c8101591fd113e11d3ab7cf9189' in sql)
check(sql.count(b'cardinality(p.proacl)<>1')==2)
for role in ['anon','authenticated','service_role']:
 check(sql.count(("pg_catalog.has_function_privilege('"+role+"',p.oid,'EXECUTE')").encode())==2)
check(b"grantor=p.proowner" in sql and b'not a.is_grantable' in sql)
assertions=(HERE/'assertions.sql').read_text()
check('positives<>4 or negatives<>12' in assertions)
check("'NATIVE_STATUS'" not in assertions and 'PUBLISHED_NEXT' not in assertions and 'banking_receipt' not in assertions)
check('exception when sqlstate \'55000\'' in assertions and 'sqlerrm is distinct from v.expected_message' in assertions)
transfer=json.loads((HERE.parent/'PROGRAMME_SOURCE_TRANSFER.json').read_bytes())
current=(HERE/'build-program.mjs').read_text()
check(sha(current.encode())==transfer['afterSha256'])
for h in reversed(transfer['hunks']):
 check(current.count(h['after'])==1);current=current.replace(h['after'],h['before'],1)
check(current.encode()==git('show',transfer['ownerCommit']+':'+transfer['ownerFile']))
print(json.dumps({'checks':checks,'grammar':'PG17','statements':6,'plpgsqlPrograms':4,'newDefinitions':1,'existingDependencyReplaced':False,'native':'NOT_RUN','restoration':'NOT_RUN'}))
