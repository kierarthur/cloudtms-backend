from pathlib import Path
import json,hashlib,re,sys,subprocess
HERE=Path(__file__).resolve().parent
REPO=HERE.parents[3]
ROOT=REPO.parent/'healthroster-install2-14102026'
sys.path.insert(0,'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast
sha=lambda b:hashlib.sha256(b).hexdigest()
d=json.loads((HERE/'EFFECT_CLOSURE.json').read_bytes())
declarations=[]
for row in d['sourceDeclarations']:
    current=(ROOT/row['physical']).read_bytes()
    assert sha(current)==row['physicalSha256']
    text=current.decode();start=text.index('create or replace function '+row['name']+'(')
    body=text.index('as $function$',start);end=text.index('$function$;',body)+len('$function$;')
    captured=(HERE/row['captured']).read_bytes()
    assert captured==text[start:end].encode() and sha(captured)==row['definitionSha256']
    content=text[body+len('as $function$'):end-len('$function$;')]
    assert not re.search(r'\b(insert|update|delete|merge|execute|nextval|setval|pg_advisory|current_setting|set_config|pg_notify|dblink)\b',content,re.I)
    assert not re.search(r'\b(?:from|join)\s+(?:public|private)\.',content,re.I)
    assert sorted(set(re.findall(r'\b((?:private|public)\.[a-z_0-9]+)\s*\(',content)))==row['applicationCallees']
    assert len(pglast.parse_sql(captured.decode()))==1
    if 'language plpgsql' in captured.decode():assert len(pglast.parse_plpgsql(captured.decode()))==1
    declarations.append(row['name'])
registry=json.loads((ROOT/'docs/bpay-next/final-definer-closures.json').read_bytes())
for row in d['actualRegisteredOwners']:assert next(v for v in registry['chains'] if v['signature']==row['signature'])==row
assert len(declarations)==2
print(json.dumps({'completeCurrentDefinitions':2,'applicationRelations':0,'applicationCalleesClosed':True,'grammar':'PG17','native':'NOT_RUN'}))
