"""Offline PG17 PL/pgSQL statement inventory. Never connects to a database."""
import hashlib, json, re, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import pglast
from pglast import parser
directory=Path(__file__).resolve().parent
extraction=json.loads((directory/'EXTRACTION.json').read_text(encoding='utf-8'))
assert parser.get_postgresql_version()[0]==17
rows=[]
def walk(x, counts):
    if isinstance(x,dict):
        for k,v in x.items():
            if k.startswith('PLpgSQL_stmt_'): counts[k]=counts.get(k,0)+1
            walk(v,counts)
    elif isinstance(x,list):
        for v in x: walk(v,counts)
for item in extraction['bodies']:
    raw=(directory/item['producerFile']).read_bytes()
    assert hashlib.sha256(raw).hexdigest()==item['producerSha256']
    sql=raw.decode('utf-8')
    # The parser has no native relation catalogue. This alias only makes the
    # offline PL/pgSQL grammar/count available; it does not qualify row fields.
    grammar=re.sub(r'\b(?:(?:public|private)\.)?[A-Za-z_][A-Za-z0-9_]*%rowtype\b','record',sql,flags=re.I)
    tree=json.loads(parser.parse_plpgsql_json(grammar));counts={};walk(tree,counts)
    rows.append({**item,'rowtypeParserAliasOnly':grammar!=sql,'statementNodes':counts,
                 'actualDynamicExecuteStatements':counts.get('PLpgSQL_stmt_dynexecute',0),
                 'dynamicForStatements':counts.get('PLpgSQL_stmt_dynfors',0)})
out={'version':'F23_CURRENT_DYNAMIC_OFFLINE_AST_V1','sourceCommit':extraction['sourceCommit'],
     'pglastVersion':pglast.__version__,'postgresqlParserVersion':parser.get_postgresql_version(),
     'counts':{'completeProducers':len(rows),'actualDynamicExecuteStatements':sum(x['actualDynamicExecuteStatements'] for x in rows),
               'dynamicForStatements':sum(x['dynamicForStatements'] for x in rows)},'producers':rows,
     'limits':'Offline grammar and statement count only. Rowtype aliases do not resolve any native object/type/field. No authority or effect was exercised.',
     'native':'NOT_RUN','databaseCalls':0}
extra_path=directory/'INHERITED_PRODUCER.json'
if extra_path.exists():
    extra=json.loads(extra_path.read_text(encoding='utf-8'))
    raw=(directory/extra['producerFile']).read_bytes()
    assert hashlib.sha256(raw).hexdigest()==extra['producerSha256']
    counts={};walk(json.loads(parser.parse_plpgsql_json(raw.decode('utf-8'))),counts)
    assert counts.get('PLpgSQL_stmt_dynexecute')==2
    out['inheritedAdditionalProducer']={**extra,'statementNodes':counts}
    out['counts']['all35FilesCompleteDynamicProducers']=len(rows)+1
    out['counts']['all35FilesActualDynamicExecuteStatements']=sum(x['actualDynamicExecuteStatements'] for x in rows)+2
    assert out['counts']['all35FilesActualDynamicExecuteStatements']==102
(directory/'AST.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
print(json.dumps(out['counts']))
