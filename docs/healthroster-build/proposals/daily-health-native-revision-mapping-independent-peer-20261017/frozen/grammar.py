from pathlib import Path
import sys,json,os
HERE=Path(__file__).resolve().parent
assert str(HERE).startswith('D:')
if os.name=='nt': HERE=Path('\\\\?\\'+str(HERE))
assert not (HERE/'MANIFEST.json').exists()
PARSER=Path(r'D:\Development\Legacy-Work\C-tmp\healthroster-v6-invoice-parser-20261016');assert PARSER.is_dir();sys.path.insert(0,str(PARSER))
import pglast
sql=(HERE/'health/10_revision_apply.sqlinc').read_text(encoding='utf-8');parsed=pglast.parse_sql(sql);bodies=pglast.parse_plpgsql(sql)
out={'postgresqlGrammar':pglast.get_postgresql_version(),'parser':pglast.__version__,'completeRevisionHelper':{'statements':len(parsed),'plpgsqlBodies':len(bodies),'grammar':'PASS','nativeBodyCompilation':'NOT_PROVED'},'ResultTemplate':'NOT_PARSED_UNPROVIDED_BANK_SLOT_AND_CURRENT_PLACEMENT_MAPPING','substitutions':[],'dbConnections':0,'native':'NOT_RUN'}
(HERE/'GRAMMAR_RESULTS.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8');print(json.dumps(out))
