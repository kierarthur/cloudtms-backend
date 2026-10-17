"""Grammar of seven unbound Health bodies; no runtime owner substitute."""
from pathlib import Path
import json,re,sys
HERE=Path(__file__).resolve().parent
OWN=HERE.parents[3]
SOURCE=OWN.parent/'healthroster-v2-report-build-13102026'/'docs/healthroster-build/proposals/v6-daily-health-composer-blocks-20261017'
PARSER=Path(r'D:\Development\Legacy-Work\C-tmp\healthroster-v6-invoice-parser-20261016')
assert PARSER.is_dir()
sys.path.insert(0,str(PARSER))
import pglast
from pglast import ast
forward=[];reverse=[];programs=[]
for directory,pattern in [('health','*.sqlinc'),('templates','*.sql.tmpl'),('reverse','*.sqlinc')]:
    for file in sorted((SOURCE/directory).glob(pattern)):
        original=file.read_text(encoding='utf-8')
        sql,count=re.subn(r'@@BANK_(?:REVIEW|ACCEPT|RESULT)@@',"raise exception 'UNBOUND_OWNER_BLOCK_GRAMMAR_ONLY' using errcode='55000';",original)
        assert count==(1 if directory=='templates' else 0)
        statements=pglast.parse_sql(sql); bodies=pglast.parse_plpgsql(sql)
        if directory!='reverse':
            for row in statements:
                if not isinstance(row.stmt,ast.CreateFunctionStmt):continue
                name='.'.join(v.sval for v in row.stmt.funcname)
                options={option.defname:option.arg for option in row.stmt.options}
                assert options['security'].boolval is True
                assert 'search_path' in str(options['set'])
                forward.append(name)
        else:
            reverse=['.'.join(v.sval for v in row.stmt.objects[0].objname) for row in statements]
        programs.append({'file':file.relative_to(SOURCE).as_posix(),'statements':len(statements),'plpgsqlBodies':len(bodies),'refusalSubstitutions':count,'grammar':'PASS','native':'NOT_RUN'})
assert len(forward)==len(set(forward))==7
assert sorted(forward)==sorted(reverse)
value={'parserVersion':pglast.__version__,'postgresqlGrammar':pglast.get_postgresql_version(),'programs':programs,'completeAdditiveReverseNamesMatch':True,'newPrivateDefiners':7,'realBankBlocksExpanded':0,'nativeBodyCompilation':'NOT_PROVED','native':'NOT_RUN'}
(HERE/'GRAMMAR_RESULTS.json').write_text(json.dumps(value,indent=2)+'\n',encoding='utf-8')
print('PASS seven Health definer grammars and additive reverse; three refusal substitutions only; native body completion NOT_PROVED')
