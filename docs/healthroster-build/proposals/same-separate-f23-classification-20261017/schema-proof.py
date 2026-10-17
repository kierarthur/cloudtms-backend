"""Independent PG17 AST column evidence; no database/catalogue compilation."""
from pathlib import Path
import json,sys,hashlib
HERE=Path(__file__).resolve().parent
ROOT=Path(r'D:\Development\GitHub\.codex-worktrees\healthroster-install2-14102026')
MONEY=Path(r'D:\Development\GitHub\.codex-worktrees\healthroster-v2-money-build-13102026')
PARSER=Path(r'D:\Development\Legacy-Work\C-tmp\healthroster-v6-invoice-parser-20261016')
assert PARSER.is_dir();sys.path.insert(0,str(PARSER))
import pglast
from pglast import ast
from pglast.stream import RawStream
census=json.loads((HERE/'CENSUS.json').read_text(encoding='utf-8'))
sha=lambda b:hashlib.sha256(b).hexdigest()
def col_type(column):
    return RawStream()(column.typeName).lower()
def source(path):
    raw=path.read_bytes();text=raw.decode().replace('\r\n','\n')
    return raw,'\n'.join(line for line in text.splitlines() if not line.startswith('\\'))
migration=next(row for row in census['files'] if row['file'].startswith('supabase/migrations/'))
raw,sql=source(MONEY/migration['sourcePath']);assert sha(raw)==migration['rawSha256']
columns=[]
for row in pglast.parse_sql(sql):
    if not isinstance(row.stmt,ast.AlterTableStmt):continue
    relation=row.stmt.relation.schemaname+'.'+row.stmt.relation.relname
    for command in row.stmt.cmds:
        if isinstance(command.def_,ast.ColumnDef):
            columns.append({'relation':relation,'name':command.def_.colname,'type':col_type(command.def_)})
assert len(columns)==13 and len({(r['relation'],r['name']) for r in columns})==13
ddl_path='supabase/migrations/13102026_1344_weekly_source_self_bill_report_schema.sql'
ddl_raw,ddl_sql=source(ROOT/ddl_path)
created=[]
for row in pglast.parse_sql(ddl_sql):
    if not isinstance(row.stmt,ast.CreateStmt):continue
    name=row.stmt.relation.schemaname+'.'+row.stmt.relation.relname
    if name!='public.weekly_source_self_bill_reports':continue
    created.append({'relation':name,'path':ddl_path,'rawSha256':sha(ddl_raw),
                    'columns':[{'name':col.colname,'type':col_type(col)} for col in row.stmt.tableElts if isinstance(col,ast.ColumnDef)]})
assert len(created)==1
result={'version':'SAME_SEPARATE_PG17_SOURCE_COLUMN_AST_V1','parser':pglast.__version__,'postgresqlGrammar':pglast.get_postgresql_version(),
        'migration':{'path':migration['file'],'rawSha256':migration['rawSha256'],'addedColumns':columns},
        'currentDDLRelations':created,'native':'NOT_RUN','actualCatalogueCompilation':'NOT_PROVED'}
(HERE/'SCHEMA_AST.json').write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'addedColumns':len(columns),'currentDDLRelations':len(created),'grammar':'PASS','native':'NOT_RUN'}))
