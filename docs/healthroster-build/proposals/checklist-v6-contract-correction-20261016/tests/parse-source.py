"""Offline PG17 grammar/declared-field checks only. Never opens a database connection."""
import argparse,json,re,sys
from pathlib import Path
args=argparse.ArgumentParser()
args.add_argument('--parser-path',required=True)
args.add_argument('--sql-file',action='append',help='Optional explicit SQL leaf file; repeat for several owner files.')
args.add_argument('--schema-file',action='append',default=[],help='Additional saved DDL containing new owner tables; read only.')
args.add_argument('--contract-file',help='Explicit data-free current-contract.json; defaults to current integration worktree.')
options=args.parse_args()
sys.path.insert(0,options.parser_path)
from pglast import parser
if parser.get_postgresql_version()[0]!=17:raise RuntimeError('PostgreSQL 17 grammar required')
base=Path('\\\\?\\'+str(Path(__file__).resolve().parent.parent))
root=base.parents[3]
contract_path=Path(options.contract_file) if options.contract_file else root.parent/'healthroster-install2-14102026/supabase/release/current-contract.json'
contract=json.loads(contract_path.read_text(encoding='utf-8'))
columns={r['schema']+'.'+r['name']:{c['name'] for c in r['columns']} for r in contract['relations']}
def sql_source(file):
 # psql directives are client transport, not server SQL. No include expansion,
 # variable interpolation or conditional execution is implied by offline parsing.
 return re.sub(r'(?m)^[ \t]*\\[^\r\n]*(?:\r?\n|$)','',Path(file).read_text(encoding='utf-8'))
for schema_file in [base/'forward/sql/physical-schema.sqlinc',*(Path(p) for p in options.schema_file)]:
 for statement in parser.parse_sql(sql_source(schema_file)):
  node=statement.stmt
  if type(node).__name__=='CreateStmt':columns[node.relation.schemaname+'.'+node.relation.relname]={c.colname for c in node.tableElts if type(c).__name__=='ColumnDef'}
  if type(node).__name__=='AlterTableStmt':
   relation=node.relation.schemaname+'.'+node.relation.relname
   for command in node.cmds:
    if command.subtype.name=='AT_AddColumn':columns.setdefault(relation,set()).add(command.def_.colname)
    if command.subtype.name=='AT_DropColumn':columns.setdefault(relation,set()).discard(command.name)
count=0
files=[Path(p) for p in options.sql_file] if options.sql_file else sorted((base/'forward/sql').glob('*.sqlinc'))
for file in files:
 source=sql_source(file);parser.parse_sql(source)
 for body in re.findall(r'create or replace function .*?\$function\$;',source,re.I|re.S):
  if re.search(r'language\s+plpgsql',body,re.I):
   # Offline libpg_query cannot resolve catalog %ROWTYPE; this is grammar, not native compilation.
   parser.parse_plpgsql_json(re.sub(r'\b(?:public|private)\.\w+%rowtype\b','record',body,flags=re.I))
  for variable,relation in re.findall(r'\b(\w+)\s+((?:public|private)\.\w+)%rowtype',body,re.I):
   if relation not in columns:raise RuntimeError('Unknown native/proposed rowtype '+relation)
   for field in re.findall(r'\b'+re.escape(variable)+r'\.(\w+)',body,re.I):
    if field not in columns[relation]:raise RuntimeError(f'{file.name}: unknown {relation}.{field}')
   count+=1
 print(file.name+': grammar parsed')
print(f'{count} declared native/proposed row variables checked against data-free source columns. Native compile/first-use NOT_RUN.')
if not options.sql_file:
 for scalar_file in sorted([*(base/'tests').glob('*native-scalars.sql'),base/'tests/evidence-compatibility.sql']):
  scalar_tests=scalar_file.read_text(encoding='utf-8')
  parser.parse_sql(scalar_tests);parser.parse_plpgsql_json(scalar_tests)
  print(scalar_file.name+': SQL/DO grammar parsed; assertions NOT_RUN (root rollback rehearsal only).')
