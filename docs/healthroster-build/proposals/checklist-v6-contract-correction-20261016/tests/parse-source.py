"""Offline PG17 grammar/declared-field checks only. Never opens a database connection."""
import argparse,json,re,sys
from pathlib import Path
args=argparse.ArgumentParser()
args.add_argument('--parser-path',required=True)
options=args.parse_args()
sys.path.insert(0,options.parser_path)
from pglast import parser
if parser.get_postgresql_version()[0]!=17:raise RuntimeError('PostgreSQL 17 grammar required')
base=Path('\\\\?\\'+str(Path(__file__).resolve().parent.parent))
root=base.parents[3]
contract=json.loads((root.parent/'healthroster-install2-14102026/supabase/release/current-contract.json').read_text(encoding='utf-8'))
columns={r['schema']+'.'+r['name']:{c['name'] for c in r['columns']} for r in contract['relations']}
for statement in parser.parse_sql((base/'forward/sql/physical-schema.sqlinc').read_text(encoding='utf-8')):
 node=statement.stmt
 if type(node).__name__=='CreateStmt':columns[node.relation.schemaname+'.'+node.relation.relname]={c.colname for c in node.tableElts if type(c).__name__=='ColumnDef'}
count=0
for file in sorted((base/'forward/sql').glob('*.sqlinc')):
 source=file.read_text(encoding='utf-8');parser.parse_sql(source)
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
