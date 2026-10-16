"""Read-only PG17 grammar/declared-field check. No database or network.

Adapted from Root's reviewed contact-03b-grammar/check.py; adds exact input
hash checks, SQL-language function body parsing and DO body parsing.
"""
import hashlib
import json
import re
import sys
from pathlib import Path

base = Path(__file__).resolve().parent.parent
manifest_file = base / 'OFFLINE_GRAMMAR_INPUTS.json'
manifest_bytes = manifest_file.read_bytes()
inputs = json.loads(manifest_bytes)
sys.path.insert(0, inputs['parserPath'])
from pglast import parser

assert parser.get_postgresql_version()[0] == 17
errors = []
counts = {'sqlLeaves': 0, 'sqlStatements': 0, 'plpgsqlBodies': 0,
          'sqlLanguageBodies': 0, 'doBodies': 0, 'rowVariables': 0}


def load(item):
    data = Path(item['file']).read_bytes()
    if hashlib.sha256(data).hexdigest() != item['sha256']:
        raise ValueError('EXACT_INPUT_BYTES_CHANGED: ' + item['file'])
    return data.decode('utf-8')


def clean(source):
    return re.sub(r'(?m)^[ \t]*\\[^\r\n]*(?:\r?\n|$)', '', source)


def rowtype_to_record(source):
    return re.sub(r'\b(?:public|private)\.\w+%rowtype\b', 'record', source, flags=re.I)


contract = json.loads(load(inputs['contract']))
columns = {r['schema'] + '.' + r['name']: {c['name'] for c in r['columns']}
           for r in contract['relations']}
for item in inputs['schemaInputs'] + [i for i in inputs['inputs'] if i['source'].startswith('one-time/')]:
    try:
        for statement in parser.parse_sql(clean(load(item))):
            node = statement.stmt
            if type(node).__name__ == 'CreateStmt':
                columns[(node.relation.schemaname or 'public') + '.' + node.relation.relname] = {
                    c.colname for c in node.tableElts if type(c).__name__ == 'ColumnDef'}
            elif type(node).__name__ == 'AlterTableStmt':
                relation = (node.relation.schemaname or 'public') + '.' + node.relation.relname
                for command in node.cmds:
                    if command.subtype.name == 'AT_AddColumn':
                        columns.setdefault(relation, set()).add(command.def_.colname)
                    elif command.subtype.name == 'AT_DropColumn':
                        columns.setdefault(relation, set()).discard(command.name)
    except Exception as error:
        errors.append({'file': item['file'], 'kind': 'SCHEMA_GRAMMAR_OR_INPUT', 'error': str(error)})

for item in inputs['inputs']:
    try:
        source = clean(load(item))
    except Exception as error:
        errors.append({'file': item['source'], 'kind': 'INPUT_DRIFT', 'error': str(error)})
        continue
    counts['sqlLeaves'] += 1
    try:
        counts['sqlStatements'] += len(parser.parse_sql(source))
    except Exception as error:
        errors.append({'file': item['source'], 'kind': 'SQL_GRAMMAR', 'error': str(error)})
    for body in re.findall(r'create or replace function .*?\$function\$;', source, re.I | re.S):
        name = re.search(r'function\s+([^\s(]+)', body, re.I).group(1)
        if re.search(r'language\s+plpgsql\b', body, re.I):
            try:
                parser.parse_plpgsql_json(rowtype_to_record(body))
                counts['plpgsqlBodies'] += 1
            except Exception as error:
                errors.append({'file': item['source'], 'routine': name, 'kind': 'PLPGSQL_GRAMMAR', 'error': str(error)})
        elif re.search(r'language\s+sql\b', body, re.I):
            try:
                sql_body = re.search(r'as\s+\$function\$(.*?)\$function\$;', body, re.I | re.S).group(1)
                parser.parse_sql(sql_body)
                counts['sqlLanguageBodies'] += 1
            except Exception as error:
                errors.append({'file': item['source'], 'routine': name, 'kind': 'SQL_FUNCTION_GRAMMAR', 'error': str(error)})
        for variable, relation in re.findall(r'\b(\w+)\s+((?:public|private)\.\w+)%rowtype', body, re.I):
            counts['rowVariables'] += 1
            if relation not in columns:
                errors.append({'file': item['source'], 'routine': name, 'kind': 'UNKNOWN_ROWTYPE', 'relation': relation})
                continue
            for field in set(re.findall(r'\b' + re.escape(variable) + r'\.(\w+)', body, re.I)):
                if field not in columns[relation]:
                    errors.append({'file': item['source'], 'routine': name, 'kind': 'UNKNOWN_DECLARED_FIELD', 'relation': relation, 'field': field})
    for do in re.finditer(r'\bdo\s+(\$[a-zA-Z_]\w*\$|\$\$)[\s\S]*?\1\s*;', source, re.I):
        try:
            parser.parse_plpgsql_json(rowtype_to_record(do.group()))
            counts['doBodies'] += 1
        except Exception as error:
            errors.append({'file': item['source'], 'kind': 'DO_GRAMMAR', 'error': str(error)})

result = {'version': 'CONTACT_SOURCE_OFFLINE_PG17_RESULT_V1',
          'input_manifest_sha256': hashlib.sha256(manifest_bytes).hexdigest(),
          'qualification': 'OFFLINE_ONLY_NATIVE_COMPILE_AND_FIRST_USE_NOT_RUN',
          'counts': counts, 'errors': errors}
(base / 'OFFLINE_GRAMMAR_RESULT.json').write_text(json.dumps(result, indent=2) + '\n', encoding='utf-8')
print(json.dumps(result, indent=2))
sys.exit(1 if errors else 0)
