"""Offline source/PG17 AST and actual closed-contract checks. No database."""
from pathlib import Path
import importlib.util
import json
import re
import subprocess
import sys
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
OWN = HERE.parents[3]
ROOT = OWN.parent / 'healthroster-install2-14102026'
PARSER = Path('D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
assert PARSER.is_dir() and str(PARSER.resolve()).replace('\\', '/').startswith('D:/Development/')
sys.dont_write_bytecode = True
sys.path.insert(0, str(PARSER))
import pglast
from pglast import ast

def save(name, value):
    (HERE / name).write_bytes((json.dumps(value, indent=2) + '\n').encode('utf-8'))

spec = importlib.util.spec_from_file_location('health_contract_assembly', HERE / 'assemble.py')
assembly = importlib.util.module_from_spec(spec)
spec.loader.exec_module(assembly)
state = assembly.verify()
save('ASSEMBLY_STATE.json', state)
raw_reader = Path.read_bytes
victim = ROOT / 'broker/src/contract-correction-v3.mjs'
def drift(path):
    return raw_reader(path) + b'\n// unrelated drift' if path == victim else raw_reader(path)
with patch.object(Path, 'read_bytes', drift):
    try:
        assembly.verify()
        raise RuntimeError('Changed current owner must refuse')
    except AssertionError:
        pass
owner_path = HERE / 'OWNER_BLOCKS.json'
def pretend_bound(path):
    raw = raw_reader(path)
    if path == owner_path:
        obj = json.loads(raw)
        for row in obj['bindings'].values():
            row.update(status='AVAILABLE', publishedContract={'flag': True}, publishedCallBlock={'symbol': 'pretend'})
        return json.dumps(obj).encode()
    return raw
with patch.object(Path, 'read_bytes', pretend_bound):
    try:
        assembly.verify()
        raise RuntimeError('A symbol/flag is not an owner interface')
    except AssertionError:
        pass
attempt = subprocess.run([sys.executable, str(HERE / 'assemble.py'), '--emit', str(HERE / 'forbidden-runtime.sql')], cwd=OWN, capture_output=True)
assert attempt.returncode == 2 and not (HERE / 'forbidden-runtime.sql').exists()

entries = []
forward = []
for directory in ['one-time', 'health', 'templates', 'verification', 'reverse']:
    for file in sorted((HERE / directory).iterdir()):
        source = file.read_text(encoding='utf-8')
        grammar = re.sub(r'@@BANK_[A-Z_]+@@', "raise exception 'UNBOUND_OWNER_GRAMMAR_ONLY' using errcode='55000';", source)
        sql = pglast.parse_sql(grammar)
        pl = pglast.parse_plpgsql(grammar) if directory != 'reverse' else []
        entries.append({'file': file.relative_to(HERE).as_posix(), 'statements': len(sql), 'plpgsqlBodies': len(pl),
            'nativeExecuted': False, 'unprovidedBlocksGrammarRefusalOnly': directory == 'templates'})
        if directory != 'reverse':
            for item in sql:
                if isinstance(item.stmt, ast.CreateFunctionStmt):
                    name = '.'.join(x.sval for x in item.stmt.funcname)
                    forward.append(name)
                    opts = {v.defname: v.arg for v in item.stmt.options}
                    assert opts['security'].boolval and 'search_path' in str(opts['set'])
        else:
            reverse = ['.'.join(x.sval for x in item.stmt.objects[0].objname) for item in sql]
assert len(forward) == 7 and set(forward) == set(reverse)

def expression(source, variable):
    hit = re.search(r'\b' + variable + r':=(.*?);\n', source, re.S)
    assert hit, variable
    return pglast.parse_sql('select ' + hit[1])[0].stmt.targetList[0].val

def evaluate(node, context):
    """Evaluate only pure expressions from actual authored assignment ASTs."""
    if isinstance(node, ast.A_Const):
        if node.isnull:
            return None
        if isinstance(node.val, ast.String):
            return node.val.sval
        if isinstance(node.val, ast.Boolean):
            return node.val.boolval
        if isinstance(node.val, ast.Integer):
            return node.val.ival
        raise TypeError(type(node.val))
    if isinstance(node, ast.ColumnRef):
        names = [field.sval for field in node.fields]
        value = context[names[0]]
        for name in names[1:]:
            value = value[name]
        return value
    if isinstance(node, ast.TypeCast):
        value = evaluate(node.arg, context)
        name = node.typeName.names[-1].sval
        if name == 'jsonb':
            return json.loads(value)
        if name == 'text':
            return None if value is None else str(value)
        raise TypeError(name)
    if isinstance(node, ast.A_ArrayExpr):
        return [evaluate(child, context) for child in node.elements]
    if isinstance(node, ast.CaseExpr):
        for branch in node.args:
            if evaluate(branch.expr, context):
                return evaluate(branch.result, context)
        return evaluate(node.defresult, context) if node.defresult else None
    if isinstance(node, ast.A_Expr):
        left, right = evaluate(node.lexpr, context), evaluate(node.rexpr, context)
        operator = node.name[0].sval
        if node.kind.name == 'AEXPR_OP_ANY':
            assert operator == '='
            return left in right
        if operator == '=':
            return left == right
        if operator in ['->', '->>']:
            value = left.get(right) if isinstance(left, dict) else left[right]
            return value if operator == '->' or value is None else str(value)
        raise TypeError(operator)
    if isinstance(node, ast.FuncCall):
        args = [evaluate(child, context) for child in node.args]
        name = node.funcname[-1].sval
        if name == 'array_to_string':
            return args[1].join(value for value in args[0] if value is not None)
        if name == 'jsonb_build_object':
            return dict(zip(args[::2], args[1::2]))
        raise TypeError(name)
    raise TypeError(type(node))

invoice_source = (HERE / 'health/01_projection.sqlinc').read_text(encoding='utf-8')
invoice_ast = expression(invoice_source, 'effect')
expected = {
    'PRESERVE_SOURCE': 'Source invoice history remains unchanged.',
    'NO_INVOICE': 'Uninvoiced work retains its native collection gates.',
    'EDIT_DRAFT': 'Selected invoice Draft lines will be updated.',
    'RELOCATE_DRAFT': 'Selected invoice Draft lines require native relocation.',
    'REPLACE_ISSUED': 'Original invoice lines require native credits and replacement Drafts.',
}
invoice_vectors = []
for chosen in [[key] for key in expected] + [list(expected), ['PRESERVE_SOURCE', 'REPLACE_ISSUED'], ['NO_INVOICE', 'EDIT_DRAFT', 'RELOCATE_DRAFT']]:
    actual = evaluate(invoice_ast, {'dispositions': chosen})
    assert actual == ' '.join(text for key, text in expected.items() if key in chosen)
    invoice_vectors.append({'dispositions': chosen, 'effect': {'visibility': 'FULL', 'text': actual}})

reader = (HERE / 'templates/30_current_result.sql.tmpl').read_text(encoding='utf-8')
placement_ast = expression(reader, 'placement')
result_ast = expression(reader, 'result')
identifier = lambda n: f'10000000-0000-0000-0000-{n:012d}'
vectors = []
for state_name, status, current, receipt in [
    ('REVIEWED', 'NOT_COMMITTED', identifier(1), None),
    ('COMPLETE', 'COMMITTED', identifier(2), identifier(20)),
    ('SUPERSEDED', 'COMMITTED', identifier(3), identifier(30)),
    ('PENDING_PLACEMENT', 'NATIVE_PENDING', None, None),
]:
    for visible in [False, True]:
        context = {'status': status, 'original': identifier(1),
            'current_placement': {'current_timesheet_id': current, 'placement_revision': '9007199254740993', 'placement_receipt_id': receipt},
            'o': {'id': identifier(10), 'state_revision': 9007199254740995}, 'result_state': state_name,
            'bank_financial': {'status': 'REGISTERED', 'command_id': identifier(40) if visible else None,
                'cut_id': identifier(41) if visible else None, 'summary': {'visibility': 'FULL', 'text': 'Native registration remains separate from payment.'} if visible else {'visibility': 'REDACTED'}},
            'billing': {'status': 'PREPARING_DRAFTS', 'revision': '9223372036854775807',
                'group_id': identifier(50) if visible else None, 'visible_summary': {'visibility': 'FULL', 'text': 'The native invoice job is preparing the correction Drafts.'} if visible else {'visibility': 'REDACTED'}},
            'bank_actions': [], 'bank_links': []}
        context['placement'] = evaluate(placement_ast, context)
        value = evaluate(result_ast, context)
        assert value['state_revision'] == '9007199254740995' and value['placement']['placement_revision'] == '9007199254740993'
        assert value['placement']['current_timesheet_id'] == current and value['placement']['original_timesheet_id'] == identifier(1)
        assert set(value) == {'version', 'purpose', 'operation_id', 'state', 'state_revision', 'placement', 'financial', 'billing', 'next_actions', 'links'}
        vectors.append({'visible': visible, 'retainedNativePending': state_name == 'PENDING_PLACEMENT', 'result': value})
save('AST_PROJECTION_VECTORS.json', {'scope': 'ACTUAL_AUTHORED_PURE_ASSIGNMENT_AST_ONLY_INDEPENDENT_FIXTURES',
    'invoice': invoice_vectors, 'results': vectors, 'nativeAuthorityProved': False})
node = subprocess.run(['node', str(HERE / 'protocol-vectors.mjs')], cwd=OWN, capture_output=True, text=True, encoding='utf-8')
assert node.returncode == 0, node.stderr
current = subprocess.run(['node', '--test', '--test-reporter=tap', 'tests/contract-correction-v3.test.mjs'], cwd=ROOT, capture_output=True, text=True, encoding='utf-8')
assert current.returncode == 0, current.stderr
save('OFFLINE_RESULTS.json', {'assembly': state, 'sourceDriftRefuses': True, 'pretendOwnerFlagRefuses': True,
    'runtimeEmissionRefusesBeforeWrite': True, 'parserVersion': pglast.__version__, 'postgresGrammar': pglast.get_postgresql_version(),
    'programs': entries, 'exactAdditiveForwardReverse': True, 'invoiceAssignmentASTVectors': len(invoice_vectors),
    'resultAssignmentASTVectors': len(vectors), 'actualProtocolVectors': json.loads(node.stdout),
    'actualExistingContractTestExitCode': current.returncode,
    'actualExistingContractTestSummary': [line for line in current.stdout.splitlines() if line.startswith('# tests') or line.startswith('# pass') or line.startswith('# fail')],
    'native': 'NOT_RUN', 'API': 'NOT_RUN', 'inApp': 'NOT_RUN', 'runtimeSQLFilesEmitted': 0})
print('PASS: actual Health source pins/reversals, PG17 grammar, assignment AST/closed-contract vectors; native joins HELD/NOT_RUN')
