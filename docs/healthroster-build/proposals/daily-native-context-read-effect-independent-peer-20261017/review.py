"""Independent finite source review. Parses text; never opens a database/network."""
from pathlib import Path
import contextlib, copy, hashlib, io, json, re, runpy, shutil, sys, tempfile

sys.dont_write_bytecode = True
sys.path.insert(0, 'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
import pglast

HERE = Path(__file__).resolve().parent
FROZEN = HERE / 'frozen'
OWN = Path('D:/Development/GitHub/.codex-worktrees/healthroster-checklist-native-14102026').resolve()
assert HERE.is_relative_to(OWN), 'OWN_PHYSICAL_D_REQUIRED'


def read(name):
    return json.loads((FROZEN / name).read_text(encoding='utf-8'))


def require(value, code):
    if not value:
        raise AssertionError(code)


def walk(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def name(parts):
    return '.'.join(part['String']['sval'] for part in parts)


def parse_sql_fragment(query):
    if ':=' in query[:100] and not re.match(r'^\s*(select|with|insert|update|delete)\b', query, re.I):
        query = query.split(':=', 1)[1]
    try:
        return json.loads(pglast.parser.parse_sql_json(query))
    except pglast.parser.ParseError:
        return json.loads(pglast.parser.parse_sql_json('SELECT ' + query))


def source_trees(raw):
    outer = json.loads(pglast.parser.parse_sql_json(raw))
    require(len(outer['stmts']) == 1, 'EXACT_ONE_COMPLETE_OWNER')
    statement = outer['stmts'][0]['stmt']
    function = statement.get('CreateFunctionStmt')
    if function is None:
        require('ViewStmt' in statement, 'FUNCTION_OR_VIEW_ONLY')
        return [outer], None
    options = {item['DefElem']['defname']: item['DefElem']['arg'] for item in function['options']}
    language = options['language']['String']['sval']
    if language == 'sql':
        return [parse_sql_fragment(options['as']['List']['items'][0]['String']['sval'])], function
    require(language == 'plpgsql', 'EXACT_REVIEWED_LANGUAGE')
    pl_tree = json.loads(pglast.parser.parse_plpgsql_json(raw))
    return [parse_sql_fragment(node['PLpgSQL_expr']['query']) for node in walk(pl_tree) if 'PLpgSQL_expr' in node], function


def scoped_ranges(tree, inherited=frozenset()):
    """Resolve CTE names per SelectStmt and per WITH definition, never per owner."""
    result = []
    def visit(value, visible):
        if isinstance(value, list):
            for child in value:
                visit(child, visible)
            return
        if not isinstance(value, dict):
            return
        if 'RangeVar' in value:
            relation = value['RangeVar']
            schema, relation_name = relation.get('schemaname'), relation['relname']
            if schema is None and relation_name in visible:
                result.append({'name': relation_name, 'kind': 'CTE'})
            else:
                result.append({'name': (schema or 'public') + '.' + relation_name, 'kind': 'PHYSICAL_OR_STORED_VIEW'})
            return
        if 'SelectStmt' in value:
            select = value['SelectStmt']
            clause = select.get('withClause', {})
            definitions = [item['CommonTableExpr'] for item in clause.get('ctes', [])]
            all_names = frozenset(item['ctename'] for item in definitions)
            previous = frozenset()
            for definition in definitions:
                visit(definition['ctequery'], visible | (all_names if clause.get('recursive') else previous))
                previous |= {definition['ctename']}
            for key, child in select.items():
                if key != 'withClause':
                    visit(child, visible | all_names)
            return
        for child in value.values():
            visit(child, visible)
    visit(tree, inherited)
    return result


def calls(trees):
    return [node['FuncCall'] for tree in trees for node in walk(tree) if 'FuncCall' in node]


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


selection = read('SOURCE_SELECTION.json')
original_ast = read('AST_CENSUS.json')
original_plan = read('EFFECT_CLOSURE.json')
ast_module = runpy.run_path(str(FROZEN / 'ast-census.py'))
plan_module = runpy.run_path(str(FROZEN / 'effect-plan.py'))
actual_ast = ast_module['run']()
require(actual_ast == original_ast, 'REPARSED_COMPLETE_CENSUS_MUST_EQUAL_FROZEN')
require(plan_module['classify'](actual_ast) == original_plan, 'RECLASSIFIED_PLAN_MUST_EQUAL_FROZEN')

# Replay the unchanged author's three programmes only in a fresh owned scratch.
# The author's main blocks write evidence, so they never run in frozen/ or Money/I2.
scratch_parent = OWN / '.codex-tmp'
scratch_parent.mkdir(exist_ok=True)
require(scratch_parent.resolve().is_relative_to(OWN), 'SCRATCH_PARENT_PHYSICAL_OWN_ONLY')
with tempfile.TemporaryDirectory(prefix='context-peer-', dir=scratch_parent) as temp:
    scratch = Path(temp).resolve()
    require(scratch.is_relative_to(scratch_parent.resolve()), 'SCRATCH_OWN_ONLY')
    for filename in ['ast-census.py', 'effect-plan.py', 'test-effect-plan.py', 'SOURCE_SELECTION.json']:
        shutil.copyfile(FROZEN / filename, scratch / filename)
    for selected in selection['captures']:
        target = scratch / selected['capture']
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(FROZEN / selected['capture'], target)
    with contextlib.redirect_stdout(io.StringIO()):
        for filename in ['ast-census.py', 'effect-plan.py', 'test-effect-plan.py']:
            runpy.run_path(str(scratch / filename), run_name='__main__')
    for filename in ['AST_CENSUS.json', 'EFFECT_CLOSURE.json', 'TEST_RESULTS.json']:
        require(json.loads((scratch / filename).read_text(encoding='utf-8')) == read(filename), 'UNCHANGED_AUTHOR_REPLAY:' + filename)

owners, trees_by_owner, function_arity, function_parameters, reviewed_calls = [], {}, {}, {}, []
for row in selection['captures']:
    # Preserve CRLF in the raw proof; grammar parsing is separate from byte identity.
    raw = (FROZEN / row['capture']).read_bytes()
    require(hashlib.sha256(raw).hexdigest() == row['statementSha256'] and len(raw) == row['bytes'], 'RAW_CAPTURE_IDENTITY')
    trees, function = source_trees(raw.decode('utf-8'))
    trees_by_owner[row['identity']] = trees
    lexical = sorted({r['name'] for tree in trees for r in scoped_ranges(tree) if r['kind'] == 'PHYSICAL_OR_STORED_VIEW'})
    candidate_owner = next(item for item in actual_ast['owners'] if item['identity'] == row['identity'])
    require(lexical == sorted(set(candidate_owner['intendedPhysicalRelations'])), 'ACTUAL_FRAGMENT_SCOPED_RELATIONS_DIFFER:' + row['identity'] + ':' + canonical({'peer': lexical, 'candidate': candidate_owner['intendedPhysicalRelations']}))
    if function is not None:
        parameters = [item['FunctionParameter'] for item in function.get('parameters', [])]
        inputs = [item for item in parameters if item.get('mode', 'FUNC_PARAM_DEFAULT') not in ['FUNC_PARAM_OUT', 'FUNC_PARAM_TABLE']]
        maximum = len(inputs)
        minimum = len([item for item in inputs if 'defexpr' not in item])
        identity_name = name(function['funcname'])
        require(identity_name not in function_arity, 'DUPLICATE_SELECTED_FUNCTION_NAME')
        function_arity[identity_name] = [minimum, maximum]
        function_parameters[identity_name] = [item.get('name') for item in inputs]
    owner_calls = calls(trees)
    reviewed_calls.extend({'owner': row['identity'], 'call': name(call['funcname']), 'arity': len(call.get('args', [])), 'node': call} for call in owner_calls)
    owners.append({'identity': row['identity'], 'sqlFragments': len(trees), 'lexicalRelations': lexical, 'callSites': len(owner_calls), 'statementSha256': row['statementSha256']})
require(len(owners) == 18 and len({r['identity'] for r in owners}) == 18, 'EXACT_UNIQUE18_OWNER_SET')
require(sum(item['sqlFragments'] for item in owners) == 283, 'EXACT283_FRAGMENTS')

def validate_selected_arities(call_rows):
    for call in call_rows:
        if call['call'] in function_arity:
            minimum, maximum = function_arity[call['call']]
            require(minimum <= call['arity'] <= maximum, 'UNREVIEWED_SELECTED_CALL_ARITY')


def validate_finance_call(call_rows):
    finance = [item for item in call_rows if item['call'] == 'public._pay_outstanding_components']
    require(len(finance) == 1, 'EXACT_ONE_FINANCE_SOURCE_CALL')
    call = finance[0]
    require(call['owner'] == 'public.v_timesheets_summary_base' and call['arity'] == 1, 'EXACT_FINANCE_OWNER_AND_ONE_ARGUMENT')
    require(not call['node'].get('func_variadic'), 'NO_VARIADIC_FINANCE_REBINDING')
    def omit_location(value):
        if isinstance(value, list):
            return [omit_location(item) for item in value]
        if isinstance(value, dict):
            return {key: omit_location(item) for key, item in value.items() if key != 'location'}
        return value
    require(omit_location(call['node']['args'][0]) == {'A_ArrayExpr': {'elements': [{'ColumnRef': {'fields': [{'String': {'sval': 'pt'}}, {'String': {'sval': 'timesheet_id'}}]}}]}}, 'EXACT_PT_TIMESHEET_ARRAY_ARGUMENT')
    return {'owner': call['owner'], 'name': call['call'], 'arity': 1, 'argument': 'ARRAY[pt.timesheet_id]', 'resolution': 'SOURCE_ONLY_ACTUAL_STORED_VIEW_AND_OVERLOAD_METADATA_REQUIRED'}


validate_selected_arities(reviewed_calls)
finance_call = validate_finance_call(reviewed_calls)

cases = []
def check(label, fn):
    fn()
    cases.append({'case': label, 'status': 'PASS', 'native': 'NOT_RUN'})


def refuses(fn):
    try:
        fn()
    except (AssertionError, ValueError):
        return
    raise AssertionError('EXPECTED_REFUSAL')


def classify_query(query):
    census = copy.deepcopy(actual_ast)
    census['owners'][0].update(ast_module['census']([parse_sql_fragment(query)]))
    return plan_module['classify'](census)


def physical(query):
    return sorted({r['name'] for r in scoped_ranges(parse_sql_fragment(query)) if r['kind'] == 'PHYSICAL_OR_STORED_VIEW'})


check('18 actual fragment-scoped relation sets equal author census', lambda: require(len(owners) == 18, 'OWNER_COUNT'))
check('actual selected helper call arities respect complete default argument declarations', lambda: validate_selected_arities(reviewed_calls))
check('actual finance call is exactly one ARRAY[pt.timesheet_id], not hardcoded output evidence', lambda: validate_finance_call(reviewed_calls))

for label, query, expected in [
    ('same SELECT real CTE is not a heap', 'WITH visible AS (SELECT 1) SELECT * FROM visible', []),
    ('qualified heap is not hidden by equal CTE name', 'WITH timesheets AS (SELECT 1) SELECT * FROM public.timesheets', ['public.timesheets']),
    ('later CTE name does not shadow earlier nonrecursive definition', 'WITH first_cte AS (SELECT * FROM unreviewed_relation), unreviewed_relation AS (SELECT 1) SELECT * FROM first_cte', ['public.unreviewed_relation']),
    ('nested CTE name does not escape into outer range', 'SELECT * FROM unreviewed_relation WHERE EXISTS (WITH unreviewed_relation AS (SELECT 1) SELECT * FROM unreviewed_relation)', ['public.unreviewed_relation']),
    ('recursive WITH self name is scoped', 'WITH RECURSIVE chain AS (SELECT 1 UNION ALL SELECT 1 FROM chain) SELECT * FROM chain', []),
    ('nested CTE shadows an outer visible CTE only within its query', 'WITH chain AS (SELECT * FROM public.timesheets) SELECT * FROM chain WHERE EXISTS (WITH chain AS (SELECT * FROM public.clients) SELECT * FROM chain)', ['public.clients', 'public.timesheets']),
]:
    check(label, lambda query=query, expected=expected: require(physical(query) == expected, 'LEXICAL_CTE_RANGE_MISMATCH'))

check('independent SQL fragments cannot inherit another fragment CTE name', lambda: require(
    sorted({r['name'] for query in ['WITH hidden AS (SELECT 1) SELECT * FROM hidden', 'SELECT * FROM hidden'] for r in scoped_ranges(parse_sql_fragment(query)) if r['kind'] == 'PHYSICAL_OR_STORED_VIEW'}) == ['public.hidden'], 'CROSS_FRAGMENT_SCOPE'))

for label, query in [
    ('nested DELETE remains an effect', 'WITH x AS (DELETE FROM public.timesheets RETURNING timesheet_id) SELECT * FROM x'),
    ('SELECT FOR SHARE remains a lock', 'SELECT * FROM public.timesheets FOR SHARE'),
    ('SELECT INTO remains a new relation', 'SELECT 1 INTO public.peer_unreviewed'),
    ('set_config remains a session effect', "SELECT pg_catalog.set_config('role','service_role',false)"),
    ('sequence mutation remains held', "SELECT pg_catalog.nextval('peer_unreviewed')"),
    ('unreviewed shadow digest remains held', "SELECT public.digest('x','sha256')"),
]:
    check(label, lambda query=query: refuses(lambda: classify_query(query)))

finance_mutations = [
    ('second finance argument cannot inherit one-argument proof', 'SELECT * FROM public._pay_outstanding_components(ARRAY[pt.timesheet_id],NULL::uuid)'),
    ('different finance array column cannot inherit fixture binding', 'SELECT * FROM public._pay_outstanding_components(ARRAY[pt.other_id])'),
    ('whole caller array cannot replace exact per-row argument', 'SELECT * FROM public._pay_outstanding_components(p_timesheet_ids)'),
    ('extra array member cannot broaden exact row scope', 'SELECT * FROM public._pay_outstanding_components(ARRAY[pt.timesheet_id,NULL::uuid])'),
    ('named argument cannot replace exact captured expression', 'SELECT * FROM public._pay_outstanding_components(p_timesheet_ids => ARRAY[pt.timesheet_id])'),
    ('empty typed array cannot inherit fixture source binding', 'SELECT * FROM public._pay_outstanding_components(ARRAY[]::uuid[])'),
]
for label, query in finance_mutations:
    def assert_finance_refusal(query=query):
        parsed_calls = calls([parse_sql_fragment(query)])
        values = [{'owner': 'public.v_timesheets_summary_base', 'call': name(item['funcname']), 'arity': len(item.get('args', [])), 'node': item} for item in parsed_calls]
        refuses(lambda: validate_finance_call(values))
    check(label, assert_finance_refusal)

def bad_helper_arity():
    copied = copy.deepcopy(reviewed_calls)
    item = next(row for row in copied if row['call'] == 'private._candidate_normalize_email')
    item['arity'] = 2
    refuses(lambda: validate_selected_arities(copied))
check('selected helper name does not admit an unreviewed second argument', bad_helper_arity)

# Demonstrate finite limitations of the original reusable classifier. These
# admissions do not alter or disprove the actual frozen-source graph above.
limitations = []
for label, queries in [
    ('CTE name leakage between PL SQL fragments', ['WITH unreviewed_relation AS (SELECT 1) SELECT * FROM unreviewed_relation', 'SELECT * FROM unreviewed_relation']),
    ('nested CTE name leakage into outer range', ['SELECT * FROM unreviewed_relation WHERE EXISTS (WITH unreviewed_relation AS (SELECT 1) SELECT * FROM unreviewed_relation)']),
    ('finance second argument admitted by name-only classifier', ['SELECT * FROM public._pay_outstanding_components(ARRAY[pt.timesheet_id],NULL::uuid)']),
    ('selected helper extra argument admitted by name-only classifier', ["SELECT private._candidate_normalize_email('x','unexpected')"]),
]:
    census = copy.deepcopy(actual_ast)
    census['owners'][0].update(ast_module['census']([parse_sql_fragment(query) for query in queries]))
    admitted = plan_module['classify'](census)
    require(admitted['nativePurity'] == 'NOT_PROVED', 'NO_NATIVE_SUCCESS_FROM_LIMITATION')
    limitations.append({'counterexample': label, 'syntheticSql': queries, 'candidateClassifiedAndHeld': True, 'nativePurity': admitted['nativePurity'], 'actualFrozenInputAffected': False})

top = (FROZEN / 'reference/public_tsfin_load_context_batch.sql').read_text(encoding='utf-8')
summary = (FROZEN / 'reference/public_v_timesheets_summary_base.sql').read_text(encoding='utf-8')
top_ast = next(owner for owner in owners if owner['identity'] == 'public.tsfin_load_context_batch(uuid[])')
check('actual top loader client identity reads hospital mapping and has no clients relation', lambda: require('public.client_hospitals' in top_ast['lexicalRelations'] and 'public.clients' not in top_ast['lexicalRelations'], 'NO_CLIENT_NAME_FALLBACK'))
check('summary has both actual finance reader and invoice-precheck view boundary', lambda: require('public.v_ts_invoice_precheck' in physical(summary) and finance_call['name'] == 'public._pay_outstanding_components', 'BOTH_FINANCE_BOUNDARIES'))
check('PDF routines default volatility is not inherited from STABLE parent', lambda: require(all('volatility' not in next(r for r in actual_ast['owners'] if r['identity'] == identity)['options'] for identity in ['public.timesheet_pdf_reference_rows(uuid)', 'public.timesheet_pdf_reference_sig(uuid)']), 'PDF_DEFAULT_VOLATILITY'))
check('public context loader has no actor parameter and grants no actor admission', lambda: require(function_parameters['public.tsfin_load_context_batch'] == ['p_timesheet_ids'] and function_arity['public.tsfin_load_context_batch'] == [1, 1], 'NO_NEW_ADMISSION'))

finance_metadata = read('reference/current-native-finance-boundary-observation.json')
require(finance_metadata['sourceOrFunctionBodyRead'] is False and finance_metadata['functionsInvoked'] is False, 'METADATA_ONLY_FINANCE_BOUNDARY')
require(original_plan['nativeInvocation'] == 'NOT_RUN' and original_plan['nativePurity'] == 'NOT_PROVED' and original_plan['runtimeTargets'] == [], 'NO_RUNTIME_OR_NATIVE_CLAIM')

result = {
    'version': 'DAILY_CONTEXT_READ_EFFECT_INDEPENDENT_PEER_V1',
    'candidateCommit': '28457faeab3269ab65dbd2df92f6b04ee074cb41',
    'candidateManifestSha256': '0977b1340702d8a1ec0a752be064801fee2af368b138a8a4b2381d793e252487',
    'disposition': 'ACCEPT_BOUNDED_SOURCE_PLAN_WITH_REUSABLE_CLASSIFIER_LIMITATIONS',
    'parser': {'pglast': pglast.__version__, 'postgresql': list(pglast.get_postgresql_version()), 'sqlFragments': 283, 'selectedOwners': 18, 'callSites': len(reviewed_calls)},
    'originalReplay': {'programmes': 3, 'counterexamples': 20, 'allFrozenJsonEqual': True},
    'owners': owners,
    'selectedFunctionSourceArities': function_arity,
    'selectedFunctionInputNames': function_parameters,
    'actualFinanceCall': finance_call,
    'independentCases': cases,
    'independentTests': len(cases),
    'reusableGuardLimitations': limitations,
    'native': 'NOT_RUN',
    'nativeReadEffect': 'NOT_PROVED',
    'actualStoredViewEquivalence': 'NOT_PROVED',
    'financeOwnerClosure': 'REQUIRED_NOT_PROVIDED_IN_THIS_LANE',
    'bankBodiesRead': 0,
    'runtimeTargets': [],
    'rootWrites': 0,
}
print(json.dumps(result, indent=2, ensure_ascii=False))
