"""Independent exact CSE source proof. Parses only; no SQL execution."""
from pathlib import Path
import collections, copy, json, runpy, sys
sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
FROZEN = HERE / 'frozen'
author = runpy.run_path(str(FROZEN / 'ast-proof.py'))
original = author['run']()
assert original == json.loads((FROZEN / 'AST_PROOF.json').read_text(encoding='utf-8'))
parent = author['tree']('rendered/PARENT_CANONICAL.sql')
candidate = author['tree']('rendered/CANDIDATE_CANONICAL.sql')
strip, walk, names = author['strip'], author['walk'], author['names']


def require(value, code):
    if not value:
        raise ValueError(code)


def prove(after):
    # Include the whole CTE node, not just its selected expression. An output
    # column override or unused recursive/search/cycle attribute cannot hide.
    changed = copy.deepcopy(after)
    require(len(changed['stmts']) == 2, 'EXACT_TWO_STATEMENTS')
    query = changed['stmts'][1]['stmt']['SelectStmt']
    rows = query['withClause']['ctes']
    selected = [row for row in rows if row['CommonTableExpr']['ctename'] == 'background_statement_snapshot']
    require(len(selected) == 1, 'ONE_CSE_PRODUCER')
    cte = selected[0]['CommonTableExpr']
    expected = {'ctename': 'background_statement_snapshot', 'ctematerialized': 'CTEMaterializeAlways', 'ctequery': {'SelectStmt': {'targetList': [{'ResTarget': {'name': 'snapshot_value', 'val': {'FuncCall': {'funcname': [{'String': {'sval': 'pg_current_snapshot'}}], 'funcformat': 'COERCE_EXPLICIT_CALL'}}}}], 'limitOption': 'LIMIT_OPTION_DEFAULT', 'op': 'SETOP_NONE'}}}
    require(strip(cte) == expected, 'COMPLETE_CTE_NODE_REQUIRED')
    return author['prove'](parent, after)


proof = prove(candidate)
parent_counts = collections.Counter(names(row['FuncCall']['funcname']) for row in walk(parent) if 'FuncCall' in row)
candidate_counts = collections.Counter(names(row['FuncCall']['funcname']) for row in walk(candidate) if 'FuncCall' in row)
require(parent_counts['pg_current_snapshot'] == 5 and candidate_counts['pg_current_snapshot'] == 1, 'EXACT_FIVE_TO_ONE')
parent_counts.pop('pg_current_snapshot'); candidate_counts.pop('pg_current_snapshot')
require(parent_counts == candidate_counts, 'ALL_OTHER_CALL_COUNTS_UNCHANGED')
require(parent_counts['pg_xact_status'] == 2 and parent_counts['clock_timestamp'] > 0, 'REAL_VOLATILE_OBSERVATIONS_PRESERVED')

cases = []
def reject(label, edit):
    changed = copy.deepcopy(candidate)
    edit(changed)
    try:
        prove(changed)
    except (ValueError, KeyError, TypeError, IndexError):
        cases.append({'case': label, 'status': 'PASS', 'native': 'NOT_RUN'})
        return
    raise AssertionError('MUTATION_ACCEPTED:' + label)


def cte(tree):
    return next(row['CommonTableExpr'] for row in tree['stmts'][1]['stmt']['SelectStmt']['withClause']['ctes'] if row['CommonTableExpr']['ctename'] == 'background_statement_snapshot')


def first(tree, predicate):
    return next(row for row in walk(tree) if predicate(row))


reject('CTE output column override cannot hide in discarded producer', lambda t: cte(t).update(aliascolnames=[{'String': {'sval': 'other_value'}}]))
reject('CTE cycle/search metadata cannot hide in discarded producer', lambda t: cte(t).update(cycle_clause={'cycle_mark_column': 'unreviewed'}))
reject('extra CTE target changes the closed scalar producer', lambda t: cte(t)['ctequery']['SelectStmt']['targetList'].append(copy.deepcopy(cte(t)['ctequery']['SelectStmt']['targetList'][0])))
reject('CTE WHERE false cannot remove actual observation', lambda t: cte(t)['ctequery']['SelectStmt'].update(whereClause={'A_Const': {'boolval': {'boolval': False}}}))
reject('CTE is explicitly materialized', lambda t: cte(t).update(ctematerialized='CTEMaterializeNever'))
reject('CTE expression cannot cache a volatile transaction-status call', lambda t: cte(t)['ctequery']['SelectStmt']['targetList'][0]['ResTarget']['val']['FuncCall'].update(funcname=[{'String': {'sval': 'pg_xact_status'}}]))
reject('bound scalar cannot read a qualified different relation', lambda t: first(t, lambda n: n.get('relname') == 'background_statement_snapshot').update(schemaname='public'))
reject('background full row hash cannot be replaced by stable-only data', lambda t: first(t, lambda n: n.get('sval') == 'rowHash').update(sval='stableHash'))
reject('volatile status observations cannot be replaced', lambda t: first(t, lambda n: 'FuncCall' in n and names(n['FuncCall']['funcname']) == 'pg_xact_status')['FuncCall'].update(funcname=[{'String': {'sval': 'pg_current_snapshot'}}]))
reject('clock observation cannot become transaction start', lambda t: first(t, lambda n: 'FuncCall' in n and names(n['FuncCall']['funcname']) == 'clock_timestamp')['FuncCall'].update(funcname=[{'String': {'sval': 'transaction_timestamp'}}]))
reject('complete relation census cannot lose a background scope', lambda t: first(t, lambda n: n.get('relname') == 'ts_financials_outbox').update(relname='other_scope'))
reject('epoch refusal boundary cannot change', lambda t: first(t, lambda n: n.get('fval', {}).get('fval') == '4294967296')['fval'].update(fval='4294967297'))

print(json.dumps({'version': 'CONTACT_CSE_INDEPENDENT_WHOLE_AST_PROOF_V1', 'authorWholeProofRawJsonReplayed': True, 'authorNegatives': 9, 'exactScalarSubstitutions': proof['boundCallSites'], 'completeCteNodeChecked': True, 'snapshotCalls': {'before': 5, 'after': 1}, 'allOtherCallsIdentical': dict(sorted(parent_counts.items())), 'cases': cases, 'independentAstNegatives': len(cases), 'native': 'NOT_RUN', 'performance': 'NOT_MEASURED'}, indent=2))
