"""Read-only finite peer-review reproduction; no database or network operations."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    parser.add_argument('--parser-path', required=True)
    args = parser.parse_args()
    root = Path(args.root).resolve()
    receipt = json.loads(Path(__file__).with_name('REVIEW.json').read_text(encoding='utf-8'))
    assert receipt['status'] == 'INTERIM_SOURCE_PEER_REVIEW_NO_BLOCKING_FINDING'
    commit = receipt['reviewedCommit']
    for item in receipt['inputFiles']:
        raw = (root / item['path']).read_bytes()
        frozen = subprocess.check_output(['git', 'show', commit + ':' + item['path']], cwd=root)
        assert sha(raw) == sha(frozen) == item['sha256'], item['path']

    # These exports only parse/read source. No release coordinator is invoked.
    script = r"""
import fs from 'node:fs';
import {specialConstructViolations,executableParts} from './scripts/bpay-next-source-gates.mjs';
import {reviewedHealthRowtypeHazards} from './scripts/healthroster-rowtype-type-review.mjs';
import {reviewedHealthUuidHazards} from './scripts/healthroster-uuid-cardinality-review.mjs';
const d='docs/healthroster-build/v6-owner-delivery-20261016/';
const tuple=JSON.parse(fs.readFileSync(d+'ROWTYPE_TUPLE_TYPE_REVIEW.json'));
const uuid=JSON.parse(fs.readFileSync(d+'UUID_CARDINALITY_CLASSIFICATION.json'));
const out=[];let uuidKeys=0,guards=0;
for(const f of tuple.files){
 const source=fs.readFileSync(f.path,'utf8'),v=specialConstructViolations(source,f.path);
 const baseline=new Map([['peer-unrelated-retained',2]]);
 const approved=reviewedHealthRowtypeHazards(source,f.path,baseline,v);
 if(approved.get('peer-unrelated-retained')!==2||baseline.size!==1)throw Error('BASELINE_CHANGED');
 for(const x of v.filter(x=>x.hazard.startsWith('ROWTYPE_SELECT:'))){
  if(!approved.has(x.key))throw Error('TUPLE_NOT_CONSUMED');
  if(/^(private|public)\.bpay_next_/.test(JSON.parse(x.key)[0]))throw Error('BANK_SCOPE_CHANGED');
  out.push({file:f.path,...x});
 }
 for(const x of v.filter(x=>!x.hazard.startsWith('ROWTYPE_SELECT:')))
  if(approved.has(x.key))throw Error('OTHER_HAZARD_APPROVED');
}
for(const f of uuid.files){
 const source=fs.readFileSync(f.path,'utf8'),v=specialConstructViolations(source,f.path);
 const approved=reviewedHealthUuidHazards(source,f.path,new Map(),v,executableParts(source,f.path));
 uuidKeys+=approved.size;guards+=f.guards.length;
 for(const x of v.filter(x=>x.hazard!=='UUID_CARDINALITY_UNCLASSIFIED'))
  if(approved.has(x.key))throw Error('OTHER_HAZARD_APPROVED');
}
process.stdout.write(JSON.stringify({rows:out,files:tuple.files.length,uuidFiles:uuid.files.length,uuidKeys,guards}));
"""
    child = subprocess.run(['node', '--input-type=module', '-e', script], cwd=root,
                           check=True, stdout=subprocess.PIPE, encoding='utf-8')
    actual = json.loads(child.stdout)
    assert (actual['files'], actual['uuidFiles'], actual['uuidKeys'], actual['guards']) == (151, 19, 21, 46)

    sys.path.insert(0, args.parser_path)
    from pglast import ast, parse_sql, scan, __version__
    assert __version__ == 'v7.20', __version__
    relations = set()
    prefixes = 0
    for row in actual['rows']:
        signature, hazard, sql = json.loads(row['key'])
        var, relation = hazard.split(':', 2)[1:]
        schema, table = relation.replace(' ', '').lower().split('.')
        tokens = scan(sql)
        into = next(i for i, token in enumerate(tokens) if token.name == 'INTO')
        select = max(i for i, token in enumerate(tokens[:into]) if token.name == 'SELECT')
        target = into + (2 if tokens[into + 1].name == 'STRICT_P' else 1)
        assert sql[tokens[target].start:tokens[target].end + 1].upper() == var
        # The diagnostic key can contain a preceding PL/pgSQL control prefix.
        # Keep the entire actual SELECT tail and remove only INTO [STRICT] var.
        normalized = sql[tokens[select].start:tokens[into].start] + sql[tokens[target].end + 1:]
        prefixes += tokens[select].start != 0
        parsed = parse_sql(normalized)
        assert len(parsed) == 1
        select_ast = parsed[0].stmt
        assert isinstance(select_ast, ast.SelectStmt)
        assert not select_ast.withClause and not select_ast.larg and not select_ast.rarg
        assert len(select_ast.targetList) == 1
        column = select_ast.targetList[0].val
        assert isinstance(column, ast.ColumnRef)
        assert len(column.fields) == 1 and isinstance(column.fields[0], ast.A_Star)
        assert len(select_ast.fromClause) == 1
        source = select_ast.fromClause[0]
        assert isinstance(source, ast.RangeVar)
        assert (source.schemaname, source.relname) == (schema, table)
        relations.add((schema, table))
    assert len(actual['rows']) == 1204
    assert len({(row['file'], row['key']) for row in actual['rows']}) == 1178
    assert len(relations) == 91 and prefixes == 19

    evidence = root / 'docs/healthroster-build/v6-owner-delivery-20261016'
    correction = json.loads((evidence / 'INVOICE_PRESENTATION_CARDINALITY_CORRECTION.json').read_text(encoding='utf-8'))
    guard = correction['insertedGuard']
    for row in correction['corrections']:
        source = (root / row['file']).read_text(encoding='utf-8')
        before = subprocess.check_output(['git', 'show', correction['sourceParent'] + ':' + row['file']],
                                         cwd=root).decode('utf-8').replace('\r\n', '\n')
        assert source.count(guard) == 1
        assert source.replace(guard, '') == before
        assert sha(before.encode()) == row['beforeLfSha256']
        assert sha(source.encode()) == row['afterLfSha256']

    tests = subprocess.run(['node', '--test',
        'tests/healthroster-rowtype-type-review.test.mjs',
        'tests/healthroster-uuid-cardinality-review.test.mjs'], cwd=root,
        check=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding='utf-8')
    assert ('tests 46' in tests.stdout or '# tests 46' in tests.stdout), tests.stdout
    result = {'status': 'PASS_SOURCE_ONLY', 'reviewedCommit': commit,
              'tupleFiles': 151, 'tupleOccurrences': 1204, 'tupleDistinctFileKeys': 1178,
              'pg17AstPass': 1204, 'nativeRelations': 91, 'controlPrefixes': 19,
              'uuidFiles': 19, 'uuidKeys': 21, 'uuidGuardWitnesses': 46,
              'exactInvoiceGuardInsertions': 2, 'focusedTests': 46,
              'native': 'NOT_RUN', 'fullF23': 'HELD_REMAINING_DYNAMIC_AND_AGGREGATE_REVIEW',
              'finalPartBCritic': False, 'databaseOrNetworkOperations': False}
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
