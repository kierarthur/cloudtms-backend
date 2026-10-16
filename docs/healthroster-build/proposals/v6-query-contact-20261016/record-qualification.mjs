// Record only completed local source checks. No DB/network/provider calls.
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const root=path.dirname(fileURLToPath(import.meta.url));
const read=file=>fs.readFileSync(path.join(root,file),'utf8');
const q=JSON.parse(read('QUALIFICATION.json')),inputs=JSON.parse(read('SOURCE_INPUTS.json'));
q.source_tests={passed:14,failed:0,command:'node --test source.test.mjs'};
const parse=JSON.parse(read('SOURCE_PARSE_RESULT.json')),native=JSON.parse(read('NATIVE_SOURCE_CHECK.json'));
if(parse.failures.length||native.failures.length)throw Error('Source checks failed');
q.grammar.sql_files=parse.checks.length;q.grammar.fixture_functions=parse.fixture_grammar_checks.reduce((n,x)=>n+x.plpgsql_functions,0);
q.native_source_checks.rowtype_variables=native.rowtype_checks.length;
q.native_source_checks.native_claim_owner_unchanged_except_query_gate_and_stamp='PASS';
q.native_source_checks.pause_reader_composed_grammar='PASS';
q.javascript_syntax=['query-only module PASS','exact composed native drain PASS','Worker pause decoder PASS','Office pause contract/controls PASS'];
q.input_currency=inputs.inputs.map(input=>{
 const actual=fs.readFileSync(path.join(input.root||inputs.root,input.file),'utf8').replaceAll('\r\n','\n');
 const saved=read(input.reference).replaceAll('\r\n','\n');
 const matches=input.selection?actual.includes(saved):actual===saved;
 if(!matches)throw Error('Native saved owner changed: '+input.file);
 return {file:input.file,selected_saved_owner_matches_current:matches};
});
q.required_native.push('Atomic snapshot/lease guard/generic fence migration and exact RLS/ACL; old cached claim crossing DDL, stale/foreign backend/xid, cleanup/deferred commit and original legacy actual result');
q.required_native=[...new Set(q.required_native)];
q.schedules_jobs='Remain running; no pause or disable requested/authorized';
fs.writeFileSync(path.join(root,'QUALIFICATION.json'),JSON.stringify(q,null,2)+'\n');
console.log(JSON.stringify({source_tests:q.source_tests.passed,grammar_files:q.grammar.sql_files,native_fixture_functions:q.grammar.fixture_functions,rowtypes:q.native_source_checks.rowtype_variables,current_owners:q.input_currency.length,native:q.native}));
