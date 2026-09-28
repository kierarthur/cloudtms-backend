import test from 'node:test';
import assert from 'node:assert/strict';
import {READER_PREPARATION,readerReleasePhases,prepareSourceReaders,readerActivationSql} from '../scripts/weekly-source-reader-release.mjs';
const pair=READER_PREPARATION.readers.map(path=>({path,sha256:'a'.repeat(64)}));
test('exact reader pair deferred, other chronological authority preserved',()=>{
 const ordinary=[{path:'first.sql'},{path:'last.sql'}];
 const phases=readerReleasePhases([ordinary[0],pair[0],ordinary[1]],[...ordinary,...pair]);
 assert.deepEqual(phases,{ordinary,readers:pair});
 assert.deepEqual(readerReleasePhases(ordinary,ordinary),{ordinary,readers:[]});
 assert.throws(()=>readerReleasePhases([pair[0]],[pair[0]]),/AUTHORITY_MISSING/);
});
test('preparation is bounded, stops on error, and does not activate',()=>{
 let calls=0;
 assert.deepEqual(prepareSourceReaders(sql=>{assert.match(sql,/statement_timeout='15s'/);return ++calls===3?'t':'f';}),{calls:3});
 assert.throws(()=>prepareSourceReaders(()=> 'garbage'),/INVALID_RESULT/);
 calls=0;assert.throws(()=>prepareSourceReaders(()=>{calls++;return 'f';}),/RESUME_REQUIRED/);
 assert.equal(calls,1000);
});
test('both reader definitions and ledger writes share one transaction',()=>{
 const sql=readerActivationSql(pair,path=>'-- '+path+'\nselect 1;',item=>'-- ledger '+item.path);
 assert.equal((sql.match(/^begin;/gm)||[]).length,1);
 assert.equal((sql.match(/^commit;/gm)||[]).length,1);
 assert.equal((sql.match(/-- ledger/g)||[]).length,2);
 assert.match(sql,/READER_PREPARATION_NOT_COMPLETE/);
 assert.throws(()=>readerActivationSql(pair,()=> 'begin;\nselect 1;\ncommit;'),/UNEXPECTED_TRANSACTION/);
 assert.throws(()=>readerActivationSql(pair,()=> '\\i elsewhere.sql'),/UNEXPECTED_TRANSACTION/);
 assert.throws(()=>readerActivationSql([pair[0]],()=>''),/EXACT_PAIR/);
});
