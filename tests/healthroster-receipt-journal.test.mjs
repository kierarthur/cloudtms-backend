import assert from 'node:assert/strict';
import test from 'node:test';
import {createReceiptJournal,latestBusyTimestamp} from '../scripts/healthroster-receipt-journal.mjs';

test('receipt generations never rewrite a previous reader-visible file; progress is a small separate entry',()=>{
 const writes=[];
 const journal=createReceiptJournal(process.cwd(),{writeFile:(file,bytes,options)=>writes.push({file,bytes,options})});
 journal.checkpoint({before:{fingerprint:'a'},status:'RUNNING'});
 journal.checkpoint({unit:112,event:'END'},{progress:true});
 journal.checkpoint({before:{fingerprint:'a'},status:'ROLLED_BACK'});
 assert.equal(new Set(writes.map(row=>row.file)).size,3);
 assert.ok(writes.every(row=>row.options.flag==='wx'));
 assert.ok(writes[1].file.endsWith('000002_PROGRESS.json'));
 assert.deepEqual(JSON.parse(writes[1].bytes),{unit:112,event:'END'});
 assert.equal(JSON.parse(writes[0].bytes).status,'RUNNING');
});
test('write failure returns only a closed diagnostic, never a raw path or OS payload',()=>{
 const journal=createReceiptJournal(process.cwd(),{writeFile:()=>{throw Error('private raw diagnostic');}});
 assert.throws(()=>journal.checkpoint({status:'RUNNING'}),error=>error.code==='RECEIPT_CHECKPOINT_WRITE_FAILED'&&!error.message.includes('private'));
});
test('authoritative local busy records preserve cooldown even when the optional progress archive is absent',()=>{
 const records=[{chat:'HealthRoster',status:'BUSY_NO_TRIAL',checkedAt:'2026-10-14T01:00:00Z'},
  {chat:'HealthRoster',status:'TAKEN',takenAt:'2026-10-14T02:00:00Z'},
  {chat:'Banking',status:'BUSY_NO_TRIAL',checkedAt:'2026-10-14T03:00:00Z'}];
 assert.equal(latestBusyTimestamp(records),Date.parse('2026-10-14T01:00:00Z'));
 assert.throws(()=>latestBusyTimestamp([{chat:'HealthRoster',status:'BUSY_NO_TRIAL',checkedAt:'invalid'}]),/TURN_BUSY_TIMESTAMP_INVALID/);
});
