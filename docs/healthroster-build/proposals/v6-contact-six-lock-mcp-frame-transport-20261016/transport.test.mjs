import test from 'node:test';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {decodeFrame,wrapCanonicalQuery,closedCliError,CHUNK_CHARS,MAX_PARTS} from './frame-transport.mjs';
import {encodeFrame,encodeText} from './test-frames.mjs';
const expected=wrapCanonicalQuery("select '{\"proof\":true}'::text;",'CANONICAL_SNAPSHOT');
const original={proof:true,canonicalMetadata:'x'.repeat(25001),unicode:'😀'.repeat(18001)};
test('lossless >20k scalar and nonBMP payload survives native-size string sanitizer with no canonical data loss',()=>{
 const statement=encodeFrame(original,expected);
 const sanitize=value=>typeof value==='string'&&value.length>20000?value.slice(0,20000)+'…':value;
 const sanitized={...statement,rows:statement.rows.map(row=>Object.fromEntries(Object.entries(row).map(([key,value])=>[key,sanitize(value)])))};
 assert.ok(statement.rows.length>1);assert.ok(statement.rows.every(row=>row.chunk.length<=16000&&Array.from(row.chunk).length<=CHUNK_CHARS));
 assert.deepEqual(decodeFrame(sanitized,expected),original);
});
test('wrapper exactly de-embeds original query including final semicolon and has one materialized canonical evaluation',()=>{
 assert.equal(expected.sql.slice(expected.innerOffset,expected.innerOffset+expected.innerLength)+';',"select '{\"proof\":true}'::text;");
 assert.equal(expected.sql.match(/contact_canonical_frame\(value\) as materialized/g)?.length,1);
});
const edits={
 missingPart:s=>{s.rows.splice(1,1);},duplicatePart:s=>{s.rows[1]=structuredClone(s.rows[0]);},outOfOrder:s=>{[s.rows[0],s.rows[1]]=[s.rows[1],s.rows[0]];},
 alteredChunk:s=>{s.rows[0].chunk='z'+s.rows[0].chunk.slice(1);},wrongHash:s=>{for(const r of s.rows)r.sha256='f'.repeat(64);},
 stringTruncationWithoutRowFlag:s=>{s.rows[0].chunk=s.rows[0].chunk.slice(0,300)+'…';},wrongCharCount:s=>{for(const r of s.rows)r.total_chars++;},
 wrongUtf8Count:s=>{for(const r of s.rows)r.total_utf8_bytes++;},wrongQueryPin:s=>{for(const r of s.rows)r.query_sha256='f'.repeat(64);},
 prematureEof:s=>{s.rows[0].eof=true;},missingEof:s=>{s.rows.at(-1).eof=false;},unknownVersion:s=>{s.rows[0].transport_version='UNKNOWN';},
 unknownFrame:s=>{s.rows[0].frame_type='UNKNOWN';},extraColumn:s=>{s.rows[0].unapproved='x';},rowTruncation:s=>{s.rowsTruncated=true;},
 statementError:s=>{s.error={code:'57014'};},extraRows:s=>{s.rows.push(structuredClone(s.rows.at(-1)));},wrongNativeFields:s=>{s.fields=['frame'];},
};
for(const [name,edit]of Object.entries(edits))test('refuses '+name+' before JSON.parse',()=>{const frame=encodeFrame(original,expected);edit(frame);assert.throws(()=>decodeFrame(frame,expected),/^Error: MCP_FRAME_/);});
test('over100-row budget is explicitly held even with row truncation flag false',()=>{
 const frame=encodeFrame({large:'x'.repeat(CHUNK_CHARS*MAX_PARTS+1)},expected);frame.rows=frame.rows.slice(0,MAX_PARTS);frame.rowCount=MAX_PARTS;
 assert.throws(()=>decodeFrame(frame,expected),/MCP_FRAME_TOTALS_REFUSED/);
});
test('complete length/hash but invalid JSON produces a closed code without frame content',()=>{
 assert.throws(()=>decodeFrame(encodeText('{"PRIVATE_METADATA":',expected),expected),/MCP_FRAME_INVALID_JSON/);
 const result=closedCliError(new SyntaxError('PRIVATE_METADATA_SENTINEL'));assert.equal(result.code,'MCP_FRAME_CLOSED_INPUT_OR_TRANSPORT_FAILURE');
 assert.deepEqual(closedCliError(new SyntaxError('Unexpected end: {"PRIVATE_METADATA":')), {status:'HOLD',code:'MCP_FRAME_CLOSED_INPUT_OR_TRANSPORT_FAILURE'});
});
test('extra empty EOF part cannot extend an exactly8000-character payload',()=>{
 const value={x:'x'.repeat(7992)},frame=encodeFrame(value,expected);assert.equal(frame.rows[0].total_chars,8000);
 frame.rows[0].eof=false;frame.rows[0].total_parts=2;
 frame.rows.push({...frame.rows[0],part:2,chunk:'',eof:true});frame.rowCount=2;
 assert.throws(()=>decodeFrame(frame,expected),/MCP_FRAME_TOTALS_REFUSED/);
});
test('lone surrogate cannot alias a real replacement character through UTF8 hashing',()=>{
 const frame=encodeFrame({value:'�'},expected);frame.rows[0].chunk=frame.rows[0].chunk.replace('�','\ud800');
 assert.throws(()=>decodeFrame(frame,expected),/MCP_FRAME_ILL_FORMED_UNICODE_REFUSED/);
});
test('nonBMP characters on both sides of an8000-codepoint boundary reassemble exactly',()=>{
 const value={value:'x'.repeat(7988)+'😀😀😀'+'x'.repeat(8000)};
 assert.deepEqual(decodeFrame(encodeFrame(value,expected),expected),value);
});
test('validator CLI never echoes malformed metadata JSON or a stack',()=>{
 const root=path.dirname(fileURLToPath(import.meta.url));
 const child=spawnSync(process.execPath,[path.join(root,'result-validator.mjs'),'prepare','missing','missing'],{encoding:'utf8',windowsHide:true});
 assert.equal(child.status,1);const value=JSON.parse(child.stderr.trim());assert.equal(value.status,'HOLD');assert.equal(child.stderr.includes('SyntaxError'),false);assert.equal(child.stderr.includes(' at '),false);
});
