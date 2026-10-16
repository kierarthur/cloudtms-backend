// Lossless transport only. Canonical query content and validators stay intact.
import {createHash} from 'node:crypto';
export const VERSION='HR_CONTACT_CANONICAL_CHUNKS_V1',CHUNK_CHARS=8000,MAX_PARTS=100;
export const FIELDS=['transport_version','frame_type','part','total_parts','total_chars','total_utf8_bytes','sha256','query_sha256','chunk','eof'];
export const hash=text=>createHash('sha256').update(text).digest('hex');
const requireThat=(ok,code)=>{if(!ok)throw Error(code);};
export function wrapCanonicalQuery(query,frameType){
 requireThat(['CANONICAL_SNAPSHOT','CANONICAL_FIXTURE_PROOF'].includes(frameType),'MCP_FRAME_TYPE_UNKNOWN');
 requireThat(typeof query==='string'&&query.endsWith(';'),'MCP_CANONICAL_QUERY_TERMINATOR_REQUIRED');
 const body=query.slice(0,-1),queryHash=hash(query);
 const prefix='with contact_canonical_frame(value) as materialized (\n',suffix=`\n), contact_frame_text as materialized (
 select value::text payload from contact_canonical_frame
), contact_frame_info as materialized (
 select payload, pg_catalog.char_length(payload) total_chars,
 pg_catalog.octet_length(pg_catalog.convert_to(payload,'UTF8')) total_utf8_bytes,
 pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(payload,'UTF8')),'hex') sha256,
 greatest(1,(pg_catalog.char_length(payload)+${CHUNK_CHARS-1})/${CHUNK_CHARS}) total_parts from contact_frame_text
)
select '${VERSION}'::text transport_version,'${frameType}'::text frame_type,
 n.part, f.total_parts, f.total_chars, f.total_utf8_bytes, f.sha256,
 '${queryHash}'::text query_sha256,
 pg_catalog.substr(f.payload,(n.part-1)*${CHUNK_CHARS}+1,${CHUNK_CHARS}) chunk,
 n.part=f.total_parts eof
from contact_frame_info f cross join lateral pg_catalog.generate_series(1,least(f.total_parts,${MAX_PARTS})) n(part)
order by n.part;`;
 const wrapped=prefix+body+suffix;
 requireThat(wrapped.slice(prefix.length,wrapped.length-suffix.length)+';'===query,'MCP_CANONICAL_FULL_QUERY_REVERSE_FAILED');
 return {sql:wrapped,transportVersion:VERSION,frameType,querySha256:queryHash,originalQuerySha256:queryHash,wrappedSqlSha256:hash(wrapped),
  innerOffset:prefix.length,innerLength:body.length,canonicalTerminatorRemovedOnly:true,chunkChars:CHUNK_CHARS,maxParts:MAX_PARTS,fields:FIELDS};
}
export function decodeFrame(statement,expected){
 requireThat(expected&&['CANONICAL_SNAPSHOT','CANONICAL_FIXTURE_PROOF'].includes(expected.frameType)&&/^[a-f0-9]{64}$/.test(expected.querySha256),'MCP_FRAME_EXPECTATION_REQUIRED');
 requireThat(statement&&!statement.error&&statement.rowsTruncated===false&&Array.isArray(statement.rows)
  &&JSON.stringify(statement.fields)===JSON.stringify(FIELDS),'MCP_FRAME_NATIVE_ENVELOPE_REFUSED');
 const rows=statement.rows;requireThat(rows.length>=1&&rows.length<=MAX_PARTS&&statement.rowCount===rows.length,'MCP_FRAME_ROW_COUNT_REFUSED');
 const first=rows[0],keys=[...FIELDS].sort();
 requireThat(Number.isSafeInteger(first.total_parts)&&first.total_parts>=1&&first.total_parts<=MAX_PARTS&&first.total_parts===rows.length
  &&Number.isSafeInteger(first.total_chars)&&first.total_chars>=0&&first.total_chars<=CHUNK_CHARS*MAX_PARTS
  &&first.total_parts===Math.max(1,Math.ceil(first.total_chars/CHUNK_CHARS))
  &&Number.isSafeInteger(first.total_utf8_bytes)&&first.total_utf8_bytes>=0&&first.total_utf8_bytes<=4*CHUNK_CHARS*MAX_PARTS
  &&/^[a-f0-9]{64}$/.test(first.sha256),'MCP_FRAME_TOTALS_REFUSED');
 for(const [index,row]of rows.entries()){
  requireThat(JSON.stringify(Object.keys(row).sort())===JSON.stringify(keys),'MCP_FRAME_COLUMNS_REFUSED');
  requireThat(row.transport_version===VERSION&&row.frame_type===expected.frameType&&row.query_sha256===expected.querySha256,'MCP_FRAME_IDENTITY_REFUSED');
  requireThat(row.part===index+1&&row.total_parts===first.total_parts&&row.total_chars===first.total_chars
   &&row.total_utf8_bytes===first.total_utf8_bytes&&row.sha256===first.sha256&&row.eof===(index===rows.length-1),'MCP_FRAME_ORDER_OR_METADATA_REFUSED');
  requireThat(typeof row.chunk==='string'&&row.chunk.isWellFormed(),'MCP_FRAME_ILL_FORMED_UNICODE_REFUSED');
  requireThat(Array.from(row.chunk).length<=CHUNK_CHARS
   &&(index===rows.length-1||Array.from(row.chunk).length===CHUNK_CHARS),'MCP_FRAME_CHUNK_LENGTH_REFUSED');
 }
 const text=rows.map(row=>row.chunk).join('');
 requireThat(Array.from(text).length===first.total_chars&&Buffer.byteLength(text,'utf8')===first.total_utf8_bytes&&hash(text)===first.sha256,'MCP_FRAME_COMPLETE_LENGTH_OR_HASH_REFUSED');
 let value;try{value=JSON.parse(text);}catch{throw Error('MCP_FRAME_INVALID_JSON');}
 requireThat(value&&typeof value==='object'&&!Array.isArray(value),'MCP_FRAME_JSON_OBJECT_REQUIRED');return value;
}
export function closedCliError(error){return{status:'HOLD',code:/^(?:MCP_|REHEARSAL_|CANONICAL_|HISTORICAL_|BACKGROUND_|EXACT_|CHUNK_|ROOT_|FRESH_|OWNED_|CONTACT_|SOURCE_)[A-Z0-9_]{1,90}$/.test(error?.message??'')?error.message:'MCP_FRAME_CLOSED_INPUT_OR_TRANSPORT_FAILURE'};}
