// Offline synthetic native-row encoder. Never a native receipt or SQL caller.
import {VERSION,FIELDS,CHUNK_CHARS,hash} from './frame-transport.mjs';
export function encodeText(text,expected){
 const chars=Array.from(text),totalParts=Math.max(1,Math.ceil(chars.length/CHUNK_CHARS));
 const rows=Array.from({length:totalParts},(_,index)=>({transport_version:VERSION,frame_type:expected.frameType,part:index+1,total_parts:totalParts,
  total_chars:chars.length,total_utf8_bytes:Buffer.byteLength(text,'utf8'),sha256:hash(text),query_sha256:expected.querySha256,
  chunk:chars.slice(index*CHUNK_CHARS,(index+1)*CHUNK_CHARS).join(''),eof:index===totalParts-1}));
 return{command:'SELECT',rowCount:rows.length,fields:FIELDS,rows,rowsTruncated:false};
}
export const encodeFrame=(value,expected)=>encodeText(JSON.stringify(value),expected);
