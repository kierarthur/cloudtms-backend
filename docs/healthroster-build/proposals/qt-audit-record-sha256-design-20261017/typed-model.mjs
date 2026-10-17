// SYNTHETIC reference framing only. Not PostgreSQL execution, send-output oracle or acceptance.
import{createHash}from'node:crypto';
export const OIDS=Object.freeze([2950,1184,2950,25,25,25,25,25,3802,3802,25,25,25,25,16,20]);
const ok=(v,c)=>{if(!v)throw Error(c)},i32=n=>{let b=Buffer.alloc(4);b.writeInt32BE(n);return b},oid=n=>{let b=Buffer.alloc(4);b.writeUInt32BE(n);return b};
function utf8(s){ok(typeof s==='string'&&!s.includes('\0'),'SYNTHETIC_NATIVE_TEXT');const b=Buffer.from(s,'utf8');ok(b.toString('utf8')===s,'SYNTHETIC_VALID_UNICODE');return b;}
export const jsonbText=s=>({kind:'ALREADY_RENDERED_JSONB_TEXT_FIXTURE',text:s});
function scalar(type,v){
 if(type===2950){ok(typeof v==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(v),'SYNTHETIC_UUID');return Buffer.from(v.replaceAll('-',''),'hex')}
 if(type===20||type===1184){ok(typeof v==='bigint'&&v>=-(1n<<63n)&&v<(1n<<63n),'SYNTHETIC_INT64');let b=Buffer.alloc(8);b.writeBigInt64BE(v);return b}
 if(type===16){ok(typeof v==='boolean','SYNTHETIC_BOOL');return Buffer.from([v?1:0])}
 if(type===25)return utf8(v);
 if(type===3802){ok(v?.kind==='ALREADY_RENDERED_JSONB_TEXT_FIXTURE'&&typeof v.text==='string','JSONB_NATIVE_OUTPUT_NOT_INFERRED');JSON.parse(v.text);return Buffer.concat([Buffer.from([1]),utf8(v.text)])}
 throw Error('SYNTHETIC_UNADMITTED_TYPE');
}
export function frame(types,values){ok(types.length===values.length,'SYNTHETIC_ARITY');return Buffer.concat([i32(types.length),...types.flatMap((type,i)=>{let v=values[i];if(v===null)return[oid(type),i32(-1)];let b=Buffer.isBuffer(v)?v:scalar(type,v);return[oid(type),i32(b.length),b]})])}
export function unframe(b){let at=0;const take=n=>{ok(n>=0&&at+n<=b.length,'FRAME_BOUNDS');const r=b.subarray(at,at+n);at+=n;return r},count=take(4).readInt32BE();ok(count>=0,'FRAME_COUNT');let fields=[];for(let i=0;i<count;i++){const type=take(4).readUInt32BE(),len=take(4).readInt32BE();ok(len>=-1,'FRAME_LENGTH');fields.push({type,bytes:len===-1?null:take(len)})}ok(at===b.length,'FRAME_TRAILER');return fields}
export function preimage(values,tz='Europe/London',auditTypeOid=40000){ok(values.length===16,'WHOLE16_REQUIRED');return frame([auditTypeOid,25],[frame(OIDS,values),tz])}
export const digest=(values,tz)=>createHash('sha256').update(preimage(values,tz)).digest();
export function fixture(){return['00000000-0000-0000-0000-000000000001',0n,'ffffffff-ffff-ffff-ffff-ffffffffffff','Name | "quoted"\n雪','admin','candidate','same|key','UPDATE',jsonbText('{"a": 1.00, "b": [null, true]}'),jsonbText('{"a": 1.00, "b": [null, false]}'),'reason','::1','agent','correlation',false,9007199254740993n]}
export const bag=(rows,{hash=digest,tz='Europe/London'}={})=>{let m=new Map;for(const r of rows){ok(typeof r[15]==='bigint','LOSSLESS_EVENT_SEQUENCE');let k=r[15].toString()+':'+hash(r,tz).toString('hex');m.set(k,(m.get(k)??0)+1)}return [...m.entries()].sort((a,b)=>a[0]<b[0]?-1:a[0]>b[0]?1:0)};
