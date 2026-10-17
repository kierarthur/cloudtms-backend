// Read the same complete Git blobs using ordered, length-framed stdin batches.
// There is no cross-invocation cache, text normalization, file sink or fetch.
const GIT_MAX_BUFFER=24e6;
const batchRequest=r=>r&&typeof r.spec==='string'&&/^(?:HEAD|[0-9a-f]{40})?:[^\0\r\n]+$/.test(r.spec)&&Number.isSafeInteger(r.bytes)&&r.bytes>=0;
export function parseContactGitBlobBatch(raw,requests){
 requireThat(Buffer.isBuffer(raw)&&raw.length<=GIT_MAX_BUFFER&&Array.isArray(requests)&&requests.length>0&&requests.every(batchRequest),'CONTACT_QT_GIT_BATCH_ENVELOPE_REQUIRED');
 const bodies=[];let offset=0;
 for(const r of requests){const newline=raw.indexOf(10,offset);requireThat(newline>=offset&&newline-offset<=128,'CONTACT_QT_GIT_BATCH_HEADER_REQUIRED');const header=raw.subarray(offset,newline);requireThat(header.every(b=>b<128),'CONTACT_QT_GIT_BATCH_HEADER_REQUIRED');const m=/^([0-9a-f]{40}) blob (0|[1-9][0-9]*)$/.exec(header.toString('ascii')),size=Number(m?.[2]);requireThat(m&&Number.isSafeInteger(size)&&size===r.bytes,'CONTACT_QT_GIT_BATCH_BLOB_REQUIRED');const end=newline+1+size;requireThat(end<raw.length&&raw[end]===10,'CONTACT_QT_GIT_BATCH_COMPLETE_BLOB_REQUIRED');bodies.push(raw.subarray(newline+1,end));offset=end+1;}
 requireThat(offset===raw.length,'CONTACT_QT_GIT_BATCH_TRAILING_DATA_REFUSED');return bodies;
}
function gitBlobs(root,requests){
 requireThat(Array.isArray(requests)&&requests.length>0&&requests.every(batchRequest)&&new Set(requests.map(r=>r.spec)).size===requests.length,'CONTACT_QT_GIT_BATCH_REQUEST_REQUIRED');
 const result=new Map();let group=[],cost=0;
 const flush=()=>{if(!group.length)return;let raw;try{raw=git(root,['cat-file','--batch'],Buffer.from(group.map(r=>r.spec).join('\n')+'\n'));}catch{throw Error('CONTACT_QT_GIT_BATCH_READ_REFUSED');}const bodies=parseContactGitBlobBatch(raw,group);for(let i=0;i<group.length;i++)result.set(group[i].spec,bodies[i]);group=[];cost=0;};
 for(const r of requests){const next=r.bytes+128;if(next>GIT_MAX_BUFFER){flush();let raw;try{raw=git(root,['show',r.spec]);}catch{throw Error('CONTACT_QT_GIT_BATCH_READ_REFUSED');}requireThat(raw.length===r.bytes,'CONTACT_QT_GIT_BATCH_BLOB_REQUIRED');result.set(r.spec,raw);continue;}if(group.length&&cost+next>GIT_MAX_BUFFER)flush();group.push(r);cost+=next;}
 flush();requireThat(result.size===requests.length,'CONTACT_QT_GIT_BATCH_COMPLETE_REQUESTS_REQUIRED');return result;
}
