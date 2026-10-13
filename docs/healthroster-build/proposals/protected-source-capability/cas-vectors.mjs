import crypto from 'node:crypto';
const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
export function originalClients(values){
 if(!Array.isArray(values)||values.length>100||values.some(x=>typeof x!=='string'||!uuid.test(x)))throw Error('CLIENT_CENSUS_INVALID');
 return [...new Set(values)].sort();
}
export function remainingClients(original,currentOpen){
 const saved=originalClients(original),now=originalClients(currentOpen);
 if(now.some(x=>!saved.includes(x)))throw Error('ORIGINAL_CLIENT_CENSUS_CHANGED');
 return saved;
}
// Independent local semantic vectors, NOT PostgreSQL hash serialization or
// native financial qualification. Source SQL is the deployed hash producer.
function canonical(value){if(Array.isArray(value))return '['+value.map(canonical).join(',')+']';if(value&&typeof value==='object')return '{'+Object.keys(value).sort().map(k=>JSON.stringify(k)+':'+canonical(value[k])).join(',')+'}';return JSON.stringify(value);}
export function sourceVectorHash(facts){return crypto.createHash('sha256').update('LOCAL_SOURCE_CAS_VECTOR\x1f'+canonical(facts)).digest('hex');}
export function projectFinancialIdentity(row){return Object.fromEntries(['id','timesheet_id','client_id','candidate_id','contract_id'].map(key=>[key,row[key]]));}
