import fs from 'node:fs';
import path from 'node:path';

// Readers only open immutable generations. Live artifacts are kept outside
// OneDrive; none of these writes replaces a file another process may be reading.
export function createReceiptJournal(directory,{writeFile=fs.writeFileSync}={}){
 if(!path.isAbsolute(directory))throw Error('RECEIPT_ABSOLUTE_DIRECTORY_REQUIRED');
 let sequence=0;
 return Object.freeze({
  checkpoint(value,{progress=false}={}){
   const file=path.join(directory,`${String(++sequence).padStart(6,'0')}_${progress?'PROGRESS':'RECEIPT'}.json`);
   try{writeFile(file,JSON.stringify(value,null,2)+'\n',{flag:'wx'});}
   catch{throw Object.assign(Error('RECEIPT_CHECKPOINT_WRITE_FAILED'),{code:'RECEIPT_CHECKPOINT_WRITE_FAILED'});}
   return file;
  }
 });
}

export function latestBusyTimestamp(records){
 let latest=null;
 for(const row of records){
  if(row?.chat!=='HealthRoster'||row.status!=='BUSY_NO_TRIAL')continue;
  const time=Date.parse(row.checkedAt);
  if(!Number.isFinite(time))throw Error('TURN_BUSY_TIMESTAMP_INVALID');
  latest=Math.max(latest??time,time);
 }
 return latest;
}
