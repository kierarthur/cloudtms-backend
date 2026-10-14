import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';import {sha256} from './weekly-source-protected-release-qualification.mjs';
const dir=path.dirname(fileURLToPath(import.meta.url)),name='09_complete_operational_evidence.mjs';
const before="  ...(bundle.report_fin_native_inputs===undefined?[]:['report_fin_native_inputs'])],'OPERATIONAL_BUNDLE_INVALID');";
const after="  ...(bundle.report_fin_native_inputs===undefined?[]:['report_fin_native_inputs']),\n  ...(bundle.report_fin_native_artifacts===undefined?[]:['report_fin_native_artifacts'])],'OPERATIONAL_BUNDLE_INVALID');";
const file=path.join(dir,name),bytes=fs.readFileSync(file,'utf8'),receiptFile=path.join(dir,'FULL_REVERSE_RECEIPT.json'),receipt=JSON.parse(fs.readFileSync(receiptFile,'utf8'));
const owner=receipt.owners.find(value=>value.output===name),hunk=owner.hunks.find(value=>value.id==='CLOSED_OPTIONAL_REPORT_BUNDLE_SECTION');
if(bytes.includes(before)){
 if(bytes.split(before).length!==2||hunk.new.split(before).length!==2)throw Error('REPORT_RAW_FORWARD_AMBIGUOUS');
 fs.writeFileSync(file,bytes.replace(before,()=>after));hunk.new=hunk.new.replace(before,()=>after);
 owner.output_sha256=sha256(fs.readFileSync(file));fs.writeFileSync(receiptFile,JSON.stringify(receipt,null,2)+'\n');
}else if(!bytes.includes(after)||!hunk.new.includes(after)||owner.output_sha256!==sha256(fs.readFileSync(file)))throw Error('REPORT_RAW_FORWARD_CHANGED');
