import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url));const source=path.resolve(process.argv[2]||'');
const pin='f2de35ec8a376a814aefab1db2d7cf099b888784';
const git=(...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:source,maxBuffer:36*1024*1024});
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
if(fs.existsSync(path.join(dir,'REVIEW_MANIFEST.json')))throw new Error('FROZEN_OFFICE_READ_DO_NOT_OVERWRITE');
const module='js/timesheet-correction-office-context.js';
const hunk={before:"// ── Canonical timesheet finance actions (advance / unadvance / snooze) ──\r\ntry {\r\n  if (typeof wireTimesheetOverviewFinanceActions === 'function') {\r\n    wireTimesheetOverviewFinanceActions(root);\r\n  }\r\n} catch (err) {\r\n  if (LOGM) console.warn('[TS][OVERVIEW] finance action wiring failed', err);\r\n}",after:`// ── Canonical timesheet finance actions (advance / unadvance / snooze) ──
try {
  if (typeof wireTimesheetOverviewFinanceActions === 'function') {
    wireTimesheetOverviewFinanceActions(root);
  }
} catch (err) {
  if (LOGM) console.warn('[TS][OVERVIEW] finance action wiring failed', err);
}

// Current native correction request read only. Original SEND, finance,
// PREPARE and action eligibility remain with their own qualified producers.
try {
  const currentUser = SESSION?.user;
  const details = mc.timesheetDetails, physical = details?.timesheet;
  const token = mc.openToken, actor = currentUser?.id;
  // Existing /api/me does not carry current active/payment flags. Request the
  // fixed read and consume its native admitted view; never mint a local role.
  const view = 'NATIVE';
  const payFlags = [currentUser?.role, currentUser?.is_active, currentUser?.payment_authoriser, currentUser?.payment_golden_key];
  const admitted = typeof actor === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(actor);
  const physicalId = physical?.timesheet_id, version = physical?.version;
  const current = () => root.isConnected && SESSION?.user?.id === actor && isActiveTimesheetModalToken(token)
    && window.modalCtx?.timesheetDetails === details && window.modalCtx?.timesheetDetails?.timesheet?.version === version
    && String(tsNow()).toLowerCase() === String(physicalId).toLowerCase()
    && String(expectedNow()).toLowerCase() === String(physicalId).toLowerCase()
    && JSON.stringify([SESSION?.user?.role, SESSION?.user?.is_active, SESSION?.user?.payment_authoriser,
      SESSION?.user?.payment_golden_key]) === JSON.stringify(payFlags);
  if (mode === 'view' && admitted && physicalId && Number.isSafeInteger(version) && version > 0 && current()) {
    window.CloudTmsCorrectionOfficeContext?.mount?.({document,host:root,timesheetId:physicalId,version,view,callerCurrent:current,
      readContext:async()=>{
        if (!current()) throw new Error('Context changed');
        const response = await authFetch(API('/api/timesheets/' + encodeURIComponent(physicalId) + '/corrections/context'));
        if (!current()) throw new Error('Context changed');
        let payload;try { payload = await response.json(); } catch { throw new Error('Correction details are unavailable'); }
        if (!current() || !response.ok) throw new Error('Correction details are unavailable');
        return payload;
      },
      openReplacement:id=>{if (current() && typeof window.openTimesheet === 'function') return window.openTimesheet(id);}
    });
  }
} catch (err) {
  if (LOGM) console.warn('[TS][OVERVIEW] correction request read wiring unavailable');
}`.replaceAll('\n','\r\n')};
const indexBefore='<script src="./js/main.js?v=20261012-sp10-r1&healthroster=20261014-r1"></script>';
const indexAfter='<script src="./js/timesheet-correction-office-context.js?v=20261014-correction-read-r1"></script>\r\n<script src="./js/main.js?v=20261012-sp10-r1&healthroster=20261014-r2"></script>';
const ownerChanges=[['js/main.js',[hunk]],['index.html',[{before:indexBefore,after:indexAfter}]]];
const owners=[];
for(const [file,hunks]of ownerChanges){const bytes=fs.readFileSync(path.join(source,file));
  if(!bytes.equals(git('show',`${pin}:${file}`)))throw new Error(`CURRENT_OFFICE_SAVED_GIT_CHANGED:${file}`);
  let after=bytes.toString('utf8');for(const h of hunks){if(after.split(h.before).length!==2)throw new Error(`OFFICE_HUNK_NOT_UNIQUE:${file}`);after=after.replace(h.before,()=>h.after);}
  let reverse=after;for(const h of [...hunks].reverse()){if(reverse.split(h.after).length!==2)throw new Error('OFFICE_REVERSE_NOT_UNIQUE');reverse=reverse.replace(h.after,()=>h.before);}
  if(reverse!==bytes.toString('utf8'))throw new Error('OFFICE_FULL_REVERSE_MISMATCH');
  for(const [folder,data]of [['before',bytes],['forward',Buffer.from(after)]]){const dest=path.join(dir,folder,file);fs.mkdirSync(path.dirname(dest),{recursive:true});fs.writeFileSync(dest,data);}
  owners.push({path:file,before_sha256:sha(bytes),after_sha256:sha(after),hunks});
}
if(git('ls-tree',pin,'--',module).toString()!=='')throw new Error('NEW_OFFICE_MODULE_ALREADY_EXISTS');
const body=fs.readFileSync(path.join(dir,'forward',module));
fs.writeFileSync(path.join(dir,'CURRENT_OFFICE_READ_FULL_REVERSE.json'),JSON.stringify({schema:'OFFICE_NATIVE_CORRECTION_READ_CURRENT_FORWARD_V1',
  actual_source_commit:pin,changed_current_owners:owners,new_modules:[{path:module,before_absent:true,after_sha256:sha(body)}],
  consumed_backend_source_commit:'83e228be1bf39d36705e55ae9dbf74da23a54649',actual_backend_mount_commit:'74a9c10f7370cd3641ee95d31f6702ecadd900a7',
  backend_context_module_sha256:'38fbbc24868d4d1725224b8ca43407854ef8146c6a0d2bd409f66fa8e1b32de0',
  read_only:true,mutation_commands:[],native_evidence:false,source_before_native_seal:null},null,2)+'\n');
console.log(JSON.stringify({owners:owners.map(row=>({path:row.path,after_sha256:row.after_sha256})),new_module_sha256:sha(body)}));
