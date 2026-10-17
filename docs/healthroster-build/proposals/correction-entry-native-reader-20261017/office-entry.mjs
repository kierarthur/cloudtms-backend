// Pure closed Health host contribution. No Banking component body or new modal.
import {decodeCorrectionEntry} from './correction-entry.mjs';
export async function readCorrectionEntryForHost({binding,timesheetId,authFetch,api}){
 if(!binding?.current?.()||binding.initialTimesheetId!==timesheetId
  ||binding.context?.timesheetDetails?.timesheet?.sheet_scope==='DAILY')throw new Error('CORRECTION_CURRENT_PARENT_REQUIRED');
 if(typeof api!=='function')throw new Error('CORRECTION_ENTRY_UNAVAILABLE');
 const response=await authFetch(api(`/api/timesheets/${encodeURIComponent(timesheetId)}/correction-entry`),{method:'GET',cache:'no-store'});
 if(!binding.current())throw new Error('CORRECTION_CURRENT_PARENT_REQUIRED');
 if(!response.ok)throw new Error(response.status===403?'TIMESHEET_CORRECTION_ENTRY_NOT_ALLOWED':'CORRECTION_ENTRY_UNAVAILABLE');
 const entry=decodeCorrectionEntry(await response.json(),timesheetId);
 if(!binding.current())throw new Error('CORRECTION_CURRENT_PARENT_REQUIRED');
 // The producer is deliberately closed until root seals complete Preview and
 // CM17. Its false value is neither a transaction capability nor a Ready state.
 return entry;
}
