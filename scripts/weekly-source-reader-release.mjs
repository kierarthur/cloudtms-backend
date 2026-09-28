// Versioned, exact reader preparation; deliberately not an arbitrary SQL hook.
export const READER_PREPARATION = Object.freeze({
  version: 'WEEKLY_SOURCE_READER_PREPARATION_V1',
  readers: Object.freeze([
    'supabase/repeatable/04082026_1146_pay_workbench_timesheet_input_fingerprint_v1.sql',
    'supabase/repeatable/04082026_2314_pay_workbench_unit_economic_occurrence_page_v1.sql',
  ]),
  pageRoutine: 'private.weekly_source_workbench_inventory_install_page_v1',
  maximumCallsPerInvocation: 1000,
});

export function readerReleasePhases(pending, inventory) {
  if (!pending.some(item => READER_PREPARATION.readers.includes(item.path))) {
    return { ordinary: pending, readers: [] };
  }
  const readers=READER_PREPARATION.readers.map(file=>{
    const item=inventory.find(row=>row.path===file);
    if(!item)throw Error('READER_PREPARATION_AUTHORITY_MISSING: '+file);
    return item;
  });
  return {ordinary:pending.filter(item=>!READER_PREPARATION.readers.includes(item.path)),readers};
}

export function prepareSourceReaders(executeSql) {
  // Each page commits independently. Persistent server progress and per-head
  // counters survive an interrupted invocation. A work quantum is not a data
  // ceiling: exhaustion leaves the old readers installed; rerun resumes.
  for(let call=0;call<READER_PREPARATION.maximumCallsPerInvocation;call++) {
    const value=executeSql(`begin; set local lock_timeout='5s'; set local statement_timeout='15s'; select ${READER_PREPARATION.pageRoutine}(); commit;`).trim();
    if(value==='t')return {calls:call+1};
    if(value!=='f')throw Error('READER_PREPARATION_INVALID_RESULT');
  }
  throw Error('READER_PREPARATION_RESUME_REQUIRED: bounded invocation finished; previous readers retained; rerun the same release to resume');
}

export function readerActivationSql(readers,readText,ledgerSql=()=> '') {
  if(readers.length!==2||readers.some((row,index)=>row.path!==READER_PREPARATION.readers[index])) {
    throw Error('READER_ACTIVATION_REQUIRES_EXACT_PAIR');
  }
  const statements=readers.map(item=>{
    const source=readText(item.path).replace(/\r\n/g,'\n');
    // These two canonical single-definition files have no transaction commands
    // or psql includes. Refuse changed packaging rather than strip unknown SQL.
    if(/^\s*(?:\\|begin\s*;|commit\s*;|rollback\s*;|start\s+transaction\b)/im.test(source)) {
      throw Error('READER_ACTIVATION_UNEXPECTED_TRANSACTION_OR_META_COMMAND');
    }
    return source+'\n'+ledgerSql(item);
  });
  return `begin;\nset local lock_timeout='5s';\nset local statement_timeout='30s';\ndo $ready$ begin if not exists(select 1 from private.weekly_source_workbench_inventory_install_v1 where singleton and complete) then raise exception 'READER_PREPARATION_NOT_COMPLETE'; end if; end $ready$;\n${statements.join('\n')}\ncommit;`;
}
