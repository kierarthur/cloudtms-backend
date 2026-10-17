// Source-only children get no ambient database, service or Git authority.
export function sourceCheckEnvironment(inherited,{gitDir,workTree,finalDefinerBase}){
 if(typeof gitDir!=='string'||typeof workTree!=='string'||!/^[a-f0-9]{40}$/.test(finalDefinerBase??''))throw Error('SOURCE_CHECK_CONTEXT_REFUSED');
 const allowed=new Set(['PATH','SYSTEMROOT','WINDIR','SYSTEMDRIVE','COMSPEC','PATHEXT','USERPROFILE','HOMEDRIVE','HOMEPATH','HOME',
  'APPDATA','LOCALAPPDATA','PROGRAMDATA','PROGRAMFILES','PROGRAMFILES(X86)','PROGRAMW6432','TEMP','TMP','TMPDIR','OS',
  'PROCESSOR_ARCHITECTURE','PROCESSOR_IDENTIFIER','PROCESSOR_LEVEL','PROCESSOR_REVISION','NUMBER_OF_PROCESSORS',
  'LANG','LC_ALL','LC_CTYPE','TZ','TERM','CI','NO_COLOR','FORCE_COLOR']);
 const clean=Object.fromEntries(Object.entries(inherited).filter(([key])=>allowed.has(key.toUpperCase())));
 return {...clean,GIT_DIR:gitDir,GIT_WORK_TREE:workTree,CLOUDTMS_FINAL_DEFINER_BASE:finalDefinerBase};
}
