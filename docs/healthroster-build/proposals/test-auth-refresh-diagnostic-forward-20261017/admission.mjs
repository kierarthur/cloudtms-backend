// The existing Office transport supplies auth. Only booleans/statuses leave the page.
export async function admission(page){
 return page.evaluate(async()=>{
  if(typeof authFetch!=='function')return {meStatus:0,actorUuid:false,admin:false,workspaceStatus:0,contract:false,queries:false};
  const me=await authFetch('https://test-cloudtms-backend.kier-88a.workers.dev/api/me',
   {auth401Mode:'return',signal:AbortSignal.timeout(15000)});
  const body=await me.json().catch(()=>({}));const user=body?.user;
  const actorUuid=typeof user?.id==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(user.id);
  const admin=Array.isArray(user?.roles)&&user.roles.some(r=>typeof r==='string'&&r.toLowerCase()==='admin');
  const workspace=await authFetch('https://test-cloudtms-backend.kier-88a.workers.dev/api/weekly-source/v1/workspace?tab=queries',
   {auth401Mode:'return',signal:AbortSignal.timeout(15000)});
  const ws=await workspace.json().catch(()=>({}));
  return {meStatus:me.status,actorUuid,admin,workspaceStatus:workspace.status,
   contract:ws?.contract==='WEEKLY_SOURCE_IMPORT_WORKSPACE_V1',
   queries:!!ws?.queries&&typeof ws.queries==='object'&&!Array.isArray(ws.queries)&&Array.isArray(ws.queries.rows)};
 });
}
