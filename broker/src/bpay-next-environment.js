// PF-13: configuration/caller boundary only; no database/provider readiness probe.
export const BPAY_NEXT_ENVIRONMENT_HEADER='X-CloudTMS-Banking-Environment';
const environments=new Set(['TEST','LIVE']);
export function bpayNextEnvironment(env,request){
  const deployment=environments.has(env?.CANDIDATE_APP_ENVIRONMENT)?env.CANDIDATE_APP_ENVIRONMENT:null;
  const requested=request.headers.get(BPAY_NEXT_ENVIRONMENT_HEADER);
  const caller=environments.has(requested)?requested:null;
  let databaseMatches=false;
  try{
    const base=new URL(env?.SUPABASE_URL);
    if(!base.username&&!base.password&&!base.search&&!base.hash&&base.pathname==='/'){
      databaseMatches=deployment==='TEST' && ((base.origin==='https://codex-cloudtms-miget-gateway.kier-88a.workers.dev')
        ||(base.protocol==='http:'&&['localhost','127.0.0.1','[::1]'].includes(base.hostname)));
    }
  }catch{}
  return Object.freeze({worker_environment:deployment,requested_environment:caller,environment_ready:Boolean(deployment&&caller===deployment&&databaseMatches)});
}
export function bpayNextCapabilities(env,request){
  const environment=bpayNextEnvironment(env,request);
  return Object.freeze({ok:true,scope:'NEXT',...environment,test_mode:environment.worker_environment==='TEST',
    env_mismatch:!environment.environment_ready,cop_available:false,provider_status:'DISABLED',funding_status:'UNAVAILABLE',csv_available:environment.environment_ready});
}
export function bpayNextEnvironmentFailure(env,request){
  const pathname='/'+new URL(request.url).pathname.split('/').filter(Boolean).join('/');
  if(!pathname.startsWith('/api/banking/pay/next/'))return null;
  if(request.method==='GET'&&pathname==='/api/banking/pay/next/owner-status')return null;
  if(bpayNextEnvironment(env,request).environment_ready)return null;
  const command=['/api/banking/pay/next/command','/api/banking/pay/next/preparation','/api/banking/pay/next/preparation-page'].includes(pathname);
  return new Response(JSON.stringify({ok:false,error_code:'BPAY_NEXT_ENVIRONMENT_MISMATCH',retryable:false,...(command?{outcome_unknown:false}:{})}),
    {status:409,headers:{'content-type':'application/json; charset=utf-8','cache-control':'no-store'}});
}
