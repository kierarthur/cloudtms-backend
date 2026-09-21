import { controlPlaneRpc } from './control-plane-client.js';
import { verifyWeeklySourceDeliveryRequest } from '../../broker/src/weekly-source/delivery-auth.mjs';
import { sendWeeklySourcePush, weeklyPushProviderReadiness } from './weekly-source-push-providers.js';

const encoder=new TextEncoder();
const decoder=new TextDecoder();
const UUID_RE=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const HASH_RE=/^[0-9a-f]{64}$/i;

function text(value) { return String(value==null?'':value).trim(); }
function upper(value) { return text(value).toUpperCase(); }

function json(status,body) {
  return new Response(JSON.stringify(body),{status,headers:{
    'content-type':'application/json; charset=utf-8','cache-control':'no-store'
  }});
}

async function boundedJson(request) {
  const declared=Number(request.headers.get('content-length')||0);
  if (declared>128*1024) throw new Error('WEEKLY_PUSH_REQUEST_INVALID');
  const bytes=new Uint8Array(await request.arrayBuffer());
  if (bytes.length>128*1024) throw new Error('WEEKLY_PUSH_REQUEST_INVALID');
  const value=JSON.parse(decoder.decode(bytes));
  if (!value || typeof value!=='object' || Array.isArray(value)) throw new Error('WEEKLY_PUSH_REQUEST_INVALID');
  return value;
}

function hexText(value) {
  const source=text(value);
  if (!/^[0-9a-f]+$/i.test(source) || source.length%2) throw new Error('WEEKLY_PUSH_MATERIAL_INVALID');
  const bytes=new Uint8Array(source.length/2);
  for (let i=0;i<bytes.length;i+=1) bytes[i]=Number.parseInt(source.slice(i*2,i*2+2),16);
  return decoder.decode(bytes);
}

async function hmacHex(secret,value) {
  if (text(secret).length<32) throw new Error('WEEKLY_PUSH_CONFIGURATION_UNAVAILABLE');
  const key=await crypto.subtle.importKey(
    'raw',encoder.encode(secret),{name:'HMAC',hash:'SHA-256'},false,['sign']
  );
  return Array.from(new Uint8Array(await crypto.subtle.sign('HMAC',key,encoder.encode(value))),
    (byte)=>byte.toString(16).padStart(2,'0')).join('');
}

async function internalContext(env) {
  return {
    service_request_verified:true,source_authority:'WEEKLY_SOURCE_DELIVERY',
    actor_identity_hmac:await hmacHex(
      env.WEEKLY_SOURCE_DELIVERY_SERVICE_SECRET,
      `weekly-source-delivery-actor-v1:${upper(env.CANDIDATE_APP_ENVIRONMENT)}`
    )
  };
}

async function rpc(env,functionName,request) {
  return controlPlaneRpc(env,'control',functionName,{
    p_internal_context:await internalContext(env),p_request:request
  });
}

export async function syncWeeklyPushPreferences(env,request) {
  return rpc(env,'weekly_push_preferences_sync_v1',request);
}

function exactKeys(value,allowed) {
  return value && typeof value==='object' && !Array.isArray(value)
    && Object.keys(value).every((key)=>allowed.includes(key));
}

async function deliver(env,body,deps) {
  if (!exactKeys(body,['operation','provider_attempt_id','target'])
      || !UUID_RE.test(text(body.provider_attempt_id))
      || !exactKeys(body.target,[
        'dispatch_target_id','dispatch_command_id','provider','tranche_kind',
        'deep_link','safe_target_snapshot','target_snapshot_hash','provider_idempotency_key'
      ])) throw new Error('WEEKLY_PUSH_REQUEST_INVALID');
  const target=body.target;
  const safe=target.safe_target_snapshot;
  if (!exactKeys(safe,[
    'control_plane_snapshot_id','snapshot_device_id','provider','target_revision_hash'
  ]) || !UUID_RE.test(text(safe.control_plane_snapshot_id))
      || !UUID_RE.test(text(safe.snapshot_device_id))
      || !HASH_RE.test(text(safe.target_revision_hash))
      || text(target.target_snapshot_hash).toLowerCase()!==text(safe.target_revision_hash).toLowerCase()
      || upper(target.provider)!==upper(safe.provider)) throw new Error('WEEKLY_PUSH_REQUEST_INVALID');
  const material=await rpc(env,'weekly_push_target_material_v1',{
    snapshot_id:safe.control_plane_snapshot_id,snapshot_device_id:safe.snapshot_device_id,
    target_revision_hash:safe.target_revision_hash
  });
  if (material?.ok!==true || upper(material.provider)!==upper(target.provider)
      || !text(material.token_ciphertext_hex)) throw new Error('WEEKLY_PUSH_MATERIAL_INVALID');
  const envelope=hexText(material.token_ciphertext_hex);
  const opened=await deps.openVersionedEnvelope(
    env,deps.deviceEncryptionAuthority,'mytms-control-plane-device-token-v1',envelope
  );
  const token=text(opened?.payload?.token);
  if (!token || upper(opened?.payload?.provider)!==upper(target.provider)) {
    throw new Error('WEEKLY_PUSH_MATERIAL_INVALID');
  }
  const result=await sendWeeklySourcePush({
    env,target,token,attemptId:body.provider_attempt_id,fetchImpl:deps.fetchImpl||fetch
  });
  if (result.invalid_target===true) {
    let retired=false;
    try {
      const retirement=await rpc(env,'weekly_push_target_retire_invalid_v1',{
        snapshot_id:safe.control_plane_snapshot_id,snapshot_device_id:safe.snapshot_device_id,
        target_revision_hash:safe.target_revision_hash,provider:upper(target.provider),
        reason:'INVALID_TARGET'
      });
      retired=retirement?.retirement_recorded===true;
    } catch { retired=false; }
    result.bounded_provider_receipt={
      ...(result.bounded_provider_receipt||{}),retirement_recorded:retired
    };
  }
  delete result.invalid_target;
  return result;
}

export async function handleWeeklySourcePushAuthority(request,env,deps) {
  if (new URL(request.url).pathname!=='/internal/weekly-source-push/v1') return null;
  try {
    if (request.method!=='POST' || !(await verifyWeeklySourceDeliveryRequest(request,env))) {
      return json(401,{ok:false,error_code:'WEEKLY_PUSH_INTERNAL_AUTH_REQUIRED'});
    }
    const body=await boundedJson(request.clone());
    const operation=upper(body.operation);
    if (operation==='READINESS') {
      return json(200,{ok:true,providers:{
        APNS:weeklyPushProviderReadiness(env,'APNS'),FCM:weeklyPushProviderReadiness(env,'FCM')
      }});
    }
    if (operation==='SNAPSHOT') {
      if (!exactKeys(body,['operation','request'])) throw new Error('WEEKLY_PUSH_REQUEST_INVALID');
      return json(200,await rpc(env,'weekly_push_target_snapshot_v1',body.request));
    }
    if (operation==='DELIVER') return json(200,await deliver(env,body,deps));
    throw new Error('WEEKLY_PUSH_OPERATION_INVALID');
  } catch (error) {
    const code=/^[A-Z][A-Z0-9_]{2,119}$/.test(text(error?.message))
      ? text(error.message) : 'WEEKLY_PUSH_DEPENDENCY_UNAVAILABLE';
    const status=code.endsWith('_INVALID') ? 400 : 503;
    return json(status,{ok:false,error_code:code});
  }
}

export const weeklySourcePushAuthorityInternals=Object.freeze({
  boundedJson,deliver,hexText,internalContext
});
