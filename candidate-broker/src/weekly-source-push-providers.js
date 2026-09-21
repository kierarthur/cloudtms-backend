const encoder = new TextEncoder();
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function text(value) { return String(value == null ? '' : value).trim(); }

function base64Url(bytes) {
  let binary='';
  for (const byte of bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes)) {
    binary+=String.fromCharCode(byte);
  }
  return btoa(binary).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
}

function jsonPart(value) { return base64Url(encoder.encode(JSON.stringify(value))); }

function pemBytes(pem) {
  const cleaned=text(pem).replace(/\\n/g,'\n')
    .replace(/-----BEGIN [^-]+-----/g,'').replace(/-----END [^-]+-----/g,'')
    .replace(/\s+/g,'');
  if (!cleaned) throw new Error('PROVIDER_NOT_READY');
  const raw=atob(cleaned);
  return Uint8Array.from(raw,(char)=>char.charCodeAt(0));
}

function derToJose(signature) {
  const bytes=signature instanceof Uint8Array ? signature : new Uint8Array(signature);
  if (bytes.length===64) return bytes;
  if (bytes[0]!==0x30) throw new Error('PROVIDER_SIGNING_FAILED');
  let offset=2;
  if (bytes[1]&0x80) offset=2+(bytes[1]&0x7f);
  if (bytes[offset++]!==0x02) throw new Error('PROVIDER_SIGNING_FAILED');
  const rLength=bytes[offset++];
  let r=bytes.slice(offset,offset+rLength); offset+=rLength;
  if (bytes[offset++]!==0x02) throw new Error('PROVIDER_SIGNING_FAILED');
  const sLength=bytes[offset++];
  let s=bytes.slice(offset,offset+sLength);
  while (r.length>32 && r[0]===0) r=r.slice(1);
  while (s.length>32 && s[0]===0) s=s.slice(1);
  if (r.length>32 || s.length>32) throw new Error('PROVIDER_SIGNING_FAILED');
  const output=new Uint8Array(64);
  output.set(r,32-r.length); output.set(s,64-s.length);
  return output;
}

async function es256Jwt(header,payload,privateKey) {
  const unsigned=`${jsonPart(header)}.${jsonPart(payload)}`;
  const key=await crypto.subtle.importKey(
    'pkcs8',pemBytes(privateKey),{name:'ECDSA',namedCurve:'P-256'},false,['sign']
  );
  const signature=derToJose(new Uint8Array(await crypto.subtle.sign(
    {name:'ECDSA',hash:'SHA-256'},key,encoder.encode(unsigned)
  )));
  return `${unsigned}.${base64Url(signature)}`;
}

async function rs256Jwt(header,payload,privateKey) {
  const unsigned=`${jsonPart(header)}.${jsonPart(payload)}`;
  const key=await crypto.subtle.importKey(
    'pkcs8',pemBytes(privateKey),{name:'RSASSA-PKCS1-v1_5',hash:'SHA-256'},false,['sign']
  );
  const signature=new Uint8Array(await crypto.subtle.sign(
    'RSASSA-PKCS1-v1_5',key,encoder.encode(unsigned)
  ));
  return `${unsigned}.${base64Url(signature)}`;
}

async function boundedJson(response) {
  const raw=(await response.text()).slice(0,4096);
  try { return raw ? JSON.parse(raw) : {}; } catch { return {}; }
}

function safeStatus(response) {
  return Number.isInteger(response?.status) ? response.status : 0;
}

export function weeklyCandidatePushContent(target) {
  const tranche=text(target?.tranche_kind).toUpperCase();
  const deepLink=target?.deep_link;
  if (!deepLink || deepLink.destination!=='WEEKLY_SOURCE_REQUEST'
      || !UUID_RE.test(text(deepLink.request_id))) throw new Error('WEEKLY_PUSH_CONTENT_INVALID');
  const submission=tranche.startsWith('TIMESHEET_SUBMISSION');
  const reminder=tranche.includes('REMINDER');
  const title=submission
    ? (reminder ? 'Reminder: submit your Timesheet' : 'Please submit your Timesheet')
    : (reminder ? 'Reminder: check your Timesheet hours' : 'Check your Timesheet hours');
  const body=submission
    ? 'Please submit your Timesheet hours so they can be checked.'
    : 'Please check the hours for your Timesheet.';
  return Object.freeze({
    title,body,
    data:Object.freeze({
      destination:'WEEKLY_SOURCE_REQUEST',request_id:text(deepLink.request_id)
    })
  });
}

export function weeklyPushProviderReadiness(env,provider) {
  const kind=text(provider).toUpperCase();
  const required=kind==='APNS'
    ? ['APNS_KEY_ID','APNS_TEAM_ID','APNS_PRIVATE_KEY_P8','APNS_TOPIC','APNS_ENVIRONMENT']
    : kind==='FCM'
      ? ['FCM_PROJECT_ID','FCM_CLIENT_EMAIL','FCM_PRIVATE_KEY'] : [];
  const missing=required.filter((name)=>!text(env[name]));
  if (!required.length) return {ok:false,provider:kind,missing:['SUPPORTED_PROVIDER']};
  if (kind==='APNS' && !['sandbox','production'].includes(text(env.APNS_ENVIRONMENT).toLowerCase())) {
    missing.push('APNS_ENVIRONMENT');
  }
  return {ok:missing.length===0,provider:kind,missing:[...new Set(missing)]};
}

async function sendApns({env,token,content,attemptId,fetchImpl}) {
  const ready=weeklyPushProviderReadiness(env,'APNS');
  if (!ready.ok) return {
    outcome:'DEFINITELY_REJECTED',bounded_provider_receipt:{},
    bounded_error:{error_code:'PROVIDER_NOT_READY'}
  };
  const now=Math.floor(Date.now()/1000);
  let jwt;
  try {
    jwt=await es256Jwt(
      {alg:'ES256',kid:text(env.APNS_KEY_ID)},
      {iss:text(env.APNS_TEAM_ID),iat:now},env.APNS_PRIVATE_KEY_P8
    );
  } catch {
    return {outcome:'DEFINITELY_REJECTED',bounded_provider_receipt:{},
      bounded_error:{error_code:'PROVIDER_NOT_READY'}};
  }
  const host=text(env.APNS_ENVIRONMENT).toLowerCase()==='production'
    ? 'https://api.push.apple.com' : 'https://api.sandbox.push.apple.com';
  let response;
  try {
    response=await fetchImpl(`${host}/3/device/${encodeURIComponent(token)}`,{
      method:'POST',headers:{
        authorization:`bearer ${jwt}`,'content-type':'application/json',
        'apns-topic':text(env.APNS_TOPIC),'apns-push-type':'alert','apns-priority':'10',
        'apns-id':attemptId
      },body:JSON.stringify({aps:{alert:{title:content.title,body:content.body},sound:'default'},...content.data})
    });
  } catch {
    return {outcome:'AMBIGUOUS',bounded_provider_receipt:{},
      bounded_error:{error_code:'PROVIDER_OUTCOME_UNKNOWN'}};
  }
  const body=await boundedJson(response);
  const requestId=text(response.headers.get('apns-id'));
  const receipt={provider_status:safeStatus(response),...(requestId?{provider_request_id:requestId}:{})};
  if (response.status===200) return {
    outcome:'ACCEPTED',provider_message_id:requestId||attemptId,bounded_provider_receipt:receipt,bounded_error:{}
  };
  const reason=text(body.reason);
  if ([400,410].includes(response.status)
      && ['BadDeviceToken','DeviceTokenNotForTopic','Unregistered'].includes(reason)) return {
    outcome:'DEFINITELY_REJECTED',invalid_target:true,bounded_provider_receipt:receipt,
    bounded_error:{error_code:'INVALID_TARGET',provider_status:response.status}
  };
  if (response.status===429 || response.status>=500) return {
    outcome:'TRANSIENT_FAILURE',bounded_provider_receipt:receipt,
    bounded_error:{error_code:'PROVIDER_TEMPORARY',provider_status:response.status}
  };
  return {outcome:'DEFINITELY_REJECTED',bounded_provider_receipt:receipt,
    bounded_error:{error_code:'PROVIDER_REJECTED',provider_status:response.status}};
}

async function fcmAccessToken(env,fetchImpl) {
  const now=Math.floor(Date.now()/1000);
  const assertion=await rs256Jwt(
    {alg:'RS256',typ:'JWT'},
    {iss:text(env.FCM_CLIENT_EMAIL),scope:'https://www.googleapis.com/auth/firebase.messaging',
      aud:'https://oauth2.googleapis.com/token',iat:now,exp:now+3600},
    env.FCM_PRIVATE_KEY
  );
  const body=new URLSearchParams({
    grant_type:'urn:ietf:params:oauth:grant-type:jwt-bearer',assertion
  });
  const response=await fetchImpl('https://oauth2.googleapis.com/token',{
    method:'POST',headers:{'content-type':'application/x-www-form-urlencoded'},body
  });
  const payload=await boundedJson(response);
  if (!response.ok || !text(payload.access_token)) throw new Error('FCM_AUTH_UNAVAILABLE');
  return text(payload.access_token);
}

async function sendFcm({env,token,content,fetchImpl}) {
  const ready=weeklyPushProviderReadiness(env,'FCM');
  if (!ready.ok) return {outcome:'DEFINITELY_REJECTED',bounded_provider_receipt:{},
    bounded_error:{error_code:'PROVIDER_NOT_READY'}};
  let accessToken;
  try { accessToken=await fcmAccessToken(env,fetchImpl); }
  catch { return {outcome:'TRANSIENT_FAILURE',bounded_provider_receipt:{},
    bounded_error:{error_code:'PROVIDER_AUTH_TEMPORARY'}}; }
  let response;
  try {
    response=await fetchImpl(
      `https://fcm.googleapis.com/v1/projects/${encodeURIComponent(text(env.FCM_PROJECT_ID))}/messages:send`,
      {method:'POST',headers:{authorization:`Bearer ${accessToken}`,'content-type':'application/json'},
        body:JSON.stringify({message:{token,notification:{title:content.title,body:content.body},data:content.data}})}
    );
  } catch {
    return {outcome:'AMBIGUOUS',bounded_provider_receipt:{},
      bounded_error:{error_code:'PROVIDER_OUTCOME_UNKNOWN'}};
  }
  const body=await boundedJson(response);
  const messageId=text(body.name);
  const receipt={provider_status:safeStatus(response),...(messageId?{provider_request_id:messageId}:{})};
  if (response.ok && messageId) return {
    outcome:'ACCEPTED',provider_message_id:messageId,bounded_provider_receipt:receipt,bounded_error:{}
  };
  const status=text(body?.error?.status).toUpperCase();
  const detailCodes=Array.isArray(body?.error?.details)
    ? body.error.details.map((item)=>text(item?.errorCode).toUpperCase()) : [];
  if (status==='UNREGISTERED' || detailCodes.includes('UNREGISTERED')) return {
    outcome:'DEFINITELY_REJECTED',invalid_target:true,bounded_provider_receipt:receipt,
    bounded_error:{error_code:'INVALID_TARGET',provider_status:response.status}
  };
  if (response.status===429 || response.status>=500) return {
    outcome:'TRANSIENT_FAILURE',bounded_provider_receipt:receipt,
    bounded_error:{error_code:'PROVIDER_TEMPORARY',provider_status:response.status}
  };
  return {outcome:'DEFINITELY_REJECTED',bounded_provider_receipt:receipt,
    bounded_error:{error_code:'PROVIDER_REJECTED',provider_status:response.status}};
}

export async function sendWeeklySourcePush({env,target,token,attemptId,fetchImpl=fetch}) {
  if (!UUID_RE.test(text(attemptId)) || !text(token) || text(token).length>8192) {
    throw new Error('WEEKLY_PUSH_DELIVERY_INVALID');
  }
  const content=weeklyCandidatePushContent(target);
  const provider=text(target?.provider).toUpperCase();
  if (provider==='APNS') return sendApns({env,token,content,attemptId,fetchImpl});
  if (provider==='FCM') return sendFcm({env,token,content,fetchImpl});
  return {outcome:'DEFINITELY_REJECTED',bounded_provider_receipt:{},
    bounded_error:{error_code:'PROVIDER_NOT_READY'}};
}

export const weeklySourcePushProviderInternals=Object.freeze({
  base64Url,derToJose,es256Jwt,rs256Jwt
});
