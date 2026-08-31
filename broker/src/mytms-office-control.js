import sanitizeHtml from 'sanitize-html';

import {
  controlPlaneEnabled,
  controlPlaneRpc
} from '../../candidate-broker/src/control-plane-client.js';
import { candidateFederatedIdentityHmac } from './candidate-route-context.js';

const encoder = new TextEncoder();
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const SHA256_RE = /^[a-f0-9]{64}$/;
const SANITIZER_POLICY_VERSION = 'MYTMS_EMAIL_HTML_V1_SANITIZE_HTML_2_17_7';
const MANAGER_SANITIZER_POLICY_VERSION = 'MANAGER_EMAIL_SAFE_HTML_V1';
const MANAGER_SUBMISSION_TYPES = Object.freeze(['TIMESHEET', 'EXPENSE_CLAIM']);
const MANAGER_MAIL_KINDS = Object.freeze([
  'INITIAL', 'REMINDER', 'RENEWAL', 'WITHDRAWAL', 'CANCELLATION'
]);
const MANAGER_LINK_MAIL_KINDS = new Set(['INITIAL', 'REMINDER', 'RENEWAL']);
const MAX_JSON_BYTES = 256 * 1024;
const MAX_AGENCY_LOGO_BYTES = 512 * 1024;
const AGENCY_LOGO_KEY_RE = /^candidate-app\/branding\/[a-f0-9]{64}\.png$/;
const DIRECTORY_KINDS = Object.freeze({
  hospital_addresses: Object.freeze({
    required: Object.freeze(['hospital_name', 'address']),
    allowed: Object.freeze(['hospital_name', 'address', 'telephone', 'map_query'])
  }),
  accommodation_contacts: Object.freeze({
    required: Object.freeze(['hospital_name', 'office_name']),
    allowed: Object.freeze([
      'hospital_name', 'office_name', 'address', 'telephone', 'email', 'working_hours'
    ])
  })
});

export class MyTmsOfficeError extends Error {
  constructor(status, code) {
    super(code);
    this.status = status;
    this.code = code;
  }
}

function text(value) {
  return String(value == null ? '' : value).trim();
}

function upper(value) {
  return text(value).toUpperCase();
}

function isObject(value) {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (isObject(value)) {
    return `{${Object.keys(value).sort().map(
      key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`
    ).join(',')}}`;
  }
  return JSON.stringify(value === undefined ? null : value);
}

function bytesToHex(bytes) {
  return Array.from(new Uint8Array(bytes), byte => byte.toString(16).padStart(2, '0')).join('');
}

function base64Url(bytes) {
  let binary = '';
  for (const byte of new Uint8Array(bytes)) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/g, '');
}

async function sha256Hex(value) {
  return bytesToHex(await crypto.subtle.digest('SHA-256', encoder.encode(String(value))));
}

async function sha256BytesHex(value) {
  return bytesToHex(await crypto.subtle.digest('SHA-256', value));
}

function bytesToBase64(bytes) {
  let binary = '';
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function decodeAgencyLogoDataUrl(value) {
  const source = String(value == null ? '' : value);
  const match = /^data:image\/png;base64,([A-Za-z0-9+/]+={0,2})$/.exec(source);
  if (!match) throw new MyTmsOfficeError(400, 'MYTMS_AGENCY_LOGO_INVALID');
  let binary;
  try {
    binary = atob(match[1]);
  } catch {
    throw new MyTmsOfficeError(400, 'MYTMS_AGENCY_LOGO_INVALID');
  }
  if (!binary.length || binary.length > MAX_AGENCY_LOGO_BYTES) {
    throw new MyTmsOfficeError(413, 'MYTMS_AGENCY_LOGO_TOO_LARGE');
  }
  const bytes = Uint8Array.from(binary, character => character.charCodeAt(0));
  const png = bytes.length >= 24
    && bytes[0] === 0x89 && bytes[1] === 0x50 && bytes[2] === 0x4e && bytes[3] === 0x47
    && bytes[4] === 0x0d && bytes[5] === 0x0a && bytes[6] === 0x1a && bytes[7] === 0x0a;
  if (!png) throw new MyTmsOfficeError(400, 'MYTMS_AGENCY_LOGO_INVALID');
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const width = view.getUint32(16, false);
  const height = view.getUint32(20, false);
  if (width < 64 || height < 64 || width > 1024 || height > 1024
      || Math.abs(width - height) > 2) {
    throw new MyTmsOfficeError(400, 'MYTMS_AGENCY_LOGO_INVALID');
  }
  return { bytes, width, height, media_type: 'image/png' };
}

async function hmacBytes(secret, purpose, value) {
  const material = text(secret);
  if (material.length < 12) {
    throw new MyTmsOfficeError(503, 'MYTMS_OFFICE_CONFIGURATION_UNAVAILABLE');
  }
  const key = await crypto.subtle.importKey(
    'raw', encoder.encode(material), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
  );
  return new Uint8Array(await crypto.subtle.sign(
    'HMAC', key, encoder.encode(`${purpose}:${value}`)
  ));
}

function agencyId(env) {
  const value = text(env.MYTMS_OFFICE_AGENCY_ID).toLowerCase();
  if (!UUID_RE.test(value)) {
    throw new MyTmsOfficeError(503, 'MYTMS_OFFICE_CONFIGURATION_UNAVAILABLE');
  }
  return value;
}

function assertOfficeControlEnabled(env) {
  if (!controlPlaneEnabled(env)
      || upper(env.MYTMS_OFFICE_CONTROL_ENABLED) !== 'TRUE') {
    throw new MyTmsOfficeError(503, 'MYTMS_OFFICE_CONTROL_DISABLED');
  }
}

async function officeContext(env, user, capabilities, additions = {}) {
  const actorIdentity = text(user?.id || user?.email).toLowerCase();
  if (!actorIdentity || !Array.isArray(capabilities) || !capabilities.length) {
    throw new MyTmsOfficeError(403, 'MYTMS_OFFICE_PERMISSION_DENIED');
  }
  return {
    actor_role: 'admin', agency_id: agencyId(env),
    service_request_verified: true, service_request_version: 1,
    authorized_capabilities: [...new Set(capabilities)],
    actor_identity_hmac: bytesToHex(await hmacBytes(
      env.MYTMS_OFFICE_ACTOR_IDENTITY_SECRET,
      'mytms-office-actor-v1', actorIdentity
    )),
    ...additions
  };
}

export function sanitizeMyTmsEmailHtml(raw) {
  const source = String(raw == null ? '' : raw);
  if (source.length > 100_000) {
    throw new MyTmsOfficeError(400, 'MYTMS_TEMPLATE_TOO_LARGE');
  }
  return sanitizeHtml(source, {
    allowedTags: [
      'p', 'br', 'strong', 'b', 'em', 'i', 'u', 's', 'ul', 'ol', 'li',
      'h1', 'h2', 'h3', 'blockquote', 'a', 'div', 'span', 'table', 'thead',
      'tbody', 'tr', 'td', 'th', 'img'
    ],
    allowedAttributes: {
      a: ['href', 'title', 'target', 'rel'],
      img: ['src', 'alt', 'title', 'width', 'height'],
      td: ['colspan', 'rowspan', 'align', 'valign', 'style'],
      th: ['colspan', 'rowspan', 'align', 'valign', 'style'],
      p: ['align', 'style'], div: ['align', 'style'], span: ['style'],
      table: ['width', 'cellpadding', 'cellspacing', 'role', 'style'],
      h1: ['align', 'style'], h2: ['align', 'style'], h3: ['align', 'style']
    },
    allowedSchemes: ['https', 'mailto'],
    allowedSchemesByTag: { img: ['https'], a: ['https', 'mailto'] },
    allowProtocolRelative: false,
    enforceHtmlBoundary: true,
    disallowedTagsMode: 'discard',
    nonTextTags: ['script', 'style', 'textarea', 'option', 'xmp', 'noembed', 'noframes'],
    allowedStyles: {
      '*': {
        color: [/^#[0-9a-f]{3,8}$/i, /^rgb\([0-9 ,.%]+\)$/i],
        'background-color': [/^#[0-9a-f]{3,8}$/i, /^rgb\([0-9 ,.%]+\)$/i],
        'font-size': [/^[0-9.]+(?:px|em|rem|%)$/i],
        'font-weight': [/^(?:normal|bold|[1-9]00)$/i],
        'font-style': [/^(?:normal|italic)$/i],
        'text-align': [/^(?:left|right|center|justify)$/i],
        'text-decoration': [/^(?:none|underline|line-through)$/i],
        'line-height': [/^[0-9.]+(?:px|em|rem|%)?$/i],
        margin: [/^[0-9 .%-]+$/], padding: [/^[0-9 .%-]+$/],
        width: [/^(?:auto|[0-9.]+(?:px|%))$/i],
        'max-width': [/^(?:none|[0-9.]+(?:px|%))$/i],
        border: [/^[0-9a-z .#()-]+$/i], 'border-collapse': [/^collapse$/i]
      }
    },
    transformTags: {
      a: (tagName, attribs) => ({
        tagName,
        attribs: {
          ...attribs,
          ...(attribs.target === '_blank' ? { rel: 'noopener noreferrer' } : {})
        }
      })
    }
  });
}

export function sanitizeManagerEmailHtml(raw) {
  const source = String(raw == null ? '' : raw);
  if (!source || source.length > 50_000) {
    throw new MyTmsOfficeError(400, 'MYTMS_MANAGER_TEMPLATE_INVALID');
  }
  if (/<(?:a|img|svg|iframe|form|script|style)\b|\b(?:href|src|on\w+)\s*=|https?:\/\//i.test(source)) {
    throw new MyTmsOfficeError(400, 'MYTMS_MANAGER_TEMPLATE_INVALID');
  }
  const sanitized = sanitizeHtml(source, {
    allowedTags: [
      'p', 'br', 'strong', 'b', 'em', 'i', 'u', 's', 'ul', 'ol', 'li',
      'h1', 'h2', 'h3', 'blockquote', 'div', 'span', 'table', 'thead',
      'tbody', 'tr', 'td', 'th'
    ],
    allowedAttributes: {
      td: ['colspan', 'rowspan', 'align'], th: ['colspan', 'rowspan', 'align'],
      p: ['align'], div: ['align'], h1: ['align'], h2: ['align'], h3: ['align']
    },
    allowedSchemes: [], allowProtocolRelative: false, enforceHtmlBoundary: true,
    disallowedTagsMode: 'discard',
    nonTextTags: ['script', 'style', 'textarea', 'option', 'xmp', 'noembed', 'noframes']
  }).trim();
  if (!sanitized || /<(?:a|img|svg|iframe|form|script|style)\b|\b(?:href|src|on\w+)\s*=|https?:\/\//i.test(sanitized)) {
    throw new MyTmsOfficeError(400, 'MYTMS_MANAGER_TEMPLATE_INVALID');
  }
  return sanitized;
}

function supabaseHeaders(env, prefer = '') {
  const key = text(env.SUPABASE_SERVICE_ROLE_KEY);
  if (!key || !text(env.SUPABASE_URL)) {
    throw new MyTmsOfficeError(503, 'MYTMS_OFFICE_DATA_PLANE_UNAVAILABLE');
  }
  return {
    apikey: key, authorization: `Bearer ${key}`,
    'content-type': 'application/json', ...(prefer ? { prefer } : {})
  };
}

async function boundedJson(response) {
  const declared = Number(response.headers.get('content-length') || 0);
  if (declared > MAX_JSON_BYTES) {
    throw new MyTmsOfficeError(502, 'MYTMS_OFFICE_RESPONSE_INVALID');
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.byteLength > MAX_JSON_BYTES) {
    throw new MyTmsOfficeError(502, 'MYTMS_OFFICE_RESPONSE_INVALID');
  }
  try {
    return bytes.byteLength ? JSON.parse(new TextDecoder().decode(bytes)) : null;
  } catch {
    throw new MyTmsOfficeError(502, 'MYTMS_OFFICE_RESPONSE_INVALID');
  }
}

async function agencyRpc(env, rpcName, parameters = {}) {
  const base = text(env.SUPABASE_URL).replace(/\/$/, '');
  const response = await fetch(`${base}/rest/v1/rpc/${encodeURIComponent(rpcName)}`, {
    method: 'POST', headers: supabaseHeaders(env), body: JSON.stringify(parameters)
  });
  const result = await boundedJson(response);
  if (!response.ok) {
    const code = upper(result?.message);
    if (code === 'MYTMS_SETTINGS_VERSION_CONFLICT') {
      throw new MyTmsOfficeError(409, code);
    }
    if (code.startsWith('CANDIDATE_MANAGER_EMAIL_SETTINGS_')) {
      throw new MyTmsOfficeError(400, 'MYTMS_MANAGER_TEMPLATE_INVALID');
    }
    if (code === 'CANDIDATE_HOME_ANNOUNCEMENT_INVALID'
        || code === 'CANDIDATE_HOME_ANNOUNCEMENT_REQUEST_INVALID') {
      throw new MyTmsOfficeError(400, 'MYTMS_HOME_ANNOUNCEMENT_INVALID');
    }
    if (code === 'CANDIDATE_HOME_ANNOUNCEMENT_IDEMPOTENCY_CONFLICT') {
      throw new MyTmsOfficeError(409, 'IDEMPOTENCY_CONFLICT');
    }
    if (code === 'CANDIDATE_DAILY_INFORMATION_INVALID'
        || code === 'CANDIDATE_DAILY_INFORMATION_REQUEST_INVALID'
        || code === 'CANDIDATE_DAILY_INFORMATION_DUPLICATE') {
      throw new MyTmsOfficeError(400, code);
    }
    if (code === 'CANDIDATE_DAILY_INFORMATION_IDEMPOTENCY_CONFLICT') {
      throw new MyTmsOfficeError(409, 'IDEMPOTENCY_CONFLICT');
    }
    throw new MyTmsOfficeError(503, 'MYTMS_MANAGER_SETTINGS_UNAVAILABLE');
  }
  if (!isObject(result) || result.ok !== true) {
    throw new MyTmsOfficeError(502, 'MYTMS_OFFICE_RESPONSE_INVALID');
  }
  return result;
}

function exactObjectKeys(value, expected) {
  return isObject(value)
    && Object.keys(value).sort().join('|') === [...expected].sort().join('|');
}

function normalizeDirectoryEntries(value, kind) {
  const shape = DIRECTORY_KINDS[kind];
  if (!shape || !Array.isArray(value)
      || value.length > (kind === 'hospital_addresses' ? 100 : 200)) {
    throw new MyTmsOfficeError(502, 'MYTMS_OFFICE_RESPONSE_INVALID');
  }
  return value.map((source) => {
    if (!isObject(source)
        || Object.keys(source).some(key => !shape.allowed.includes(key))
        || shape.required.some(key => !Object.hasOwn(source, key))) {
      throw new MyTmsOfficeError(502, 'MYTMS_OFFICE_RESPONSE_INVALID');
    }
    const output = {};
    for (const key of shape.allowed) {
      if (!Object.hasOwn(source, key)) continue;
      if (typeof source[key] !== 'string') {
        throw new MyTmsOfficeError(502, 'MYTMS_OFFICE_RESPONSE_INVALID');
      }
      const valueText = String(source[key]);
      const maximum = ['address', 'map_query'].includes(key) ? 600
        : key === 'email' ? 254 : key === 'working_hours' ? 240
          : key === 'telephone' ? 40 : 160;
      if (valueText.length > maximum
          || (shape.required.includes(key) && !valueText.trim())) {
        throw new MyTmsOfficeError(502, 'MYTMS_OFFICE_RESPONSE_INVALID');
      }
      output[key] = valueText;
    }
    return output;
  });
}

function publicDailyInformationSettings(result) {
  if (!isObject(result) || result.ok !== true) {
    throw new MyTmsOfficeError(502, 'MYTMS_OFFICE_RESPONSE_INVALID');
  }
  const version = Number(result.version);
  const semanticHash = text(result.semantic_sha256_hex).toLowerCase();
  if (!Number.isSafeInteger(version) || version < 1 || !SHA256_RE.test(semanticHash)) {
    throw new MyTmsOfficeError(502, 'MYTMS_OFFICE_RESPONSE_INVALID');
  }
  return {
    ok: true,
    hospital_addresses: normalizeDirectoryEntries(result.hospital_addresses, 'hospital_addresses'),
    accommodation_contacts: normalizeDirectoryEntries(
      result.accommodation_contacts, 'accommodation_contacts'
    ),
    version,
    semantic_sha256_hex: semanticHash,
    updated_at_utc: result.updated_at_utc || null,
    idempotent_replay: result.idempotent_replay === true
  };
}

export async function getMyTmsDailyInformationSettings(env, user) {
  assertOfficeControlEnabled(env);
  await officeContext(env, user, ['MYTMS_SETTINGS_READ']);
  return publicDailyInformationSettings(
    await agencyRpc(env, 'candidate_daily_information_settings_get_v1')
  );
}

export async function setMyTmsDailyInformationSettings(env, user, request) {
  assertOfficeControlEnabled(env);
  const source = isObject(request) ? request : {};
  const expectedVersion = Number(source.expected_version);
  const idempotencyKey = text(source.idempotency_key);
  if (!Number.isSafeInteger(expectedVersion) || expectedVersion < 1
      || idempotencyKey.length < 16 || idempotencyKey.length > 200
      || !Array.isArray(source.hospital_addresses)
      || !Array.isArray(source.accommodation_contacts)) {
    throw new MyTmsOfficeError(400, 'CANDIDATE_DAILY_INFORMATION_REQUEST_INVALID');
  }
  const context = await officeContext(env, user, ['MYTMS_SETTINGS_WRITE']);
  return publicDailyInformationSettings(await agencyRpc(
    env, 'candidate_daily_information_settings_set_v1', {
      p_expected_version: expectedVersion,
      p_hospital_addresses: source.hospital_addresses,
      p_accommodation_contacts: source.accommodation_contacts,
      p_actor_identity_hmac_hex: context.actor_identity_hmac,
      p_idempotency_key: idempotencyKey,
      p_now_utc: new Date().toISOString()
    }
  ));
}

function normalizeManagerTemplates(value) {
  if (!exactObjectKeys(value, ['schema_version', ...MANAGER_SUBMISSION_TYPES])
      || value.schema_version !== 'CANDIDATE_MANAGER_EMAIL_TEMPLATES_V1') {
    throw new MyTmsOfficeError(400, 'MYTMS_MANAGER_TEMPLATE_INVALID');
  }
  const output = { schema_version: value.schema_version };
  for (const submissionType of MANAGER_SUBMISSION_TYPES) {
    const sourceType = value[submissionType];
    if (!exactObjectKeys(sourceType, MANAGER_MAIL_KINDS)) {
      throw new MyTmsOfficeError(400, 'MYTMS_MANAGER_TEMPLATE_INVALID');
    }
    output[submissionType] = {};
    for (const kind of MANAGER_MAIL_KINDS) {
      const source = sourceType[kind];
      if (!exactObjectKeys(source, ['subject', 'body_text', 'body_html', 'button_text', 'include_link'])) {
        throw new MyTmsOfficeError(400, 'MYTMS_MANAGER_TEMPLATE_INVALID');
      }
      const subject = text(source.subject);
      const bodyText = text(source.body_text);
      const bodyHtml = sanitizeManagerEmailHtml(source.body_html);
      const expectsLink = MANAGER_LINK_MAIL_KINDS.has(kind);
      const buttonText = source.button_text == null ? null : text(source.button_text);
      if (!subject || subject.length > 240 || !bodyText || bodyText.length > 20_000
          || /\{\{|\}\}|https?:\/\//i.test(`${subject}\n${bodyText}`)
          || source.include_link !== expectsLink
          || (expectsLink && (!buttonText || buttonText.length > 80))
          || (!expectsLink && buttonText !== null)) {
        throw new MyTmsOfficeError(400, 'MYTMS_MANAGER_TEMPLATE_INVALID');
      }
      output[submissionType][kind] = {
        subject, body_text: bodyText, body_html: bodyHtml,
        button_text: buttonText, include_link: expectsLink
      };
    }
  }
  return output;
}

export async function getMyTmsManagerEmailSettings(env, user) {
  assertOfficeControlEnabled(env);
  const id = agencyId(env);
  const context = await officeContext(env, user, ['MYTMS_SETTINGS_READ']);
  const [agencyTemplates, managerOrigin] = await Promise.all([
    agencyRpc(env, 'candidate_manager_email_settings_get_v1'),
    controlPlaneRpc(env, 'control', 'manager_review_origin_get_v1', {
      p_office_context: context, p_agency_id: id
    })
  ]);
  return {
    ok: true,
    agency_templates: normalizeManagerTemplates(agencyTemplates.templates),
    agency_template_version: Number(agencyTemplates.version),
    agency_template_semantic_sha256_hex: text(agencyTemplates.semantic_sha256_hex),
    agency_template_sanitizer_policy_version: text(agencyTemplates.sanitizer_policy_version),
    agency_template_updated_at_utc: agencyTemplates.updated_at_utc || null,
    manager_origin: {
      public_origin: text(managerOrigin.manager_review_public_origin) || null,
      state: upper(managerOrigin.manager_review_origin_state) || 'UNCONFIGURED',
      settings_version: Number(managerOrigin.settings_version),
      semantic_sha256_hex: text(managerOrigin.manager_review_origin_semantic_sha256_hex) || null,
      verified_at_utc: managerOrigin.manager_review_origin_verified_at_utc || null,
      ownership: 'PLATFORM'
    }
  };
}

export async function previewMyTmsManagerEmailTemplate(env, user, request) {
  assertOfficeControlEnabled(env);
  await officeContext(env, user, ['MYTMS_SETTINGS_READ']);
  const source = isObject(request) ? request : {};
  const selectedKind = MANAGER_MAIL_KINDS.includes(upper(source.kind)) ? upper(source.kind) : 'INITIAL';
  const templatesForType = Object.fromEntries(MANAGER_MAIL_KINDS.map(kind => [kind,
    kind === selectedKind ? source.template : {
      subject: 'Preview placeholder', body_text: 'Preview placeholder.',
      body_html: '<p>Preview placeholder.</p>',
      button_text: MANAGER_LINK_MAIL_KINDS.has(kind) ? 'Review and approve' : null,
      include_link: MANAGER_LINK_MAIL_KINDS.has(kind)
    }
  ]));
  const template = normalizeManagerTemplates({
    schema_version: 'CANDIDATE_MANAGER_EMAIL_TEMPLATES_V1',
    TIMESHEET: templatesForType, EXPENSE_CLAIM: structuredClone(templatesForType)
  }).TIMESHEET[selectedKind];
  const expiry = template.include_link
    ? '<p>This secure link expires seven days after it is issued.</p><p><strong>Review and approve Kier Arthur timesheet</strong></p>'
    : '';
  return {
    ok: true, template, preview_html: `${template.body_html}${expiry}`,
    preview_text: `${template.body_text}${template.include_link ? '\n\nThis secure link expires seven days after it is issued.\n\nReview and approve Kier Arthur timesheet' : ''}`,
    sanitizer_policy_version: MANAGER_SANITIZER_POLICY_VERSION,
    semantic_sha256_hex: await sha256Hex(canonicalJson(template))
  };
}

export async function setMyTmsManagerEmailTemplates(env, user, request) {
  assertOfficeControlEnabled(env);
  const source = isObject(request) ? request : {};
  const expectedVersion = Number(source.expected_version);
  const idempotencyKey = text(source.idempotency_key);
  if (!Number.isSafeInteger(expectedVersion) || expectedVersion < 1
      || !idempotencyKey || idempotencyKey.length > 200) {
    throw new MyTmsOfficeError(400, 'MYTMS_SETTINGS_REQUEST_INVALID');
  }
  const context = await officeContext(env, user, ['MYTMS_SETTINGS_WRITE']);
  const templates = normalizeManagerTemplates(source.templates);
  return agencyRpc(env, 'candidate_manager_email_settings_set_v1', {
    p_expected_version: expectedVersion,
    p_templates: templates,
    p_sanitizer_policy_version: MANAGER_SANITIZER_POLICY_VERSION,
    p_actor_identity_hmac_hex: context.actor_identity_hmac,
    p_idempotency_key: idempotencyKey,
    p_now_utc: new Date().toISOString()
  });
}

export async function resetMyTmsManagerEmailTemplates(env, user, request) {
  assertOfficeControlEnabled(env);
  const source = isObject(request) ? request : {};
  const expectedVersion = Number(source.expected_version);
  const idempotencyKey = text(source.idempotency_key);
  if (!Number.isSafeInteger(expectedVersion) || expectedVersion < 1
      || !idempotencyKey || idempotencyKey.length > 200) {
    throw new MyTmsOfficeError(400, 'MYTMS_SETTINGS_REQUEST_INVALID');
  }
  const context = await officeContext(env, user, ['MYTMS_SETTINGS_WRITE']);
  return agencyRpc(env, 'candidate_manager_email_settings_reset_v1', {
    p_expected_version: expectedVersion,
    p_actor_identity_hmac_hex: context.actor_identity_hmac,
    p_idempotency_key: idempotencyKey,
    p_now_utc: new Date().toISOString()
  });
}

function publicHomeAnnouncement(result) {
  if (!isObject(result) || result.ok !== true) {
    throw new MyTmsOfficeError(502, 'MYTMS_OFFICE_RESPONSE_INVALID');
  }
  const announcementText = String(result.announcement_text == null
    ? '' : result.announcement_text);
  const version = Number(result.version);
  const semanticHash = text(result.semantic_sha256_hex).toLowerCase();
  if (announcementText.length > 600 || !Number.isSafeInteger(version) || version < 1
      || !SHA256_RE.test(semanticHash)) {
    throw new MyTmsOfficeError(502, 'MYTMS_OFFICE_RESPONSE_INVALID');
  }
  return {
    ok: true,
    announcement_text: announcementText,
    version,
    semantic_sha256_hex: semanticHash,
    updated_at_utc: result.updated_at_utc || null,
    idempotent_replay: result.idempotent_replay === true
  };
}

export async function getMyTmsHomeAnnouncementSettings(env, user) {
  assertOfficeControlEnabled(env);
  await officeContext(env, user, ['MYTMS_SETTINGS_READ']);
  return publicHomeAnnouncement(
    await agencyRpc(env, 'candidate_home_announcement_settings_get_v1')
  );
}

export async function previewMyTmsHomeAnnouncement(env, user, request) {
  assertOfficeControlEnabled(env);
  await officeContext(env, user, ['MYTMS_SETTINGS_READ']);
  const source = isObject(request) ? request : {};
  return publicHomeAnnouncement({
    ...await agencyRpc(env, 'candidate_home_announcement_settings_preview_v1', {
      p_announcement_text: String(source.announcement_text == null
        ? '' : source.announcement_text)
    }),
    version: 1,
    updated_at_utc: null
  });
}

export async function setMyTmsHomeAnnouncement(env, user, request) {
  assertOfficeControlEnabled(env);
  const source = isObject(request) ? request : {};
  const expectedVersion = Number(source.expected_version);
  const idempotencyKey = text(source.idempotency_key);
  if (!Number.isSafeInteger(expectedVersion) || expectedVersion < 1
      || idempotencyKey.length < 16 || idempotencyKey.length > 200) {
    throw new MyTmsOfficeError(400, 'MYTMS_SETTINGS_REQUEST_INVALID');
  }
  const context = await officeContext(env, user, ['MYTMS_SETTINGS_WRITE']);
  return publicHomeAnnouncement(await agencyRpc(
    env, 'candidate_home_announcement_settings_set_v1', {
      p_expected_version: expectedVersion,
      p_announcement_text: String(source.announcement_text == null
        ? '' : source.announcement_text),
      p_actor_identity_hmac_hex: context.actor_identity_hmac,
      p_idempotency_key: idempotencyKey,
      p_now_utc: new Date().toISOString()
    }
  ));
}

export async function resetMyTmsHomeAnnouncement(env, user, request) {
  assertOfficeControlEnabled(env);
  const source = isObject(request) ? request : {};
  const expectedVersion = Number(source.expected_version);
  const idempotencyKey = text(source.idempotency_key);
  if (!Number.isSafeInteger(expectedVersion) || expectedVersion < 1
      || idempotencyKey.length < 16 || idempotencyKey.length > 200) {
    throw new MyTmsOfficeError(400, 'MYTMS_SETTINGS_REQUEST_INVALID');
  }
  const context = await officeContext(env, user, ['MYTMS_SETTINGS_WRITE']);
  return publicHomeAnnouncement(await agencyRpc(
    env, 'candidate_home_announcement_settings_reset_v1', {
      p_expected_version: expectedVersion,
      p_actor_identity_hmac_hex: context.actor_identity_hmac,
      p_idempotency_key: idempotencyKey,
      p_now_utc: new Date().toISOString()
    }
  ));
}

async function localCandidate(env, candidateId) {
  const id = text(candidateId).toLowerCase();
  if (!UUID_RE.test(id)) throw new MyTmsOfficeError(400, 'MYTMS_CANDIDATE_ID_INVALID');
  const base = text(env.SUPABASE_URL).replace(/\/$/, '');
  const response = await fetch(
    `${base}/rest/v1/candidates?id=eq.${encodeURIComponent(id)}`
      + '&select=id,email,display_name,first_name,last_name,key_norm,active&limit=1',
    { headers: supabaseHeaders(env) }
  );
  if (!response.ok) throw new MyTmsOfficeError(503, 'MYTMS_OFFICE_DATA_PLANE_UNAVAILABLE');
  const rows = await boundedJson(response);
  return Array.isArray(rows) && rows.length === 1 ? rows[0] : null;
}

async function agencyDisplayNameFromSettings(env) {
  const base = text(env.SUPABASE_URL).replace(/\/$/, '');
  const response = await fetch(
    `${base}/rest/v1/settings_defaults?id=eq.1&select=agency_name&limit=1`,
    { headers: supabaseHeaders(env) }
  );
  if (!response.ok) {
    throw new MyTmsOfficeError(503, 'MYTMS_OFFICE_DATA_PLANE_UNAVAILABLE');
  }
  const rows = await boundedJson(response);
  const agencyDisplayName = text(Array.isArray(rows) && rows.length === 1
    ? rows[0]?.agency_name : '');
  if (agencyDisplayName.length < 1 || agencyDisplayName.length > 160) {
    throw new MyTmsOfficeError(503, 'MYTMS_AGENCY_PRESENTATION_UNAVAILABLE');
  }
  return agencyDisplayName;
}

function publicSettings(result) {
  if (!isObject(result) || result.ok !== true) {
    throw new MyTmsOfficeError(502, 'MYTMS_OFFICE_RESPONSE_INVALID');
  }
  const safe = { ...result, sanitizer_policy_version: SANITIZER_POLICY_VERSION };
  delete safe.agency_id;
  delete safe.internal_only;
  return safe;
}

export async function getMyTmsOfficeSettings(env, user) {
  assertOfficeControlEnabled(env);
  const id = agencyId(env);
  const result = await controlPlaneRpc(env, 'control', 'agency_app_settings_get_v1', {
    p_office_context: await officeContext(env, user, ['MYTMS_SETTINGS_READ']),
    p_agency_id: id
  });
  return publicSettings(result);
}

function agencyLogoBucket(env) {
  const bucket = env.R2 || env.R2_BUCKET;
  if (!bucket || typeof bucket.get !== 'function' || typeof bucket.put !== 'function') {
    throw new MyTmsOfficeError(503, 'MYTMS_AGENCY_LOGO_UNAVAILABLE');
  }
  return bucket;
}

async function readAgencyLogoPointer(env) {
  const base = text(env.SUPABASE_URL).replace(/\/$/, '');
  const response = await fetch(
    `${base}/rest/v1/settings_defaults?id=eq.1&select=agency_name,candidate_app_logo_asset_key&limit=1`,
    { headers: supabaseHeaders(env) }
  );
  if (!response.ok) {
    throw new MyTmsOfficeError(503, 'MYTMS_OFFICE_DATA_PLANE_UNAVAILABLE');
  }
  const rows = await boundedJson(response);
  if (!Array.isArray(rows) || rows.length !== 1) {
    throw new MyTmsOfficeError(503, 'MYTMS_OFFICE_DATA_PLANE_UNAVAILABLE');
  }
  return {
    agency_name: text(rows[0].agency_name),
    logo_asset_key: text(rows[0].candidate_app_logo_asset_key) || null
  };
}

async function updateAgencyLogoPointer(env, expectedKey, nextKey) {
  const base = text(env.SUPABASE_URL).replace(/\/$/, '');
  const expectedFilter = expectedKey
    ? `candidate_app_logo_asset_key=eq.${encodeURIComponent(expectedKey)}`
    : 'candidate_app_logo_asset_key=is.null';
  const response = await fetch(
    `${base}/rest/v1/settings_defaults?id=eq.1&${expectedFilter}`,
    {
      method: 'PATCH',
      headers: supabaseHeaders(env, 'return=representation'),
      body: JSON.stringify({ candidate_app_logo_asset_key: nextKey || null })
    }
  );
  if (!response.ok) {
    throw new MyTmsOfficeError(503, 'MYTMS_OFFICE_DATA_PLANE_UNAVAILABLE');
  }
  const rows = await boundedJson(response);
  if (!Array.isArray(rows) || rows.length !== 1) {
    throw new MyTmsOfficeError(409, 'MYTMS_AGENCY_LOGO_VERSION_CONFLICT');
  }
}

async function updateControlPlaneLogo(env, user, logoAssetKey, idempotencyKey) {
  const settings = await getMyTmsOfficeSettings(env, user);
  const expectedVersion = Number(settings.version);
  if (!Number.isSafeInteger(expectedVersion) || expectedVersion < 1) {
    throw new MyTmsOfficeError(502, 'MYTMS_OFFICE_RESPONSE_INVALID');
  }
  return controlPlaneRpc(env, 'control', 'agency_app_settings_set_v1', {
    p_office_context: await officeContext(env, user, ['MYTMS_SETTINGS_WRITE']),
    p_agency_id: agencyId(env),
    p_expected_version: expectedVersion,
    p_settings: { logo_asset_key: logoAssetKey || null },
    p_idempotency_key: idempotencyKey,
    p_correlation_id: crypto.randomUUID(),
    p_now_utc: new Date().toISOString()
  });
}

async function publicAgencyLogo(env, pointer) {
  const key = text(pointer?.logo_asset_key) || null;
  if (!key) {
    return {
      ok: true,
      agency_name: text(pointer?.agency_name) || null,
      has_logo: false,
      logo_asset_key: null,
      preview_data_url: null,
      media_type: null,
      size_bytes: 0,
      sha256_hex: null
    };
  }
  if (!AGENCY_LOGO_KEY_RE.test(key)) {
    throw new MyTmsOfficeError(502, 'MYTMS_AGENCY_LOGO_INVALID');
  }
  const object = await agencyLogoBucket(env).get(key);
  if (!object) throw new MyTmsOfficeError(502, 'MYTMS_AGENCY_LOGO_UNAVAILABLE');
  const bytes = new Uint8Array(await object.arrayBuffer());
  if (!bytes.length || bytes.length > MAX_AGENCY_LOGO_BYTES) {
    throw new MyTmsOfficeError(502, 'MYTMS_AGENCY_LOGO_INVALID');
  }
  const sha256 = await sha256BytesHex(bytes);
  if (!key.includes(sha256)) {
    throw new MyTmsOfficeError(502, 'MYTMS_AGENCY_LOGO_INVALID');
  }
  return {
    ok: true,
    agency_name: text(pointer?.agency_name) || null,
    has_logo: true,
    logo_asset_key: key,
    preview_data_url: `data:image/png;base64,${bytesToBase64(bytes)}`,
    media_type: 'image/png',
    size_bytes: bytes.byteLength,
    sha256_hex: sha256
  };
}

export async function getMyTmsAgencyLogo(env, user) {
  assertOfficeControlEnabled(env);
  await officeContext(env, user, ['MYTMS_SETTINGS_READ']);
  return publicAgencyLogo(env, await readAgencyLogoPointer(env));
}

export async function setMyTmsAgencyLogo(env, user, request) {
  assertOfficeControlEnabled(env);
  const source = isObject(request) ? request : {};
  const expectedKey = text(source.expected_logo_asset_key) || null;
  const idempotencyKey = text(source.idempotency_key);
  if (idempotencyKey.length < 16 || idempotencyKey.length > 200
      || (expectedKey && !AGENCY_LOGO_KEY_RE.test(expectedKey))) {
    throw new MyTmsOfficeError(400, 'MYTMS_AGENCY_LOGO_REQUEST_INVALID');
  }
  await officeContext(env, user, ['MYTMS_SETTINGS_WRITE']);
  const current = await readAgencyLogoPointer(env);
  if (current.logo_asset_key !== expectedKey) {
    throw new MyTmsOfficeError(409, 'MYTMS_AGENCY_LOGO_VERSION_CONFLICT');
  }
  const decoded = decodeAgencyLogoDataUrl(source.data_url);
  const digest = await sha256BytesHex(decoded.bytes);
  const nextKey = `candidate-app/branding/${digest}.png`;
  const bucket = agencyLogoBucket(env);
  const existing = await bucket.head(nextKey);
  if (!existing) {
    await bucket.put(nextKey, decoded.bytes, {
      httpMetadata: { contentType: 'image/png', cacheControl: 'private, max-age=31536000, immutable' },
      customMetadata: {
        sha256: digest,
        width: String(decoded.width),
        height: String(decoded.height),
        purpose: 'agency-app-logo-v1'
      }
    });
  }
  if (current.logo_asset_key !== nextKey) {
    await updateAgencyLogoPointer(env, current.logo_asset_key, nextKey);
    try {
      await updateControlPlaneLogo(env, user, nextKey, idempotencyKey);
    } catch (error) {
      await updateAgencyLogoPointer(env, nextKey, current.logo_asset_key);
      throw error;
    }
  }
  return publicAgencyLogo(env, { ...current, logo_asset_key: nextKey });
}

export async function deleteMyTmsAgencyLogo(env, user, request) {
  assertOfficeControlEnabled(env);
  const source = isObject(request) ? request : {};
  const expectedKey = text(source.expected_logo_asset_key) || null;
  const idempotencyKey = text(source.idempotency_key);
  if (!expectedKey || !AGENCY_LOGO_KEY_RE.test(expectedKey)
      || idempotencyKey.length < 16 || idempotencyKey.length > 200) {
    throw new MyTmsOfficeError(400, 'MYTMS_AGENCY_LOGO_REQUEST_INVALID');
  }
  await officeContext(env, user, ['MYTMS_SETTINGS_WRITE']);
  const current = await readAgencyLogoPointer(env);
  if (current.logo_asset_key !== expectedKey) {
    throw new MyTmsOfficeError(409, 'MYTMS_AGENCY_LOGO_VERSION_CONFLICT');
  }
  await updateAgencyLogoPointer(env, current.logo_asset_key, null);
  try {
    await updateControlPlaneLogo(env, user, null, idempotencyKey);
  } catch (error) {
    await updateAgencyLogoPointer(env, null, current.logo_asset_key);
    throw error;
  }
  return publicAgencyLogo(env, { ...current, logo_asset_key: null });
}

function activationAllowed(env, field) {
  if (['invitation_email_enabled', 'access_reminder_enabled'].includes(field)) {
    return upper(env.MYTMS_OFFICE_INVITATION_ACTIVATION_AUTHORIZED) === 'TRUE';
  }
  if (field === 'provisioning_enabled') {
    return upper(env.MYTMS_GOOGLE_PROVISIONING_ACTIVATION_AUTHORIZED) === 'TRUE';
  }
  if (field === 'google_target_switch_enabled') {
    return upper(env.MYTMS_GOOGLE_SWITCH_ACTIVATION_AUTHORIZED) === 'TRUE';
  }
  if (field === 'membership_admin_enabled') {
    return upper(env.MYTMS_MEMBERSHIP_ADMIN_ACTIVATION_AUTHORIZED) === 'TRUE';
  }
  if (field === 'push_delivery_enabled') {
    return upper(env.MYTMS_PUSH_DELIVERY_ACTIVATION_AUTHORIZED) === 'TRUE';
  }
  return true;
}

export async function setMyTmsOfficeSettings(env, user, request) {
  assertOfficeControlEnabled(env);
  const source = isObject(request) ? request : {};
  const settings = isObject(source.settings) ? { ...source.settings } : {};
  const expectedVersion = Number(source.expected_version);
  const idempotencyKey = text(source.idempotency_key);
  if (!Number.isSafeInteger(expectedVersion) || expectedVersion < 1
      || !idempotencyKey || idempotencyKey.length > 200) {
    throw new MyTmsOfficeError(400, 'MYTMS_SETTINGS_REQUEST_INVALID');
  }
  const activationFields = [
    'invitation_email_enabled', 'access_reminder_enabled', 'provisioning_enabled',
    'membership_admin_enabled', 'google_target_switch_enabled', 'push_delivery_enabled'
  ];
  for (const field of activationFields) {
    if (Object.hasOwn(settings, field)) {
      if (settings[field] === true && !activationAllowed(env, field)) {
        throw new MyTmsOfficeError(403, 'MYTMS_ACTIVATION_NOT_AUTHORIZED');
      }
      throw new MyTmsOfficeError(403, 'MYTMS_PLATFORM_SETTING_READ_ONLY');
    }
  }
  const agencyEditableFields = new Set([
    'invitation_subject', 'invitation_html_sanitized', 'invitation_text',
    'access_reminder_subject', 'access_reminder_html_sanitized', 'access_reminder_text',
    'invitation_expiry_seconds', 'resend_minimum_seconds', 'maximum_resends'
  ]);
  if (Object.keys(settings).some(field => !agencyEditableFields.has(field))) {
    throw new MyTmsOfficeError(403, 'MYTMS_PLATFORM_SETTING_READ_ONLY');
  }
  const invitationExpiry = Number(settings.invitation_expiry_seconds);
  const resendMinimum = Number(settings.resend_minimum_seconds);
  const maximumResends = Number(settings.maximum_resends);
  if ((Object.hasOwn(settings, 'invitation_expiry_seconds')
        && (!Number.isSafeInteger(invitationExpiry) || invitationExpiry < 86_400 || invitationExpiry > 604_800))
      || (Object.hasOwn(settings, 'resend_minimum_seconds')
        && (!Number.isSafeInteger(resendMinimum) || resendMinimum < 900 || resendMinimum > 86_400))
      || (Object.hasOwn(settings, 'maximum_resends')
        && (!Number.isSafeInteger(maximumResends) || maximumResends < 0 || maximumResends > 5))) {
    throw new MyTmsOfficeError(400, 'MYTMS_SETTINGS_REQUEST_INVALID');
  }
  if (Object.hasOwn(settings, 'invitation_html_sanitized')) {
    settings.invitation_html_sanitized = sanitizeMyTmsEmailHtml(
      settings.invitation_html_sanitized
    );
  }
  if (Object.hasOwn(settings, 'access_reminder_html_sanitized')) {
    settings.access_reminder_html_sanitized = sanitizeMyTmsEmailHtml(
      settings.access_reminder_html_sanitized
    );
  }
  settings.sanitizer_policy_version = SANITIZER_POLICY_VERSION;
  settings.sanitized_content_sha256_hex = await sha256Hex(canonicalJson({
    invitation_html_sanitized: settings.invitation_html_sanitized || null,
    access_reminder_html_sanitized: settings.access_reminder_html_sanitized || null,
    sanitizer_policy_version: SANITIZER_POLICY_VERSION
  }));
  const id = agencyId(env);
  const result = await controlPlaneRpc(env, 'control', 'agency_app_settings_set_v1', {
    p_office_context: await officeContext(env, user, ['MYTMS_SETTINGS_WRITE']),
    p_agency_id: id, p_expected_version: expectedVersion, p_settings: settings,
    p_idempotency_key: idempotencyKey,
    p_correlation_id: text(source.correlation_id || crypto.randomUUID()),
    p_now_utc: new Date().toISOString()
  });
  return publicSettings(result);
}

export async function previewMyTmsTemplate(env, user, request) {
  assertOfficeControlEnabled(env);
  await officeContext(env, user, ['MYTMS_SETTINGS_READ']);
  const source = isObject(request) ? request : {};
  const sanitizedHtml = sanitizeMyTmsEmailHtml(source.html);
  return {
    ok: true, sanitizer_policy_version: SANITIZER_POLICY_VERSION,
    sanitized_html: sanitizedHtml,
    sanitized_content_sha256_hex: await sha256Hex(sanitizedHtml)
  };
}

async function internalCandidateStatus(env, user, candidate) {
  const id = agencyId(env);
  const candidateExists = Boolean(candidate);
  const email = text(candidate?.email).toLowerCase();
  const eligible = candidateExists && candidate.active !== false && EMAIL_RE.test(email);
  const result = await controlPlaneRpc(env, 'control', 'candidate_mytms_status_get_v1', {
    p_office_context: await officeContext(env, user, ['MYTMS_CANDIDATE_READ']),
    p_agency_id: id, p_candidate_id: candidate?.id || crypto.randomUUID(),
    p_candidate_facts: {
      candidate_exists: candidateExists, eligible,
      reason_code: !candidateExists ? 'CANDIDATE_NOT_FOUND'
        : candidate.active === false ? 'CANDIDATE_INACTIVE'
          : !EMAIL_RE.test(email) ? 'CANDIDATE_EMAIL_INVALID' : null
    },
    p_correlation_id: crypto.randomUUID(), p_now_utc: new Date().toISOString()
  });
  const [settings, agencyDisplayName] = await Promise.all([
    getMyTmsOfficeSettings(env, user),
    agencyDisplayNameFromSettings(env)
  ]);
  const makeAction = (requestedCode) => {
    const code = text(requestedCode) || 'NONE';
    const membershipAction = ['CANCEL_INVITATION','CANCEL_PENDING_MEMBERSHIP','REVOKE_MEMBERSHIP']
      .includes(code);
    const featureEnabled = membershipAction
      ? settings.membership_admin_enabled === true
        && activationAllowed(env, 'membership_admin_enabled')
      : code === 'SEND_ACCESS_REMINDER'
        ? settings.access_reminder_enabled === true
        : ['INVITE_TO_MYTMS', 'RESEND_INVITATION'].includes(code)
          ? settings.invitation_email_enabled === true : false;
    return {
      code,
    label: ({
      INVITE_TO_MYTMS: 'Invite to MyTMS',
      RESEND_INVITATION: 'Resend invitation',
      SEND_ACCESS_REMINDER: 'Send MyTMS Access Reminder',
      CANCEL_INVITATION: 'Cancel invitation',
      CANCEL_PENDING_MEMBERSHIP: 'Cancel pending membership',
      REVOKE_MEMBERSHIP: 'Revoke MyTMS access'
      })[code] || '',
    enabled: featureEnabled,
    disabled_reason_code: featureEnabled ? null
        : code === 'NONE' ? text(result.reason_code || 'NO_ACTION_AVAILABLE')
        : membershipAction ? 'MYTMS_MEMBERSHIP_ADMIN_DISABLED'
          : 'MYTMS_INVITATION_DELIVERY_DISABLED'
    };
  };
  const action = makeAction(text(result.action_code));
  const actions = action.code === 'CANCEL_INVITATION'
    ? [makeAction('RESEND_INVITATION'), action]
    : action.code === 'NONE' ? [] : [action];
  return {
    ok: true, agency_display_name: agencyDisplayName,
    candidate_id: candidate?.id || null,
    candidate_display_name: text(candidate?.display_name),
    candidate_email: email || null,
    state: text(result.state || 'INELIGIBLE'),
    delivery_state: text(result.delivery_state) || null,
    invitation_generation: Number(result.invitation_generation || 0) || null,
    membership_id: UUID_RE.test(text(result.membership_id))
      ? text(result.membership_id).toLowerCase() : null,
    membership_generation: Number(result.membership_generation || 0) || null,
    settings_version: Number(settings.version), action, actions
  };
}

export async function getMyTmsCandidateStatus(env, user, candidateId) {
  assertOfficeControlEnabled(env);
  const candidate = await localCandidate(env, candidateId);
  return internalCandidateStatus(env, user, candidate);
}

async function setAgencyLocalMembershipLink(
  env, candidate, globalAccountId, membershipId, generation, targetState
) {
  const environment = upper(env.CANDIDATE_APP_ENVIRONMENT);
  if (!['TEST', 'LIVE'].includes(environment)
      || !UUID_RE.test(text(globalAccountId)) || !UUID_RE.test(text(membershipId))
      || !Number.isSafeInteger(Number(generation)) || Number(generation) < 1) {
    throw new MyTmsOfficeError(503, 'MYTMS_MEMBERSHIP_LINK_UNAVAILABLE');
  }
  let accountHmac;
  try {
    accountHmac = await candidateFederatedIdentityHmac(
      env.CANDIDATE_FEDERATED_IDENTITY_SECRET, environment, globalAccountId
    );
  } catch {
    throw new MyTmsOfficeError(503, 'MYTMS_MEMBERSHIP_LINK_UNAVAILABLE');
  }
  const base = text(env.SUPABASE_URL).replace(/\/$/, '');
  const response = await fetch(
    `${base}/rest/v1/rpc/candidate_app_federated_membership_link_set_v1`,
    {
      method: 'POST', headers: supabaseHeaders(env),
      body: JSON.stringify({
        p_internal_context: {
          route_context_verified: true,
          audience: 'FEDERATED_MEMBERSHIP_LINK'
        },
        p_environment: environment,
        p_global_account_identity_hmac: `\\x${accountHmac}`,
        p_membership_id: text(membershipId).toLowerCase(),
        p_membership_generation: Number(generation),
        p_candidate_id: candidate.id,
        p_candidate_code: text(candidate.key_norm) || null,
        p_target_state: upper(targetState),
        p_now_utc: new Date().toISOString()
      })
    }
  );
  if (!response.ok) throw new MyTmsOfficeError(503, 'MYTMS_MEMBERSHIP_LINK_UNAVAILABLE');
  const result = await boundedJson(response);
  if (!isObject(result) || result.ok !== true
      || !['LINKED', 'UPDATED', 'UNCHANGED'].includes(text(result.status))) {
    throw new MyTmsOfficeError(502, 'MYTMS_OFFICE_RESPONSE_INVALID');
  }
  return result;
}

export async function adoptMyTmsCandidate(env, user, candidateId, request) {
  assertOfficeControlEnabled(env);
  const source = isObject(request) ? request : {};
  const proof = isObject(source.proof) ? { ...source.proof } : {};
  const idempotencyKey = text(source.idempotency_key);
  const globalAccountId = text(proof.account_id).toLowerCase();
  if (!UUID_RE.test(globalAccountId) || !idempotencyKey || idempotencyKey.length > 200) {
    throw new MyTmsOfficeError(400, 'MYTMS_ADOPTION_REQUEST_INVALID');
  }
  const settings = await getMyTmsOfficeSettings(env, user);
  if (settings.membership_admin_enabled !== true
      || !activationAllowed(env, 'membership_admin_enabled')) {
    throw new MyTmsOfficeError(403, 'MYTMS_MEMBERSHIP_ADMIN_DISABLED');
  }
  const candidate = await localCandidate(env, candidateId);
  if (!candidate) throw new MyTmsOfficeError(404, 'MYTMS_CANDIDATE_NOT_FOUND');
  const result = await controlPlaneRpc(env, 'control', 'candidate_adopt_v1', {
    p_office_context: await officeContext(env, user, ['MYTMS_CANDIDATE_ADOPT']),
    p_agency_id: agencyId(env), p_candidate_id: candidate.id, p_proof: proof,
    p_idempotency_key: idempotencyKey,
    p_correlation_id: text(source.correlation_id || crypto.randomUUID()),
    p_now_utc: new Date().toISOString()
  });
  if (!isObject(result) || result.ok !== true
      || !['LINKED', 'ALREADY_LINKED', 'REVIEW_REQUIRED', 'CONFLICT'].includes(text(result.status))) {
    throw new MyTmsOfficeError(502, 'MYTMS_OFFICE_RESPONSE_INVALID');
  }
  if (['LINKED', 'ALREADY_LINKED'].includes(text(result.status))) {
    await setAgencyLocalMembershipLink(
      env, candidate, globalAccountId, result.membership_id,
      Number(result.generation), 'PENDING'
    );
  }
  return {
    ok: true, status: text(result.status), state: text(result.state) || null,
    membership_id: UUID_RE.test(text(result.membership_id)) ? text(result.membership_id) : null,
    membership_generation: Number(result.generation || 0) || null,
    reason_code: text(result.reason_code) || null,
    idempotent_replay: result.idempotent_replay === true
  };
}

export async function setMyTmsMembershipState(env, user, membershipId, request) {
  assertOfficeControlEnabled(env);
  const source = isObject(request) ? request : {};
  const candidateId = text(source.candidate_id).toLowerCase();
  const transition = upper(source.transition);
  const expectedGeneration = Number(source.expected_generation);
  const idempotencyKey = text(source.idempotency_key);
  if (!UUID_RE.test(text(membershipId)) || !UUID_RE.test(candidateId)
      || !['ACTIVATE', 'DISABLE', 'REVOKE'].includes(transition)
      || !Number.isSafeInteger(expectedGeneration) || expectedGeneration < 1
      || !idempotencyKey || idempotencyKey.length > 200) {
    throw new MyTmsOfficeError(400, 'MYTMS_MEMBERSHIP_REQUEST_INVALID');
  }
  const settings = await getMyTmsOfficeSettings(env, user);
  if (settings.membership_admin_enabled !== true
      || !activationAllowed(env, 'membership_admin_enabled')) {
    throw new MyTmsOfficeError(403, 'MYTMS_MEMBERSHIP_ADMIN_DISABLED');
  }
  const candidate = await localCandidate(env, candidateId);
  if (!candidate) throw new MyTmsOfficeError(404, 'MYTMS_CANDIDATE_NOT_FOUND');
  const result = await controlPlaneRpc(env, 'control', 'membership_state_set_v1', {
    p_office_context: await officeContext(env, user, ['MYTMS_MEMBERSHIP_MANAGE']),
    p_agency_id: agencyId(env), p_membership_id: text(membershipId).toLowerCase(),
    p_expected_generation: expectedGeneration, p_transition: transition,
    p_reason: text(source.reason) || null, p_idempotency_key: idempotencyKey,
    p_correlation_id: text(source.correlation_id || crypto.randomUUID()),
    p_now_utc: new Date().toISOString()
  });
  if (!isObject(result) || result.ok !== true
      || !['ACTIVE', 'DISABLED', 'REVOKED', 'UNCHANGED'].includes(text(result.status))) {
    throw new MyTmsOfficeError(502, 'MYTMS_OFFICE_RESPONSE_INVALID');
  }
  const state = text(result.state);
  const globalAccountId = text(result.global_account_id_internal).toLowerCase();
  if (!UUID_RE.test(globalAccountId)
      || text(result.local_candidate_id_internal).toLowerCase() !== candidateId) {
    throw new MyTmsOfficeError(502, 'MYTMS_OFFICE_RESPONSE_INVALID');
  }
  await setAgencyLocalMembershipLink(
    env, candidate, globalAccountId, membershipId, Number(result.generation), state
  );
  return {
    ok: true, status: text(result.status), state,
    membership_generation: Number(result.generation),
    idempotent_replay: result.idempotent_replay === true
  };
}

function escapeHtml(value) {
  return String(value == null ? '' : value)
    .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;').replaceAll("'", '&#39;');
}

function mergeTemplate(template, values) {
  let output = String(template || '');
  for (const [name, value] of Object.entries(values)) {
    output = output.replaceAll(`{{${name}}}`, String(value));
  }
  return output;
}

async function invitationToken(env, candidateId, intent, requestKey) {
  return base64Url(await hmacBytes(
    env.MYTMS_INVITATION_TOKEN_SECRET, 'mytms-invitation-token-v1',
    `${agencyId(env)}:${candidateId}:${intent}:${requestKey}`
  ));
}

async function insertInvitationOutbox(
  env, user, candidate, invitation, settings, token, agencyDisplayName
) {
  const intent = upper(invitation.intent);
  const origin = text(settings.planned_test_web_origin).replace(/\/$/, '');
  let parsedOrigin;
  try { parsedOrigin = new URL(origin); } catch { parsedOrigin = null; }
  if (!parsedOrigin || parsedOrigin.protocol !== 'https:' || parsedOrigin.origin !== origin) {
    throw new MyTmsOfficeError(503, 'MYTMS_INVITATION_LINK_UNAVAILABLE');
  }
  // Keep the bearer in the URL fragment so it is captured by the app but is
  // never sent to the public static host, CDN or ordinary request logs.
  const link = `${origin}/invite#token=${encodeURIComponent(token)}`;
  const reminder = intent === 'ACCESS_REMINDER';
  const candidateName = candidate.display_name
    || `${candidate.first_name || ''} ${candidate.last_name || ''}`.trim();
  const plainValues = {
    candidate_name: candidateName,
    agency_name: agencyDisplayName,
    mytms_invitation_url: link
  };
  const subject = text(mergeTemplate(
    reminder ? settings.access_reminder_subject : settings.invitation_subject,
    plainValues
  ));
  const htmlTemplate = reminder
    ? settings.access_reminder_html_sanitized : settings.invitation_html_sanitized;
  const textTemplate = reminder ? settings.access_reminder_text : settings.invitation_text;
  if (!subject || (!text(htmlTemplate) && !text(textTemplate))) {
    throw new MyTmsOfficeError(503, 'MYTMS_INVITATION_TEMPLATE_UNAVAILABLE');
  }
  const values = {
    candidate_name: escapeHtml(candidateName),
    agency_name: escapeHtml(agencyDisplayName),
    mytms_invitation_url: escapeHtml(link)
  };
  const bodyHtml = sanitizeMyTmsEmailHtml(mergeTemplate(htmlTemplate, values));
  const bodyText = mergeTemplate(textTemplate, plainValues);
  const semanticHash = await sha256Hex(canonicalJson({
    invitation_id: invitation.invitation_id, generation: invitation.generation,
    to: text(candidate.email).toLowerCase(), subject, body_html: bodyHtml, body_text: bodyText
  }));
  const deterministicOutboxKey = `mytms:invitation:${invitation.invitation_id}:generation:${invitation.generation}`;
  const reference = `mytms_invitation:${invitation.invitation_id}:${invitation.generation}:${semanticHash}`;
  const base = text(env.SUPABASE_URL).replace(/\/$/, '');
  const response = await fetch(
    `${base}/rest/v1/mail_outbox?on_conflict=deterministic_outbox_key`,
    {
      method: 'POST',
      headers: supabaseHeaders(env, 'resolution=ignore-duplicates,return=representation'),
      body: JSON.stringify({
        type: 'BROADCAST', to: text(candidate.email).toLowerCase(),
        importance: 'Normal', email_type: bodyHtml ? 'html' : 'plain',
        subject, body_html: bodyHtml || null, body_text: bodyText || null,
        status: 'QUEUED', reference, created_by: user?.id || null,
        recipient_kind: 'CANDIDATE', recipient_id: candidate.id,
        context_kind: 'MYTMS_INVITATION', context_id: invitation.invitation_id,
        deterministic_outbox_key: deterministicOutboxKey
      })
    }
  );
  if (!response.ok) throw new MyTmsOfficeError(503, 'MYTMS_OUTBOX_UNAVAILABLE');
  let rows = await boundedJson(response);
  if (!Array.isArray(rows) || !rows.length) {
    const lookup = await fetch(
      `${base}/rest/v1/mail_outbox?deterministic_outbox_key=eq.${encodeURIComponent(deterministicOutboxKey)}`
        + '&select=id,reference,context_kind,context_id&limit=1',
      { headers: supabaseHeaders(env) }
    );
    if (!lookup.ok) throw new MyTmsOfficeError(503, 'MYTMS_OUTBOX_UNAVAILABLE');
    rows = await boundedJson(lookup);
  }
  const row = Array.isArray(rows) ? rows[0] : null;
  if (!row || !UUID_RE.test(text(row.id))) {
    throw new MyTmsOfficeError(503, 'MYTMS_OUTBOX_UNAVAILABLE');
  }
  return { row, semanticHash };
}

async function recordInvitationDelivery(
  env, officeActor, invitationId, generation, outboxId, outcome,
  semanticHash, providerReference = null
) {
  const result = await controlPlaneRpc(env, 'control', 'invitation_delivery_record_v1', {
    p_office_context: officeActor, p_invitation_id: invitationId,
    p_generation: generation, p_mail_outbox_id: outboxId, p_outcome: outcome,
    p_provider_reference_hash: providerReference ? await sha256Hex(providerReference) : null,
    p_semantic_hash: semanticHash, p_correlation_id: crypto.randomUUID(),
    p_now_utc: new Date().toISOString()
  });
  if (!isObject(result) || result.ok !== true || result.status === 'CONFLICT') {
    throw new MyTmsOfficeError(409, 'MYTMS_INVITATION_DELIVERY_CONFLICT');
  }
  return result;
}

export async function reserveAndQueueMyTmsInvitation(env, user, candidateId, request) {
  assertOfficeControlEnabled(env);
  if (upper(env.MYTMS_OFFICE_INVITATION_DELIVERY_ENABLED) !== 'TRUE') {
    throw new MyTmsOfficeError(403, 'MYTMS_INVITATION_DELIVERY_DISABLED');
  }
  const source = isObject(request) ? request : {};
  const intent = upper(source.intent);
  const requestKey = text(source.idempotency_key).toLowerCase();
  if (!['INVITE', 'RESEND', 'ACCESS_REMINDER'].includes(intent)
      || !UUID_RE.test(requestKey)) {
    throw new MyTmsOfficeError(400, 'MYTMS_INVITATION_REQUEST_INVALID');
  }
  const candidate = await localCandidate(env, candidateId);
  if (!candidate) throw new MyTmsOfficeError(404, 'MYTMS_CANDIDATE_NOT_FOUND');
  const status = await internalCandidateStatus(env, user, candidate);
  const matchingAction = (Array.isArray(status.actions) ? status.actions : [status.action])
    .find((candidateAction) => ({
      INVITE_TO_MYTMS: 'INVITE', RESEND_INVITATION: 'RESEND',
      SEND_ACCESS_REMINDER: 'ACCESS_REMINDER'
    })[candidateAction?.code] === intent);
  if (!matchingAction?.enabled) {
    throw new MyTmsOfficeError(409, matchingAction?.disabled_reason_code || 'MYTMS_ACTION_STALE');
  }
  const settings = await getMyTmsOfficeSettings(env, user);
  if (Number(source.expected_settings_version) !== Number(settings.version)) {
    throw new MyTmsOfficeError(409, 'MYTMS_SETTINGS_VERSION_CONFLICT');
  }
  const token = await invitationToken(env, candidate.id, intent, requestKey);
  const expiresAt = new Date(Date.now() + Number(settings.invitation_expiry_seconds) * 1000);
  const requestSemanticHash = await sha256Hex(canonicalJson({
    agency_id: agencyId(env), candidate_id: candidate.id,
    destination: text(candidate.email).toLowerCase(), intent,
    settings_version: Number(settings.version),
    agency_display_name: status.agency_display_name
  }));
  const actor = await officeContext(env, user, ['MYTMS_INVITATION_MANAGE'], {
    invitation_token_hash_hex: await sha256Hex(token),
    request_semantic_hash_hex: requestSemanticHash,
    expires_at_utc: expiresAt.toISOString(),
    agency_display_name: status.agency_display_name
  });
  const invitation = await controlPlaneRpc(env, 'control', 'invitation_reserve_v1', {
    p_office_context: actor, p_agency_id: agencyId(env),
    p_candidate_id: candidate.id, p_destination: text(candidate.email).toLowerCase(),
    p_intent: intent, p_staff_request_key: requestKey,
    p_settings_version: Number(settings.version), p_correlation_id: crypto.randomUUID()
  });
  if (invitation.status !== 'RESERVED') {
    return {
      ok: true, status: text(invitation.status),
      reason_code: text(invitation.reason_code) || null,
      retry_after_seconds: Number(invitation.retry_after_seconds || 0) || null
    };
  }
  const queued = await insertInvitationOutbox(
    env, user, candidate, { ...invitation, intent }, settings, token,
    status.agency_display_name
  );
  try {
    await recordInvitationDelivery(
      env, actor, invitation.invitation_id, Number(invitation.generation),
      queued.row.id, 'OUTBOX_ACCEPTED', queued.semanticHash
    );
  } catch {
    try {
      await recordInvitationDelivery(
        env, actor, invitation.invitation_id, Number(invitation.generation),
        queued.row.id, 'DELIVERY_UNCERTAIN', queued.semanticHash
      );
    } catch {}
    return {
      ok: true, status: 'DELIVERY_UNCERTAIN',
      invitation_generation: Number(invitation.generation),
      message: 'Delivery status uncertain—check status before retrying.'
    };
  }
  return {
    ok: true, status: 'OUTBOX_ACCEPTED',
    invitation_generation: Number(invitation.generation),
    idempotent_replay: invitation.idempotent_replay === true
  };
}

function parseInvitationReference(row) {
  if (upper(row?.context_kind) !== 'MYTMS_INVITATION') return null;
  const parts = text(row?.reference).split(':');
  if (parts.length !== 4 || parts[0] !== 'mytms_invitation'
      || !UUID_RE.test(parts[1]) || !/^[1-9][0-9]{0,8}$/.test(parts[2])
      || !SHA256_RE.test(parts[3])
      || text(row?.context_id).toLowerCase() !== parts[1].toLowerCase()
      || !UUID_RE.test(text(row?.id))) return null;
  return {
    invitationId: parts[1].toLowerCase(), generation: Number(parts[2]),
    semanticHash: parts[3].toLowerCase(), outboxId: text(row.id).toLowerCase()
  };
}

export async function recordMyTmsInvitationOutboxOutcome(
  env, row, outcome, providerReference = null
) {
  const parsed = parseInvitationReference(row);
  if (!parsed) return { applicable: false, recorded: false };
  assertOfficeControlEnabled(env);
  const actor = await officeContext(
    env, { id: text(row.created_by) || 'mytms-outbox-worker' },
    ['MYTMS_INVITATION_MANAGE']
  );
  await recordInvitationDelivery(
    env, actor, parsed.invitationId, parsed.generation, parsed.outboxId,
    upper(outcome), parsed.semanticHash, providerReference
  );
  return { applicable: true, recorded: true };
}

export async function queueMyTmsIdentityChallenge(env, request, user = null) {
  if (upper(env.MYTMS_IDENTITY_DELIVERY_ENABLED) !== 'TRUE') {
    throw new MyTmsOfficeError(403, 'MYTMS_IDENTITY_DELIVERY_DISABLED');
  }
  const source = isObject(request) ? request : {};
  const email = text(source.email).toLowerCase();
  const purpose = upper(source.purpose);
  const challengeId = text(source.challenge_id).toLowerCase();
  const token = text(source.token);
  const outboxKey = text(source.deterministic_outbox_key);
  if (!EMAIL_RE.test(email) || !['ACTIVATE', 'RESET', 'RECOVERY'].includes(purpose)
      || !UUID_RE.test(challengeId) || token.length < 32 || token.length > 4096
      || !outboxKey || outboxKey.length > 200) {
    throw new MyTmsOfficeError(400, 'MYTMS_IDENTITY_DELIVERY_INVALID');
  }
  const linkOrigin = text(env.MYTMS_IDENTITY_WEB_ORIGIN).replace(/\/$/, '');
  let origin;
  try { origin = new URL(linkOrigin); } catch { origin = null; }
  if (!origin || origin.protocol !== 'https:' || origin.origin !== linkOrigin) {
    throw new MyTmsOfficeError(503, 'MYTMS_IDENTITY_DELIVERY_UNAVAILABLE');
  }
  const challengePath = purpose === 'ACTIVATE'
    ? '/candidate/activate'
    : '/candidate/reset-password';
  // The Candidate app owns the two purpose-specific routes. Keep both the
  // bearer and its opaque challenge identity in the fragment so GitHub Pages
  // never receives either value in an HTTP request.
  const link = `${linkOrigin}${challengePath}#token=${encodeURIComponent(token)}`
    + `&challenge=${encodeURIComponent(challengeId)}`;
  const bodyHtml = sanitizeMyTmsEmailHtml(
    `<p>Use the secure link below to continue setting up or recovering your MyTMS access.</p>`
      + `<p><a href="${escapeHtml(link)}">Continue to MyTMS</a></p>`
  );
  const base = text(env.SUPABASE_URL).replace(/\/$/, '');
  const response = await fetch(
    `${base}/rest/v1/mail_outbox?on_conflict=deterministic_outbox_key`,
    {
      method: 'POST', headers: supabaseHeaders(
        env, 'resolution=ignore-duplicates,return=representation'
      ),
      body: JSON.stringify({
        type: 'BROADCAST', to: email, importance: 'Normal', email_type: 'html',
        subject: purpose === 'ACTIVATE' ? 'Set up your MyTMS access' : 'Recover your MyTMS access',
        body_html: bodyHtml,
        body_text: `Continue to MyTMS: ${link}`,
        status: 'QUEUED', reference: `mytms_identity:${challengeId}`,
        created_by: user?.id || null, recipient_kind: 'CANDIDATE',
        context_kind: 'MYTMS_IDENTITY_CHALLENGE', context_id: challengeId,
        deterministic_outbox_key: outboxKey
      })
    }
  );
  if (!response.ok) throw new MyTmsOfficeError(503, 'MYTMS_OUTBOX_UNAVAILABLE');
  return { ok: true, accepted: true };
}

export const myTmsOfficeInternals = Object.freeze({
  SANITIZER_POLICY_VERSION,
  activationAllowed,
  canonicalJson,
  mergeTemplate,
  parseInvitationReference,
  publicSettings,
  sha256Hex
});
