async function createClientWithSettingsAtomic(env, user, data) {
  const source = (data && typeof data === 'object' && !Array.isArray(data)) ? data : {};
  const nestedSettings = (source.client_settings && typeof source.client_settings === 'object' && !Array.isArray(source.client_settings))
    ? source.client_settings
    : {};
  const clientJson = {};
  const settingsJson = {};
  for (const key of CLIENT_CREATE_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(source, key)) clientJson[key] = source[key];
  }
  for (const key of CLIENT_SETTINGS_CREATE_FIELDS) {
    if (Object.prototype.hasOwnProperty.call(nestedSettings, key)) settingsJson[key] = nestedSettings[key];
    else if (Object.prototype.hasOwnProperty.call(source, key)) settingsJson[key] = source[key];
  }
  const suppliedId = String(source.client_id || source.id || '').trim();
  const clientId = suppliedId || crypto.randomUUID();
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(clientId)) {
    throw new Error('CLIENT_CREATE_ID_INVALID');
  }
  const raw = await sbRpc(env, 'client_create_with_settings_v1', {
    p_client_id: clientId,
    p_client_json: clientJson,
    p_actor_user_id: user.id,
    p_settings_json: settingsJson,
    p_now_utc: new Date().toISOString()
  }, { timeoutMs: 30000 });
  return unwrapRpcJsonb(raw, 'client_create_with_settings_v1');
}