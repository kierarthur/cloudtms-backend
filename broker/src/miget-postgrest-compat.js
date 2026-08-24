/**
 * Preserve CloudTMS's historical Supabase-style REST contract when the
 * configured database API is a standalone PostgREST application on Miget.
 *
 * Supabase exposes PostgREST below /rest/v1; standalone PostgREST exposes the
 * equivalent table and RPC routes at / and /rpc. Requests to every other host
 * and path are returned untouched.
 */
export function normaliseMigetPostgrestRequest(input) {
  const rawUrl = typeof input === 'string'
    ? input
    : input instanceof URL
      ? input.href
      : input instanceof Request
        ? input.url
        : '';

  if (!rawUrl) return input;

  let target;
  try {
    target = new URL(rawUrl);
  } catch {
    return input;
  }

  const isMigetHost = target.hostname === 'migetapp.com' || target.hostname.endsWith('.migetapp.com');
  const hasSupabaseRestPrefix = target.pathname === '/rest/v1' || target.pathname.startsWith('/rest/v1/');
  if (!isMigetHost || !hasSupabaseRestPrefix) return input;

  target.pathname = target.pathname.slice('/rest/v1'.length) || '/';
  if (input instanceof Request) return new Request(target.href, input);
  if (input instanceof URL) return target;
  return target.href;
}
