async function handleUpsertClientRate(env, req, clientId) {
  const user = await requireUser(env, req, ['admin']);
  if (!user) return unauthorized();

  const body = await parseJSONBody(req);
  if (!body) return withCORS(env, req, badRequest("Invalid JSON"));

  // Expect unified fields in the body
  const rec = {
    client_id: body.client_id || clientId,
    role:      body.role ?? null,
    band:      (body.band === '' ? null : body.band ?? null),
    date_from: body.date_from ?? null,
    date_to:   (body.date_to === '' ? null : body.date_to ?? null),

    // charge (5)
    charge_day:   body.charge_day   ?? null,
    charge_night: body.charge_night ?? null,
    charge_sat:   body.charge_sat   ?? null,
    charge_sun:   body.charge_sun   ?? null,
    charge_bh:    body.charge_bh    ?? null,

    // PAYE (5)
    paye_day:   body.paye_day   ?? null,
    paye_night: body.paye_night ?? null,
    paye_sat:   body.paye_sat   ?? null,
    paye_sun:   body.paye_sun   ?? null,
    paye_bh:    body.paye_bh    ?? null,

    // Umbrella (5)
    umb_day:    body.umb_day    ?? null,
    umb_night:  body.umb_night  ?? null,
    umb_sat:    body.umb_sat    ?? null,
    umb_sun:    body.umb_sun    ?? null,
    umb_bh:     body.umb_bh     ?? null
  };

  if (!rec.client_id) return withCORS(env, req, badRequest("client_id required"));
  if (!rec.role)       return withCORS(env, req, badRequest("role required"));
  if (!rec.date_from)  return withCORS(env, req, badRequest("date_from required"));

  const client_id = rec.client_id;
  const role      = rec.role;
  const band      = rec.band;
  const dateFrom  = rec.date_from;

  // Helper: encode "(band is null)" vs "band = value"
  const bandFilter = (b) => (b == null ? 'band=is.null' : `band=eq.${encodeURIComponent(b)}`);

  // Helper: YMD day before
  function dayBeforeYmd(ymd) {
    if (!ymd) return null;
    const d = new Date(`${ymd}T00:00:00Z`);
    d.setUTCDate(d.getUTCDate() - 1);
    const y = d.getUTCFullYear();
    const m = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(d.getUTCDate()).padStart(2, '0');
    return `${y}-${m}-${dd}`;
  }

  // Helper: is today within [from, to] inclusive (to may be null = open)
  function todayWithinWindow(fromYmd, toYmd) {
    if (!fromYmd) return false;
    const today = new Date().toISOString().slice(0, 10);
    if (today < String(fromYmd)) return false;
    if (toYmd && today > String(toYmd)) return false;
    return true;
  }

  try {
    // 0) UPDATE path? (exact unique key exists among ENABLED rows)
    {
      let q = `${env.SUPABASE_URL}/rest/v1/rates_client_defaults` +
              `?client_id=eq.${encodeURIComponent(client_id)}` +
              `&role=eq.${encodeURIComponent(role)}` +
              `&${bandFilter(band)}` +
              `&date_from=eq.${encodeURIComponent(dateFrom)}` +
              `&disabled_at_utc=is.null` +                 // ← ignore disabled rows
              `&select=id`;
      const { rows: exactRows } = await sbFetch(env, q);
      if (Array.isArray(exactRows) && exactRows.length === 1) {
        const id = exactRows[0].id;
        const res = await fetch(
          `${env.SUPABASE_URL}/rest/v1/rates_client_defaults?id=eq.${encodeURIComponent(id)}`,
          { method: "PATCH", headers: { ...sbHeaders(env), "Prefer": "return=representation" }, body: JSON.stringify({ ...rec, updated_at: nowIso() }) }
        );
        if (!res.ok) {
          const err = await res.text();
          return withCORS(env, req, badRequest(`Rate update failed: ${err}`));
        }
        const json = await res.json().catch(() => ({}));
        const result = Array.isArray(json) ? json[0] : json;

        // Enqueue recompute ONLY if window is currently effective (inclusive)
        if (todayWithinWindow(rec.date_from, rec.date_to)) {
          await enqueueTsfinRecomputeForClient(env, client_id);
        }

        return withCORS(env, req, ok({ rate: result }));
      }
    }

    // 1) INSERT path — rollover behavior & no overlaps (consider ENABLED rows only)
    // 1a) Find incumbent window (same category) active at new start N = dateFrom
    {
      let qInc = `${env.SUPABASE_URL}/rest/v1/rates_client_defaults` +
                 `?client_id=eq.${encodeURIComponent(client_id)}` +
                 `&role=eq.${encodeURIComponent(role)}` +
                 `&${bandFilter(band)}` +
                 `&date_from=lte.${encodeURIComponent(dateFrom)}` +
                 `&or=(date_to.gte.${encodeURIComponent(dateFrom)},date_to.is.null)` +
                 `&disabled_at_utc=is.null` +               // ← ignore disabled rows
                 `&select=id,date_from,date_to` +
                 `&order=date_from.desc&limit=2`;
      const { rows: incumbents } = await sbFetch(env, qInc);

      if (incumbents.length > 1) {
        return withCORS(env, req, badRequest("Multiple incumbent windows found at the proposed start date. Please tidy overlaps first."));
      }
      if (incumbents.length === 1) {
        const inc = incumbents[0];
        if (String(inc.date_from) === String(dateFrom)) {
          return withCORS(env, req, badRequest("A window for this role/band already starts on the same date. Edit that window instead or choose a different start."));
        }
        const cut = dayBeforeYmd(dateFrom);
        if (inc.date_from && cut < inc.date_from) {
          return withCORS(env, req, badRequest("Proposed start would back-cut before the incumbent's start. Adjust dates."));
        }
        // Truncate incumbent to N−1
        const p = await fetch(
          `${env.SUPABASE_URL}/rest/v1/rates_client_defaults?id=eq.${encodeURIComponent(inc.id)}`,
          { method: "PATCH", headers: { ...sbHeaders(env), "Prefer": "return=representation" }, body: JSON.stringify({ date_to: cut, updated_at: nowIso() }) }
        );
        if (!p.ok) {
          const err = await p.text();
          return withCORS(env, req, badRequest(`Failed to truncate incumbent: ${err}`));
        }
      }
    }

    // 1b) If a later ENABLED window exists, clamp new.date_to to the day before the next start
    {
      let qNext = `${env.SUPABASE_URL}/rest/v1/rates_client_defaults` +
                  `?client_id=eq.${encodeURIComponent(client_id)}` +
                  `&role=eq.${encodeURIComponent(role)}` +
                  `&${bandFilter(band)}` +
                  `&date_from=gt.${encodeURIComponent(dateFrom)}` +
                  `&disabled_at_utc=is.null` +             // ← ignore disabled rows
                  `&select=date_from` +
                  `&order=date_from.asc&limit=1`;
      const { rows: nexts } = await sbFetch(env, qNext);
      if (Array.isArray(nexts) && nexts.length === 1) {
        const nextStart = nexts[0].date_from;
        const clampTo = dayBeforeYmd(nextStart);
        if (!rec.date_to || rec.date_to > clampTo) {
          rec.date_to = clampTo;
        }
      }
    }

    // 2) Insert the new unified window
    {
      const res = await fetch(`${env.SUPABASE_URL}/rest/v1/rates_client_defaults`, {
        method: "POST",
        headers: { ...sbHeaders(env), "Prefer": "return=representation" },
        body: JSON.stringify({ ...rec, created_at: nowIso() })
      });
      if (!res.ok) {
        const err = await res.text();
        return withCORS(env, req, badRequest(`Rate insert failed: ${err}`));
      }
      const json = await res.json().catch(() => ({}));
      const result = Array.isArray(json) ? json[0] : json;

      // Enqueue recompute ONLY if window is currently effective (inclusive)
      if (todayWithinWindow(rec.date_from, rec.date_to)) {
        await enqueueTsfinRecomputeForClient(env, client_id);
      }

      return withCORS(env, req, ok({ rate: result }));
    }
  } catch (e) {
    return withCORS(env, req, serverError("Failed to upsert client default window"));
  }
}