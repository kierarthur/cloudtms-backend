// Exact local helper extraction from the ordinary TSFIN worker. No finance or transport.
  const norm = (s) => String(s || "").trim();
  const normRole = (s) => norm(s).replace(/\s+/g, " ").trim().toUpperCase();
  const normBand = (v) => {
    const s = norm(v);
    if (!s) return null;
    let m = s.match(/^\s*band\s*([0-9]+)\s*$/i);
    if (!m) m = s.match(/^\s*b\s*([0-9]+)\s*$/i);
    if (!m) m = s.match(/^\s*([0-9]+)\s*$/);
    if (m && m[1]) return `Band ${parseInt(m[1], 10)}`;
    return s;
  };

  const parseCandidateRoleCodes = (rolesVal) => {
    try {
      let roles = rolesVal;
      if (typeof roles === "string") roles = JSON.parse(roles);
      if (!Array.isArray(roles)) return [];
      return roles
        .map((r) => (r && typeof r === "object" ? r.code : r))
        .map((c) => normRole(c))
        .filter(Boolean);
    } catch {
      return [];
    }
  };


export { normRole as normaliseRole, normBand as normaliseBand, parseCandidateRoleCodes };
