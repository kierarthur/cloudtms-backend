2. Why the import modal & resolve modal only work once

You described two related behaviours:

The import screen lets you import once; after that the summary modal “stops working”.

The child modal (candidate pick / resolve) works once; after you’ve resolved a candidate, trying to resolve another row stops doing anything.

Both are caused by how the weekly import summary wiring is written on the frontend.

2.1 The wiring only initialises once, with a stale copy of rows/importId

In FRONTEND FOR CLOUDTMS.js you have this function:

function wireWeeklyImportSummaryActions(type, importId) {
  const t = String(type || '').toUpperCase();
  if (t !== 'NHSP' && t !== 'HR_WEEKLY') return;

  const root = document.getElementById('weeklyImportSummary');
  if (!root) return;
  if (root.__weeklyWired) return;   // <── guard
  root.__weeklyWired = true;

  const state = window.__importSummaryState && window.__importSummaryState[t];
  const rows  = state && Array.isArray(state.rows) ? state.rows : [];

  ...
  root.addEventListener('click', (ev) => {
    ...
    const idx = Number(btn.getAttribute('data-row-idx') || '-1');
    if (idx < 0 || idx >= rows.length) return;
    const row = rows[idx];                  // <── captured once
    const mappings = ensureWeeklyImportMappings(t, importId);
    ...
    await postWeeklyResolveMappings(importId, t);
    await refreshWeeklyImportSummary(type, importId);
  });

  root.addEventListener('change', (ev) => { /* uses rows & importId */ });
}


And the summary modal calls it after each preview fetch:

function renderImportSummaryModal(type, summaryState) {
  ...
  window.__importSummaryState[t] = summaryState;

  showModal('NHSP Weekly Import Summary', tabs, renderTab('main'), { ... });

  setTimeout(() => {
    try {
      wireWeeklyImportSummaryActions(type, importId);
    } catch (e) { console.error(e); }
  }, 0);
}


What this means:

On the first weekly import:

wireWeeklyImportSummaryActions('NHSP', importId1) runs.

It sets root.__weeklyWired = true.

It reads window.__importSummaryState.NHSP and captures rows for importId1 into the closure.

The event handlers are attached and keep the old rows and old importId1 in their closure forever.

When you resolve a candidate, the handler:

Posts resolve-conflicts for importId1.

Calls refreshWeeklyImportSummary('NHSP', importId1), which refetches preview and calls renderImportSummaryModal('NHSP', summary2).

That sets a new window.__importSummaryState.NHSP with possibly new rows, but…

On refresh:

The setTimeout tries to call wireWeeklyImportSummaryActions('NHSP', importId1) again.

But because root.__weeklyWired is true, the function returns early and does not re-read window.__importSummaryState.

The click handler still points at the old rows and old importId.

Now look at the two failure modes you feel:

Resolve modal only works once (within a single import)

After you resolve the first candidate and the preview refreshes, the button you click for the second candidate uses data-row-idx pointing into the new rows array.

The handler still uses its old rows array; indexes no longer match or are pointing at previously-resolved entries.

In some cases that just silently fails the bounds check (idx >= rows.length), so nothing happens.

Import screen only works once (across multiple imports)

On the second import you get a new importId2, new rows and a new preview.

renderImportSummaryModal('NHSP', summaryForImport2) runs and updates window.__importSummaryState.NHSP.

But wireWeeklyImportSummaryActions never re-runs, because root.__weeklyWired is still true from the very first import.

So when you click “Resolve” on import2, the handler still posts mappings to importId1 and depends on the old rows array – effectively “nothing happens” from your point of view.

2.2 Fix: make handlers read the current state, not a captured snapshot

You don’t actually need to re-attach handlers on each refresh; you just need those handlers to read the current import_id and rows from window.__importSummaryState each time they run.

A safe change is:

Keep the root.__weeklyWired guard (so you only attach listeners once).

Replace the captured state/rows with a getState() helper that always reads the latest state.

Something like this:

function wireWeeklyImportSummaryActions(type, importId) {
  const t = String(type || '').toUpperCase();
  if (t !== 'NHSP' && t !== 'HR_WEEKLY') return;

  const root = document.getElementById('weeklyImportSummary');
  if (!root) return;

  if (root.__weeklyWired) {
    // Already wired; the handlers will read from window.__importSummaryState
    return;
  }
  root.__weeklyWired = true;

  const norm = (s) => String(s || '').trim().toLowerCase();

  const getState = () => {
    const all = window.__importSummaryState || {};
    const st  = all[t] || {};
    const rows = Array.isArray(st.rows) ? st.rows : [];
    const id   = st.import_id || importId;   // fallback to the argument
    return { state: st, rows, importId: id };
  };

  const ensureMappings = (importId) => {
    window.__weeklyImportMappings = window.__weeklyImportMappings || {};
    const m = window.__weeklyImportMappings;
    m[t] = m[t] || {};
    m[t][importId] = m[t][importId] || {
      candidate_mappings: [],
      client_aliases: [],
      selectedGroups: new Set()
    };
    return m[t][importId];
  };

  root.addEventListener('click', async (ev) => {
    const btn = ev.target.closest('[data-act]');
    if (!btn) return;

    const { rows, importId: currentImportId } = getState();
    const mappings = ensureMappings(currentImportId);

    const act = btn.getAttribute('data-act');
    const idx = Number(btn.getAttribute('data-row-idx') || '-1');

    if (act === 'weekly-resolve-candidate') {
      if (idx < 0 || idx >= rows.length) return;
      const row = rows[idx];

      // Open picker as before
      const staffNorm = norm(row.staff_norm || row.staff_name);
      const hospRaw   = row.hospital_or_trust || row.unit || row.hospital_norm || '';
      const hospNorm  = norm(hospRaw);

      const selected = await openCandidatePicker({ staff: row.staff_name });
      if (!selected) return;

      mappings.candidate_mappings.push({
        staff_norm:        staffNorm,
        hospital_or_trust: hospNorm,
        candidate_id:      selected.id
      });

      await postWeeklyResolveMappings(currentImportId, t);
      await refreshWeeklyImportSummary(type, currentImportId);
      return;
    }

    // ... other data-act branches (weekly-resolve-client, refresh, etc)
  });

  root.addEventListener('change', (ev) => {
    const cb = ev.target.closest('[data-act="weekly-select-group"]');
    if (!cb) return;

    const { rows, importId: currentImportId } = getState();
    const mappings = ensureMappings(currentImportId);
    const set      = mappings.selectedGroups;

    const id = cb.getAttribute('data-group-id');
    if (!id) return;

    if (cb.checked) set.add(id);
    else set.delete(id);
  });
}


Key changes:

state and rows are no longer captured at wiring time; they’re fetched via getState() every time a click/change happens.

importId used for postWeeklyResolveMappings and refreshWeeklyImportSummary always comes from the latest summary state.

The __weeklyWired flag is still there, but now it only guards attaching multiple handlers – it no longer freezes your state.

This should fix both:

The “second candidate can’t be resolved” problem within one import, and

The “second import’s summary is dead” problem across imports.
