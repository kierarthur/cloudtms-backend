# SCR-17 actual unsaved-selection confirmation review

Independent A4 review, 10 October2026. Read the complete SCREEN_DESIGN_POLICY.md (v1.2), tools/policy.json, SCR17 inventory/WB21 card and T04f's Unsaved ticks subpattern. Visually inspected all three saved actual product PNGs and the current Office/standard adapter/main close path. The ignored capture helper opens the actual saved frontend/Office with fixed local reads, unticks a real Ready checkbox and clicks the actual shared header Close; it aborts hosted network. Reviewer ran no browser, capture, tests, imports, SQL, business, fixtures, generators or pack tools. Documentation only. Item16 is manual wording;25 and readerAdditions26 are N/A. This is only the completed local Close slice, not whole WB21.

**Final independent A4 approval:28 PASS /0 FAIL /14 N/A.** All applicable42-point policy/user-eyes checks pass for this completed local Close slice. This table supersedes the earlier pre-correction captures. N/A does not waive future whole-WB21 obligations, and actual business acceptance is not asserted.

## Corrections independently closed

- **SC17-01 closed (10/36):** Office uses `btn btn-warn` for Discard and the real action layout (`:562,565`). All refreshed images show a clear red destructive button, separate from neutral safe Keep, with8px spacing. The confirmation-specific safe-first order remains.
- **SC17-02 closed (5/22):** The existing adapter's explicit confirmation option hides only this child's generic header. The actual question is visible once and receives a unique label ID; panel role is `alertdialog`, labelled by that question (`ui:47–51`). Initial useful focus is the first safe footer control, Keep (`:58`). Dismissal removes the confirmation class and restores normal parent dialog labeling (`:60`). Native token/stack/Escape closure remains, no second modal framework.
- **SC17-03 closed (22/23/41):** Fresh phone screenshot shows both buttons44px high, fully within390×844. Narrow confirmation CSS sets min-height44px and retains shared spacing (`CSS:73–75`), without broadening unrelated window selectors.

## All42 policy and user-eyes results

| # | Result | Evidence / scope |
|---|---|---|
| 1 | PASS | Current local warning exposes Keep and Discard; full submit/sequencer obligations remain separate. |
| 2 | PASS | Cited LUI049 close warning is reachable through the actual shared Close/Escape path; no server discard. |
| 3 | PASS | Exact `Discard your unsaved selection?`, Keep and Discard are present; short persistence-qualified consequence. |
| 4 | PASS | Tick changes local intent only; no monetary recalculation or command starts at close. |
| 5 | PASS | Real standard small600 frame with header hidden for this confirmation; question only, no repeated Close. |
| 6 | PASS | No tabs or extra navigation tiers on the confirmation. |
| 7 | PASS | One question and one consequence; no nested card layout. |
| 8 | N/A | This dialog contains no table or financial figures. |
| 9 | N/A | No status badges in this dialog. |
| 10 | PASS | Confirmation-specific two named actions: safe Keep first, clearly destructive btn-warn Discard second; no extra workflow/footer control. |
| 11 | PASS | Existing frame/tokens/danger/action classes reused; only three narrowly scoped confirmation rules, no new !important or second overlay. |
| 12 | N/A | No list or paging controls in the confirmation. |
| 13 | N/A | No data columns; dialogs are explicitly sort-exempt. |
| 14 | N/A | No selection table in this dialog; it qualifies the retained local intent. |
| 15 | N/A | No total/count is shown or manufactured. |
| 16 | PASS | Manual wording review: question4 words, consequence7 words, no technical/chatter terms or format violations. No wording tool executed. |
| 17 | N/A | No failed save/read is performed or represented by this local confirmation. |
| 18 | PASS | One-line question, one-line consequence, two buttons: Keep first, Discard second. |
| 19 | N/A | No remote read/loading/empty figures are displayed here; source failure preserves local state. |
| 20 | PASS | No loan, technical code, UUID or ID is visible. |
| 21 | N/A | No date/time/money or unknown numeric value is displayed. |
| 22 | PASS | Real Tab/Enter/Escape path, alertdialog labelled by unique question, safe Keep initial focus, native focus restoration;44px actions. Source evidence, no keyboard runtime claimed. |
| 23 | PASS | Actual390×844 render fits without sideways clipping; both44px footer targets remain fully reachable, no unnecessary sorting/list controls. |
| 24 | PASS | Desktop dark and device-light are visually identical and byte-identical. |
| 25 | N/A | Owner requires actual product capture, not regenerated spec/page; supplied pack remains read-only. |
| 26 | N/A | No reader-derived value or readerAdditions needed; prompt derives actual local dirty state/persistence. |
| 27 | PASS | No official pay date or execution date is visible. |
| 28 | PASS | Payroll staff can understand the plain question and action labels immediately. |
| 29 | PASS | Actions sit immediately below the selection question in the reachable footer. |
| 30 | PASS | Keep and Discard clearly express the two available local decisions; no unrelated workflow button. |
| 31 | PASS | Answers the current task's real question: retain or discard unsent ticks; does not invent financial facts. |
| 32 | N/A | No Waiting/Blocked/Action-required row in this confirmation. |
| 33 | PASS | Source Keep closes with intent retained; Discard resets only local choices; canceled question returns to parent. Runtime not executed. |
| 34 | PASS | Body shows only the immediate question and consequence, with no system explanation. |
| 35 | PASS | Keep, Discard, ticks and Banking Pay use consistent plain names. |
| 36 | PASS | Keep is first and initially focused; red Discard is visually distinct/separated and requires this explicit confirmation. |
| 37 | PASS | Escape on the child leaves decision unset and preserves parent intent; safe Keep preserves choices; authority loss permits safe local dismissal. |
| 38 | PASS | One question on actual close, no navigation detour or network action. |
| 39 | N/A | No monetary figures/counts in this confirmation to add up. |
| 40 | N/A | This is local close, not Create Draft review/acceptance; submit-side confidence is a separate required join. |
| 41 | PASS | Finite local state, no awaited business read, real safe-focus/keyboard path and actual44px phone actions; no large-data/app test claimed. |
| 42 | PASS | The short question plainly identifies the pending unsent-selection decision on return. |

## Evidence identities

| Evidence | SHA-256 |
|---|---|
| `dark.png` | `312e7c1aeead57dd1251653728b25be1e6eb840de72afe64513dbf14c35dee78` |
| `device-light.png` | `312e7c1aeead57dd1251653728b25be1e6eb840de72afe64513dbf14c35dee78` |
| `phone-dark.png` | `43726ba5ec8811378ffe8dc92bd73866453aa5fea14f28a0fb1892779cefc1ce` |
| `fixed-read-responses.json` | `a0615c95e3cbfc268c1da77cc832b6f5ea312256e74822e128caa2f6cdb40d19` |
| ignored `WB21-SCR17-screenshots.cjs` | `795c225889581e5d26666fb7883402be508f86e578da32d2323d461e91e7a16d` |
| FE Office | `d2acc6113ce462f24db8347ca726023ccf06aa2b99d38c986adb5fb64066d79a` |
| FE UI adapter | `34e3286cc697c150542bb7f2ab61b5d6033e2cdfdc36272c3620d9b445a5dfd1` |
| FE main | `e66e48eed68cd2c8db817bb0838df0cc126bedc8f147a50f952d93bf665acf1b` |
| FE NEXT CSS | `a7b8101b73150360b21335e9f8079ff86f8da911d86f98545ff4789fd7f836ce` |

All three final images were individually inspected after the fixes. Dark/device-light remain byte-identical. No actual DB/application/payment acceptance or whole-card completion is implied; remaining WB21 submit/readback/progress joins retain their real required owners.

Builder recapture10/10/2026 against final FEc4fab4c871a056124cbf3d152cd1eb0e26435cde: all three image hashes remain exactly those independently inspected above. The submission journal source additions do not change this confirmation presentation. Independent journal review is separate; whole WB21/SCR160 remain open.
