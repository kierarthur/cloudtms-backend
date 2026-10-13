# SCR116 supplied design comparison — FC03 shared component

Read-only supplied design: SCREEN_DESIGN_POLICY_20261010/batches/B9/png/SCR-116_preview_apply_dark.png and matching specs/SCR-116_preview_apply.json. Reviewer views it beside FC03-preview-dark.png; the design is never copied or edited.

Visible differences and card-required scope explanations:

1. The supplied full window contains the future FC04 creation form and blurred FC01 Case list. FC03-D09 owns only the shared Preview/Apply panel/footer. The actual reusable component is shown in the standard native small window with minimal fixed caller context (Bethany Clarke, £300.00, six weeks). No early FC01/FC04 menus, controls or invented list counts. Future owning windows mount this same component.
2. Repayment start/last dates are 12/10/2026 and16/11/2026, the FC03 Done example. The supplied design's19/10 and23/11 are sample inputs. Both render actual server-returned dates.
3. Warning is “This Candidate already has repayments.” The reader supplies the exact EXISTING_REPAYMENTS fact. No reader supplies the design's fictional £150 weekly aggregate/name; owner A2 requires leaving it out. This warning never disables Apply.
4. Native small window is vertically centred with the component-only shorter body; supplied full form is tall. Frame, width, dark colours, Preview card structure, first/last/total/threshold/floor labels and footer controls follow the supplied style. Card radius13 and footer gap8 use policy geometry. No box in a box or new controls.
5. Initial, changed-input, failed-preview, uncertain-command and accepted-but-not-yet-created states add one truthful inline result sentence. Exact retained retry says Try again; accepted pending readback says Refresh. Native accepted receipt is not claimed completed. This is required by FC03 typed idempotent receipt/current readback and existing journal; supplied picture covers after-preview state only.
6. Phone wraps the existing facts into two columns and retains the same footer. No list belongs to FC03, hence no early sorting/list controls. Device-light uses the same dark Office screen.

Control purpose: Preview is required to see the server consequence; Apply records only the successfully previewed exact inputs; Try again reuses only an existing unresolved command; Refresh reads only its accepted Case; header Close preserves the existing journal. All are expected by a payroll user. No extra Expand, export, paging, technical diagnostics, money calculations or pay-date control is present. Optional note/comment forms remain owned by upcoming cards; the backend stores both separately and never substitutes note for required reason.

Only fixed local responses in FC03-fixed-responses.json were used. No database rows or hosted requests. Screenshots are real native component renders. POLICY CHECKLIST review is separate and required before the item tick.
