export const NHSP_PREFINAL_HEADERS = Object.freeze([
  'Date',
  'Ref Num',
  'Agency Worker Name',
  'Agency Worker Unique Id',
  'Trust',
  'Ward',
  'Assignment',
  'Contract',
  '',
  '',
  '',
  'Actual',
  '',
  '',
  '',
  'Commission',
  'Total Cost'
]);

export const NHSP_FINAL_HEADERS = Object.freeze([
  'Date',
  'Ref Num',
  'Agency Worker Name',
  'Agency Worker Unique Id',
  'Trust',
  'Ward',
  'Assignment',
  'Contract',
  '',
  '',
  '',
  'Actual',
  '',
  '',
  '',
  'Commission',
  'FMC',
  'Total Cost',
  'Rate'
]);

export const NHSP_SUBHEADERS = Object.freeze([
  '', '', '', '', '', '', '',
  'Start', 'End', 'Break In Minutes', 'Total',
  'Start', 'End', 'Break In Minutes', 'Total'
]);

export const HEALTHROSTER_LAYOUT_A_HEADERS = Object.freeze([
  'Request Id', 'Staff', 'Agency', 'Year', 'Date', 'Unit', 'Location Name', 'Grade', 'Assignment Id',
  'From', 'To', 'Break', 'Start', 'End', 'Actual Break', 'Hours', 'Original Shift Duration',
  'Timesheet Reason', 'Finalised Date', 'Submitted Date', 'Agency Purchase Order', 'Cost Centre Name',
  'Cost Centre Desc', 'Payment Override Cost Centre', 'Payment Override Cost Centre Description',
  'Estimated Cost', 'Indicative Cost', 'Actual Cost', 'Timesheet Entered Date', 'Supplier Reference',
  'Authoriser', 'Requested On', 'Date Transferred', 'Booked Date', 'Skill', 'Staff Group',
  'Agency Account Code', 'Agency Invoice Number', 'Agency Invoice Received Date', 'Agency Invoice Paid Date',
  'Works within Framework', 'Capped Rates', 'Wage Cap', 'Request Reason', 'Total Hours Worked',
  'Agency VAT Amount', 'Agency Commission Amount', 'Agency NI Amount', 'Agency Worker Pay',
  'Timesheet Entered By', 'Timesheet Finalised By', 'Block ID Number', 'Direct Engagement',
  'DE Contract Status', '50% Above Cap Compliant', 'Basic', 'Night', 'Saturday', 'Sunday', 'Public Holiday'
]);

export const HEALTHROSTER_LAYOUT_B_HEADERS = Object.freeze([
  'Request Id', 'Status', 'Date', 'Day', 'Shift', 'Start', 'End', 'Trust', 'Unit', 'Unit Description',
  'Location Name', 'Staff Group', 'Booked Grade', 'Request Grade', 'Activity Type', 'Activity Profile',
  'Activity Benchmark', 'Activity Category', 'Skill', 'Agency', 'Staff', 'Assignment Number',
  'Request Reason', 'Person Informed', 'Confirmed By', 'Booked Hours', 'Booking Source', 'Indicative Cost',
  'Actual Cost', 'Estimated Cost', 'Requester', 'Requested On', 'Request Method', 'Pos. Mgmt. Code',
  'Agency Purchase Order', 'High Priority', 'Gender', 'Payment Override Cost Centre',
  'Payment Override Cost Centre Description', 'Preferred Staff', 'Grade Type Category', 'Org Structure',
  'Estimated Minutes', 'Speciality', 'Fallback Speciality', 'Agency Account Code', 'Actual Start',
  'Actual End', 'Actual Break', 'Original Grade', 'Actual Hours', 'Works within Framework', 'Capped Rates',
  'Wage Cap', 'CloudStaff Unit', 'CloudStaff Unit Site', 'Block ID Number', 'Agency Worker Pay',
  'Agency VAT Amount', 'Agency Commission Amount', 'Agency NI Amount', 'Direct Engagement',
  'DE Contract Status', 'Timesheet Entered By', 'Timesheet Finalised By',
  'Medical or AfC Banding(National Reporting)', 'Reporting Staff Group', 'Framework Provider',
  'Booking User', '50% Above Cap Compliant', 'Date First Transferred', 'Shift Type', 'Agency Invoice Number',
  'Agency Invoice Paid Date', 'Agency Invoice Received Date', 'Original Shift Duration', 'Timesheet Reason',
  'Timesheet Entered Date', 'Total Hours Worked', 'Year', 'Allowed for Agency', 'Basic', 'Night', 'Saturday',
  'Sunday', 'Public Holiday'
]);

// This is a source-profile layout, not a Client identity. The evidence that
// established it happens to come from one supplier, but no supplier or Client
// name is used to select it.
export const SOURCE_FIXED_EXPENSE_WHOLE_SHIFT_CSV_HEADERS = Object.freeze([
  'grand_parent_business_unit_name', 'parent_business_unit_name', 'business_unit_name',
  'Billing Group Name', 'Client Weekend Date', 'Weekend Date', 'Custom Code 1', 'Custom Code 2',
  'Custom Code 3', 'Job Category', 'Vat Option', 'Permanent Equivalent Rates', 'Candidate',
  'Payroll Number', 'Vacancy Id', 'Booking Id', 'Timesheet Id', 'Agency', 'Agency Location',
  'Candidate Uid', 'Candidate Id', 'TNA Reference', 'Approved By', 'Approved Date', 'monday', 'tuesday',
  'wednesday', 'thursday', 'friday', 'saturday', 'sunday', 'Bonus', 'Bonus NI', 'Expenses', 'Total Hours',
  'Total Cost', 'Line ID', 'STD Hours', 'STD Unit Cost', 'STD Sub Cost', 'OT Hours', 'OT Unit Cost',
  'OT Sub Cost', 'SAT Hours', 'SAT Unit Cost', 'SAT Sub Cost', 'SUN Hours', 'SUN Unit Cost',
  'SUN Sub Cost', 'BH Hours', 'BH Unit Cost', 'BH Sub Cost', 'Nmw-Midweek-Adj/sat Hours',
  'Nmw-Midweek-Adj/sat Unit Cost', 'Nmw-Midweek-Adj/sat Sub Cost', 'Booking Reference',
  'Booking Reason', 'Supply Type', 'Type', 'Business Unit ID', 'Job ID', 'Job Type', 'Booking Start',
  'Booking End', 'Invoice ID'
]);

