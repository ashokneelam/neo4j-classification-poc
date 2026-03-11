/**
 * Data Security Demo — Snowflake + Neo4j
 * Google Sheets Builder Script
 *
 * HOW TO USE:
 *   1. Open a new Google Sheet
 *   2. Click Extensions > Apps Script
 *   3. Paste this entire file, replacing the default code
 *   4. Click Run > buildSecurityDemoSheet
 *   5. Approve permissions when prompted
 *
 * This builds 6 sheets matching the interactive HTML demo dashboard.
 */

// ─── Colour palette (matches the HTML dashboard) ────────────────────────────
const C = {
  // Classification tier colours
  RESTRICTED_BG:  '#FDE8E8', RESTRICTED_FG:  '#B91C1C', RESTRICTED_BD:  '#F87171',
  PII_BG:         '#FEF0E7', PII_FG:         '#C2410C', PII_BD:         '#FB923C',
  INTERNAL_BG:    '#FEFCE8', INTERNAL_FG:    '#854D0E', INTERNAL_BD:    '#FACC15',
  PUBLIC_BG:      '#F0FDF4', PUBLIC_FG:      '#15803D', PUBLIC_BD:      '#4ADE80',
  // UI chrome
  HEADER_BG:      '#0F172A', HEADER_FG:      '#E2E8F0',
  SUBHEADER_BG:   '#1E293B', SUBHEADER_FG:   '#94A3B8',
  ACCENT:         '#0284C7', ACCENT_FG:      '#FFFFFF',
  SECTION_BG:     '#F8FAFC', SECTION_FG:     '#0F172A',
  ALLOW_FG:       '#15803D', DENY_FG:        '#B91C1C', PARTIAL_FG: '#92400E',
  // Role colours
  ROLE_GOV:       '#0369A1', ROLE_ENG:       '#7C3AED', ROLE_FIN:       '#EA580C',
  ROLE_HR:        '#9333EA', ROLE_SEC:       '#0891B2', ROLE_ANA:       '#CA8A04',
  ROLE_PUB:       '#64748B',
};

// ─── Helpers ─────────────────────────────────────────────────────────────────
function getOrCreate(ss, name) {
  return ss.getSheetByName(name) || ss.insertSheet(name);
}

function clearSheet(sheet) {
  sheet.clearContents();
  sheet.clearFormats();
}

function headerRow(sheet, row, values, bgColor, fgColor, fontSize) {
  const range = sheet.getRange(row, 1, 1, values.length);
  range.setValues([values]);
  range.setBackground(bgColor || C.HEADER_BG);
  range.setFontColor(fgColor || C.HEADER_FG);
  range.setFontWeight('bold');
  range.setFontSize(fontSize || 10);
  range.setVerticalAlignment('middle');
}

function sectionTitle(sheet, row, col, text, colSpan) {
  const r = sheet.getRange(row, col, 1, colSpan || 10);
  r.merge();
  r.setValue(text);
  r.setBackground(C.SUBHEADER_BG);
  r.setFontColor(C.ACCENT);
  r.setFontWeight('bold');
  r.setFontSize(10);
  r.setPaddingTop && r.setPaddingTop(6);
}

function clsChip(sheet, row, col, classification) {
  const cell = sheet.getRange(row, col);
  cell.setValue(classification);
  cell.setFontWeight('bold');
  cell.setFontSize(9);
  switch (classification) {
    case 'Restricted': cell.setBackground(C.RESTRICTED_BG); cell.setFontColor(C.RESTRICTED_FG); break;
    case 'PII':        cell.setBackground(C.PII_BG);        cell.setFontColor(C.PII_FG);        break;
    case 'Internal':   cell.setBackground(C.INTERNAL_BG);   cell.setFontColor(C.INTERNAL_FG);   break;
    case 'Public':     cell.setBackground(C.PUBLIC_BG);     cell.setFontColor(C.PUBLIC_FG);      break;
    default:           cell.setBackground('#F1F5F9');         cell.setFontColor('#475569');
  }
}

function accessCell(sheet, row, col, decision) {
  const cell = sheet.getRange(row, col);
  if (decision === 'FULL ACCESS' || decision === 'ACCESS') {
    cell.setValue('✓  ' + decision);
    cell.setFontColor(C.ALLOW_FG);
    cell.setBackground('#F0FDF4');
  } else if (decision === 'DENIED') {
    cell.setValue('✗  DENIED');
    cell.setFontColor(C.DENY_FG);
    cell.setBackground('#FEF2F2');
  } else {
    cell.setValue('~  ' + decision);
    cell.setFontColor(C.PARTIAL_FG);
    cell.setBackground('#FFFBEB');
  }
  cell.setFontWeight('bold');
  cell.setFontSize(9);
}

function freezeAndResize(sheet, freezeRows, freezeCols, colWidths) {
  if (freezeRows) sheet.setFrozenRows(freezeRows);
  if (freezeCols) sheet.setFrozenColumns(freezeCols);
  if (colWidths) colWidths.forEach((w, i) => w && sheet.setColumnWidth(i + 1, w));
}

// ═══════════════════════════════════════════════════════════════════════════════
// SHEET 1: OVERVIEW
// ═══════════════════════════════════════════════════════════════════════════════
function buildOverviewSheet(ss) {
  const sheet = getOrCreate(ss, '1. Overview');
  clearSheet(sheet);
  sheet.setTabColor('#0284C7');

  // ── Title block ──────────────────────────────────────────────────────────
  const title = sheet.getRange('A1:J1');
  title.merge();
  title.setValue('DATA SECURITY DEMO  —  Snowflake Tags → Neo4j Access Control');
  title.setBackground(C.HEADER_BG);
  title.setFontColor('#38BDF8');
  title.setFontWeight('bold');
  title.setFontSize(14);
  title.setHorizontalAlignment('center');
  sheet.setRowHeight(1, 40);

  const subtitle = sheet.getRange('A2:J2');
  subtitle.merge();
  subtitle.setValue('Column-level tags extracted from Snowflake via TAG_REFERENCES_ALL_COLUMNS() and ingested into Neo4j as node properties + CLASSIFIED_AS relationships');
  subtitle.setBackground(C.SUBHEADER_BG);
  subtitle.setFontColor(C.SUBHEADER_FG);
  subtitle.setFontSize(10);
  subtitle.setHorizontalAlignment('center');
  sheet.setRowHeight(2, 28);

  // ── Stats row ────────────────────────────────────────────────────────────
  sheet.getRange('A3:J3').merge().setValue('').setBackground('#FFFFFF');
  sheet.setRowHeight(3, 14);

  const statLabels = ['Snowflake Tables', 'Tagged Columns', 'Security Roles', 'Masking Policies', 'Row Access Policies', 'Neo4j Nodes', 'Relationships', 'Classification Tiers'];
  const statValues = [5, 81, 7, 6, 3, '120+', '230+', 4];
  const statColors = ['#0284C7','#7C3AED','#0891B2','#9333EA','#EA580C','#15803D','#CA8A04','#B91C1C'];

  statLabels.forEach((label, i) => {
    const col = i + 1;
    const numCell = sheet.getRange(4, col);
    numCell.setValue(statValues[i]);
    numCell.setFontSize(22);
    numCell.setFontWeight('bold');
    numCell.setFontColor(statColors[i]);
    numCell.setHorizontalAlignment('center');
    numCell.setBackground('#FAFAFA');
    numCell.setVerticalAlignment('bottom');
    sheet.setRowHeight(4, 36);

    const lblCell = sheet.getRange(5, col);
    lblCell.setValue(label);
    lblCell.setFontSize(9);
    lblCell.setFontColor('#64748B');
    lblCell.setHorizontalAlignment('center');
    lblCell.setBackground('#FAFAFA');
    sheet.setRowHeight(5, 22);

    sheet.getRange(4, col, 2, 1).setBorder(false, true, false, i === 0, false, false, '#E2E8F0', SpreadsheetApp.BorderStyle.SOLID);
  });

  // ── Spacer ───────────────────────────────────────────────────────────────
  sheet.setRowHeight(6, 16);

  // ── Architecture flow ────────────────────────────────────────────────────
  sectionTitle(sheet, 7, 1, 'ARCHITECTURE FLOW', 10);
  sheet.setRowHeight(7, 26);

  const archData = [
    ['SNOWFLAKE (Source)', '', 'PYTHON ETL PIPELINE', '', 'NEO4J AURA (Target)'],
    ['• Column-level tags (DATA_CLASSIFICATION, DATA_CATEGORY,', '', '• Connects to Snowflake', '', '• Database → Schema → Table → Column graph'],
    ['  ENCRYPTION_REQUIRED, RETENTION_POLICY, DATA_OWNER)', '', '• Calls TAG_REFERENCES_ALL_COLUMNS()', '', '• Column nodes carry all Snowflake tag metadata'],
    ['• Masking Policies (SSN, Email, Amount, Account, IP, Salary)', '', '• Pivots tags into structured metadata', '', '• CLASSIFIED_AS relationships link columns to tiers'],
    ['• Row Access Policies (Customer, Employee, Transaction)', '', '• Extracts data rows from 5 tables', '', '• Role graph: Role -[:CAN_ACCESS]-> Classification'],
    ['• Role hierarchy (7 roles)', '', '• Writes graph model to Neo4j', '', '• Policy nodes: Policy -[:MASKS]-> Column'],
  ];

  archData.forEach((row, i) => {
    const r = 8 + i;
    sheet.setRowHeight(r, 20);
    const cell1 = sheet.getRange(r, 1, 1, 2);
    cell1.merge();
    cell1.setValue(row[0]);
    cell1.setFontColor(i === 0 ? '#0369A1' : '#334155');
    if (i === 0) cell1.setFontWeight('bold');
    cell1.setFontSize(10);
    cell1.setBackground(i === 0 ? '#EFF6FF' : '#F8FAFC');

    const arrowCell = sheet.getRange(r, 3, 1, 1);
    arrowCell.setValue(i === 0 ? '→' : row[2]);
    arrowCell.setHorizontalAlignment('center');
    arrowCell.setFontColor(i === 0 ? '#64748B' : '#475569');
    if (i === 0) { arrowCell.setFontSize(18); arrowCell.setFontWeight('bold'); }
    else arrowCell.setFontSize(9);
    arrowCell.setBackground('#FAFAFA');

    const cell2 = sheet.getRange(r, 4, 1, 1);
    cell2.setValue(i === 0 ? '→' : '');
    if (i === 0) { cell2.setFontSize(18); cell2.setFontWeight('bold'); cell2.setHorizontalAlignment('center'); }
    cell2.setBackground('#FAFAFA');
    cell2.setFontColor('#64748B');

    const cell3 = sheet.getRange(r, 5, 1, 6);
    cell3.merge();
    cell3.setValue(i === 0 ? row[4] : row[4]);
    cell3.setFontColor(i === 0 ? '#15803D' : '#334155');
    if (i === 0) cell3.setFontWeight('bold');
    cell3.setFontSize(10);
    cell3.setBackground(i === 0 ? '#F0FDF4' : '#F8FAFC');
  });

  // ── Classification distribution ──────────────────────────────────────────
  sheet.setRowHeight(14, 14);
  sectionTitle(sheet, 15, 1, 'SNOWFLAKE TAG CLASSIFICATION DISTRIBUTION  (81 tagged columns)', 10);
  sheet.setRowHeight(15, 26);

  const tiers = [
    {name:'RESTRICTED',count:18, bg:C.RESTRICTED_BG, fg:C.RESTRICTED_FG, desc:'SSN, Salary, Account Numbers, Fraud Flags, IPs, Cost Prices — highest sensitivity'},
    {name:'PII',        count:16, bg:C.PII_BG,        fg:C.PII_FG,        desc:'Names, Emails, Phone Numbers, Date of Birth, Addresses — personal identifiable'},
    {name:'INTERNAL',   count:33, bg:C.INTERNAL_BG,   fg:C.INTERNAL_FG,   desc:'IDs, Statuses, Departments, Hire Dates, Quantities — internal business data'},
    {name:'PUBLIC',     count:14, bg:C.PUBLIC_BG,      fg:C.PUBLIC_FG,     desc:'Country, Merchant Category, Product SKU, Location Office — safe to share'},
  ];

  headerRow(sheet, 16, ['Classification', 'Column Count', '% of Total', 'Example Columns', 'Encryption Required', 'Typical Masking'], C.SUBHEADER_BG, C.SUBHEADER_FG);
  sheet.setRowHeight(16, 24);

  const tierExamples = [
    'ssn, salary, account_number, fraud_flag, ip_address, cost_price',
    'first_name, last_name, email, phone, date_of_birth, address_line1',
    'customer_id, department, hire_date, stock_quantity, status, manager_id',
    'country, merchant_category, product_sku, location_office, currency',
  ];
  const tierMasking = ['SSN, Account, Amount, IP, Salary policies', 'Email masking policy', 'No masking', 'No masking'];
  const tierEncryption = ['Yes (6 columns)', 'Yes (3 columns)', 'No', 'No'];

  tiers.forEach((t, i) => {
    const r = 17 + i;
    sheet.setRowHeight(r, 22);
    const row = sheet.getRange(r, 1, 1, 6);
    row.setBackground(t.bg);

    const nameCell = sheet.getRange(r, 1);
    nameCell.setValue(t.name);
    nameCell.setFontColor(t.fg);
    nameCell.setFontWeight('bold');
    nameCell.setFontSize(10);

    sheet.getRange(r, 2).setValue(t.count).setHorizontalAlignment('center').setFontWeight('bold').setFontColor(t.fg);
    sheet.getRange(r, 3).setValue((t.count / 81 * 100).toFixed(0) + '%').setHorizontalAlignment('center').setFontColor('#64748B');
    sheet.getRange(r, 4).setValue(tierExamples[i]).setFontSize(9).setFontColor('#475569').setWrap(false);
    sheet.getRange(r, 5).setValue(tierEncryption[i]).setFontSize(9).setFontColor('#475569').setHorizontalAlignment('center');
    sheet.getRange(r, 6).setValue(tierMasking[i]).setFontSize(9).setFontColor('#475569');
  });

  // ── Neo4j graph model summary ────────────────────────────────────────────
  sheet.setRowHeight(21, 14);
  sectionTitle(sheet, 22, 1, 'NEO4J GRAPH NODES & RELATIONSHIPS AFTER INGESTION', 10);
  sheet.setRowHeight(22, 26);

  headerRow(sheet, 23, ['Node / Relationship', 'Count', 'Description'], C.SUBHEADER_BG, C.SUBHEADER_FG);

  const graphData = [
    ['NODE: Column',              81, 'One per Snowflake column — carries all 5 tag values as properties'],
    ['NODE: Table',                5, 'CUSTOMERS, EMPLOYEES, PRODUCTS, FINANCIAL_TRANSACTIONS, AUDIT_LOGS'],
    ['NODE: Classification',       4, 'Restricted | PII | Internal | Public — classification tier nodes'],
    ['NODE: Role',                 7, 'DATA_GOVERNANCE_ADMIN, DATA_ENGINEER, HR_MANAGER, FINANCE_ANALYST, SECURITY_AUDITOR, DATA_ANALYST, PUBLIC_USER'],
    ['NODE: Policy',               9, 'SSN, Email, Amount, Account, IP, Salary masking + 3 row access policies'],
    ['NODE: Product / Transaction / AuditLog', '10/9/10', 'Data nodes from Snowflake tables with classification propagated'],
    ['REL: CLASSIFIED_AS',        81, 'Column -[:CLASSIFIED_AS]-> Classification  (tag propagated from Snowflake)'],
    ['REL: HAS_CLASSIFICATION',   29, 'DataNode -[:HAS_CLASSIFICATION]-> Classification  (for access control queries)'],
    ['REL: CAN_ACCESS',           19, 'Role -[:CAN_ACCESS]-> Classification  (permission assignments)'],
    ['REL: MASKS',                12, 'Policy -[:MASKS]-> Column  (masking policy coverage)'],
    ['REL: INHERITS_FROM',         6, 'Role -[:INHERITS_FROM]-> Role  (role hierarchy)'],
    ['REL: HAS_COLUMN',           81, 'Table -[:HAS_COLUMN]-> Column  (schema structure)'],
    ['REL: CONTAINS_TABLE/SCHEMA', 6, 'Database -> Schema -> Table catalog hierarchy'],
  ];

  graphData.forEach((row, i) => {
    const r = 24 + i;
    sheet.setRowHeight(r, 20);
    const isNode = row[0].startsWith('NODE');
    const isRel  = row[0].startsWith('REL');
    const bg = isNode ? '#EFF6FF' : isRel ? '#F0FDF4' : '#FFFFFF';
    const fg = isNode ? '#1E40AF' : isRel ? '#166534' : '#374151';
    sheet.getRange(r, 1).setValue(row[0]).setFontColor(fg).setFontWeight(isNode || isRel ? 'bold' : 'normal').setFontSize(9.5).setBackground(bg);
    sheet.getRange(r, 2).setValue(row[1]).setHorizontalAlignment('center').setFontWeight('bold').setFontColor(fg).setBackground(bg);
    sheet.getRange(r, 3, 1, 8).merge().setValue(row[2]).setFontSize(9).setFontColor('#475569').setBackground(bg);
  });

  freezeAndResize(sheet, 2, 0, [200, 80, 80, 260, 130, 200, 160, 130]);
}

// ═══════════════════════════════════════════════════════════════════════════════
// SHEET 2: ACCESS MATRIX
// ═══════════════════════════════════════════════════════════════════════════════
function buildAccessMatrixSheet(ss) {
  const sheet = getOrCreate(ss, '2. Access Matrix');
  clearSheet(sheet);
  sheet.setTabColor('#7C3AED');

  sectionTitle(sheet, 1, 1, 'ROLE × CLASSIFICATION ACCESS MATRIX  —  Node-Level Access Control (simulated via Cypher graph traversal)', 9);
  sheet.setRowHeight(1, 30);

  headerRow(sheet, 2, ['Role', 'Hierarchy Level', 'RESTRICTED', 'PII', 'INTERNAL', 'PUBLIC', 'Notes'], C.HEADER_BG, C.HEADER_FG, 10);
  sheet.setRowHeight(2, 28);

  // Add classification sub-header
  const subheaders = ['', '', 'SSN, Salary, Accounts, IPs', 'Names, Email, DOB, Address', 'IDs, Dept, Status, Dates', 'Country, SKU, Category', ''];
  subheaders.forEach((s, i) => {
    const cell = sheet.getRange(3, i + 1);
    cell.setValue(s);
    cell.setFontSize(8);
    cell.setFontColor('#94A3B8');
    cell.setBackground(C.SUBHEADER_BG);
    cell.setItalic(true);
    cell.setHorizontalAlignment('center');
  });
  sheet.setRowHeight(3, 18);

  const roles = [
    {name:'DATA_GOVERNANCE_ADMIN', level:'L1 — Admin', restricted:'FULL ACCESS', pii:'FULL ACCESS', internal:'FULL ACCESS', public:'FULL ACCESS', notes:'Manages tags and policies. Full visibility across all tiers.', color: C.ROLE_GOV},
    {name:'DATA_ENGINEER',         level:'L2 — Engineer', restricted:'FULL ACCESS', pii:'FULL ACCESS', internal:'FULL ACCESS', public:'FULL ACCESS', notes:'Inherits from Data Gov Admin. Full read access for data operations.', color: C.ROLE_ENG},
    {name:'FINANCE_ANALYST',       level:'L3 — Analyst', restricted:'ACCESS', pii:'DENIED', internal:'ACCESS', public:'FULL ACCESS', notes:'Can see financial Restricted data (account numbers, amounts). No PII.', color: C.ROLE_FIN},
    {name:'HR_MANAGER',            level:'L3 — Manager', restricted:'DENIED', pii:'ACCESS', internal:'ACCESS', public:'FULL ACCESS', notes:'Can see PII for employee management. No financial Restricted data.', color: C.ROLE_HR},
    {name:'SECURITY_AUDITOR',      level:'L3 — Auditor', restricted:'DENIED', pii:'DENIED', internal:'ACCESS', public:'FULL ACCESS', notes:'Can see audit logs and IPs (via IP_MASKING_POLICY). No PII/Restricted.', color: C.ROLE_SEC},
    {name:'DATA_ANALYST',          level:'L4 — Analyst', restricted:'DENIED', pii:'DENIED', internal:'ACCESS', public:'FULL ACCESS', notes:'Internal + Public only. Email shown as domain only (email masking).', color: C.ROLE_ANA},
    {name:'PUBLIC_USER',           level:'L5 — Public', restricted:'DENIED', pii:'DENIED', internal:'DENIED', public:'FULL ACCESS', notes:'Public data only. Row access policies block most table rows.', color: C.ROLE_PUB},
  ];

  roles.forEach((role, i) => {
    const r = 4 + i;
    sheet.setRowHeight(r, 26);

    const nameCell = sheet.getRange(r, 1);
    nameCell.setValue(role.name);
    nameCell.setFontWeight('bold');
    nameCell.setFontSize(10);
    nameCell.setFontColor(role.color);
    nameCell.setBackground('#F8FAFC');

    sheet.getRange(r, 2).setValue(role.level).setFontSize(9).setFontColor('#64748B').setBackground('#F8FAFC').setHorizontalAlignment('center');

    accessCell(sheet, r, 3, role.restricted);
    accessCell(sheet, r, 4, role.pii);
    accessCell(sheet, r, 5, role.internal);
    accessCell(sheet, r, 6, role.public);

    sheet.getRange(r, 7).setValue(role.notes).setFontSize(9).setFontColor('#475569').setBackground('#FAFAFA').setWrap(true);
  });

  // ── Role hierarchy section ───────────────────────────────────────────────
  sheet.setRowHeight(12, 16);
  sectionTitle(sheet, 13, 1, 'ROLE HIERARCHY  (INHERITS_FROM relationships in Neo4j)', 9);
  sheet.setRowHeight(13, 26);

  const hierarchy = [
    ['DATA_GOVERNANCE_ADMIN',  '(root)',               'Manages tags, policies, full governance control'],
    ['  └─ DATA_ENGINEER',     'inherits from: DGA',  'Full read access, ETL operations'],
    ['       └─ DATA_ANALYST', 'inherits from: DE',   'Read Internal + Public columns only'],
    ['            ├─ HR_MANAGER',      'inherits from: DA', 'PII access for employee management'],
    ['            ├─ FINANCE_ANALYST', 'inherits from: DA', 'Restricted financial data access'],
    ['            ├─ SECURITY_AUDITOR','inherits from: DA', 'Internal + audit log access'],
    ['            └─ PUBLIC_USER',     'inherits from: DA', 'Public data only'],
  ];

  hierarchy.forEach((row, i) => {
    const r = 14 + i;
    sheet.setRowHeight(r, 22);
    sheet.getRange(r, 1).setValue(row[0]).setFontSize(9.5).setFontFamily('Courier New').setFontColor('#1E293B').setBackground('#F8FAFC');
    sheet.getRange(r, 2).setValue(row[1]).setFontSize(9).setFontColor('#64748B').setBackground('#F8FAFC').setItalic(true);
    sheet.getRange(r, 3, 1, 5).merge().setValue(row[2]).setFontSize(9).setFontColor('#475569').setBackground('#F8FAFC');
  });

  // ── Row access policy summary ────────────────────────────────────────────
  sheet.setRowHeight(22, 16);
  sectionTitle(sheet, 23, 1, 'ROW-LEVEL ACCESS POLICIES  (additional row filtering on top of classification)', 9);
  sheet.setRowHeight(23, 26);

  headerRow(sheet, 24, ['Policy Name', 'Table', 'Filter Column', 'DATA_ENGINEER / ADMIN', 'HR_MANAGER / FINANCE', 'DATA_ANALYST / SECURITY', 'PUBLIC_USER'], C.SUBHEADER_BG, C.SUBHEADER_FG);
  sheet.setRowHeight(24, 24);

  const rowPolicies = [
    ['CUSTOMER_ROW_POLICY',     'CUSTOMERS',              'is_active',         'All rows',           'All rows',                'Active only (is_active=TRUE)', 'No rows'],
    ['EMPLOYEE_ROW_POLICY',     'EMPLOYEES',              'termination_date',   'All rows',           'All rows (incl. terminated)', 'Active only (term_date IS NULL)', 'No rows'],
    ['TRANSACTION_FRAUD_POLICY','FINANCIAL_TRANSACTIONS', 'fraud_flag',         'All rows',           'All rows (incl. fraud)', 'Non-fraud only (fraud_flag=FALSE)', 'Non-fraud only'],
  ];

  rowPolicies.forEach((row, i) => {
    const r = 25 + i;
    sheet.setRowHeight(r, 22);
    sheet.getRange(r, 1).setValue(row[0]).setFontWeight('bold').setFontSize(9).setFontColor('#7C3AED').setBackground('#FAF5FF');
    sheet.getRange(r, 2).setValue(row[1]).setFontSize(9).setFontColor('#334155').setBackground('#FAFAFA');
    sheet.getRange(r, 3).setValue(row[2]).setFontSize(9).setFontColor('#64748B').setBackground('#FAFAFA').setItalic(true);
    sheet.getRange(r, 4).setValue(row[3]).setFontSize(9).setFontColor(C.ALLOW_FG).setBackground('#F0FDF4');
    sheet.getRange(r, 5).setValue(row[4]).setFontSize(9).setFontColor(C.ALLOW_FG).setBackground('#F0FDF4');
    sheet.getRange(r, 6).setValue(row[5]).setFontSize(9).setFontColor(C.PARTIAL_FG).setBackground('#FFFBEB');
    sheet.getRange(r, 7).setValue(row[6]).setFontSize(9).setFontColor(C.DENY_FG).setBackground('#FEF2F2');
  });

  freezeAndResize(sheet, 3, 1, [200, 130, 160, 120, 120, 120, 260]);
}

// ═══════════════════════════════════════════════════════════════════════════════
// SHEET 3: COLUMN CLASSIFICATION MAP
// ═══════════════════════════════════════════════════════════════════════════════
function buildColumnMapSheet(ss) {
  const sheet = getOrCreate(ss, '3. Column Tags (Snowflake→Neo4j)');
  clearSheet(sheet);
  sheet.setTabColor('#EA580C');

  sectionTitle(sheet, 1, 1, 'SNOWFLAKE COLUMN TAGS  →  NEO4J COLUMN NODE PROPERTIES  (extracted via TAG_REFERENCES_ALL_COLUMNS)', 11);
  sheet.setRowHeight(1, 30);

  const note = sheet.getRange('A2:K2');
  note.merge();
  note.setValue('Each row below = one :Column node in Neo4j. The last 5 columns are Snowflake tag values stored as node properties and also as :CLASSIFIED_AS relationships.');
  note.setFontSize(9);
  note.setFontColor('#64748B');
  note.setBackground('#FAFAFA');
  note.setItalic(true);
  sheet.setRowHeight(2, 20);

  headerRow(sheet, 3,
    ['Table', 'Column', 'Data Type', 'Position', 'Nullable', 'DATA_CLASSIFICATION (tag)', 'DATA_CATEGORY (tag)', 'ENCRYPTION_REQUIRED (tag)', 'RETENTION_POLICY (tag)', 'DATA_OWNER (tag)', 'Masking Policy Applied'],
    C.HEADER_BG, C.HEADER_FG, 9
  );
  sheet.setRowHeight(3, 30);

  const columns = [
    // CUSTOMERS
    ['CUSTOMERS','customer_id','NUMBER',1,'N','Internal','—','No','—','—','—'],
    ['CUSTOMERS','first_name','VARCHAR',2,'N','PII','Personal','No','—','Customer Success Team','—'],
    ['CUSTOMERS','last_name','VARCHAR',3,'N','PII','Personal','No','—','Customer Success Team','—'],
    ['CUSTOMERS','email','VARCHAR',4,'N','PII','Personal','Yes','—','Customer Success Team','EMAIL_MASKING_POLICY'],
    ['CUSTOMERS','phone','VARCHAR',5,'Y','PII','Personal','No','—','—','—'],
    ['CUSTOMERS','date_of_birth','DATE',6,'Y','PII','Personal','Yes','—','—','—'],
    ['CUSTOMERS','address_line1','VARCHAR',7,'Y','PII','Personal','No','—','—','—'],
    ['CUSTOMERS','address_line2','VARCHAR',8,'Y','PII','Personal','No','—','—','—'],
    ['CUSTOMERS','ssn','VARCHAR',9,'Y','Restricted','Personal','Yes','7_years','Compliance Team','SSN_MASKING_POLICY'],
    ['CUSTOMERS','city','VARCHAR',10,'Y','Internal','—','No','—','—','—'],
    ['CUSTOMERS','state','VARCHAR',11,'Y','Internal','—','No','—','—','—'],
    ['CUSTOMERS','zip_code','VARCHAR',12,'Y','Internal','—','No','—','—','—'],
    ['CUSTOMERS','country','VARCHAR',13,'Y','Public','—','No','—','—','—'],
    ['CUSTOMERS','customer_tier','VARCHAR',14,'Y','Internal','—','No','—','—','—'],
    ['CUSTOMERS','created_at','TIMESTAMP',15,'Y','Internal','—','No','—','—','—'],
    ['CUSTOMERS','is_active','BOOLEAN',16,'Y','Internal','—','No','—','—','—'],
    // EMPLOYEES
    ['EMPLOYEES','employee_id','NUMBER',1,'N','Internal','—','No','—','—','—'],
    ['EMPLOYEES','employee_number','VARCHAR',2,'Y','Internal','—','No','—','—','—'],
    ['EMPLOYEES','first_name','VARCHAR',3,'N','PII','Personal','No','—','HR Team','—'],
    ['EMPLOYEES','last_name','VARCHAR',4,'N','PII','Personal','No','—','HR Team','—'],
    ['EMPLOYEES','email','VARCHAR',5,'N','PII','Personal','No','—','HR Team','EMAIL_MASKING_POLICY'],
    ['EMPLOYEES','personal_email','VARCHAR',6,'Y','PII','Personal','No','—','—','EMAIL_MASKING_POLICY'],
    ['EMPLOYEES','phone','VARCHAR',7,'Y','PII','Personal','No','—','—','—'],
    ['EMPLOYEES','date_of_birth','DATE',8,'Y','PII','Personal','No','—','—','—'],
    ['EMPLOYEES','ssn','VARCHAR',9,'Y','Restricted','Personal','Yes','7_years','HR Team','SSN_MASKING_POLICY'],
    ['EMPLOYEES','salary','NUMBER',10,'Y','Restricted','Financial','Yes','7_years','Finance Team','SALARY_MASKING_POLICY'],
    ['EMPLOYEES','bonus','NUMBER',11,'Y','Restricted','Financial','No','—','Finance Team','SALARY_MASKING_POLICY'],
    ['EMPLOYEES','bank_account','VARCHAR',12,'Y','Restricted','Financial','Yes','—','Finance Team','—'],
    ['EMPLOYEES','clearance_level','VARCHAR',13,'Y','Restricted','—','Yes','—','Security Team','—'],
    ['EMPLOYEES','department','VARCHAR',14,'Y','Internal','—','No','—','—','—'],
    ['EMPLOYEES','job_title','VARCHAR',15,'Y','Internal','—','No','—','—','—'],
    ['EMPLOYEES','hire_date','DATE',16,'Y','Internal','—','No','—','—','—'],
    ['EMPLOYEES','termination_date','DATE',17,'Y','Internal','—','No','—','—','—'],
    ['EMPLOYEES','manager_id','NUMBER',18,'Y','Internal','—','No','—','—','—'],
    ['EMPLOYEES','performance_rating','VARCHAR',19,'Y','Internal','—','No','—','—','—'],
    ['EMPLOYEES','remote_work','BOOLEAN',20,'Y','Internal','—','No','—','—','—'],
    ['EMPLOYEES','location_office','VARCHAR',21,'Y','Public','—','No','—','—','—'],
    // FINANCIAL_TRANSACTIONS
    ['FINANCIAL_TRANSACTIONS','transaction_id','NUMBER',1,'N','Internal','—','No','—','—','—'],
    ['FINANCIAL_TRANSACTIONS','customer_id','NUMBER',2,'N','Internal','—','No','—','—','—'],
    ['FINANCIAL_TRANSACTIONS','transaction_date','TIMESTAMP',3,'N','Internal','—','No','—','—','—'],
    ['FINANCIAL_TRANSACTIONS','transaction_type','VARCHAR',4,'N','Internal','—','No','—','—','—'],
    ['FINANCIAL_TRANSACTIONS','transaction_amount','NUMBER',5,'N','Restricted','Financial','No','—','—','AMOUNT_MASKING_POLICY'],
    ['FINANCIAL_TRANSACTIONS','account_number','VARCHAR',6,'Y','Restricted','Financial','Yes','7_years','—','ACCOUNT_MASKING_POLICY'],
    ['FINANCIAL_TRANSACTIONS','routing_number','VARCHAR',7,'Y','Restricted','Financial','No','—','—','—'],
    ['FINANCIAL_TRANSACTIONS','card_last_four','VARCHAR',8,'Y','Restricted','Financial','No','—','—','—'],
    ['FINANCIAL_TRANSACTIONS','merchant_name','VARCHAR',9,'N','Internal','—','No','—','—','—'],
    ['FINANCIAL_TRANSACTIONS','merchant_category','VARCHAR',10,'N','Public','—','No','—','—','—'],
    ['FINANCIAL_TRANSACTIONS','currency','VARCHAR',11,'N','Public','—','No','—','—','—'],
    ['FINANCIAL_TRANSACTIONS','fraud_flag','BOOLEAN',12,'Y','Restricted','Financial','No','—','Risk & Fraud Team','—'],
    ['FINANCIAL_TRANSACTIONS','status','VARCHAR',13,'N','Internal','—','No','—','—','—'],
    ['FINANCIAL_TRANSACTIONS','ip_address','VARCHAR',14,'Y','Restricted','Personal','No','—','—','IP_MASKING_POLICY'],
    ['FINANCIAL_TRANSACTIONS','device_fingerprint','VARCHAR',15,'Y','Restricted','—','No','—','—','—'],
    // PRODUCTS
    ['PRODUCTS','product_id','NUMBER',1,'N','Internal','—','No','—','—','—'],
    ['PRODUCTS','product_sku','VARCHAR',2,'N','Public','—','No','—','—','—'],
    ['PRODUCTS','product_name','VARCHAR',3,'N','Public','—','No','—','—','—'],
    ['PRODUCTS','product_description','TEXT',4,'Y','Public','—','No','—','—','—'],
    ['PRODUCTS','category','VARCHAR',5,'N','Public','—','No','—','—','—'],
    ['PRODUCTS','subcategory','VARCHAR',6,'Y','Public','—','No','—','—','—'],
    ['PRODUCTS','unit_price','NUMBER',7,'N','Internal','—','No','—','—','—'],
    ['PRODUCTS','cost_price','NUMBER',8,'N','Restricted','Financial','No','—','Finance Team','—'],
    ['PRODUCTS','profit_margin','NUMBER',9,'Y','Restricted','Financial','No','—','Finance Team','—'],
    ['PRODUCTS','stock_quantity','NUMBER',10,'Y','Internal','—','No','—','—','—'],
    ['PRODUCTS','reorder_threshold','NUMBER',11,'Y','Internal','—','No','—','—','—'],
    ['PRODUCTS','supplier_id','NUMBER',12,'Y','Internal','—','No','—','—','—'],
    ['PRODUCTS','launch_date','DATE',13,'Y','Public','—','No','—','—','—'],
    ['PRODUCTS','is_active','BOOLEAN',14,'Y','Internal','—','No','—','—','—'],
    ['PRODUCTS','last_modified','TIMESTAMP',15,'Y','Internal','—','No','—','—','—'],
    // AUDIT_LOGS
    ['AUDIT_LOGS','log_id','NUMBER',1,'N','Internal','—','No','—','—','—'],
    ['AUDIT_LOGS','event_timestamp','TIMESTAMP',2,'N','Internal','—','No','—','—','—'],
    ['AUDIT_LOGS','user_id','NUMBER',3,'N','Internal','—','No','—','—','—'],
    ['AUDIT_LOGS','user_email','VARCHAR',4,'Y','PII','Personal','No','—','—','EMAIL_MASKING_POLICY'],
    ['AUDIT_LOGS','action_type','VARCHAR',5,'N','Internal','—','No','—','—','—'],
    ['AUDIT_LOGS','resource_accessed','VARCHAR',6,'Y','Internal','—','No','—','—','—'],
    ['AUDIT_LOGS','data_classification','VARCHAR',7,'Y','Internal','—','No','—','—','—'],
    ['AUDIT_LOGS','source_ip','VARCHAR',8,'Y','Restricted','Personal','No','1_year','Security Team','IP_MASKING_POLICY'],
    ['AUDIT_LOGS','session_id','VARCHAR',9,'Y','Restricted','—','No','1_year','—','—'],
    ['AUDIT_LOGS','user_agent','VARCHAR',10,'Y','Restricted','—','No','—','—','—'],
    ['AUDIT_LOGS','success_flag','BOOLEAN',11,'Y','Internal','—','No','—','—','—'],
    ['AUDIT_LOGS','failure_reason','VARCHAR',12,'Y','Internal','—','No','—','—','—'],
    ['AUDIT_LOGS','records_accessed','NUMBER',13,'Y','Internal','—','No','—','—','—'],
    ['AUDIT_LOGS','query_hash','VARCHAR',14,'Y','Internal','—','No','—','—','—'],
  ];

  let lastTable = '';
  let tableRowStart = 4;
  columns.forEach((row, i) => {
    const r = 4 + i;
    sheet.setRowHeight(r, 20);

    // Alternate table background
    if (row[0] !== lastTable) { lastTable = row[0]; }
    const tableBg = ['CUSTOMERS','PRODUCTS'].includes(row[0]) ? '#FAFAFA' : '#F8FAFC';

    sheet.getRange(r, 1).setValue(row[0]).setFontSize(9).setFontColor('#1E40AF').setFontWeight('bold').setBackground(tableBg);
    sheet.getRange(r, 2).setValue(row[1]).setFontSize(9).setFontFamily('Courier New').setFontColor('#1E293B').setBackground(tableBg);
    sheet.getRange(r, 3).setValue(row[2]).setFontSize(9).setFontColor('#64748B').setBackground(tableBg);
    sheet.getRange(r, 4).setValue(row[3]).setHorizontalAlignment('center').setFontSize(9).setFontColor('#64748B').setBackground(tableBg);
    sheet.getRange(r, 5).setValue(row[4]).setHorizontalAlignment('center').setFontSize(9).setFontColor('#94A3B8').setBackground(tableBg);

    // Classification chip (column 6)
    clsChip(sheet, r, 6, row[5]);

    sheet.getRange(r, 7).setValue(row[6]).setFontSize(9).setFontColor('#64748B').setBackground(tableBg).setHorizontalAlignment('center');

    // Encryption (col 8)
    const encCell = sheet.getRange(r, 8);
    encCell.setValue(row[7]);
    encCell.setHorizontalAlignment('center');
    encCell.setFontSize(9);
    if (row[7] === 'Yes') { encCell.setFontColor('#15803D'); encCell.setFontWeight('bold'); encCell.setBackground('#F0FDF4'); }
    else { encCell.setFontColor('#94A3B8'); encCell.setBackground(tableBg); }

    sheet.getRange(r, 9).setValue(row[8]).setFontSize(9).setFontColor('#64748B').setBackground(tableBg).setHorizontalAlignment('center');
    sheet.getRange(r, 10).setValue(row[9]).setFontSize(9).setFontColor('#475569').setBackground(tableBg);

    // Masking policy (col 11)
    const polCell = sheet.getRange(r, 11);
    polCell.setValue(row[10]);
    polCell.setFontSize(9);
    if (row[10] !== '—') { polCell.setFontColor('#7C3AED'); polCell.setFontWeight('bold'); polCell.setBackground('#FAF5FF'); }
    else { polCell.setFontColor('#CBD5E1'); polCell.setBackground(tableBg); }
  });

  freezeAndResize(sheet, 3, 2, [160, 160, 80, 60, 60, 110, 110, 140, 110, 160, 180]);

  // Add filter to header row
  sheet.getRange(3, 1, columns.length + 1, 11).createFilter();
}

// ═══════════════════════════════════════════════════════════════════════════════
// SHEET 4: POLICIES
// ═══════════════════════════════════════════════════════════════════════════════
function buildPoliciesSheet(ss) {
  const sheet = getOrCreate(ss, '4. Masking & Row Policies');
  clearSheet(sheet);
  sheet.setTabColor('#9333EA');

  sectionTitle(sheet, 1, 1, 'SNOWFLAKE SECURITY POLICIES  →  NEO4J :Policy NODES  (Policy -[:MASKS]-> Column relationships)', 9);
  sheet.setRowHeight(1, 30);

  // ── Masking policies ─────────────────────────────────────────────────────
  sectionTitle(sheet, 2, 1, 'COLUMN-LEVEL MASKING POLICIES  (6 policies — stored as :Policy nodes in Neo4j)', 7);
  sheet.setRowHeight(2, 26);

  headerRow(sheet, 3, ['Policy Name', 'Input Type', 'Columns Protected', 'DATA_ENGINEER / ADMIN', 'HR_MANAGER', 'DATA_ANALYST', 'FINANCE_ANALYST', 'SECURITY_AUDITOR', 'PUBLIC_USER'], C.HEADER_BG, C.HEADER_FG, 9);
  sheet.setRowHeight(3, 28);

  const maskingPolicies = [
    {
      name:'SSN_MASKING_POLICY', type:'STRING',
      columns:'CUSTOMERS.ssn\nEMPLOYEES.ssn',
      eng:'Full value\n123-45-6789',
      hr:'Last 4 only\n***-**-6789',
      ana:'Fully masked\n***-**-****',
      fin:'Last 4 only\n***-**-6789',
      sec:'Fully masked\n***-**-****',
      pub:'Fully masked\n***-**-****',
    },
    {
      name:'EMAIL_MASKING_POLICY', type:'STRING',
      columns:'CUSTOMERS.email\nEMPLOYEES.email\nEMPLOYEES.personal_email\nAUDIT_LOGS.user_email',
      eng:'Full email\njohn@example.com',
      hr:'Full email\njohn@example.com',
      ana:'Domain only\n***@example.com',
      fin:'Domain only\n***@example.com',
      sec:'Domain only\n***@example.com',
      pub:'Fully masked\n***@***.***',
    },
    {
      name:'AMOUNT_MASKING_POLICY', type:'NUMBER',
      columns:'FINANCIAL_TRANSACTIONS\n.transaction_amount',
      eng:'Exact value\n$1,234.56',
      hr:'Blocked (-1)',
      ana:'Rounded to $100\n$1,200.00',
      fin:'Exact value\n$1,234.56',
      sec:'Blocked (-1)',
      pub:'Blocked (-1)',
    },
    {
      name:'ACCOUNT_MASKING_POLICY', type:'STRING',
      columns:'FINANCIAL_TRANSACTIONS\n.account_number',
      eng:'Full number\n4532-1234-5678-9012',
      hr:'Fully masked\n****-****-****-****',
      ana:'Fully masked\n****-****-****-****',
      fin:'Last 4 only\n****-****-****-9012',
      sec:'Fully masked\n****-****-****-****',
      pub:'Fully masked\n****-****-****-****',
    },
    {
      name:'IP_MASKING_POLICY', type:'STRING',
      columns:'FINANCIAL_TRANSACTIONS\n.ip_address\nAUDIT_LOGS.source_ip',
      eng:'Full IP\n192.168.1.100',
      hr:'Masked\n***.***.***.***',
      ana:'Masked\n***.***.***.***',
      fin:'Masked\n***.***.***.***',
      sec:'Full IP\n192.168.1.100',
      pub:'Masked\n***.***.***.***',
    },
    {
      name:'SALARY_MASKING_POLICY', type:'NUMBER',
      columns:'EMPLOYEES.salary\nEMPLOYEES.bonus',
      eng:'Exact value\n$145,000',
      hr:'Exact value\n$145,000',
      ana:'Blocked (-1)',
      fin:'Exact value\n$145,000',
      sec:'Blocked (-1)',
      pub:'Blocked (-1)',
    },
  ];

  const maskBgAllow = '#F0FDF4', maskBgPartial = '#FFFBEB', maskBgDeny = '#FEF2F2';
  maskingPolicies.forEach((p, i) => {
    const r = 4 + i;
    sheet.setRowHeight(r, 42);

    sheet.getRange(r, 1).setValue(p.name).setFontWeight('bold').setFontSize(9).setFontColor('#7C3AED').setBackground('#FAF5FF').setWrap(true);
    sheet.getRange(r, 2).setValue(p.type).setFontSize(9).setFontColor('#64748B').setHorizontalAlignment('center');
    sheet.getRange(r, 3).setValue(p.columns).setFontSize(8.5).setFontFamily('Courier New').setFontColor('#1E40AF').setBackground('#EFF6FF').setWrap(true);

    const vals = [p.eng, p.hr, p.ana, p.fin, p.sec, p.pub];
    const bgs = [maskBgAllow, maskBgPartial, maskBgDeny, maskBgAllow, maskBgPartial, maskBgDeny];
    const fgs = [C.ALLOW_FG, C.PARTIAL_FG, C.DENY_FG, C.ALLOW_FG, C.PARTIAL_FG, C.DENY_FG];
    vals.forEach((val, j) => {
      const cell = sheet.getRange(r, 4 + j);
      // Override bg/fg based on actual access
      let bg = bgs[j], fg = fgs[j];
      if (val.startsWith('Full') || val.startsWith('Exact')) { bg = maskBgAllow; fg = C.ALLOW_FG; }
      else if (val.startsWith('Last') || val.startsWith('Domain') || val.startsWith('Round')) { bg = maskBgPartial; fg = C.PARTIAL_FG; }
      else { bg = maskBgDeny; fg = C.DENY_FG; }
      cell.setValue(val).setFontSize(8.5).setBackground(bg).setFontColor(fg).setWrap(true);
    });
  });

  // ── Cypher pattern ───────────────────────────────────────────────────────
  sheet.setRowHeight(11, 16);
  sectionTitle(sheet, 12, 1, 'NEO4J CYPHER PATTERN — Querying policy coverage', 7);
  sheet.setRowHeight(12, 26);

  const cypherBlock = [
    ['// Find all columns protected by masking policies'],
    ['MATCH (p:Policy)-[:MASKS]->(c:Column)-[:CLASSIFIED_AS]->(cl:Classification)'],
    ['RETURN p.name AS policy, p.type AS policy_type,'],
    ['       c.table_name + \'.\' + c.name AS column,'],
    ['       cl.name AS classification'],
    ['ORDER BY p.name'],
  ];
  cypherBlock.forEach((row, i) => {
    const r = 13 + i;
    sheet.setRowHeight(r, 20);
    const cell = sheet.getRange(r, 1, 1, 9);
    cell.merge();
    cell.setValue(row[0]);
    cell.setFontFamily('Courier New');
    cell.setFontSize(9.5);
    cell.setBackground('#1E293B');
    cell.setFontColor(row[0].startsWith('//') ? '#64748B' : row[0].startsWith('MATCH') || row[0].startsWith('RETURN') || row[0].startsWith('ORDER') ? '#7DD3FC' : '#A5F3FC');
  });

  freezeAndResize(sheet, 3, 1, [210, 70, 220, 130, 130, 130, 130, 130, 130]);
}

// ═══════════════════════════════════════════════════════════════════════════════
// SHEET 5: NODE ACCESS SIMULATION
// ═══════════════════════════════════════════════════════════════════════════════
function buildNodeAccessSheet(ss) {
  const sheet = getOrCreate(ss, '5. Node Access Simulation');
  clearSheet(sheet);
  sheet.setTabColor('#0891B2');

  sectionTitle(sheet, 1, 1, 'NODE-LEVEL ACCESS SIMULATION  —  "What data nodes can each role see?"  (MATCH (r:Role)-[:CAN_ACCESS]->(cl:Classification)<-[:HAS_CLASSIFICATION]-(n))', 11);
  sheet.setRowHeight(1, 30);

  headerRow(sheet, 2, ['Node Type', 'Classification', 'Total Nodes', 'DATA_GOVERNANCE_ADMIN', 'DATA_ENGINEER', 'FINANCE_ANALYST', 'HR_MANAGER', 'SECURITY_AUDITOR', 'DATA_ANALYST', 'PUBLIC_USER'], C.HEADER_BG, C.HEADER_FG, 9);
  sheet.setRowHeight(2, 28);

  const nodes = [
    {type:'Customer',    cls:'Restricted', total:10, gov:10, eng:10, fin:10, hr:0,  sec:0, ana:0, pub:0},
    {type:'Employee',    cls:'Restricted', total:10, gov:10, eng:10, fin:0,  hr:0,  sec:0, ana:0, pub:0},
    {type:'Product',     cls:'Restricted', total:10, gov:10, eng:10, fin:10, hr:0,  sec:0, ana:0, pub:0},
    {type:'Transaction', cls:'Restricted', total:9,  gov:9,  eng:9,  fin:9,  hr:0,  sec:0, ana:0, pub:0},
    {type:'AuditLog',    cls:'Internal',   total:10, gov:10, eng:10, fin:10, hr:10, sec:10,ana:10,pub:0},
  ];

  nodes.forEach((node, i) => {
    const r = 3 + i;
    sheet.setRowHeight(r, 24);
    sheet.getRange(r, 1).setValue(node.type).setFontWeight('bold').setFontSize(10).setFontColor('#1E293B').setBackground('#F8FAFC');
    clsChip(sheet, r, 2, node.cls);
    sheet.getRange(r, 3).setValue(node.total).setHorizontalAlignment('center').setFontWeight('bold').setFontColor('#334155').setBackground('#F8FAFC');

    const counts = [node.gov, node.eng, node.fin, node.hr, node.sec, node.ana, node.pub];
    counts.forEach((count, j) => {
      const cell = sheet.getRange(r, 4 + j);
      if (count === node.total) {
        cell.setValue(count + '  (all)').setFontColor(C.ALLOW_FG).setBackground('#F0FDF4').setFontWeight('bold');
      } else if (count === 0) {
        cell.setValue('0  (blocked)').setFontColor(C.DENY_FG).setBackground('#FEF2F2');
      } else {
        cell.setValue(count + '  (partial)').setFontColor(C.PARTIAL_FG).setBackground('#FFFBEB');
      }
      cell.setFontSize(9).setHorizontalAlignment('center');
    });
  });

  // ── Column access simulation for EMPLOYEES ───────────────────────────────
  sheet.setRowHeight(9, 14);
  sectionTitle(sheet, 10, 1, 'COLUMN-LEVEL ACCESS — EMPLOYEES TABLE  (showing what each role sees per column)', 10);
  sheet.setRowHeight(10, 26);

  headerRow(sheet, 11, ['Column', 'Classification', 'Masking Policy', 'DATA_ENGINEER', 'HR_MANAGER', 'FINANCE_ANALYST', 'DATA_ANALYST', 'PUBLIC_USER'], C.HEADER_BG, C.HEADER_FG, 9);
  sheet.setRowHeight(11, 28);

  const empCols = [
    {col:'employee_id',      cls:'Internal',   pol:'—',                    eng:'ID value', hr:'ID value',    fin:'ID value',    ana:'ID value',    pub:'BLOCKED'},
    {col:'first_name',       cls:'PII',         pol:'—',                    eng:'John',     hr:'John',        fin:'BLOCKED',     ana:'BLOCKED',     pub:'BLOCKED'},
    {col:'last_name',        cls:'PII',         pol:'—',                    eng:'Smith',    hr:'Smith',       fin:'BLOCKED',     ana:'BLOCKED',     pub:'BLOCKED'},
    {col:'email',            cls:'PII',         pol:'EMAIL_MASKING_POLICY', eng:'j@co.com', hr:'j@co.com',    fin:'***@co.com',  ana:'BLOCKED',     pub:'BLOCKED'},
    {col:'department',       cls:'Internal',    pol:'—',                    eng:'Finance',  hr:'Finance',     fin:'Finance',     ana:'Finance',     pub:'BLOCKED'},
    {col:'job_title',        cls:'Internal',    pol:'—',                    eng:'Analyst',  hr:'Analyst',     fin:'Analyst',     ana:'Analyst',     pub:'BLOCKED'},
    {col:'ssn',              cls:'Restricted',  pol:'SSN_MASKING_POLICY',   eng:'111-22-3', hr:'***-**-3333', fin:'BLOCKED',     ana:'BLOCKED',     pub:'BLOCKED'},
    {col:'salary',           cls:'Restricted',  pol:'SALARY_MASKING_POLICY',eng:'$145,000', hr:'$145,000',    fin:'$145,000',    ana:'BLOCKED',     pub:'BLOCKED'},
    {col:'bank_account',     cls:'Restricted',  pol:'—',                    eng:'visible',  hr:'BLOCKED',     fin:'BLOCKED',     ana:'BLOCKED',     pub:'BLOCKED'},
    {col:'clearance_level',  cls:'Restricted',  pol:'—',                    eng:'Level 3',  hr:'BLOCKED',     fin:'BLOCKED',     ana:'BLOCKED',     pub:'BLOCKED'},
    {col:'location_office',  cls:'Public',      pol:'—',                    eng:'SF HQ',    hr:'SF HQ',       fin:'SF HQ',       ana:'SF HQ',       pub:'SF HQ'},
  ];

  empCols.forEach((col, i) => {
    const r = 12 + i;
    sheet.setRowHeight(r, 22);
    sheet.getRange(r, 1).setValue(col.col).setFontFamily('Courier New').setFontSize(9).setFontColor('#1E293B').setBackground('#F8FAFC');
    clsChip(sheet, r, 2, col.cls);
    const polCell = sheet.getRange(r, 3);
    polCell.setValue(col.pol).setFontSize(8.5);
    if (col.pol !== '—') { polCell.setFontColor('#7C3AED').setBackground('#FAF5FF').setFontWeight('bold'); }
    else { polCell.setFontColor('#CBD5E1').setBackground('#F8FAFC'); }

    const vals = [col.eng, col.hr, col.fin, col.ana, col.pub];
    vals.forEach((val, j) => {
      const cell = sheet.getRange(r, 4 + j);
      cell.setValue(val).setFontSize(9).setHorizontalAlignment('center');
      if (val === 'BLOCKED') { cell.setFontColor(C.DENY_FG).setBackground('#FEF2F2').setFontWeight('bold'); }
      else if (val.startsWith('***')) { cell.setFontColor(C.PARTIAL_FG).setBackground('#FFFBEB'); }
      else { cell.setFontColor(C.ALLOW_FG).setBackground('#F0FDF4'); }
    });
  });

  freezeAndResize(sheet, 2, 1, [160, 110, 200, 120, 120, 120, 120, 120]);
}

// ═══════════════════════════════════════════════════════════════════════════════
// SHEET 6: CYPHER QUERIES
// ═══════════════════════════════════════════════════════════════════════════════
function buildCypherSheet(ss) {
  const sheet = getOrCreate(ss, '6. Demo Cypher Queries');
  clearSheet(sheet);
  sheet.setTabColor('#15803D');

  const titleCell = sheet.getRange('A1:D1');
  titleCell.merge();
  titleCell.setValue('DEMO CYPHER QUERIES FOR NEO4J BROWSER');
  titleCell.setBackground(C.HEADER_BG);
  titleCell.setFontColor('#38BDF8');
  titleCell.setFontWeight('bold');
  titleCell.setFontSize(13);
  titleCell.setHorizontalAlignment('center');
  sheet.setRowHeight(1, 36);

  const noteCell = sheet.getRange('A2:D2');
  noteCell.merge();
  noteCell.setValue('Copy any query block into Neo4j Browser (https://browser.neo4j.io or your Aura console) and run it. Switch to Graph view for visual queries.');
  noteCell.setBackground(C.SUBHEADER_BG);
  noteCell.setFontColor(C.SUBHEADER_FG);
  noteCell.setFontSize(9);
  noteCell.setHorizontalAlignment('center');
  sheet.setRowHeight(2, 22);

  headerRow(sheet, 3, ['#', 'Section', 'Description', 'Cypher Query (copy into Neo4j Browser)'], C.HEADER_BG, C.HEADER_FG, 10);
  sheet.setRowHeight(3, 28);

  const queries = [
    {n:'1a', section:'Schema Structure', desc:'Full catalog: Database → Schema → Table → Column (use Graph view)',
     cypher:`MATCH path = (db:Database)-[:CONTAINS_SCHEMA]->(s:Schema)-[:CONTAINS_TABLE]->(t:Table)-[:HAS_COLUMN]->(c:Column)\nRETURN path LIMIT 80`},
    {n:'1b', section:'Schema Structure', desc:'Table inventory with column counts',
     cypher:`MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)\nRETURN t.name AS table, count(c) AS columns\nORDER BY table`},
    {n:'2a', section:'Tag Propagation', desc:'All columns with Snowflake tag metadata as node properties',
     cypher:`MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)\nRETURN t.name AS table, c.name AS column,\n       c.data_classification, c.data_category,\n       c.encryption_required, c.retention_policy, c.data_owner\nORDER BY table, c.ordinal_position`},
    {n:'2b', section:'Tag Propagation', desc:'Column → Classification CLASSIFIED_AS graph (Graph view recommended)',
     cypher:`MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)-[:CLASSIFIED_AS]->(cl:Classification)\nRETURN t, c, cl LIMIT 60`},
    {n:'2c', section:'Tag Propagation', desc:'Tag propagation proof: column count per tier',
     cypher:`MATCH (c:Column)-[:CLASSIFIED_AS]->(cl:Classification)\nRETURN cl.name AS classification,\n       cl.sensitivity_rank AS rank,\n       count(c) AS column_count\nORDER BY rank DESC`},
    {n:'3a', section:'Sensitive Column Audit', desc:'All Restricted columns with owner and encryption status',
     cypher:`MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)-[:CLASSIFIED_AS]->(cl:Classification {name: 'Restricted'})\nRETURN t.name AS table, c.name AS column,\n       c.data_owner AS owner, c.encryption_required AS encrypted\nORDER BY table, column`},
    {n:'3b', section:'Sensitive Column Audit', desc:'PII column count per table',
     cypher:`MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)-[:CLASSIFIED_AS]->(cl:Classification {name: 'PII'})\nRETURN t.name AS table, collect(c.name) AS pii_columns, count(c) AS count\nORDER BY count DESC`},
    {n:'3c', section:'Compliance Audit', desc:'COMPLIANCE RISK: sensitive columns not flagged for encryption',
     cypher:`MATCH (c:Column)-[:CLASSIFIED_AS]->(cl:Classification)\nWHERE cl.name IN ['Restricted','PII']\n  AND (c.encryption_required IS NULL OR c.encryption_required = 'No')\nRETURN c.table_name AS table, c.name AS column,\n       cl.name AS classification,\n       'COMPLIANCE RISK: encryption not enforced' AS warning`},
    {n:'3d', section:'Data Ownership', desc:'Data ownership accountability map',
     cypher:`MATCH (c:Column)\nWHERE c.data_owner IS NOT NULL\nRETURN c.data_owner AS owner_team, count(c) AS columns,\n       collect(c.table_name + '.' + c.name) AS owned_columns\nORDER BY columns DESC`},
    {n:'4a', section:'Masking Policies', desc:'Policy → Column masking graph (use Graph view)',
     cypher:`MATCH (p:Policy)-[:MASKS]->(c:Column)\nRETURN p, c LIMIT 40`},
    {n:'4b', section:'Masking Policies', desc:'Policy masking table view',
     cypher:`MATCH (p:Policy)-[:MASKS]->(c:Column)\nRETURN p.name AS policy, p.type AS type,\n       c.table_name AS table, c.name AS column,\n       c.data_classification AS classification\nORDER BY table, column`},
    {n:'5a', section:'Role Hierarchy', desc:'Role hierarchy graph (GRAPH view — shows INHERITS_FROM chain)',
     cypher:`MATCH (r:Role)\nOPTIONAL MATCH (r)-[:INHERITS_FROM]->(parent:Role)\nRETURN r, parent`},
    {n:'5b', section:'Role Permissions', desc:'What classifications can each role access?',
     cypher:`MATCH (r:Role)-[:CAN_ACCESS]->(cl:Classification)\nRETURN r.name AS role, r.hierarchy_level AS level,\n       collect(cl.name) AS can_access\nORDER BY level`},
    {n:'5c', section:'Role Permissions', desc:'Who has access to Restricted data?',
     cypher:`MATCH (r:Role)-[:CAN_ACCESS]->(cl:Classification {name: 'Restricted'})\nRETURN r.name AS role, r.description AS description, r.hierarchy_level AS level\nORDER BY level`},
    {n:'6a', section:'Access Simulation', desc:'What can DATA_ANALYST see at node level?',
     cypher:`WITH 'DATA_ANALYST' AS role\nMATCH (r:Role {name: role})-[:CAN_ACCESS]->(cl:Classification)<-[:HAS_CLASSIFICATION]-(n)\nRETURN labels(n)[0] AS node_type, count(n) AS accessible, collect(DISTINCT cl.name) AS via\nORDER BY node_type`},
    {n:'6b', section:'Access Simulation', desc:'HR_MANAGER column-level visibility on EMPLOYEES',
     cypher:`WITH 'HR_MANAGER' AS role\nMATCH (r:Role {name: role})-[:CAN_ACCESS]->(cl:Classification)<-[:CLASSIFIED_AS]-(c:Column {table_name: 'EMPLOYEES'})\nWITH role, collect(c.name) AS visible\nMATCH (col:Column {table_name: 'EMPLOYEES'})\nRETURN col.name AS column, col.data_classification AS cls,\n       CASE WHEN col.name IN visible THEN 'VISIBLE' ELSE 'MASKED' END AS access\nORDER BY col.ordinal_position`},
    {n:'6c', section:'Access Simulation', desc:'ACCESS DENIED simulation: DATA_ANALYST tries Restricted',
     cypher:`WITH 'DATA_ANALYST' AS role, 'Restricted' AS want\nMATCH (r:Role {name: role})\nOPTIONAL MATCH (r)-[:CAN_ACCESS]->(cl:Classification {name: want})\nRETURN role, want,\n  CASE WHEN cl IS NULL THEN 'ACCESS DENIED' ELSE 'ACCESS GRANTED' END AS decision`},
    {n:'7a', section:'Full Audit', desc:'End-to-end: policies protecting columns that FINANCE_ANALYST can access',
     cypher:`MATCH (r:Role {name: 'FINANCE_ANALYST'})-[:CAN_ACCESS]->(cl:Classification)\n      <-[:CLASSIFIED_AS]-(c:Column)<-[:MASKS]-(p:Policy)\nRETURN r.name AS role, cl.name AS classification,\n       c.table_name AS table, c.name AS column, p.name AS policy\nORDER BY table, column`},
    {n:'7b', section:'Full Audit', desc:'Sensitive data exposure map: all Restricted/PII nodes + who can see them',
     cypher:`MATCH (cl:Classification)<-[:HAS_CLASSIFICATION]-(n)\nWHERE cl.name IN ['Restricted','PII']\nWITH cl, labels(n)[0] AS type, count(n) AS count\nMATCH (r:Role)-[:CAN_ACCESS]->(cl)\nRETURN cl.name AS classification, type, count, collect(r.name) AS roles_with_access\nORDER BY cl.name, type`},
    {n:'7c', section:'Full Graph', desc:'MONEY SHOT: Full security graph in one visual (Graph view)',
     cypher:`MATCH (r:Role)-[:CAN_ACCESS]->(cl:Classification)\nMATCH (cl)<-[:CLASSIFIED_AS]-(c:Column)\nOPTIONAL MATCH (p:Policy)-[:MASKS]->(c)\nRETURN r, cl, c, p LIMIT 50`},
  ];

  queries.forEach((q, i) => {
    const r = 4 + i;
    sheet.setRowHeight(r, 54);
    const isHighlight = ['2c','3c','6c','7c'].includes(q.n);
    const rowBg = isHighlight ? '#FFFBEB' : (i % 2 === 0 ? '#FAFAFA' : '#FFFFFF');

    sheet.getRange(r, 1).setValue(q.n).setHorizontalAlignment('center').setFontWeight('bold')
      .setFontColor(isHighlight ? '#92400E' : '#64748B').setBackground(rowBg).setFontSize(10);
    sheet.getRange(r, 2).setValue(q.section).setFontWeight('bold').setFontSize(9)
      .setFontColor('#1E40AF').setBackground(rowBg);
    sheet.getRange(r, 3).setValue(q.desc).setFontSize(9).setFontColor('#374151')
      .setBackground(rowBg).setWrap(true);
    sheet.getRange(r, 4).setValue(q.cypher).setFontFamily('Courier New').setFontSize(8.5)
      .setFontColor('#1E3A5F').setBackground('#F0F7FF').setWrap(true).setVerticalAlignment('top');
  });

  freezeAndResize(sheet, 3, 2, [30, 160, 260, 600]);
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN ENTRY POINT
// ═══════════════════════════════════════════════════════════════════════════════
function buildSecurityDemoSheet() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  // Delete any existing demo sheets to start clean
  ['1. Overview','2. Access Matrix','3. Column Tags (Snowflake→Neo4j)','4. Masking & Row Policies','5. Node Access Simulation','6. Demo Cypher Queries'].forEach(name => {
    const existing = ss.getSheetByName(name);
    if (existing) ss.deleteSheet(existing);
  });

  // Build each sheet
  SpreadsheetApp.getActiveSpreadsheet().toast('Building sheet 1/6: Overview...', 'Progress', 3);
  buildOverviewSheet(ss);

  SpreadsheetApp.getActiveSpreadsheet().toast('Building sheet 2/6: Access Matrix...', 'Progress', 3);
  buildAccessMatrixSheet(ss);

  SpreadsheetApp.getActiveSpreadsheet().toast('Building sheet 3/6: Column Tags...', 'Progress', 3);
  buildColumnMapSheet(ss);

  SpreadsheetApp.getActiveSpreadsheet().toast('Building sheet 4/6: Policies...', 'Progress', 3);
  buildPoliciesSheet(ss);

  SpreadsheetApp.getActiveSpreadsheet().toast('Building sheet 5/6: Node Access Simulation...', 'Progress', 3);
  buildNodeAccessSheet(ss);

  SpreadsheetApp.getActiveSpreadsheet().toast('Building sheet 6/6: Cypher Queries...', 'Progress', 3);
  buildCypherSheet(ss);

  // Activate the first sheet
  ss.getSheetByName('1. Overview').activate();

  SpreadsheetApp.getActiveSpreadsheet().toast('Done! All 6 sheets created.', 'Complete', 5);
  SpreadsheetApp.getUi().alert('✓ Security Demo spreadsheet built successfully!\n\n6 sheets created:\n  1. Overview\n  2. Access Matrix\n  3. Column Tags (Snowflake→Neo4j)\n  4. Masking & Row Policies\n  5. Node Access Simulation\n  6. Demo Cypher Queries');
}
