/*************************************************
 * DRIVE TOOLS v7
 * Google Sheets + Apps Script
 * Requires Advanced Service: Drive API
 *
 * Architecture:
 * 1) Main source of truth = Drive Inventory
 * 2) Inventory scan is resumable
 * 3) Paths enrichment is separate and resumable
 * 4) Permissions enrichment is separate and resumable
 * 5) Count is derived from Drive Inventory
 * 6) STATE sheet stores durable service state
 * 7) LOG sheet is diagnostic
 *************************************************/

const DT = {
  VERSION: 'v7',

  MENU: 'Drive Tools',

  SHEET_README: 'README',
  SHEET_PROGRESS: 'Progress',
  SHEET_LOG: 'LOG',
  SHEET_STATE: 'STATE',

  SHEET_INVENTORY: 'Drive Inventory',
  SHEET_PERMISSIONS: 'Permissions',
  SHEET_COUNT: 'Drive Count',
  SHEET_DUPLICATES: 'Duplicate Files',
  SHEET_LARGEST: 'Largest Files',

  DATA_HEADER_ROW: 5,
  DATA_START_ROW: 6,

  INVENTORY_BATCH_SIZE: 200,
  PATHS_BATCH_ROWS: 150,
  PERMISSIONS_BATCH_ROWS: 100,

  MAX_RUNTIME_MS: 240000,
  FLUSH_SLEEP_MS: 120,
  API_SLEEP_MS: 100,
  RETRY_BASE_MS: 700,
  RETRY_MAX_ATTEMPTS: 4,

  AUTO_TRIGGER_HANDLER: 'resumePendingTasks',
  AUTO_TRIGGER_INTERVAL_MIN: 1,

  // STATE keys
  KEY_ACTIVE_JOB: 'ACTIVE_JOB',

  // Inventory state
  KEY_INV_STATUS: 'INV_STATUS',
  KEY_INV_PHASE: 'INV_PHASE',
  KEY_INV_TOKEN: 'INV_TOKEN',
  KEY_INV_PROCESSED: 'INV_PROCESSED',
  KEY_INV_STARTED_AT: 'INV_STARTED_AT',
  KEY_INV_DRIVE_INDEX: 'INV_DRIVE_INDEX',
  KEY_INV_DRIVE_ID: 'INV_DRIVE_ID',
  KEY_INV_DRIVE_NAME: 'INV_DRIVE_NAME',

  // Paths state
  KEY_PATH_STATUS: 'PATH_STATUS',
  KEY_PATH_ROW: 'PATH_ROW',
  KEY_PATH_PROCESSED: 'PATH_PROCESSED',
  KEY_PATH_STARTED_AT: 'PATH_STARTED_AT',

  // Permissions state
  KEY_PERM_STATUS: 'PERM_STATUS',
  KEY_PERM_ROW: 'PERM_ROW',
  KEY_PERM_PROCESSED: 'PERM_PROCESSED',
  KEY_PERM_STARTED_AT: 'PERM_STARTED_AT'
};

const INVENTORY_HEADERS = [
  'File ID',
  'File Name',
  'Link',
  'Mime Type',
  'Extension',
  'Size (bytes)',
  'Size (MB)',
  'Created Time',
  'Modified Time',
  'Owner Email',
  'Owner Name',
  'Is Folder',
  'Drive Type',
  'Shared Drive Name',
  'Shared Drive ID',
  'Shared With Me',
  'Trashed',
  'Starred',
  'Parent IDs',
  'Source Scope',

  // path & permissions summary enrichment
  'Full Path',
  'Path Status',
  'Permission Summary',
  'Anyone Access',
  'Domain Access',
  'Direct Users Count',
  'Direct Groups Count',
  'Permissions Status'
];

const PERMISSIONS_HEADERS = [
  'File ID',
  'File Name',
  'Permission ID',
  'Type',
  'Role',
  'Email Address',
  'Display Name',
  'Domain',
  'Allow File Discovery',
  'Deleted',
  'Inherited',
  'Inherited From',
  'Permission Type',
  'Source',
  'Fetched At'
];

const LOG_HEADERS = [
  'Timestamp',
  'Operation',
  'Level',
  'Phase',
  'Message',
  'Processed',
  'Checkpoint',
  'Duration ms',
  'Run Source'
];

const STATE_HEADERS = ['Key', 'Value', 'Updated At'];

/**********************
 * MENU
 **********************/

function onOpen() {
  initDriveTools_();

  SpreadsheetApp.getUi()
    .createMenu(DT.MENU)
    .addItem('Scan Drive Inventory', 'scanDriveInventory')
    .addItem('Build Full Paths', 'buildFullPaths')
    .addItem('Fetch Permissions', 'fetchPermissions')
    .addItem('Build Count from Inventory', 'buildCountFromInventory')
    .addItem('Find Largest Files', 'findLargestFiles')
    .addItem('Find Duplicate Files', 'findDuplicateFiles')
    .addItem('Show Progress', 'showScanProgress')
    .addItem('Enable Auto Resume', 'enableAutoResume')
    .addItem('Disable Auto Resume', 'disableAutoResume')
    .addItem('Reset Checkpoints', 'resetAllCheckpoints')
    .addItem('Clear Log', 'clearLog')
    .addToUi();
}

/**********************
 * INIT
 **********************/

function initDriveTools_() {
  ensureReadmeSheet_();
  ensureProgressSheet_();
  ensureLogSheet_();
  ensureStateSheet_();
  ensureInventorySheet_();
  ensurePermissionsSheet_();
  ensureCountSheet_();
  ensureLargestSheet_();
  ensureDuplicatesSheet_();
}

function ensureReadmeSheet_() {
  const sheet = getOrCreateSheet_(DT.SHEET_README);
  sheet.clear();

  sheet.getRange('A1:B2').setValues([
    ['Report', 'README'],
    ['Generated at', [new Date()]]
  ]);
  formatDateCell_(sheet.getRange('B2'));

  sheet.getRange('A4:B14').setValues([
    ['Menu item', 'Description'],
    ['Scan Drive Inventory', 'Starts or continues the main resumable scan of My Drive, Shared Drives, and Shared with me.'],
    ['Build Full Paths', 'Builds full paths from parent relationships for rows already present in Drive Inventory.'],
    ['Fetch Permissions', 'Fetches detailed permissions into Permissions sheet and writes permission summary into Drive Inventory.'],
    ['Build Count from Inventory', 'Builds totals from the current Drive Inventory sheet without a separate heavy API scan.'],
    ['Find Largest Files', 'Builds a top-100 largest files report from Drive Inventory.'],
    ['Find Duplicate Files', 'Builds a possible duplicates report from Drive Inventory using name + size + mimeType.'],
    ['Show Progress', 'Opens the Progress sheet with current operation, phase, counters, timestamps, and checkpoint.'],
    ['Enable Auto Resume', 'Creates a minute trigger that resumes paused Inventory, Paths, and Permissions stages automatically.'],
    ['Disable Auto Resume', 'Removes the auto-resume trigger.'],
    ['Reset Checkpoints', 'Clears saved checkpoints for Inventory, Paths, and Permissions.'],
    ['Clear Log', 'Clears the LOG sheet and starts a fresh log.']
  ]);

  formatHeaderRow_(sheet, 4, 2);
  sheet.autoResizeColumns(1, 2);
}

function ensureProgressSheet_() {
  const sheet = getOrCreateSheet_(DT.SHEET_PROGRESS);

  if (sheet.getLastRow() === 0) {
    writeProgressSheet_({
      operation: '',
      status: 'NOT STARTED',
      phase: '',
      processed: 0,
      checkpoint: '',
      startedAt: '',
      lastUpdate: new Date(),
      finishedAt: '',
      note: ''
    });
  }
}

function ensureLogSheet_() {
  const sheet = getOrCreateSheet_(DT.SHEET_LOG);
  if (sheet.getLastRow() === 0) {
    rebuildLogSheet_();
  }
}

function ensureStateSheet_() {
  const sheet = getOrCreateSheet_(DT.SHEET_STATE);
  if (sheet.getLastRow() === 0) {
    sheet.getRange('A1:B2').setValues([
      ['Report', 'STATE'],
      ['Generated at', [new Date()]]
    ]);
    formatDateCell_(sheet.getRange('B2'));

    sheet.getRange(DT.DATA_HEADER_ROW, 1, 1, STATE_HEADERS.length).setValues([STATE_HEADERS]);
    formatHeaderRow_(sheet, DT.DATA_HEADER_ROW, STATE_HEADERS.length);
    sheet.setFrozenRows(DT.DATA_HEADER_ROW);
    sheet.autoResizeColumns(1, STATE_HEADERS.length);
  }
}

function ensureInventorySheet_() {
  const sheet = getOrCreateSheet_(DT.SHEET_INVENTORY);
  if (sheet.getLastRow() === 0) {
    rebuildInventorySheet_();
  }
}

function ensurePermissionsSheet_() {
  const sheet = getOrCreateSheet_(DT.SHEET_PERMISSIONS);
  if (sheet.getLastRow() === 0) {
    rebuildPermissionsSheet_();
  }
}

function ensureCountSheet_() {
  const sheet = getOrCreateSheet_(DT.SHEET_COUNT);
  if (sheet.getLastRow() === 0) {
    rebuildCountSheet_();
  }
}

function ensureLargestSheet_() {
  const sheet = getOrCreateSheet_(DT.SHEET_LARGEST);
  if (sheet.getLastRow() === 0) {
    sheet.getRange('A1:B2').setValues([
      ['Report', 'Largest Files'],
      ['Generated at', [new Date()]]
    ]);
    formatDateCell_(sheet.getRange('B2'));
  }
}

function ensureDuplicatesSheet_() {
  const sheet = getOrCreateSheet_(DT.SHEET_DUPLICATES);
  if (sheet.getLastRow() === 0) {
    sheet.getRange('A1:B2').setValues([
      ['Report', 'Duplicate Files'],
      ['Generated at', [new Date()]]
    ]);
    formatDateCell_(sheet.getRange('B2'));
  }
}

function rebuildLogSheet_() {
  const sheet = getOrCreateSheet_(DT.SHEET_LOG);
  sheet.clear();

  sheet.getRange('A1:B3').setValues([
    ['Report', 'LOG'],
    ['Generated at', [new Date()]],
    ['Minutes since start', '']
  ]);
  formatDateCell_(sheet.getRange('B2'));

  sheet.getRange(DT.DATA_HEADER_ROW, 1, 1, LOG_HEADERS.length).setValues([LOG_HEADERS]);
  formatHeaderRow_(sheet, DT.DATA_HEADER_ROW, LOG_HEADERS.length);
  sheet.setFrozenRows(DT.DATA_HEADER_ROW);
  sheet.autoResizeColumns(1, LOG_HEADERS.length);
}

function rebuildInventorySheet_() {
  const sheet = getOrCreateSheet_(DT.SHEET_INVENTORY);
  sheet.clear();

  sheet.getRange('A1:B2').setValues([
    ['Report', 'Drive Inventory'],
    ['Generated at', [new Date()]]
  ]);
  formatDateCell_(sheet.getRange('B2'));

  sheet.getRange(DT.DATA_HEADER_ROW, 1, 1, INVENTORY_HEADERS.length).setValues([INVENTORY_HEADERS]);
  formatHeaderRow_(sheet, DT.DATA_HEADER_ROW, INVENTORY_HEADERS.length);
  sheet.setFrozenRows(DT.DATA_HEADER_ROW);
  sheet.autoResizeColumns(1, INVENTORY_HEADERS.length);
}

function rebuildPermissionsSheet_() {
  const sheet = getOrCreateSheet_(DT.SHEET_PERMISSIONS);
  sheet.clear();

  sheet.getRange('A1:B2').setValues([
    ['Report', 'Permissions'],
    ['Generated at', [new Date()]]
  ]);
  formatDateCell_(sheet.getRange('B2'));

  sheet.getRange(DT.DATA_HEADER_ROW, 1, 1, PERMISSIONS_HEADERS.length).setValues([PERMISSIONS_HEADERS]);
  formatHeaderRow_(sheet, DT.DATA_HEADER_ROW, PERMISSIONS_HEADERS.length);
  sheet.setFrozenRows(DT.DATA_HEADER_ROW);
  sheet.autoResizeColumns(1, PERMISSIONS_HEADERS.length);
}

function rebuildCountSheet_() {
  const sheet = getOrCreateSheet_(DT.SHEET_COUNT);
  sheet.clear();

  sheet.getRange('A1:B2').setValues([
    ['Report', 'Drive Count'],
    ['Generated at', [new Date()]]
  ]);
  formatDateCell_(sheet.getRange('B2'));

  sheet.getRange('A4:B14').setValues([
    ['Metric', 'Value'],
    ['Status', 'NOT STARTED'],
    ['Started at', ''],
    ['Finished at', ''],
    ['Total inventory rows', 0],
    ['Total files', 0],
    ['Total folders', 0],
    ['My Drive items', 0],
    ['Shared Drive items', 0],
    ['Shared with me items', 0]
  ]);

  formatHeaderRow_(sheet, 4, 2);
  sheet.autoResizeColumns(1, 2);
}

/**********************
 * BASIC HELPERS
 **********************/

function getOrCreateSheet_(name) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  return ss.getSheetByName(name) || ss.insertSheet(name);
}

function formatHeaderRow_(sheet, row, width) {
  sheet.getRange(row, 1, 1, width)
    .setFontWeight('bold')
    .setBackground('#d9ead3');
}

function formatDateCell_(range) {
  range.setNumberFormat('yyyy-mm-dd hh:mm:ss');
}

function writeReportTimestamp_(sheet, reportName) {
  sheet.getRange('A1:B2').setValues([
    ['Report', reportName],
    ['Generated at', [new Date()]]
  ]);
  formatDateCell_(sheet.getRange('B2'));
}

function roundSafe_(value, digits) {
  const factor = Math.pow(10, digits || 0);
  return Math.round(Number(value || 0) * factor) / factor;
}

function getExtension_(name) {
  if (!name || String(name).indexOf('.') === -1) return '';
  const parts = String(name).split('.');
  return parts.length > 1 ? String(parts.pop()).toLowerCase() : '';
}

function appendRows_(sheet, startRow, rows) {
  if (!rows || !rows.length) return;
  const row = Math.max(sheet.getLastRow() + 1, startRow);
  sheet.getRange(row, 1, rows.length, rows[0].length).setValues(rows);
  SpreadsheetApp.flush();
  Utilities.sleep(DT.FLUSH_SLEEP_MS);
}

function parseJsonSafe_(text, fallback) {
  try {
    return text ? JSON.parse(text) : fallback;
  } catch (e) {
    return fallback;
  }
}

/**********************
 * STATE HELPERS
 **********************/

function stateGet_(key, fallback) {
  const sheet = getOrCreateSheet_(DT.SHEET_STATE);
  const lastRow = sheet.getLastRow();
  if (lastRow < DT.DATA_START_ROW) return fallback;

  const data = sheet.getRange(DT.DATA_START_ROW, 1, lastRow - DT.DATA_START_ROW + 1, 2).getValues();
  for (let i = 0; i < data.length; i++) {
    if (String(data[i][0]) === String(key)) return data[i][1];
  }
  return fallback;
}

function stateSet_(key, value) {
  const sheet = getOrCreateSheet_(DT.SHEET_STATE);
  const lastRow = sheet.getLastRow();

  if (lastRow < DT.DATA_START_ROW) {
    sheet.getRange(DT.DATA_START_ROW, 1, 1, 3).setValues([[key, value, new Date()]]);
    formatDateCell_(sheet.getRange(DT.DATA_START_ROW, 3));
    return;
  }

  const data = sheet.getRange(DT.DATA_START_ROW, 1, lastRow - DT.DATA_START_ROW + 1, 1).getValues().flat();
  let foundRow = -1;

  for (let i = 0; i < data.length; i++) {
    if (String(data[i]) === String(key)) {
      foundRow = DT.DATA_START_ROW + i;
      break;
    }
  }

  if (foundRow === -1) {
    foundRow = lastRow + 1;
  }

  sheet.getRange(foundRow, 1, 1, 3).setValues([[key, value, new Date()]]);
  formatDateCell_(sheet.getRange(foundRow, 3));
}

function stateDelete_(key) {
  const sheet = getOrCreateSheet_(DT.SHEET_STATE);
  const lastRow = sheet.getLastRow();
  if (lastRow < DT.DATA_START_ROW) return;

  const data = sheet.getRange(DT.DATA_START_ROW, 1, lastRow - DT.DATA_START_ROW + 1, 1).getValues().flat();
  for (let i = 0; i < data.length; i++) {
    if (String(data[i]) === String(key)) {
      sheet.deleteRow(DT.DATA_START_ROW + i);
      return;
    }
  }
}

function stateSnapshot_(keys) {
  const out = {};
  keys.forEach(function (k) {
    out[k] = stateGet_(k, '');
  });
  return JSON.stringify(out);
}

/**********************
 * PROGRESS
 **********************/

function writeProgressSheet_(state) {
  const sheet = getOrCreateSheet_(DT.SHEET_PROGRESS);
  sheet.clear();

  sheet.getRange('A1:B2').setValues([
    ['Report', 'Progress'],
    ['Generated at', [new Date()]]
  ]);
  formatDateCell_(sheet.getRange('B2'));

  sheet.getRange('A4:B13').setValues([
    ['Metric', 'Value'],
    ['Operation', state.operation || ''],
    ['Status', state.status || ''],
    ['Phase', state.phase || ''],
    ['Processed', state.processed || 0],
    ['Checkpoint', state.checkpoint || ''],
    ['Started at', state.startedAt || ''],
    ['Last update', state.lastUpdate || ''],
    ['Finished at', state.finishedAt || ''],
    ['Note', state.note || '']
  ]);

  formatHeaderRow_(sheet, 4, 2);
  formatDateCell_(sheet.getRange('B2'));
  formatDateCell_(sheet.getRange('B10'));
  formatDateCell_(sheet.getRange('B11'));
  formatDateCell_(sheet.getRange('B12'));
  sheet.autoResizeColumns(1, 2);

  SpreadsheetApp.flush();
  Utilities.sleep(DT.FLUSH_SLEEP_MS);
}

function updateProgress_(operation, status, phase, processed, checkpoint, note, startedAt, finishedAt) {
  writeProgressSheet_({
    operation: operation,
    status: status,
    phase: phase,
    processed: processed,
    checkpoint: checkpoint,
    startedAt: startedAt || '',
    lastUpdate: new Date(),
    finishedAt: finishedAt || '',
    note: note || ''
  });
}

function showScanProgress() {
  SpreadsheetApp.getActiveSpreadsheet().setActiveSheet(getOrCreateSheet_(DT.SHEET_PROGRESS));
}

/**********************
 * LOG
 **********************/

function getCurrentRunSource_() {
  return 'manual_or_trigger';
}

function getAnyStartedAt_() {
  return (
    stateGet_(DT.KEY_INV_STARTED_AT, '') ||
    stateGet_(DT.KEY_PATH_STARTED_AT, '') ||
    stateGet_(DT.KEY_PERM_STARTED_AT, '') ||
    ''
  );
}

function updateLogMinutesSinceStart_(startedAt) {
  const sheet = getOrCreateSheet_(DT.SHEET_LOG);
  let minutes = '';
  if (startedAt) {
    const ms = new Date(startedAt).getTime();
    if (!isNaN(ms)) {
      minutes = roundSafe_((Date.now() - ms) / 60000, 2);
    }
  }
  sheet.getRange('A3:B3').setValues([['Minutes since start', minutes]]);
}

function logEvent_(operation, level, phase, message, processed, checkpoint, durationMs, startedAt) {
  const sheet = getOrCreateSheet_(DT.SHEET_LOG);
  if (sheet.getLastRow() === 0) rebuildLogSheet_();

  updateLogMinutesSinceStart_(startedAt || getAnyStartedAt_());

  const row = [
    new Date(),
    operation || '',
    level || '',
    phase || '',
    message || '',
    processed || 0,
    checkpoint || '',
    durationMs || '',
    getCurrentRunSource_()
  ];

  sheet.appendRow(row);
  formatDateCell_(sheet.getRange(sheet.getLastRow(), 1));
}

function clearLogData_() {
  rebuildLogSheet_();
}

function clearLog() {
  initDriveTools_();
  clearLogData_();
  logEvent_('LOG', 'INFO', 'CLEAR', 'Log cleared manually.', 0, '', '', '');
  SpreadsheetApp.getUi().alert('LOG sheet cleared.');
}

/**********************
 * ACTIVE JOB / LOCK
 **********************/

function setActiveJob_(jobName) {
  stateSet_(DT.KEY_ACTIVE_JOB, jobName);
}

function clearActiveJob_() {
  stateDelete_(DT.KEY_ACTIVE_JOB);
}

function getActiveJob_() {
  return stateGet_(DT.KEY_ACTIVE_JOB, '');
}

/**********************
 * AUTO RESUME
 **********************/

function ensureSingleAutoTrigger_() {
  const triggers = ScriptApp.getProjectTriggers();
  let exists = false;

  triggers.forEach(function (t) {
    if (t.getHandlerFunction() === DT.AUTO_TRIGGER_HANDLER) exists = true;
  });

  if (!exists) {
    ScriptApp.newTrigger(DT.AUTO_TRIGGER_HANDLER)
      .timeBased()
      .everyMinutes(DT.AUTO_TRIGGER_INTERVAL_MIN)
      .create();
  }
}

function enableAutoResume() {
  ensureSingleAutoTrigger_();
  logEvent_('AUTO_RESUME', 'INFO', 'ENABLE', 'Auto resume trigger enabled.', 0, '', '', '');
  SpreadsheetApp.getUi().alert('Auto resume enabled.');
}

function disableAutoResume() {
  ScriptApp.getProjectTriggers().forEach(function (t) {
    if (t.getHandlerFunction() === DT.AUTO_TRIGGER_HANDLER) {
      ScriptApp.deleteTrigger(t);
    }
  });
  logEvent_('AUTO_RESUME', 'INFO', 'DISABLE', 'Auto resume trigger disabled.', 0, '', '', '');
  SpreadsheetApp.getUi().alert('Auto resume disabled.');
}

function hasPendingWork_() {
  const statuses = [
    stateGet_(DT.KEY_INV_STATUS, ''),
    stateGet_(DT.KEY_PATH_STATUS, ''),
    stateGet_(DT.KEY_PERM_STATUS, '')
  ];
  return statuses.some(function (s) {
    return ['RUNNING', 'PAUSED'].includes(String(s));
  });
}

function resumePendingTasks() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(1)) return;

  try {
    const invStatus = stateGet_(DT.KEY_INV_STATUS, '');
    const pathStatus = stateGet_(DT.KEY_PATH_STATUS, '');
    const permStatus = stateGet_(DT.KEY_PERM_STATUS, '');

    if (['RUNNING', 'PAUSED'].includes(invStatus)) {
      try {
        scanDriveInventory();
      } catch (e) {
        logEvent_('AUTO_RESUME', 'ERROR', 'INVENTORY', `Auto resume inventory failed: ${String(e)}`, 0, '', '', stateGet_(DT.KEY_INV_STARTED_AT, ''));
      }
      return;
    }

    if (['RUNNING', 'PAUSED'].includes(pathStatus)) {
      try {
        buildFullPaths();
      } catch (e) {
        logEvent_('AUTO_RESUME', 'ERROR', 'PATHS', `Auto resume paths failed: ${String(e)}`, 0, '', '', stateGet_(DT.KEY_PATH_STARTED_AT, ''));
      }
      return;
    }

    if (['RUNNING', 'PAUSED'].includes(permStatus)) {
      try {
        fetchPermissions();
      } catch (e) {
        logEvent_('AUTO_RESUME', 'ERROR', 'PERMISSIONS', `Auto resume permissions failed: ${String(e)}`, 0, '', '', stateGet_(DT.KEY_PERM_STARTED_AT, ''));
      }
      return;
    }

    if (!hasPendingWork_()) {
      disableAutoResume();
    }
  } finally {
    lock.releaseLock();
  }
}

/**********************
 * API WRAPPER
 **********************/

function callWithRetry_(fn, operation, phase, processed, checkpoint, requestLabel, startedAt) {
  let attempt = 1;
  let lastErr = null;

  while (attempt <= DT.RETRY_MAX_ATTEMPTS) {
    const started = Date.now();
    try {
      logEvent_(operation, 'INFO', phase, `Request start: ${requestLabel}. Attempt ${attempt}.`, processed, checkpoint, '', startedAt);
      const res = fn();
      logEvent_(operation, 'INFO', phase, `Request success: ${requestLabel}. Attempt ${attempt}.`, processed, checkpoint, Date.now() - started, startedAt);
      return res;
    } catch (err) {
      lastErr = err;
      logEvent_(operation, 'WARN', phase, `Request failed: ${requestLabel}. Attempt ${attempt}. Error: ${String(err)}`, processed, checkpoint, Date.now() - started, startedAt);
      if (attempt >= DT.RETRY_MAX_ATTEMPTS) break;
      Utilities.sleep(DT.RETRY_BASE_MS * attempt);
      attempt++;
    }
  }

  throw lastErr;
}

/**********************
 * SHARED DRIVES
 **********************/

function getSharedDrivesList_() {
  const startedAt = getAnyStartedAt_();
  let token = '';
  const out = [];

  do {
    const resp = callWithRetry_(
      function () {
        return Drive.Drives.list({
          pageSize: 100,
          pageToken: token || ''
        });
      },
      'COMMON',
      'LIST_SHARED_DRIVES',
      out.length,
      token,
      'Drive.Drives.list',
      startedAt
    );

    const drives = resp.drives || [];
    drives.forEach(function (d) {
      out.push({ id: d.id || '', name: d.name || '' });
    });

    token = resp.nextPageToken || '';
    logEvent_('COMMON', 'INFO', 'LIST_SHARED_DRIVES', `Shared drives page loaded: ${drives.length}`, out.length, token, '', startedAt);
    Utilities.sleep(DT.API_SLEEP_MS);
  } while (token);

  return out;
}

function getSharedDriveMap_() {
  const drives = getSharedDrivesList_();
  const map = {};
  drives.forEach(function (d) { map[d.id] = d.name; });
  return map;
}

/**********************
 * INVENTORY
 **********************/

function scanDriveInventory() {
  initDriveTools_();

  const lock = LockService.getScriptLock();
  if (!lock.tryLock(10000)) {
    SpreadsheetApp.getUi().alert('Another Drive task is already running.');
    return;
  }

  try {
    setActiveJob_('INVENTORY');

    let status = stateGet_(DT.KEY_INV_STATUS, '');
    let phase = stateGet_(DT.KEY_INV_PHASE, 'USER');
    let token = stateGet_(DT.KEY_INV_TOKEN, '');
    let processed = Number(stateGet_(DT.KEY_INV_PROCESSED, 0));
    let startedAt = stateGet_(DT.KEY_INV_STARTED_AT, '');
    let driveIndex = Number(stateGet_(DT.KEY_INV_DRIVE_INDEX, 0));

    const sheet = getOrCreateSheet_(DT.SHEET_INVENTORY);

    if (!status || status === 'DONE') {
      rebuildInventorySheet_();
      startedAt = new Date().toISOString();

      stateSet_(DT.KEY_INV_STATUS, 'RUNNING');
      stateSet_(DT.KEY_INV_PHASE, 'USER');
      stateSet_(DT.KEY_INV_TOKEN, '');
      stateSet_(DT.KEY_INV_PROCESSED, 0);
      stateSet_(DT.KEY_INV_STARTED_AT, startedAt);
      stateSet_(DT.KEY_INV_DRIVE_INDEX, 0);
      stateDelete_(DT.KEY_INV_DRIVE_ID);
      stateDelete_(DT.KEY_INV_DRIVE_NAME);

      status = 'RUNNING';
      phase = 'USER';
      token = '';
      processed = 0;
      driveIndex = 0;

      clearLogData_();
      logEvent_('INVENTORY', 'INFO', phase, 'Inventory started.', processed, '', '', startedAt);
    }

    ensureSingleAutoTrigger_();

    const existingIds = getExistingInventoryIds_();
    const sharedDrives = getSharedDrivesList_();
    const sharedDriveMap = {};
    sharedDrives.forEach(function (d) { sharedDriveMap[d.id] = d.name; });

    updateProgress_('INVENTORY', 'RUNNING', phase, processed, token, 'Inventory scan in progress.', startedAt, '');
    const stopAt = Date.now() + DT.MAX_RUNTIME_MS;

    while (Date.now() < stopAt) {
      if (phase === 'USER') {
        updateProgress_('INVENTORY', 'RUNNING', 'USER_BEFORE_REQUEST', processed, token, 'About to request My Drive page.', startedAt, '');
        logEvent_('INVENTORY', 'INFO', 'USER', 'Before request: My Drive page.', processed, token, '', startedAt);

        const resp = callWithRetry_(
          function () {
            return Drive.Files.list({
              pageSize: DT.INVENTORY_BATCH_SIZE,
              pageToken: token || '',
              corpora: 'user',
              includeItemsFromAllDrives: true,
              supportsAllDrives: true,
              fields: 'nextPageToken, files(id,name,mimeType,size,createdTime,modifiedTime,webViewLink,parents,trashed,starred,driveId,sharedWithMeTime,owners(emailAddress,displayName))'
            });
          },
          'INVENTORY',
          'USER',
          processed,
          token,
          'Drive.Files.list user',
          startedAt
        );

        const files = resp.files || [];
        const nextToken = resp.nextPageToken || '';

        updateProgress_('INVENTORY', 'RUNNING', 'USER_AFTER_RESPONSE', processed, nextToken, `Received ${files.length} items.`, startedAt, '');

        const rows = files
          .filter(function (f) { return f && f.id && !existingIds.has(f.id); })
          .map(function (f) { return mapInventoryRow_(f, 'USER', sharedDriveMap); });

        if (rows.length) {
          appendRows_(sheet, DT.DATA_START_ROW, rows);
          rows.forEach(function (r) { existingIds.add(r[0]); });
        }

        processed += rows.length;
        stateSet_(DT.KEY_INV_PROCESSED, processed);
        stateSet_(DT.KEY_INV_TOKEN, nextToken);

        const checkpoint = stateSnapshot_([DT.KEY_INV_PHASE, DT.KEY_INV_TOKEN, DT.KEY_INV_PROCESSED, DT.KEY_INV_DRIVE_INDEX]);
        logEvent_('INVENTORY', 'INFO', 'USER', `Page written: ${rows.length} rows.`, processed, checkpoint, '', startedAt);
        updateProgress_('INVENTORY', 'RUNNING', 'USER_PAGE_DONE', processed, checkpoint, 'My Drive page written.', startedAt, '');

        if (nextToken) {
          token = nextToken;
          Utilities.sleep(DT.API_SLEEP_MS);
          continue;
        }

        phase = 'SHARED_DRIVES';
        token = '';
        driveIndex = 0;
        stateSet_(DT.KEY_INV_PHASE, phase);
        stateSet_(DT.KEY_INV_TOKEN, '');
        stateSet_(DT.KEY_INV_DRIVE_INDEX, 0);
        logEvent_('INVENTORY', 'INFO', phase, 'Transition to Shared Drives.', processed, '', '', startedAt);
        Utilities.sleep(DT.API_SLEEP_MS);
        continue;
      }

      if (phase === 'SHARED_DRIVES') {
        if (driveIndex >= sharedDrives.length) {
          phase = 'SHARED_WITH_ME';
          token = '';
          stateSet_(DT.KEY_INV_PHASE, phase);
          stateSet_(DT.KEY_INV_TOKEN, '');
          stateDelete_(DT.KEY_INV_DRIVE_ID);
          stateDelete_(DT.KEY_INV_DRIVE_NAME);
          logEvent_('INVENTORY', 'INFO', phase, 'Transition to Shared with me.', processed, '', '', startedAt);
          Utilities.sleep(DT.API_SLEEP_MS);
          continue;
        }

        const drive = sharedDrives[driveIndex];
        stateSet_(DT.KEY_INV_DRIVE_ID, drive.id);
        stateSet_(DT.KEY_INV_DRIVE_NAME, drive.name);
        stateSet_(DT.KEY_INV_DRIVE_INDEX, driveIndex);

        updateProgress_('INVENTORY', 'RUNNING', `SHARED_DRIVE_BEFORE_REQUEST [${drive.name}]`, processed, token, `About to request ${drive.name}`, startedAt, '');

        const resp = callWithRetry_(
          function () {
            return Drive.Files.list({
              pageSize: DT.INVENTORY_BATCH_SIZE,
              pageToken: token || '',
              corpora: 'drive',
              driveId: drive.id,
              includeItemsFromAllDrives: true,
              supportsAllDrives: true,
              fields: 'nextPageToken, files(id,name,mimeType,size,createdTime,modifiedTime,webViewLink,parents,trashed,starred,driveId,sharedWithMeTime,owners(emailAddress,displayName))'
            });
          },
          'INVENTORY',
          `SHARED_DRIVE:${drive.name}`,
          processed,
          token,
          `Drive.Files.list drive ${drive.name}`,
          startedAt
        );

        const files = resp.files || [];
        const nextToken = resp.nextPageToken || '';

        const rows = files
          .filter(function (f) { return f && f.id && !existingIds.has(f.id); })
          .map(function (f) { return mapInventoryRow_(f, 'SHARED_DRIVE', sharedDriveMap); });

        if (rows.length) {
          appendRows_(sheet, DT.DATA_START_ROW, rows);
          rows.forEach(function (r) { existingIds.add(r[0]); });
        }

        processed += rows.length;
        stateSet_(DT.KEY_INV_PROCESSED, processed);
        stateSet_(DT.KEY_INV_TOKEN, nextToken);

        const checkpoint = stateSnapshot_([DT.KEY_INV_PHASE, DT.KEY_INV_TOKEN, DT.KEY_INV_PROCESSED, DT.KEY_INV_DRIVE_INDEX, DT.KEY_INV_DRIVE_NAME]);
        logEvent_('INVENTORY', 'INFO', `SHARED_DRIVE:${drive.name}`, `Page written: ${rows.length} rows.`, processed, checkpoint, '', startedAt);
        updateProgress_('INVENTORY', 'RUNNING', `SHARED_DRIVE_PAGE_DONE [${drive.name}]`, processed, checkpoint, 'Shared Drive page written.', startedAt, '');

        if (nextToken) {
          token = nextToken;
          Utilities.sleep(DT.API_SLEEP_MS);
          continue;
        }

        driveIndex++;
        token = '';
        stateSet_(DT.KEY_INV_DRIVE_INDEX, driveIndex);
        stateSet_(DT.KEY_INV_TOKEN, '');
        logEvent_('INVENTORY', 'INFO', `SHARED_DRIVE:${drive.name}`, 'Drive finished.', processed, '', '', startedAt);
        Utilities.sleep(DT.API_SLEEP_MS);
        continue;
      }

      if (phase === 'SHARED_WITH_ME') {
        updateProgress_('INVENTORY', 'RUNNING', 'SHARED_WITH_ME_BEFORE_REQUEST', processed, token, 'About to request Shared with me.', startedAt, '');

        const resp = callWithRetry_(
          function () {
            return Drive.Files.list({
              pageSize: DT.INVENTORY_BATCH_SIZE,
              pageToken: token || '',
              q: 'sharedWithMe = true and trashed = false',
              includeItemsFromAllDrives: true,
              supportsAllDrives: true,
              fields: 'nextPageToken, files(id,name,mimeType,size,createdTime,modifiedTime,webViewLink,parents,trashed,starred,driveId,sharedWithMeTime,owners(emailAddress,displayName))'
            });
          },
          'INVENTORY',
          'SHARED_WITH_ME',
          processed,
          token,
          'Drive.Files.list sharedWithMe',
          startedAt
        );

        const files = resp.files || [];
        const nextToken = resp.nextPageToken || '';

        const rows = files
          .filter(function (f) { return f && f.id && !existingIds.has(f.id); })
          .map(function (f) { return mapInventoryRow_(f, 'SHARED_WITH_ME', sharedDriveMap); });

        if (rows.length) {
          appendRows_(sheet, DT.DATA_START_ROW, rows);
          rows.forEach(function (r) { existingIds.add(r[0]); });
        }

        processed += rows.length;
        stateSet_(DT.KEY_INV_PROCESSED, processed);
        stateSet_(DT.KEY_INV_TOKEN, nextToken);

        const checkpoint = stateSnapshot_([DT.KEY_INV_PHASE, DT.KEY_INV_TOKEN, DT.KEY_INV_PROCESSED]);
        logEvent_('INVENTORY', 'INFO', 'SHARED_WITH_ME', `Page written: ${rows.length} rows.`, processed, checkpoint, '', startedAt);
        updateProgress_('INVENTORY', 'RUNNING', 'SHARED_WITH_ME_PAGE_DONE', processed, checkpoint, 'Shared with me page written.', startedAt, '');

        if (nextToken) {
          token = nextToken;
          Utilities.sleep(DT.API_SLEEP_MS);
          continue;
        }

        stateSet_(DT.KEY_INV_STATUS, 'DONE');
        stateDelete_(DT.KEY_INV_TOKEN);
        writeReportTimestamp_(sheet, 'Drive Inventory');
        sheet.autoResizeColumns(1, INVENTORY_HEADERS.length);

        updateProgress_('INVENTORY', 'DONE', 'FINISHED', processed, '', 'Inventory completed successfully.', startedAt, new Date());
        logEvent_('INVENTORY', 'INFO', 'FINISHED', 'Inventory completed successfully.', processed, '', '', startedAt);
        disableAutoResumeIfNoPending_();
        SpreadsheetApp.getUi().alert('Drive inventory completed.');
        return;
      }
    }

    stateSet_(DT.KEY_INV_STATUS, 'PAUSED');
    const checkpoint = stateSnapshot_([DT.KEY_INV_PHASE, DT.KEY_INV_TOKEN, DT.KEY_INV_PROCESSED, DT.KEY_INV_DRIVE_INDEX, DT.KEY_INV_DRIVE_NAME]);
    updateProgress_('INVENTORY', 'PAUSED', phase, processed, checkpoint, 'Time window reached. Auto Resume or manual rerun can continue.', startedAt, '');
    logEvent_('INVENTORY', 'WARN', phase, 'Paused due to time window.', processed, checkpoint, '', startedAt);
    SpreadsheetApp.getUi().alert('Inventory paused safely. Run again or use Auto Resume.');

  } catch (err) {
    const startedAt = stateGet_(DT.KEY_INV_STARTED_AT, '');
    const checkpoint = stateSnapshot_([DT.KEY_INV_PHASE, DT.KEY_INV_TOKEN, DT.KEY_INV_PROCESSED, DT.KEY_INV_DRIVE_INDEX, DT.KEY_INV_DRIVE_NAME]);
    stateSet_(DT.KEY_INV_STATUS, 'PAUSED');
    updateProgress_('INVENTORY', 'PAUSED', stateGet_(DT.KEY_INV_PHASE, ''), Number(stateGet_(DT.KEY_INV_PROCESSED, 0)), checkpoint, `Paused after error: ${String(err)}`, startedAt, '');
    logEvent_('INVENTORY', 'ERROR', stateGet_(DT.KEY_INV_PHASE, ''), String(err), Number(stateGet_(DT.KEY_INV_PROCESSED, 0)), checkpoint, '', startedAt);
    SpreadsheetApp.getUi().alert(`Inventory paused after error.\n${String(err)}`);
  } finally {
    clearActiveJob_();
    lock.releaseLock();
  }
}

function mapInventoryRow_(file, sourceScope, sharedDriveMap) {
  const isFolder = file.mimeType === 'application/vnd.google-apps.folder';
  const sizeBytes = isFolder ? '' : Number(file.size || 0);
  const sizeMb = isFolder ? '' : roundSafe_(sizeBytes / 1024 / 1024, 3);
  const owners = file.owners || [];
  const ownerEmail = owners.length ? (owners[0].emailAddress || '') : '';
  const ownerName = owners.length ? (owners[0].displayName || '') : '';
  const sharedDriveId = file.driveId || '';
  const sharedDriveName = sharedDriveId ? (sharedDriveMap[sharedDriveId] || '') : '';
  const driveType = sharedDriveId ? 'Shared Drive' : 'My Drive';
  const parents = (file.parents || []).join(', ');
  const extension = getExtension_(file.name || '');

  return [
    file.id || '',
    file.name || '',
    file.webViewLink || '',
    file.mimeType || '',
    extension,
    sizeBytes,
    sizeMb,
    file.createdTime || '',
    file.modifiedTime || '',
    ownerEmail,
    ownerName,
    isFolder,
    driveType,
    sharedDriveName,
    sharedDriveId,
    sourceScope === 'SHARED_WITH_ME',
    file.trashed === true,
    file.starred === true,
    parents,
    sourceScope,
    '',       // Full Path
    '',       // Path Status
    '',       // Permission Summary
    '',       // Anyone Access
    '',       // Domain Access
    '',       // Direct Users Count
    '',       // Direct Groups Count
    ''        // Permissions Status
  ];
}

function getExistingInventoryIds_() {
  const sheet = getOrCreateSheet_(DT.SHEET_INVENTORY);
  const lastRow = sheet.getLastRow();
  if (lastRow < DT.DATA_START_ROW) return new Set();

  const values = sheet.getRange(DT.DATA_START_ROW, 1, lastRow - DT.DATA_START_ROW + 1, 1).getValues().flat().filter(Boolean);
  return new Set(values);
}

/**********************
 * PATH ENRICHMENT
 **********************/

function buildFullPaths() {
  initDriveTools_();

  const lock = LockService.getScriptLock();
  if (!lock.tryLock(10000)) {
    SpreadsheetApp.getUi().alert('Another Drive task is already running.');
    return;
  }

  try {
    setActiveJob_('PATHS');

    let status = stateGet_(DT.KEY_PATH_STATUS, '');
    let startRow = Number(stateGet_(DT.KEY_PATH_ROW, DT.DATA_START_ROW));
    let processed = Number(stateGet_(DT.KEY_PATH_PROCESSED, 0));
    let startedAt = stateGet_(DT.KEY_PATH_STARTED_AT, '');

    const sheet = getOrCreateSheet_(DT.SHEET_INVENTORY);
    const lastRow = sheet.getLastRow();

    if (lastRow < DT.DATA_START_ROW) {
      SpreadsheetApp.getUi().alert('Drive Inventory is empty. Run Scan Drive Inventory first.');
      return;
    }

    if (!status || status === 'DONE') {
      startedAt = new Date().toISOString();
      stateSet_(DT.KEY_PATH_STATUS, 'RUNNING');
      stateSet_(DT.KEY_PATH_ROW, DT.DATA_START_ROW);
      stateSet_(DT.KEY_PATH_PROCESSED, 0);
      stateSet_(DT.KEY_PATH_STARTED_AT, startedAt);

      status = 'RUNNING';
      startRow = DT.DATA_START_ROW;
      processed = 0;

      logEvent_('PATHS', 'INFO', 'START', 'Path enrichment started.', processed, '', '', startedAt);
    }

    ensureSingleAutoTrigger_();

    const folderMap = buildFolderMapFromInventory_();
    const stopAt = Date.now() + DT.MAX_RUNTIME_MS;

    while (Date.now() < stopAt && startRow <= lastRow) {
      const batchEnd = Math.min(startRow + DT.PATHS_BATCH_ROWS - 1, lastRow);
      const rows = sheet.getRange(startRow, 1, batchEnd - startRow + 1, INVENTORY_HEADERS.length).getValues();

      updateProgress_('PATHS', 'RUNNING', 'PATHS_BATCH', processed, `rows ${startRow}-${batchEnd}`, 'Building full paths.', startedAt, '');
      logEvent_('PATHS', 'INFO', 'PATHS_BATCH', `Processing rows ${startRow}-${batchEnd}.`, processed, `rows ${startRow}-${batchEnd}`, '', startedAt);

      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const rowIndex = startRow + i;

        const fileId = row[0];
        const fileName = row[1];
        const parentIdsRaw = row[18];
        const currentPath = row[20];

        if (!fileId || currentPath) continue;

        const result = buildPathForRow_(row, folderMap);

        sheet.getRange(rowIndex, 21, 1, 2).setValues([[result.path, result.status]]);
        processed++;

        if (processed % 20 === 0) {
          SpreadsheetApp.flush();
          Utilities.sleep(DT.FLUSH_SLEEP_MS);
        }
      }

      stateSet_(DT.KEY_PATH_ROW, batchEnd + 1);
      stateSet_(DT.KEY_PATH_PROCESSED, processed);

      const checkpoint = stateSnapshot_([DT.KEY_PATH_ROW, DT.KEY_PATH_PROCESSED, DT.KEY_PATH_STATUS]);
      logEvent_('PATHS', 'INFO', 'PATHS_BATCH_DONE', `Batch completed: rows ${startRow}-${batchEnd}.`, processed, checkpoint, '', startedAt);

      startRow = batchEnd + 1;
      Utilities.sleep(DT.API_SLEEP_MS);
    }

    if (startRow > lastRow) {
      stateSet_(DT.KEY_PATH_STATUS, 'DONE');
      updateProgress_('PATHS', 'DONE', 'FINISHED', processed, '', 'Path enrichment completed.', startedAt, new Date());
      logEvent_('PATHS', 'INFO', 'FINISHED', 'Path enrichment completed.', processed, '', '', startedAt);
      writeReportTimestamp_(sheet, 'Drive Inventory');
      disableAutoResumeIfNoPending_();
      SpreadsheetApp.getUi().alert('Full paths completed.');
      return;
    }

    stateSet_(DT.KEY_PATH_STATUS, 'PAUSED');
    const checkpoint = stateSnapshot_([DT.KEY_PATH_ROW, DT.KEY_PATH_PROCESSED, DT.KEY_PATH_STATUS]);
    updateProgress_('PATHS', 'PAUSED', 'PAUSED', processed, checkpoint, 'Time window reached. Continue later.', startedAt, '');
    logEvent_('PATHS', 'WARN', 'PAUSED', 'Paused due to time window.', processed, checkpoint, '', startedAt);
    SpreadsheetApp.getUi().alert('Paths paused safely. Run again or use Auto Resume.');

  } catch (err) {
    const startedAt = stateGet_(DT.KEY_PATH_STARTED_AT, '');
    const checkpoint = stateSnapshot_([DT.KEY_PATH_ROW, DT.KEY_PATH_PROCESSED, DT.KEY_PATH_STATUS]);
    stateSet_(DT.KEY_PATH_STATUS, 'PAUSED');
    updateProgress_('PATHS', 'PAUSED', 'ERROR', Number(stateGet_(DT.KEY_PATH_PROCESSED, 0)), checkpoint, `Paused after error: ${String(err)}`, startedAt, '');
    logEvent_('PATHS', 'ERROR', 'ERROR', String(err), Number(stateGet_(DT.KEY_PATH_PROCESSED, 0)), checkpoint, '', startedAt);
    SpreadsheetApp.getUi().alert(`Paths paused after error.\n${String(err)}`);
  } finally {
    clearActiveJob_();
    lock.releaseLock();
  }
}

function buildFolderMapFromInventory_() {
  const sheet = getOrCreateSheet_(DT.SHEET_INVENTORY);
  const lastRow = sheet.getLastRow();
  const map = {};

  if (lastRow < DT.DATA_START_ROW) return map;

  const values = sheet.getRange(DT.DATA_START_ROW, 1, lastRow - DT.DATA_START_ROW + 1, INVENTORY_HEADERS.length).getValues();
  values.forEach(function (r) {
    const fileId = r[0];
    const name = r[1];
    const isFolder = r[11] === true;
    const existingPath = r[20];
    const parentIds = r[18];
    if (isFolder) {
      map[fileId] = {
        id: fileId,
        name: name,
        path: existingPath || '',
        parents: parentIds ? String(parentIds).split(',').map(function (x) { return String(x).trim(); }).filter(Boolean) : []
      };
    }
  });

  return map;
}

function buildPathForRow_(row, folderMap) {
  const fileId = row[0];
  const fileName = row[1];
  const parentIdsRaw = row[18];

  const parentIds = parentIdsRaw
    ? String(parentIdsRaw).split(',').map(function (x) { return String(x).trim(); }).filter(Boolean)
    : [];

  if (!parentIds.length) {
    return { path: '/' + fileName, status: 'ROOT_OR_NO_PARENT' };
  }

  // choose first parent for practical path
  const parentId = parentIds[0];
  const builtParentPath = resolveFolderPath_(parentId, folderMap, 0, {});

  if (builtParentPath.path) {
    return { path: builtParentPath.path + '/' + fileName, status: builtParentPath.status };
  }

  return { path: '', status: 'SKIPPED_MISSING_PARENT_PATH' };
}

function resolveFolderPath_(folderId, folderMap, depth, seen) {
  if (!folderId) return { path: '', status: 'EMPTY_FOLDER_ID' };
  if (depth > 50) return { path: '', status: 'SKIPPED_DEPTH_LIMIT' };
  if (seen[folderId]) return { path: '', status: 'SKIPPED_CYCLE' };
  seen[folderId] = true;

  const folder = folderMap[folderId];
  if (!folder) return { path: '', status: 'SKIPPED_MISSING_FOLDER' };
  if (folder.path) return { path: folder.path, status: 'OK_CACHED' };

  if (!folder.parents || !folder.parents.length) {
    folder.path = '/' + folder.name;
    return { path: folder.path, status: 'OK_ROOT_FOLDER' };
  }

  const firstParent = folder.parents[0];
  const parentRes = resolveFolderPath_(firstParent, folderMap, depth + 1, seen);

  if (!parentRes.path) return parentRes;

  folder.path = parentRes.path + '/' + folder.name;
  return { path: folder.path, status: 'OK_BUILT' };
}

/**********************
 * PERMISSIONS ENRICHMENT
 **********************/

function fetchPermissions() {
  initDriveTools_();

  const lock = LockService.getScriptLock();
  if (!lock.tryLock(10000)) {
    SpreadsheetApp.getUi().alert('Another Drive task is already running.');
    return;
  }

  try {
    setActiveJob_('PERMISSIONS');

    let status = stateGet_(DT.KEY_PERM_STATUS, '');
    let startRow = Number(stateGet_(DT.KEY_PERM_ROW, DT.DATA_START_ROW));
    let processed = Number(stateGet_(DT.KEY_PERM_PROCESSED, 0));
    let startedAt = stateGet_(DT.KEY_PERM_STARTED_AT, '');

    const invSheet = getOrCreateSheet_(DT.SHEET_INVENTORY);
    const permSheet = getOrCreateSheet_(DT.SHEET_PERMISSIONS);
    const lastRow = invSheet.getLastRow();

    if (lastRow < DT.DATA_START_ROW) {
      SpreadsheetApp.getUi().alert('Drive Inventory is empty. Run Scan Drive Inventory first.');
      return;
    }

    if (!status || status === 'DONE') {
      rebuildPermissionsSheet_();

      startedAt = new Date().toISOString();
      stateSet_(DT.KEY_PERM_STATUS, 'RUNNING');
      stateSet_(DT.KEY_PERM_ROW, DT.DATA_START_ROW);
      stateSet_(DT.KEY_PERM_PROCESSED, 0);
      stateSet_(DT.KEY_PERM_STARTED_AT, startedAt);

      status = 'RUNNING';
      startRow = DT.DATA_START_ROW;
      processed = 0;

      logEvent_('PERMISSIONS', 'INFO', 'START', 'Permissions enrichment started.', processed, '', '', startedAt);
    }

    ensureSingleAutoTrigger_();
    const stopAt = Date.now() + DT.MAX_RUNTIME_MS;

    while (Date.now() < stopAt && startRow <= lastRow) {
      const batchEnd = Math.min(startRow + DT.PERMISSIONS_BATCH_ROWS - 1, lastRow);
      const values = invSheet.getRange(startRow, 1, batchEnd - startRow + 1, INVENTORY_HEADERS.length).getValues();

      updateProgress_('PERMISSIONS', 'RUNNING', 'PERMISSIONS_BATCH', processed, `rows ${startRow}-${batchEnd}`, 'Fetching permissions.', startedAt, '');
      logEvent_('PERMISSIONS', 'INFO', 'PERMISSIONS_BATCH', `Processing rows ${startRow}-${batchEnd}.`, processed, `rows ${startRow}-${batchEnd}`, '', startedAt);

      const permRows = [];

      for (let i = 0; i < values.length; i++) {
        const row = values[i];
        const rowIndex = startRow + i;

        const fileId = row[0];
        const fileName = row[1];
        const driveId = row[14];
        const permStatus = row[27];

        if (!fileId || permStatus === 'DONE') continue;

        try {
          const checkpoint = `file ${fileId}`;
          const resp = callWithRetry_(
            function () {
              return Drive.Permissions.list(fileId, {
                supportsAllDrives: true,
                fields: 'permissions(id,type,role,emailAddress,displayName,domain,allowFileDiscovery,deleted,permissionDetails(inherited,inheritedFrom,permissionType,role))'
              });
            },
            'PERMISSIONS',
            'FETCH_FILE_PERMISSIONS',
            processed,
            checkpoint,
            `Drive.Permissions.list ${fileId}`,
            startedAt
          );

          const permissions = resp.permissions || [];
          const summary = summarizePermissions_(permissions);

          permissions.forEach(function (p) {
            const details = p.permissionDetails || [];
            if (!details.length) {
              permRows.push([
                fileId,
                fileName,
                p.id || '',
                p.type || '',
                p.role || '',
                p.emailAddress || '',
                p.displayName || '',
                p.domain || '',
                p.allowFileDiscovery === true,
                p.deleted === true,
                '',
                '',
                '',
                'PERMISSIONS',
                new Date()
              ]);
            } else {
              details.forEach(function (d) {
                permRows.push([
                  fileId,
                  fileName,
                  p.id || '',
                  p.type || '',
                  p.role || '',
                  p.emailAddress || '',
                  p.displayName || '',
                  p.domain || '',
                  p.allowFileDiscovery === true,
                  p.deleted === true,
                  d.inherited === true,
                  d.inheritedFrom || '',
                  d.permissionType || '',
                  'PERMISSIONS',
                  new Date()
                ]);
              });
            }
          });

          invSheet.getRange(rowIndex, 23, 1, 6).setValues([[
            summary.summaryText,
            summary.anyoneAccess,
            summary.domainAccess,
            summary.directUsersCount,
            summary.directGroupsCount,
            'DONE'
          ]]);

        } catch (err) {
          invSheet.getRange(rowIndex, 28).setValue('SKIPPED_ERROR');
          logEvent_('PERMISSIONS', 'WARN', 'FETCH_FILE_PERMISSIONS', `Skipped file ${fileId}: ${String(err)}`, processed, `file ${fileId}`, '', startedAt);
        }

        processed++;
        if (processed % 10 === 0) {
          SpreadsheetApp.flush();
          Utilities.sleep(DT.FLUSH_SLEEP_MS);
        }
      }

      if (permRows.length) {
        appendRows_(permSheet, DT.DATA_START_ROW, permRows);
        const fetchedAtCol = 15;
        permSheet.getRange(DT.DATA_START_ROW, fetchedAtCol, Math.max(permSheet.getLastRow() - DT.DATA_START_ROW + 1, 1), 1).setNumberFormat('yyyy-mm-dd hh:mm:ss');
      }

      stateSet_(DT.KEY_PERM_ROW, batchEnd + 1);
      stateSet_(DT.KEY_PERM_PROCESSED, processed);

      const checkpoint = stateSnapshot_([DT.KEY_PERM_ROW, DT.KEY_PERM_PROCESSED, DT.KEY_PERM_STATUS]);
      logEvent_('PERMISSIONS', 'INFO', 'PERMISSIONS_BATCH_DONE', `Batch completed: rows ${startRow}-${batchEnd}.`, processed, checkpoint, '', startedAt);

      startRow = batchEnd + 1;
      Utilities.sleep(DT.API_SLEEP_MS);
    }

    if (startRow > lastRow) {
      stateSet_(DT.KEY_PERM_STATUS, 'DONE');
      updateProgress_('PERMISSIONS', 'DONE', 'FINISHED', processed, '', 'Permissions enrichment completed.', startedAt, new Date());
      logEvent_('PERMISSIONS', 'INFO', 'FINISHED', 'Permissions enrichment completed.', processed, '', '', startedAt);
      writeReportTimestamp_(permSheet, 'Permissions');
      disableAutoResumeIfNoPending_();
      SpreadsheetApp.getUi().alert('Permissions completed.');
      return;
    }

    stateSet_(DT.KEY_PERM_STATUS, 'PAUSED');
    const checkpoint = stateSnapshot_([DT.KEY_PERM_ROW, DT.KEY_PERM_PROCESSED, DT.KEY_PERM_STATUS]);
    updateProgress_('PERMISSIONS', 'PAUSED', 'PAUSED', processed, checkpoint, 'Time window reached. Continue later.', startedAt, '');
    logEvent_('PERMISSIONS', 'WARN', 'PAUSED', 'Paused due to time window.', processed, checkpoint, '', startedAt);
    SpreadsheetApp.getUi().alert('Permissions paused safely. Run again or use Auto Resume.');

  } catch (err) {
    const startedAt = stateGet_(DT.KEY_PERM_STARTED_AT, '');
    const checkpoint = stateSnapshot_([DT.KEY_PERM_ROW, DT.KEY_PERM_PROCESSED, DT.KEY_PERM_STATUS]);
    stateSet_(DT.KEY_PERM_STATUS, 'PAUSED');
    updateProgress_('PERMISSIONS', 'PAUSED', 'ERROR', Number(stateGet_(DT.KEY_PERM_PROCESSED, 0)), checkpoint, `Paused after error: ${String(err)}`, startedAt, '');
    logEvent_('PERMISSIONS', 'ERROR', 'ERROR', String(err), Number(stateGet_(DT.KEY_PERM_PROCESSED, 0)), checkpoint, '', startedAt);
    SpreadsheetApp.getUi().alert(`Permissions paused after error.\n${String(err)}`);
  } finally {
    clearActiveJob_();
    lock.releaseLock();
  }
}

function summarizePermissions_(permissions) {
  let anyoneAccess = false;
  let domainAccess = false;
  let directUsersCount = 0;
  let directGroupsCount = 0;

  const parts = [];

  permissions.forEach(function (p) {
    const details = p.permissionDetails || [];
    const inherited = details.length ? details.every(function (d) { return d.inherited === true; }) : false;
    const direct = details.length ? details.some(function (d) { return d.inherited !== true; }) : (p.role !== 'owner');

    if (p.type === 'anyone') anyoneAccess = true;
    if (p.type === 'domain') domainAccess = true;
    if (p.type === 'user' && direct && p.role !== 'owner') directUsersCount++;
    if (p.type === 'group' && direct && p.role !== 'owner') directGroupsCount++;

    if (p.role !== 'owner') {
      const target = p.emailAddress || p.displayName || p.domain || p.type || '';
      parts.push(target + ' [' + (p.type || '') + ':' + (p.role || '') + (inherited ? ';inherited' : ';direct') + ']');
    }
  });

  return {
    summaryText: parts.join(' ; '),
    anyoneAccess: anyoneAccess,
    domainAccess: domainAccess,
    directUsersCount: directUsersCount,
    directGroupsCount: directGroupsCount
  };
}

/**********************
 * COUNT FROM INVENTORY
 **********************/

function buildCountFromInventory() {
  initDriveTools_();

  const startedAt = new Date().toISOString();
  const sheet = getOrCreateSheet_(DT.SHEET_COUNT);
  const invSheet = getOrCreateSheet_(DT.SHEET_INVENTORY);
  const lastRow = invSheet.getLastRow();

  if (lastRow < DT.DATA_START_ROW) {
    SpreadsheetApp.getUi().alert('Drive Inventory is empty. Run Scan Drive Inventory first.');
    return;
  }

  updateProgress_('COUNT_FROM_INVENTORY', 'RUNNING', 'READ_INVENTORY', 0, '', 'Counting from inventory sheet.', startedAt, '');
  logEvent_('COUNT_FROM_INVENTORY', 'INFO', 'READ_INVENTORY', 'Count from inventory started.', 0, '', '', startedAt);

  const values = invSheet.getRange(DT.DATA_START_ROW, 1, lastRow - DT.DATA_START_ROW + 1, INVENTORY_HEADERS.length).getValues();
  const counts = {
    total: 0,
    files: 0,
    folders: 0,
    myDrive: 0,
    sharedDrive: 0,
    sharedWithMe: 0
  };

  values.forEach(function (r) {
    if (!r[0]) return;
    counts.total++;
    if (r[11] === true) counts.folders++;
    else counts.files++;

    if (r[12] === 'Shared Drive') counts.sharedDrive++;
    else counts.myDrive++;

    if (r[15] === true) counts.sharedWithMe++;
  });

  sheet.clear();
  sheet.getRange('A1:B2').setValues([
    ['Report', 'Drive Count'],
    ['Generated at', [new Date()]]
  ]);
  formatDateCell_(sheet.getRange('B2'));

  sheet.getRange('A4:B14').setValues([
    ['Metric', 'Value'],
    ['Status', 'DONE'],
    ['Started at', startedAt],
    ['Finished at', new Date()],
    ['Total inventory rows', counts.total],
    ['Total files', counts.files],
    ['Total folders', counts.folders],
    ['My Drive items', counts.myDrive],
    ['Shared Drive items', counts.sharedDrive],
    ['Shared with me items', counts.sharedWithMe]
  ]);

  formatHeaderRow_(sheet, 4, 2);
  formatDateCell_(sheet.getRange('B6'));
  formatDateCell_(sheet.getRange('B7'));
  sheet.autoResizeColumns(1, 2);

  updateProgress_('COUNT_FROM_INVENTORY', 'DONE', 'FINISHED', counts.total, '', 'Count built from inventory.', startedAt, new Date());
  logEvent_('COUNT_FROM_INVENTORY', 'INFO', 'FINISHED', 'Count from inventory completed.', counts.total, '', '', startedAt);
}

/**********************
 * REPORTS FROM INVENTORY
 **********************/

function getInventoryObjects_() {
  const sheet = getOrCreateSheet_(DT.SHEET_INVENTORY);
  const lastRow = sheet.getLastRow();
  if (lastRow < DT.DATA_START_ROW) return [];

  const values = sheet.getRange(DT.DATA_START_ROW, 1, lastRow - DT.DATA_START_ROW + 1, INVENTORY_HEADERS.length).getValues();
  return values.map(function (r) {
    return {
      fileId: r[0],
      fileName: r[1],
      link: r[2],
      mimeType: r[3],
      extension: r[4],
      sizeBytes: r[5],
      sizeMb: r[6],
      createdTime: r[7],
      modifiedTime: r[8],
      ownerEmail: r[9],
      ownerName: r[10],
      isFolder: r[11] === true,
      driveType: r[12],
      sharedDriveName: r[13],
      sharedDriveId: r[14],
      sharedWithMe: r[15] === true,
      trashed: r[16] === true,
      starred: r[17] === true,
      parentIds: r[18],
      sourceScope: r[19],
      fullPath: r[20],
      pathStatus: r[21],
      permissionSummary: r[22],
      anyoneAccess: r[23],
      domainAccess: r[24],
      directUsersCount: r[25],
      directGroupsCount: r[26],
      permissionsStatus: r[27]
    };
  }).filter(function (x) { return x.fileId; });
}

function findLargestFiles() {
  initDriveTools_();
  const startedAt = new Date().toISOString();

  try {
    updateProgress_('LARGEST_FILES', 'RUNNING', 'BUILD', 0, '', 'Building largest files report.', startedAt, '');
    const data = getInventoryObjects_();
    if (!data.length) {
      SpreadsheetApp.getUi().alert('Drive Inventory is empty. Run Scan Drive Inventory first.');
      return;
    }

    const rows = data
      .filter(function (r) { return !r.isFolder; })
      .filter(function (r) { return Number(r.sizeBytes || 0) > 0; })
      .sort(function (a, b) { return Number(b.sizeBytes || 0) - Number(a.sizeBytes || 0); })
      .slice(0, 100)
      .map(function (r) {
        return [r.fileName, r.sizeBytes, r.sizeMb, r.ownerEmail, r.driveType, r.fullPath || '', r.link];
      });

    const sheet = getOrCreateSheet_(DT.SHEET_LARGEST);
    sheet.clear();
    writeReportTimestamp_(sheet, 'Largest Files');
    sheet.getRange(5, 1, 1, 7).setValues([[
      'File Name', 'Size (bytes)', 'Size (MB)', 'Owner Email', 'Drive Type', 'Full Path', 'Link'
    ]]);
    formatHeaderRow_(sheet, 5, 7);
    if (rows.length) sheet.getRange(6, 1, rows.length, 7).setValues(rows);
    sheet.autoResizeColumns(1, 7);

    updateProgress_('LARGEST_FILES', 'DONE', 'FINISHED', rows.length, '', 'Largest files report completed.', startedAt, new Date());
    logEvent_('LARGEST_FILES', 'INFO', 'FINISHED', `Largest files rows: ${rows.length}`, rows.length, '', '', startedAt);

  } catch (err) {
    updateProgress_('LARGEST_FILES', 'ERROR', 'FAILED', 0, '', String(err), startedAt, '');
    logEvent_('LARGEST_FILES', 'ERROR', 'FAILED', String(err), 0, '', '', startedAt);
    throw err;
  }
}

function findDuplicateFiles() {
  initDriveTools_();
  const startedAt = new Date().toISOString();

  try {
    updateProgress_('DUPLICATES', 'RUNNING', 'BUILD', 0, '', 'Building duplicates report.', startedAt, '');
    const data = getInventoryObjects_();
    if (!data.length) {
      SpreadsheetApp.getUi().alert('Drive Inventory is empty. Run Scan Drive Inventory first.');
      return;
    }

    const map = new Map();

    data
      .filter(function (r) { return !r.isFolder; })
      .filter(function (r) { return r.fileName && r.mimeType; })
      .filter(function (r) { return String(r.sizeBytes || '') !== ''; })
      .forEach(function (r) {
        const key = [r.fileName, r.sizeBytes, r.mimeType].join('__');
        if (!map.has(key)) map.set(key, []);
        map.get(key).push(r);
      });

    const rows = [];
    let group = 1;
    Array.from(map.values())
      .filter(function (arr) { return arr.length > 1; })
      .forEach(function (arr) {
        arr.forEach(function (r) {
          rows.push([
            group,
            r.fileName,
            r.mimeType,
            r.sizeBytes,
            r.sizeMb,
            r.ownerEmail,
            r.driveType,
            r.fullPath || '',
            r.link,
            r.fileId
          ]);
        });
        group++;
      });

    const sheet = getOrCreateSheet_(DT.SHEET_DUPLICATES);
    sheet.clear();
    writeReportTimestamp_(sheet, 'Duplicate Files');
    sheet.getRange(5, 1, 1, 10).setValues([[
      'Group', 'File Name', 'Mime Type', 'Size (bytes)', 'Size (MB)', 'Owner Email', 'Drive Type', 'Full Path', 'Link', 'File ID'
    ]]);
    formatHeaderRow_(sheet, 5, 10);
    if (rows.length) sheet.getRange(6, 1, rows.length, 10).setValues(rows);
    sheet.autoResizeColumns(1, 10);

    updateProgress_('DUPLICATES', 'DONE', 'FINISHED', rows.length, '', 'Duplicate files report completed.', startedAt, new Date());
    logEvent_('DUPLICATES', 'INFO', 'FINISHED', `Duplicate rows: ${rows.length}`, rows.length, '', '', startedAt);

  } catch (err) {
    updateProgress_('DUPLICATES', 'ERROR', 'FAILED', 0, '', String(err), startedAt, '');
    logEvent_('DUPLICATES', 'ERROR', 'FAILED', String(err), 0, '', '', startedAt);
    throw err;
  }
}

/**********************
 * RESET / CLEANUP
 **********************/

function resetAllCheckpoints() {
  initDriveTools_();

  [
    DT.KEY_ACTIVE_JOB,

    DT.KEY_INV_STATUS, DT.KEY_INV_PHASE, DT.KEY_INV_TOKEN, DT.KEY_INV_PROCESSED, DT.KEY_INV_STARTED_AT,
    DT.KEY_INV_DRIVE_INDEX, DT.KEY_INV_DRIVE_ID, DT.KEY_INV_DRIVE_NAME,

    DT.KEY_PATH_STATUS, DT.KEY_PATH_ROW, DT.KEY_PATH_PROCESSED, DT.KEY_PATH_STARTED_AT,

    DT.KEY_PERM_STATUS, DT.KEY_PERM_ROW, DT.KEY_PERM_PROCESSED, DT.KEY_PERM_STARTED_AT
  ].forEach(function (k) { stateDelete_(k); });

  updateProgress_('RESET', 'RESET', '', 0, '', 'All checkpoints cleared.', '', new Date());
  updateLogMinutesSinceStart_('');
  logEvent_('RESET', 'INFO', 'RESET', 'All checkpoints cleared manually.', 0, '', '', '');
  SpreadsheetApp.getUi().alert('All checkpoints cleared.');
}

function disableAutoResumeIfNoPending_() {
  if (!hasPendingWork_()) {
    disableAutoResume();
  }
}
