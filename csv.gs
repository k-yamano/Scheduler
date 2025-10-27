/*******************************************
 * csv.gs
 * CSVのインポート・エクスポートに関するすべての機能を集約
 *******************************************/

function exportAllSheetsForPython() {
  try {
    LOG.info('exportAllSheetsForPython 開始');
    const ss = SpreadsheetApp.openById(CONFIG.targetSpreadsheetId);

    if (!CONFIG.pythonInFolderId || !CONFIG.SHEET_NAMES) {
      throw new Error('config.gsに pythonInFolderId または SHEET_NAMES が設定されていません。');
    }

    const shMain = getOrCreateSheet_(ss, CONFIG.SHEET_NAMES.MAIN);
    const shLog  = getOrCreateSheet_(ss, CONFIG.SHEET_NAMES.LOG);
    const shCal  = getOrCreateSheet_(ss, CONFIG.SHEET_NAMES.CAL);

    const folder = DriveApp.getFolderById(CONFIG.pythonInFolderId);
    const tasks = [
      { sheet: shMain, name: 'main_sheet.csv' },
      { sheet: shLog,  name: 'log.csv' },
      { sheet: shCal,  name: 'calendar.csv' }
    ];

    tasks.forEach(t => {
      const values = t.sheet.getDataRange().getValues();
      const csv = toCsv_(values);
      const iter = folder.getFilesByName(t.name);
      while (iter.hasNext()) iter.next().setTrashed(true);
      folder.createFile(t.name, csv, MimeType.CSV);
      LOG.info(`  -> ${t.name} を出力完了 (${values.length}行)`);
    });

    LOG.info('exportAllSheetsForPython 完了');
    SpreadsheetApp.getUi().alert('main/log/calendar を Python入力フォルダにCSV出力しました。');
  } catch (e) {
    LOG.error(`exportAllSheetsForPython: ${e.message}`);
    SpreadsheetApp.getUi().alert('エラー: ' + e.message);
  }
}

function backupLogSheet() {
  try {
    LOG.info('logシートのバックアップ 開始');
    const ss = SpreadsheetApp.openById(CONFIG.targetSpreadsheetId);
    const sheet = getOrCreateSheet_(ss, 'log');
    const values = sheet.getDataRange().getValues();
    if (values.length <= 1) throw new Error('logシートが空かヘッダーのみです。');

    const csv = toCsv_(values);
    const tz  = Session.getScriptTimeZone();
    const today = Utilities.formatDate(new Date(), tz, 'yyyyMMdd');
    const folderPath = CONFIG.BACKUP_BASE_PATH + '/' + today;

    const folder = getOrCreateFolderByPath_(folderPath);
    const fileName = 'log.csv';
    const iter = folder.getFilesByName(fileName);
    while (iter.hasNext()) iter.next().setTrashed(true);
    folder.createFile(fileName, csv, MimeType.CSV);

    LOG.info(`バックアップ完了: ${folderPath}/${fileName}`);
    SpreadsheetApp.getUi().alert(`logシートをDriveにバックアップしました:\n${folderPath}/${fileName}`);
  } catch (e) {
    LOG.error(`backupLogSheet: ${e.message}`);
    SpreadsheetApp.getUi().alert('エラー: ' + e.message);
  }
}

function restoreLogFromBackupPrompt() {
  const ui = SpreadsheetApp.getUi();
  const tz = Session.getScriptTimeZone();
  const todayStr = Utilities.formatDate(new Date(), tz, 'yyyyMMdd');
  const resp = ui.prompt('CSV復元', `復元したい日付（フォルダ名）を入力してください。\n(例: ${todayStr})`, ui.ButtonSet.OK_CANCEL);
  if (resp.getSelectedButton() !== ui.Button.OK) return;
  const dateFolder = String(resp.getResponseText() || '').trim();
  if (!dateFolder) return;

  try {
    LOG.info(`CSV復元（${dateFolder}） 開始`);
    const folderPath = CONFIG.BACKUP_BASE_PATH + '/' + dateFolder;
    const folder = getOrCreateFolderByPath_(folderPath);
    if (!folder) throw new Error(`フォルダ ${folderPath} が見つかりません。`);

    const fileName = 'log.csv';
    const file = findFileByNameInFolder_(fileName, folder.getId());
    if (!file) throw new Error(`フォルダ ${folderPath} 内に ${fileName} が見つかりません。`);

    const csv = readCsvFromDrive_(file.getId());
    if (!csv || csv.length === 0) throw new Error('CSVが空、または読込に失敗しました。');

    const ss = SpreadsheetApp.openById(CONFIG.targetSpreadsheetId);
    const sheet = getOrCreateSheet_(ss, 'log');
    sheet.clear({ contentsOnly: true });
    sheet.getRange(1, 1, csv.length, csv[0].length).setValues(csv);

    LOG.info(`CSV復元完了（${csv.length}行）`);
    ui.alert(`${dateFolder} の ${fileName} をlogシートに復元しました。`);
  } catch (e) {
    LOG.error(`restoreLogFromBackupPrompt: ${e.message}`);
    ui.alert('エラー: ' + e.message);
  }
}

// =============== 追加：workday.csv 出力（calendar から移管） ===============
/**
 * 稼働日(Date[])を "MM/dd" のCSVにして、
 * CONFIG.calendarOutputFolderId / CONFIG.calendarOutputFileName に保存
 * @param {Date[]} workingDays
 * @param {string} tz 可変：指定なければスクリプトTZ
 */
function exportWorkdaysCsvDates(workingDays, tz) {
  if (!CONFIG.calendarOutputFolderId) throw new Error('CONFIG.calendarOutputFolderId が未設定です。');
  if (!CONFIG.calendarOutputFileName) throw new Error('CONFIG.calendarOutputFileName が未設定です。');
  if (!workingDays || workingDays.length === 0) {
    LOG.info('exportWorkdaysCsvDates: 稼働日データなし→出力スキップ');
    return;
  }

  const tzUse = tz || Session.getScriptTimeZone();
  const formatted = workingDays.map(d => Utilities.formatDate(d, tzUse, 'MM/dd'));
  const csvContent = formatted.join(',');

  let outputFolder;
  try {
    outputFolder = DriveApp.getFolderById(CONFIG.calendarOutputFolderId);
  } catch (e) {
    throw new Error(`フォルダID "${CONFIG.calendarOutputFolderId}" が無効かアクセス権なし: ${e.message}`);
  }

  const name = CONFIG.calendarOutputFileName;
  const files = outputFolder.getFilesByName(name);
  while (files.hasNext()) {
    const f = files.next();
    LOG.info(`既存ファイル "${name}" (ID: ${f.getId()}) を削除`);
    f.setTrashed(true);
  }
  const newFile = outputFolder.createFile(name, csvContent, MimeType.CSV);
  LOG.info(`✓ 稼働日CSV作成: "${name}" (ID: ${newFile.getId()}) in ${outputFolder.getName()}`);
}

// =======================================
// 共通ユーティリティ（既存）
// =======================================
function toCsv_(values) {
  return values
    .map(row => row
      .map(v => {
        const s = v === null || v === undefined ? '' : String(v);
        if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
        return s;
      })
      .join(','))
    .join('\n');
}

function readCsvFromDrive_(fileId) {
  const file = DriveApp.getFileById(fileId);
  let text = file.getBlob().getDataAsString('UTF-8');
  if (!text || text.indexOf('\uFFFD') !== -1) {
    LOG.warn(`readCsvFromDrive_: UTF-8失敗。Shift_JISで再試行 (FileID: ${fileId})`);
    text = file.getBlob().getDataAsString('Shift_JIS');
  }
  return Utilities.parseCsv(text);
}

function getOrCreateSheet_(ss, name) {
  if (!name || String(name).trim() === '') {
    LOG.warn('getOrCreateSheet_: シート名が空');
    return null;
  }
  let sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
    LOG.info(`getOrCreateSheet_: シート "${name}" 作成`);
  }
  return sh;
}

function findFileByNameInFolder_(name, folderId) {
  try {
    const folder = DriveApp.getFolderById(folderId);
    const iter = folder.getFilesByName(name);
    return iter.hasNext() ? iter.next() : null;
  } catch (e) {
    LOG.error(`findFileByNameInFolder_: エラー (ID: ${folderId}, Name: ${name}). ${e.message}`);
    return null;
  }
}

function getOrCreateFolderByPath_(folderPath) {
  try {
    const folders = folderPath.split('/');
    let currentFolder = DriveApp.getRootFolder();
    for (const folderName of folders) {
      if (!folderName) continue;
      const subFolders = currentFolder.getFoldersByName(folderName);
      currentFolder = subFolders.hasNext() ? subFolders.next() : currentFolder.createFolder(folderName);
    }
    return currentFolder;
  } catch (e) {
    LOG.error(`getOrCreateFolderByPath_: 失敗 '${folderPath}'. ${e.message}`);
    return null;
  }
}

function saveLogToInputMaster_() {
  try {
    LOG.info('saveLogToInputMaster_ 開始');
    const ss = SpreadsheetApp.openById(CONFIG.targetSpreadsheetId);
    const sheet = getOrCreateSheet_(ss, 'log');
    const values = sheet.getDataRange().getValues();
    if (values.length <= 1) throw new Error('logシートが空かヘッダーのみです。');

    const headers = values[0].map(h => String(h || '').trim());
    const dayIdx = headers.indexOf('day');
    const tsIdx  = headers.indexOf('Timestamp');
    const tz = Session.getScriptTimeZone();

    const formatted = values.map((row, rowIndex) => {
      if (rowIndex === 0) return row;
      const newRow = row.slice();
      if (dayIdx >= 0 && newRow[dayIdx] instanceof Date) newRow[dayIdx] = Utilities.formatDate(newRow[dayIdx], tz, 'yyyy/MM/dd');
      if (tsIdx  >= 0 && newRow[tsIdx]  instanceof Date) newRow[tsIdx]  = Utilities.formatDate(newRow[tsIdx],  tz, 'yyyy/MM/dd HH:mm:ss');
      return newRow;
    });

    const csv = toCsv_(formatted);
    const folderPath = 'dp_Scheduler/Input/Master';
    const folder = getOrCreateFolderByPath_(folderPath);
    if (!folder) throw new Error(`フォルダパス ${folderPath} が見つかりません。`);

    const fileName = 'log.csv';
    const iter = folder.getFilesByName(fileName);
    while (iter.hasNext()) iter.next().setTrashed(true);
    folder.createFile(fileName, csv, MimeType.CSV);

    LOG.info(`  -> ${folderPath}/${fileName} に log.csv を保存しました。`);
  } catch (e) {
    LOG.error(`saveLogToInputMaster_: ${e.message}`);
    throw e;
  }
}