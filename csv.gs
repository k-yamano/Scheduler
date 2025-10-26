/*******************************************
 * csv.gs
 * CSVのインポート・エクスポートに関するすべての機能を集約
 *******************************************/

/**
 * 複数のシートをPython入力用にCSVエクスポートします
 * (logformtest.txt の exportAllSheetsForPython 相当)
 */
function exportAllSheetsForPython() {
  try {
    LOG.info('exportAllSheetsForPython 開始');
    const ss = SpreadsheetApp.openById(CONFIG.targetSpreadsheetId);
    
    // (注: CONFIGに pythonInFolderId と SHEET_NAMES の定義が必要です)
    // config.gs に以下を追加してください:
    // pythonInFolderId: 'YOUR_PYTHON_INPUT_FOLDER_ID',
    // SHEET_NAMES: {
    //   MAIN: 'main_sheet', // (例)
    //   LOG: 'log',
    //   CAL: 'calendar'
    // },

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
      while (iter.hasNext()) iter.next().setTrashed(true); // 既存ファイル削除
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

/**
 * ★★★ 修正版 ★★★
 * 'log'シートをCSVとしてDriveにバックアップします
 * (config.gs の BACKUP_BASE_PATH 配下 /yyyyMMdd/log.csv として保存)
 */
function backupLogSheet() {
  try {
    LOG.info('logシートのバックアップ 開始');
    const ss = SpreadsheetApp.openById(CONFIG.targetSpreadsheetId);
    const sheet = getOrCreateSheet_(ss, 'log');
    const values = sheet.getDataRange().getValues();
    if (values.length <= 1) { // ヘッダーのみの場合も除外
      throw new Error('logシートが空かヘッダーのみです。');
    }

    const csv = toCsv_(values);
    
    // 1. yyyymmdd フォルダパスを作成
    const today = Utilities.formatDate(new Date(), TZ, 'yyyyMMdd');
    const folderPath = CONFIG.BACKUP_BASE_PATH + '/' + today;
    
    // 2. フォルダを取得または作成 (このファイルの下部にあるヘルパー関数)
    const folder = getOrCreateFolderByPath_(folderPath); 
    
    // 3. 'log.csv' という名前で保存
    const fileName = 'log.csv';
    
    // 4. 既存ファイルを削除 (同じ日に複数回バックアップした場合の上書き)
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

/**
 * ★★★ 修正版 ★★★
 * DriveのCSVを'log'シートに復元します（日付フォルダを指定）
 */
function restoreLogFromBackupPrompt() {
  const ui = SpreadsheetApp.getUi();
  const todayStr = Utilities.formatDate(new Date(), TZ, 'yyyyMMdd');
  const resp = ui.prompt('CSV復元', `復元したい日付（フォルダ名）を入力してください。\n(例: ${todayStr})`, ui.ButtonSet.OK_CANCEL);
  
  if (resp.getSelectedButton() !== ui.Button.OK) return;
  const dateFolder = String(resp.getResponseText() || '').trim();
  if (!dateFolder) return;

  try {
    LOG.info(`CSV復元（${dateFolder}） 開始`);
    
    // 1. フォルダパスを構築
    const folderPath = CONFIG.BACKUP_BASE_PATH + '/' + dateFolder;
    const folder = getOrCreateFolderByPath_(folderPath); // 存在確認
    if (!folder) throw new Error(`フォルダ ${folderPath} が見つかりません。`);

    // 2. フォルダ内の 'log.csv' を探す
    const fileName = 'log.csv';
    // findFileByNameInFolder_ はフォルダIDが必要
    const file = findFileByNameInFolder_(fileName, folder.getId()); 
    
    if (!file) {
      throw new Error(`フォルダ ${folderPath} 内に ${fileName} が見つかりません。`);
    }

    // 3. CSVを読み込む
    const csv = readCsvFromDrive_(file.getId());
    if (!csv || csv.length === 0) {
      throw new Error('CSVが空、または読込に失敗しました。');
    }

    // 4. logシートに復元
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
// =======================================
// 共通ユーティリティ関数（他ファイルから移植）
// =======================================

/**
 * 2次元配列 → CSV文字列（単純版）
 * (LogFrom.gs / logformtest.txt から移植)
 */
function toCsv_(values) {
  return values
    .map(row =>
      row
        .map(v => {
          const s = v === null || v === undefined ? '' : String(v);
          // ダブルクォート/カンマ/改行がある場合はクォート
          if (/[",\n]/.test(s)) {
            return `"${s.replace(/"/g, '""')}"`;
          }
          return s;
        })
        .join(',')
    )
    .join('\n');
}

/**
 * CSV読み込み（UTF-8→失敗ならShift_JIS）
 * (LogFrom.gs / logformtest.txt から移植)
 */
function readCsvFromDrive_(fileId) {
  const file = DriveApp.getFileById(fileId);
  let text = file.getBlob().getDataAsString('UTF-8');
  if (!text || text.indexOf('\uFFFD') !== -1) { // 文字化けの簡易検知
    LOG.warn(`readCsvFromDrive_: UTF-8デコード失敗 (FileID: ${fileId}). Shift_JISで再試行します。`);
    text = file.getBlob().getDataAsString('Shift_JIS');
  }
  return Utilities.parseCsv(text);
}

/**
 * シート取得（無ければ作成）
 * (LogFrom.gs / logformtest.txt から移植)
 */
function getOrCreateSheet_(ss, name) {
  if (!name || String(name).trim() === "") {
     LOG.warn(`getOrCreateSheet_: シート名が空です。`);
     return null; // またはエラーをスロー
  }
  let sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
    LOG.info(`getOrCreateSheet_: シート "${name}" を新規作成しました。`);
  }
  return sh;
}

/**
 * Driveフォルダ内でファイル名検索（最初の1件）
 * (logformtest.txt から移植)
 */
function findFileByNameInFolder_(name, folderId) {
  try {
    const folder = DriveApp.getFolderById(folderId);
    const iter = folder.getFilesByName(name);
    return iter.hasNext() ? iter.next() : null;
  } catch (e) {
    LOG.error(`findFileByNameInFolder_: フォルダ検索エラー (ID: ${folderId}, Name: ${name}). ${e.message}`);
    return null;
  }
}

/**
 * フォルダパスからフォルダを取得または作成 ( / 区切り)
 * (k-yamano/scheduler/Scheduler-d49c4aa1a460538eba9890a2a090d6093dd546cc/csv.gs から移植・修正)
 */
function getOrCreateFolderByPath_(folderPath) {
  try {
    const folders = folderPath.split('/');
    let currentFolder = DriveApp.getRootFolder();
    
    for (const folderName of folders) {
      if (!folderName) continue;
      
      const subFolders = currentFolder.getFoldersByName(folderName);
      if (subFolders.hasNext()) {
        currentFolder = subFolders.next();
      } else {
        currentFolder = currentFolder.createFolder(folderName);
        LOG.info(`  -> フォルダを作成しました: ${currentFolder.getPath()}`);
      }
    }
    return currentFolder;
  } catch (e) {
    LOG.error(`getOrCreateFolderByPath_: フォルダパス '${folderPath}' の処理に失敗. ${e.message}`);
    return null;
  }
}