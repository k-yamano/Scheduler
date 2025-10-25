/** ========= 設定 ========= */
const CONFIG = {
  // カレンダーとスプレッドシート
  calendarId: 'c_1qdjjdd90422v8qcba65oumcso@group.calendar.google.com',
  targetSpreadsheetId: SpreadsheetApp.getActive().getId(), // 現ブック
  // calendar出力の列設定
  START_COL: 2,           // B列開始
  COLS_PER_DAY: 4,        // 1日4列（コード/品名/セル/式の想定）
  MAX_ITEMS_PER_CELL: 4,  // 1日1ラインあたりの最大行

  // バックアップ出力
  outputFolderId: '13EoohP_R4zZXt5uMu_EgVR9ES8iwzBtc',
  outputFileName: 'workday.csv',

  // マスタCSV（logへ取り込み）
  MASTER_CSV_FILE_ID: '1qP30aCp4TrXmj4dS-9jn18g1zOVhLf91',   // 例: マスタ
  // 予備で別CSVを使う場合（未使用なら空でOK）
  CAPACITY_CSV_FILE_ID: '1mjbTBNOPMyFuuR288-0A-1PtXRcpyKK7',

  // タイムゾーン
  TIMEZONE: 'Asia/Tokyo'
};

// タイムゾーン（統一）
const TZ = CONFIG.TIMEZONE || Session.getScriptTimeZone() || 'Asia/Tokyo';

/** ========= 統一ロガー（絵文字なし） ========= */
const LOG = {
  info:  (m) => Logger.log(`[INFO] ${m}`),
  warn:  (m) => Logger.log(`[WARN] ${m}`),
  error: (m) => Logger.log(`[ERROR] ${m}`)
};

/** ========= logシート更新（マスタCSV取り込み） ========= */
function updateLogFromMasterCSV() {
  try {
    LOG.info('logシート更新（マスタCSV） 開始');

    const ss = SpreadsheetApp.openById(CONFIG.targetSpreadsheetId);
    const sheet = getOrCreateSheet_(ss, 'log');

    const csv = readCsvFromDrive_(CONFIG.MASTER_CSV_FILE_ID);
    if (!csv || csv.length === 0) {
      throw new Error('マスタCSVが空、または読込に失敗しました。');
    }

    // logシート初期化→貼り付け
    sheet.clear({ contentsOnly: true });
    const rows = csv.length;
    const cols = csv[0].length;
    sheet.getRange(1, 1, rows, cols).setValues(csv);

    LOG.info(`logシート更新完了（${rows}行 x ${cols}列）`);
    SpreadsheetApp.getUi().alert('logシートをマスタCSVで更新しました。');
  } catch (e) {
    LOG.error(`updateLogFromMasterCSV: ${e.message}`);
    SpreadsheetApp.getUi().alert('エラー: ' + e.message);
  }
}

/** ========= calendar出力 =========
 * 想定：
 * - calendarシートの横方向に日付を展開（B列～）
 * - 1日あたり4列（コード/品名/セル/式）
 * - 行方向にライン（line）を並べる想定
 * - サンプルでは log シートのヘッダ名を仮定（"date","line","code","productName","cell"）
 */
function outputToCalendar() {
  const CONFIG_LOCAL = {
    START_COL: CONFIG.START_COL,
    COLS_PER_DAY: CONFIG.COLS_PER_DAY,
    MAX_ITEMS_PER_CELL: CONFIG.MAX_ITEMS_PER_CELL
  };

  try {
    LOG.info('calendar出力 開始');

    const ss = SpreadsheetApp.openById(CONFIG.targetSpreadsheetId);
    const calendarSheet = getOrCreateSheet_(ss, 'calendar');
    const logSheet = getOrCreateSheet_(ss, 'log');

    // 1) logからデータ取得
    const log = readSheetAsObjects_(logSheet); // ヘッダ行ありの想定
    if (log.length === 0) {
      throw new Error('logシートにデータがありません。先に「logシート更新（マスタCSV）」を実行してください。');
    }

    // 必須カラム（date, line, code, productName, cell）を確認
    const needed = ['date', 'line', 'code', 'productName', 'cell'];
    const cols = Object.keys(log[0]);
    needed.forEach(h => {
      if (!cols.includes(h)) {
        throw new Error(`logヘッダに必要な列が見つかりません: ${h}`);
      }
    });

    // 2) 日付範囲/ライン一覧を作成
    const allDates = Array.from(new Set(log.map(r => normalizeDateKey_(r.date)))).sort();
    const lineNames = Array.from(new Set(log.map(r => String(r.line || '').trim()))).filter(Boolean).sort();

    if (allDates.length === 0) throw new Error('logに日付データがありません。');
    if (lineNames.length === 0) throw new Error('logにラインデータがありません。');

    LOG.info(`日付: ${allDates[0]} ～ ${allDates[allDates.length - 1]}（${allDates.length}日） / ライン数: ${lineNames.length}`);

    // 3) calendarシート初期化（ヘッダ/罫線/列幅）
    clearAndInitializeCalendar_(calendarSheet, allDates, lineNames, CONFIG_LOCAL);

    // 4) データを配置
    placeDataOnCalendar_(calendarSheet, allDates, lineNames, log, CONFIG_LOCAL);

    // 5) 列幅（まとめて）
    try {
      const numCols = allDates.length * CONFIG_LOCAL.COLS_PER_DAY;
      calendarSheet.setColumnWidths(CONFIG_LOCAL.START_COL, numCols, 50);
    } catch (e) {
      LOG.warn(`列幅調整をスキップ: ${e.message}`);
    }

    LOG.info('calendar出力 完了');
    SpreadsheetApp.getUi().alert('calendarへの出力が完了しました。');
  } catch (e) {
    LOG.error(`outputToCalendar: ${e.message}`);
    SpreadsheetApp.getUi().alert('エラー: ' + e.message);
  }
}

/** ========= CSV保存（log → Drive） ========= */
function backupLogToDrive() {
  try {
    LOG.info('CSV保存（log→Drive） 開始');

    const ss = SpreadsheetApp.openById(CONFIG.targetSpreadsheetId);
    const sheet = getOrCreateSheet_(ss, 'log');
    const values = sheet.getDataRange().getValues();
    if (values.length === 0 || values[0].length === 0) {
      throw new Error('logシートが空です。');
    }

    const csv = toCsv_(values);
    const folder = DriveApp.getFolderById(CONFIG.outputFolderId);
    // 既存同名ファイルがあれば削除（上書き扱い）
    const iter = folder.getFilesByName(CONFIG.outputFileName);
    while (iter.hasNext()) {
      iter.next().setTrashed(true);
    }
    folder.createFile(CONFIG.outputFileName, csv, MimeType.CSV);

    LOG.info(`CSV保存完了: ${CONFIG.outputFileName}`);
    SpreadsheetApp.getUi().alert('logシートをCSVとしてDriveに保存しました。');
  } catch (e) {
    LOG.error(`backupLogToDrive: ${e.message}`);
    SpreadsheetApp.getUi().alert('エラー: ' + e.message);
  }
}

/** ========= CSV復元（Drive → log） ========= */
function restoreLogFromBackupPrompt() {
  const ui = SpreadsheetApp.getUi();
  const resp = ui.prompt('CSV復元', 'DriveのCSVファイルIDを入力してください。', ui.ButtonSet.OK_CANCEL);
  if (resp.getSelectedButton() !== ui.Button.OK) return;
  const fileId = String(resp.getResponseText() || '').trim();
  if (!fileId) return;

  try {
    LOG.info('CSV復元（Drive→log） 開始');

    const csv = readCsvFromDrive_(fileId);
    if (!csv || csv.length === 0) {
      throw new Error('CSVが空、または読込に失敗しました。');
    }

    const ss = SpreadsheetApp.openById(CONFIG.targetSpreadsheetId);
    const sheet = getOrCreateSheet_(ss, 'log');
    sheet.clear({ contentsOnly: true });
    sheet.getRange(1, 1, csv.length, csv[0].length).setValues(csv);

    LOG.info(`CSV復元完了（${csv.length}行）`);
    ui.alert('CSVをlogシートに復元しました。');
  } catch (e) {
    LOG.error(`restoreLogFromBackupPrompt: ${e.message}`);
    ui.alert('エラー: ' + e.message);
  }
}

/** ========= シートを月順にソート（yyyy/mm 形式） ========= */
function sortSheetsByMonth() {
  try {
    LOG.info('シート月順ソート 開始');
    const ss = SpreadsheetApp.openById(CONFIG.targetSpreadsheetId);
    const sheets = ss.getSheets();

    // 「yyyy/mm」形式のシートのみ対象にし、日付に変換してソート
    const targets = sheets
      .map(s => ({ sheet: s, name: s.getName(), date: parseYearMonth_(s.getName()) }))
      .filter(x => x.date !== null)
      .sort((a, b) => a.date.getTime() - b.date.getTime());

    // 先頭から順に並び替え
    targets.forEach((x, idx) => {
      ss.setActiveSheet(x.sheet);
      ss.moveActiveSheet(idx + 1);
    });

    LOG.info(`ソート完了（対象 ${targets.length} 枚）`);
    SpreadsheetApp.getUi().alert('シートを月順に並べ替えました。');
  } catch (e) {
    LOG.error(`sortSheetsByMonth: ${e.message}`);
    SpreadsheetApp.getUi().alert('エラー: ' + e.message);
  }
}

/** ========= 内部関数群 ========= */

// シート取得（無ければ作成）
function getOrCreateSheet_(ss, name) {
  let sh = ss.getSheetByName(name);
  if (!sh) sh = ss.insertSheet(name);
  return sh;
}

// CSV読み込み（UTF-8→失敗ならShift_JIS）
function readCsvFromDrive_(fileId) {
  const file = DriveApp.getFileById(fileId);
  let text = file.getBlob().getDataAsString('UTF-8');
  if (!text || text.indexOf('\uFFFD') !== -1) { // 文字化けの簡易検知
    text = file.getBlob().getDataAsString('Shift_JIS');
  }
  return Utilities.parseCsv(text);
}

// 2次元配列→CSV文字列（単純版）
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

// yyyy/mm → Date(yyyy, mm-1, 1) 変換
function parseYearMonth_(name) {
  const m = /^(\d{4})[\/\-](\d{1,2})$/.exec(name);
  if (!m) return null;
  const y = Number(m[1]);
  const mo = Number(m[2]);
  if (y < 1900 || mo < 1 || mo > 12) return null;
  return new Date(y, mo - 1, 1);
}

// date-like を yyyy-MM-dd 文字列に正規化
function normalizeDateKey_(v) {
  if (v instanceof Date && !isNaN(v)) {
    return Utilities.formatDate(v, TZ, 'yyyy-MM-dd');
  }
  const d = new Date(String(v));
  if (!isNaN(d)) {
    return Utilities.formatDate(d, TZ, 'yyyy-MM-dd');
  }
  return String(v || '').trim();
}

// ヘッダ付きでシート→配列オブジェクト
function readSheetAsObjects_(sheet) {
  const values = sheet.getDataRange().getValues();
  if (values.length === 0) return [];
  const headers = values[0].map(h => String(h || '').trim());
  const out = [];
  for (let i = 1; i < values.length; i++) {
    const row = {};
    for (let j = 0; j < headers.length; j++) {
      row[headers[j]] = values[i][j];
    }
    out.push(row);
  }
  return out;
}

// calendarシート初期化（ヘッダ行とライン行の下地）
function clearAndInitializeCalendar_(sheet, allDates, lineNames, cfg) {
  sheet.clear({ contentsOnly: true });

  // 見出し: A列=ライン名, B列以降=日付×COLS_PER_DAY（4列単位）
  const header = ['line'];
  allDates.forEach(d => {
    // 例: 2025-10-26 → "10/26" を4列ぶん
    const mmdd = Utilities.formatDate(new Date(d), TZ, 'MM/dd');
    header.push(mmdd + ' code', mmdd + ' product', mmdd + ' cell', mmdd + ' calc');
  });

  // 必要サイズ確保
  const rows = 1 + lineNames.length + cfg.MAX_ITEMS_PER_CELL; // ヘッダー + ライン行 + 追記用の余白
  const cols = 1 + allDates.length * cfg.COLS_PER_DAY;
  if (sheet.getMaxRows() < rows) sheet.insertRowsAfter(sheet.getMaxRows(), rows - sheet.getMaxRows());
  if (sheet.getMaxColumns() < cols) sheet.insertColumnsAfter(sheet.getMaxColumns(), cols - sheet.getMaxColumns());

  // ヘッダー
  sheet.getRange(1, 1, 1, header.length).setValues([header]);

  // ライン名を1列目に
  const lineCol = lineNames.map(n => [n]);
  sheet.getRange(2, 1, lineCol.length, 1).setValues(lineCol);

  // ちょい整形
  sheet.getRange(1, 1, 1, header.length).setFontWeight('bold');
  sheet.setFrozenRows(1);
  sheet.setFrozenColumns(1);
}

// データ配置（1日4列ブロックを一括書き込み）
function placeDataOnCalendar_(sheet, allDates, lineNames, log, cfg) {
  // 索引を作る： date -> line -> items[]
  /** item = {code, productName, cell} を想定 */
  const map = {};
  for (const r of log) {
    const d = normalizeDateKey_(r.date);
    const line = String(r.line || '').trim();
    if (!d || !line) continue;
    if (!map[d]) map[d] = {};
    if (!map[d][line]) map[d][line] = [];
    map[d][line].push({
      code: String(r.code || '').trim(),
      productName: String(r.productName || '').trim(),
      cell: Number(r.cell || 0)
    });
  }

  // 書き込み
  const startCol = cfg.START_COL;
  allDates.forEach((d, i) => {
    const baseCol = startCol + i * cfg.COLS_PER_DAY; // 当日の先頭列
    lineNames.forEach((line, lnIdx) => {
      const rowTop = 2 + lnIdx; // ラインの先頭行（2行目から）
      const items = (map[d] && map[d][line]) ? map[d][line] : [];

      if (items.length === 0) {
        // 何もしない（空欄のまま）
        return;
      }

      // 行数確保（MAX_ITEMS_PER_CELL を超える可能性もあるため余裕確保）
      const needRows = rowTop + Math.max(items.length, cfg.MAX_ITEMS_PER_CELL) + 2;
      if (sheet.getMaxRows() < needRows) {
        sheet.insertRowsAfter(sheet.getMaxRows(), needRows - sheet.getMaxRows());
      }

      // まとめて書く（コード/品名/セル）
      const rowData = [];
      const formulaData = [];
      items.forEach(it => {
        rowData.push([it.code, it.productName, it.cell]);
        // 4列目の計算式（例：セル値を何らかのレートで換算）→ここではダミー例
        // 必要に応じてシート名/参照先を書き換えてください
        formulaData.push(['=IFERROR(R[0]C[-1] / 60, "")']); // cell を 60 で割る例
      });

      // 一括書き込み
      sheet.getRange(rowTop, baseCol, items.length, 3).setValues(rowData);
      sheet.getRange(rowTop, baseCol + 3, items.length, 1).setFormulasR1C1(formulaData);
    });
  });
}
