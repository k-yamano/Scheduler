/**
 * LogFrom.gs
 * * 主な機能：
 * 1. logシートのマスタCSVによる初期化（上書き）
 * 2. logシートへのマスタ情報付与（Enrich）
 * 3. 'calendar'シートへの出力 (指定フォーマット最終版 - 全カレンダー表示)
 * 4. シートのソート
 * 5. 内部ヘルパー関数
 * * (設定は config.gs、CSV入出力は csv.gs を参照)
 */


/**
 * ========= 1. logシート更新（マスタCSV取り込み・上書き） =========
 * logシートをマスタCSVの内容で完全に上書き（初期化）します。
 */
function updateLogFromMasterCSV() {
  try {
    LOG.info('logシート更新（マスタCSVで初期化・上書き） 開始');

    const ss = SpreadsheetApp.openById(CONFIG.targetSpreadsheetId);
    const sheet = getOrCreateSheet_(ss, 'log'); // csv.gs の関数

    // config.gs で定義されたマスタCSV（上書き用）を読み込む
    const csv = readCsvFromDrive_(CONFIG.MASTER_CSV_FILE_ID); // csv.gs の関数
    if (!csv || csv.length === 0) {
      throw new Error('マスタCSVが空、または読込に失敗しました。');
    }

    // logシート初期化→貼り付け
    sheet.clear({ contentsOnly: true });
    const rows = csv.length;
    // CSVが空でないことを確認してから列数を取得
    const cols = (rows > 0 && csv[0]) ? csv[0].length : 0;
    if (cols === 0) {
       LOG.warn('マスタCSVが空またはヘッダーがありません。logシートはクリアされました。');
       SpreadsheetApp.getUi().alert('警告: マスタCSVが空またはヘッダーがありません。logシートはクリアされました。');
       return;
    }
    sheet.getRange(1, 1, rows, cols).setValues(csv);

    LOG.info(`logシート初期化・上書き完了（${rows}行 x ${cols}列）`);
    SpreadsheetApp.getUi().alert('logシートをマスタCSVで更新（上書き）しました。');
  } catch (e) {
    LOG.error(`updateLogFromMasterCSV: ${e.message}`);
    SpreadsheetApp.getUi().alert('エラー: ' + e.message);
  }
}


/**
 * ========= 2. logシートへのマスタ情報付与（Enrich） ★重要★ =========
 * 「log」シートの基本情報（A-H列）に対し、
 * formulation.csv と machine.csv を読み込み、
 * log.csv と同じ形式になるよう情報を付与・計算する。
 */
function enrichLogSheetFromMasters() {
  try {
    LOG.info('logシートへのマスタ情報付与（Enrich） 開始');
    const ss = SpreadsheetApp.openById(CONFIG.targetSpreadsheetId);
    const logSheet = getOrCreateSheet_(ss, 'log'); // csv.gs の関数

    // 1. 既存のlogデータを読み込む (A-H列のみが入力されている想定)
    const logData = readSheetAsObjects_(logSheet); // このファイル内の関数
    if (logData.length === 0) {
      // alertではなくエラーをスローして処理を中断
      throw new Error('logシートが空です。先にデータを入力するか、復元してください。');
    }

    // --- 2. 必要なマスタCSVを全て読み込む ---
    const FOLDER_ID = CONFIG.MASTER_FOLDER_ID;
    const FILE_NAMES = CONFIG.MASTER_FILES;
    if (!FOLDER_ID || !FILE_NAMES || !FILE_NAMES.FORMULATION || !FILE_NAMES.MACHINE) {
      throw new Error('config.gs に FOLDER_ID または MASTER_FILES (FORMULATION, MACHINE) が設定されていません。');
    }

    // 読み込み関数
    const loadCsv = (fileName) => {
      LOG.info(`  -> マスタ検索: ${fileName}`);
      const file = findFileByNameInFolder_(fileName, FOLDER_ID); // csv.gs の関数
      if (!file) {
        throw new Error(`マスタフォルダ(ID:${FOLDER_ID})内に ${fileName} が見つかりません。`);
      }
      return readCsvFromDrive_(file.getId()); // csv.gs の関数
    };

    // 2a. formulation.csv を読み込み、'品番'をキーにしたマップを作成
    const formulationCsv = loadCsv(FILE_NAMES.FORMULATION);
    const formulationMap = new Map();
    const fHeader = formulationCsv[0];
    const fCodeIdx = fHeader.indexOf('品番'); // キー
    if (fCodeIdx < 0) throw new Error(`${FILE_NAMES.FORMULATION} に '品番' 列がありません。`);

    formulationCsv.slice(1).forEach(row => {
      const code = String(row[fCodeIdx] || '').trim();
      if (code) formulationMap.set(code, row);
    });
    LOG.info(`  -> ${FILE_NAMES.FORMULATION} から ${formulationMap.size} 件のマップを作成`);

    // 2b. machine.csv を読み込み、'品番'をキーにしたマップを作成
    const machineCsv = loadCsv(FILE_NAMES.MACHINE);
    const machineMap = new Map();
    const mHeader = machineCsv[0];
    const mCodeIdx = mHeader.indexOf('品番'); // キー
    const mLineIdx = mHeader.indexOf('ライン名'); // 取得したい値
    if (mCodeIdx < 0) throw new Error(`${FILE_NAMES.MACHINE} に '品番' 列がありません。`);
    if (mLineIdx < 0) throw new Error(`${FILE_NAMES.MACHINE} に 'ライン名' 列がありません。`);

    machineCsv.slice(1).forEach(row => {
      const code = String(row[mCodeIdx] || '').trim();
      if (code) machineMap.set(code, row[mLineIdx]); // Key: 品番, Value: ライン名
    });
    LOG.info(`  -> ${FILE_NAMES.MACHINE} から ${machineMap.size} 件のマップを作成`);


    // --- 3. 付与する列のヘッダーを定義 (log.csv と一致) ---
    const newHeaders = [
      'Account', 'code', 'productname', 'cell', 'day', 'ID', 'Timestamp', 'CellPosition',
      'Recipe', 'BulkRecipe', 'purelevel', 'preparation', 'batchsize/1', 'product/1',
      'bulk', 'requiredmaterials', 'batchsize', 'line'
    ];

    // formulation.csv のヘッダーインデックス（計算用）
    const fRecipeIdx = fHeader.indexOf('素地');
    const fBulkRecipeIdx = fHeader.indexOf('バルク');
    const fPureLevelIdx = fHeader.indexOf('製品石けん分');
    const fPreparationIdx = fHeader.indexOf('素地石けん分');
    const fBatchsize1Idx = fHeader.indexOf('素地量');
    const fProduct1Idx = fHeader.indexOf('仕込み量');
    const fBulkIdx = fHeader.indexOf('バルク液量／個(kg)');

    // ヘッダーインデックスが見つからない場合はエラー
    [fRecipeIdx, fBulkRecipeIdx, fPureLevelIdx, fPreparationIdx, fBatchsize1Idx, fProduct1Idx, fBulkIdx].forEach((idx, i) => {
        const names = ['素地', 'バルク', '製品石けん分', '素地石けん分', '素地量', '仕込み量', 'バルク液量／個(kg)'];
        if (idx < 0) throw new Error(`${FILE_NAMES.FORMULATION} に '${names[i]}' 列がありません。`);
    });


    const out = [newHeaders]; // 出力用の配列（ヘッダー行）

    // --- 4. 既存logデータにマスタ情報を結合 ---
    logData.forEach(r => {
      const code = String(r['code'] || '').trim();
      // codeが無い行もエラーにはせず、空欄で出力する
      // if (!code) return; // 必要に応じてコメント解除

      const cell = Number(r['cell'] || 0);

      const outRow = {};

      // 4a. A-H列の基本データをコピー
      newHeaders.slice(0, 8).forEach(h => {
        outRow[h] = r[h] !== undefined ? r[h] : '';
      });

      // 4b. formulation.csv から情報を付与
      const fRow = code ? formulationMap.get(code) : null; // codeが空ならnull
      if (fRow) {
        outRow['Recipe'] = fRow[fRecipeIdx];
        outRow['BulkRecipe'] = fRow[fBulkRecipeIdx];
        outRow['purelevel'] = fRow[fPureLevelIdx];
        outRow['preparation'] = fRow[fPreparationIdx];
        outRow['batchsize/1'] = fRow[fBatchsize1Idx];
        outRow['product/1'] = fRow[fProduct1Idx];
        outRow['bulk'] = fRow[fBulkIdx];

        // 4c. 計算を実行
        const batchsize1 = Number(fRow[fBatchsize1Idx] || 0);
        const product1 = Number(fRow[fProduct1Idx] || 0);

        outRow['requiredmaterials'] = cell * batchsize1;
        outRow['batchsize'] = cell * product1;

      } else {
        // マスタにない場合 (A-H列以降は空欄)
        newHeaders.slice(8).forEach(h => outRow[h] = '');
      }

      // 4d. machine.csv から 'line' を付与
      const line = code ? machineMap.get(code) : null; // codeが空ならnull
      if (line) {
        outRow['line'] = line;
      } else if (!fRow) {
          // formulationにもmachineにも情報がなければ、lineも空にする
          outRow['line'] = '';
      }

      // 4e. ヘッダーの順序通りに配列に戻す
      out.push(newHeaders.map(h => outRow[h]));
    });

    // --- 5. logシートをクリアし、マスタ付与後のデータで上書き ---
    logSheet.clear({ contentsOnly: true });
    // out配列が空（ヘッダーのみ）でないことを確認
    if (out.length > 0) {
        // ヘッダーが存在する場合、列数を取得
        const numOutputCols = out[0].length;
        if (numOutputCols > 0) {
            logSheet.getRange(1, 1, out.length, numOutputCols).setValues(out);
        } else {
             LOG.warn('enrichLogSheetFromMasters: 出力ヘッダーが空です。');
        }
    } else {
        LOG.warn('enrichLogSheetFromMasters: 出力データがありません。');
    }


    LOG.info(`logへマスタ付与（Enrich） 完了（${out.length - 1}件）`);
    SpreadsheetApp.getUi().alert('log シートの情報をマスタで更新（付与）しました。');
  } catch (e) {
    LOG.error(`enrichLogSheetFromMasters: ${e.message}\n${e.stack}`); // スタックトレースも出力
    SpreadsheetApp.getUi().alert('エラー: ' + e.message);
  }
}

// [LogFrom.gs の outputToCalendar 関数 (source: 235-251) を以下に置き換え]

/**
 * ========= 3. calendar出力（'calendar'シートへの描画） =========
 */
function outputToCalendar() { //
  const CONFIG_LOCAL = {
    START_COL: CONFIG.logFromStartCol, // 2 (B列)
    COLS_PER_DAY: CONFIG.logFromColsPerDay, // 4 (B,C,D,E)
    MAX_ITEMS_PER_CELL: CONFIG.logFromMaxItems // 4
  };
  try { //
    LOG.info('calendar出力 開始');

    const ss = SpreadsheetApp.openById(CONFIG.targetSpreadsheetId);
    const calendarSheet = getOrCreateSheet_(ss, 'calendar'); //
    const logSheet = getOrCreateSheet_(ss, 'log'); //

    // 1) logからデータ取得とライン名抽出
    const log = readSheetAsObjects_(logSheet); //
    const lineNames = Array.from(new Set(log.map(r => String(r.line || '').trim()))).filter(Boolean).sort(); //
    if (lineNames.length === 0) LOG.warn('logシートにラインデータがありません。'); //

    // 2) ★★★ 全期間の日付リストを生成 (config.monthsAhead を使用) ★★★
    const allDates = []; //
    const today = new Date(); //
    today.setHours(0,0,0,0);
    const startMonth = new Date(today.getFullYear(), today.getMonth(), 1); //
    const endMonth = new Date(today.getFullYear(), today.getMonth() + CONFIG.monthsAhead + 1, 0); //
    
    let currentDate = new Date(startMonth); //
    while (currentDate <= endMonth) { //
      allDates.push(new Date(currentDate));
      currentDate.setDate(currentDate.getDate() + 1); //
    }
    if (allDates.length === 0) throw new Error('処理対象期間の日付が取得できませんでした。'); //

    LOG.info(`カレンダー期間: ${normalizeDateKey_(allDates[0])} ～ ${normalizeDateKey_(allDates[allDates.length - 1])}（${allDates.length}日）`);
    LOG.info(`ライン数: ${lineNames.length}`); //

    // 3) calendarシート初期化（ヘッダ/罫線/列幅/データ行書式）
    clearAndInitializeCalendar_(calendarSheet, allDates, lineNames, CONFIG); //
    
    // 4) データを配置 (logデータを使用)
    placeDataOnCalendar_(calendarSheet, allDates, lineNames, log, CONFIG); //

    // 5) 列幅（まとめて）
    try {
      allDates.forEach((d, i) => {
        const baseCol = CONFIG_LOCAL.START_COL + i * CONFIG_LOCAL.COLS_PER_DAY; // B, F, J...
        if (baseCol > 0) calendarSheet.setColumnWidth(baseCol, 80); // B列 (code)
        if (baseCol + 1 > 0) calendarSheet.setColumnWidth(baseCol + 1, 150); // C列 (product)
        if (baseCol + 2 > 0) calendarSheet.setColumnWidth(baseCol + 2, 50); // D列 (cell)
        if (baseCol + 3 > 0) calendarSheet.setColumnWidth(baseCol + 3, 50); // E列 (calc)
      });
    } catch (e) {
      LOG.warn(`列幅調整をスキップ: ${e.message}`); //
    }

    LOG.info('calendar出力 完了');
      
    try {
      // 1. log.csv を Input/Master フォルダに保存 (Python 2.py 用)
      LOG.info('-> (追加処理 1/3) log.csv を Input/Master に保存中...');
      saveLogToInputMaster_(); // (csv.gs で定義した関数)

      // 2. log.csv をバックアップ (日付フォルダ)
      LOG.info('-> (追加処理 2/3) backupLogSheet (日付別バックアップ) を実行中...');
      backupLogSheet(); // (csv.gs の関数)

      // 3. log を archive に「上書き」し、log をクリア
      LOG.info('-> (追加処理 3/3) archiveLogData (アーカイブ上書き) を実行中...');
      archiveLogData(); // (Archive.gs の修正版関数)

      LOG.info('-> (追加処理) 全て完了');
      SpreadsheetApp.getUi().alert(
        'calendarへの出力が完了しました。\n\n' +
        '続けて、以下の処理を実行しました:\n' +
        '1. log.csv を Input/Master (Python用) に保存\n' +
        '2. log.csv を 日付別フォルダにバックアップ\n' +
        '3. log データを archive に上書き (logシートはクリアされました)'
      );

    } catch (e) {
        LOG.error(`outputToCalendar (追加処理中): ${e.message}\n${e.stack}`);
        SpreadsheetApp.getUi().alert(
           'calendar出力は完了しましたが、その後の追加処理 (CSV保存/アーカイブ) に失敗しました。\n\n' +
           'エラー: ' + e.message
        );
    }
    // [Source: 250] (元のアラートは上記のアラートに統合)
    
  } catch (e) {
    LOG.error(`outputToCalendar: ${e.message}\n${e.stack}`); //
    SpreadsheetApp.getUi().alert('エラー: ' + e.message);
  } //
} 

/**
 * ========= 4. シートを月順にソート（yyyy/mm 形式） =========
 */
function sortSheetsByMonth() {
  try {
    LOG.info('シート月順ソート 開始');
    const ss = SpreadsheetApp.openById(CONFIG.targetSpreadsheetId);
    const sheets = ss.getSheets();

    // 「yyyy/mm」形式のシートのみ対象にし、日付に変換してソート
    const targets = sheets
      .map(s => ({ sheet: s, name: s.getName(), date: parseYearMonth_(s.getName()) })) // このファイル内の関数
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
} // ★★★ `sortSheetsByMonth` 関数はここで終了 ★★★


/**
 * ========= 5. 内部ヘルパー関数群 =========
 */

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
  const timeZone = TZ;
  if (!timeZone) {
      LOG.error("normalizeDateKey_: Timezone (TZ) is not defined in config.gs");
      return String(v || '').trim();
  }

  if (v instanceof Date && !isNaN(v)) {
    return Utilities.formatDate(v, timeZone, 'yyyy-MM-dd');
  }
  try {
      if (typeof v === 'string' && /^\d{1,2}\/\d{1,2}$/.test(v)) {
         // 年推定ロジックは省略
      }
      const d = new Date(String(v));
      if (!isNaN(d)) {
          return Utilities.formatDate(d, timeZone, 'yyyy-MM-dd');
      }
  } catch(e) { /* パース失敗時はそのまま */ }
  return String(v || '').trim();
}


// ヘッダ付きでシート→配列オブジェクト
function readSheetAsObjects_(sheet) {
  if (!sheet) {
      LOG.error('readSheetAsObjects_: sheet is null or undefined');
      return [];
  }
  const values = sheet.getDataRange().getValues();
  if (values.length <= 1) return []; // ヘッダーのみ、または空の場合
  const headers = values[0].map(h => String(h || '').trim());
  const out = [];
  for (let i = 1; i < values.length; i++) {
    const row = {};
    const rowLength = values[i].length;
    for (let j = 0; j < headers.length; j++) {
      if (headers[j]) {
         row[headers[j]] = (j < rowLength) ? values[i][j] : '';
      }
    }
    if (Object.values(row).some(val => val !== '' && val !== null && val !== undefined)) {
        out.push(row);
    }
  }
  return out;
}


// calendarシート初期化（'calendar'シート出力用ヘルパー）
// (★★★ 修正版：ヘッダー列結合のみ、データ行書式設定、A4テキスト、上枠線追加 ★★★)
function clearAndInitializeCalendar_(sheet, allDates, lineNames, cfg) {
  sheet.clear(); // 完全クリア

  const tz = TZ;
  const calendar = cfg.calendarId ? CalendarApp.getCalendarById(cfg.calendarId) : null;

  if (!calendar) {
    LOG.warn('clearAndInitializeCalendar_: カレンダーIDが未設定。公休情報は取得できません');
  }
  if (!tz) {
      LOG.error('clearAndInitializeCalendar_: タイムゾーン(TZ)が未設定。日付フォーマットに失敗します。');
  }

  // --- 公休イベント取得 ---
  const eventsMap = {};
  if (calendar && allDates.length > 0) {
    const start = allDates[0];
    const end = new Date(allDates[allDates.length - 1]);
    end.setHours(23, 59, 59, 999);
    try {
      calendar.getEvents(start, end).forEach(event => {
        if (event.getTitle().includes('公休')) {
          let current = new Date(event.getStartTime());
          const eveEnd = new Date(event.getEndTime());
          if (event.isAllDayEvent()) {
            eveEnd.setMilliseconds(eveEnd.getMilliseconds() - 1);
          }
          while (current <= eveEnd) {
            const dateKey = normalizeDateKey_(current);
            eventsMap[dateKey] = event.getTitle();
            current.setDate(current.getDate() + 1);
          }
        }
      });
      LOG.info(`公休イベント: ${Object.keys(eventsMap).length}件`);
    } catch (e) {
      LOG.error(`カレンダー取得エラー: ${e.message}\n${e.stack}`);
    }
  }

  // --- ヘッダー4行作成 (1日4列 B,C,D,E) ---
  const header1_date = [''];  // A1
  const header2_day = [''];   // A2
  const header3_event = ['']; // A3
  const header4_text = ['ライン／備考']; // ★★★ A4にテキスト設定 ★★★

  const daysOfWeek = ['日', '月', '火', '水', '木', '金', '土'];

  allDates.forEach(d => {
    const mmdd = Utilities.formatDate(d, tz, 'MM/dd');
    const dow = daysOfWeek[d.getDay()];
    const dateKey = normalizeDateKey_(d);

    // 各日の1列目 (B, F, J...)
    header1_date.push(mmdd);
    header2_day.push(dow);
    header3_event.push(eventsMap[dateKey] || '');
    header4_text.push(''); // B4, F4, J4... は空欄

    // 残り3列 (C,D,E / G,H,I...) - ヘッダー用は空欄
    for (let i = 1; i < cfg.logFromColsPerDay; i++) {
      header1_date.push('');
      header2_day.push('');
      header3_event.push('');
      header4_text.push('');
    }
  });

  const headerRows = [header1_date, header2_day, header3_event, header4_text]; // header4_blank -> header4_text
  const numCols = 1 + allDates.length * cfg.logFromColsPerDay; // A列 + (日付 * 4列)

  // --- シートサイズ確保 ---
  const rowsPerLine = 1 + 3; // ライン名1行 + 空行3行
  const requiredRows = 4 + (lineNames.length * rowsPerLine) + cfg.logFromMaxItems;
  const minRows = 4 + (lineNames.length > 0 ? lineNames.length * rowsPerLine : rowsPerLine) + cfg.logFromMaxItems;
  const minCols = 1 + (allDates.length > 0 ? allDates.length * cfg.logFromColsPerDay : 10);
  const finalRows = Math.max(requiredRows, minRows);
  const finalCols = Math.max(numCols, minCols);

  if (sheet.getMaxRows() < finalRows) {
    sheet.insertRowsAfter(sheet.getMaxRows(), finalRows - sheet.getMaxRows());
  }
  if (sheet.getMaxColumns() < finalCols) {
    sheet.insertColumnsAfter(sheet.getMaxColumns(), finalCols - sheet.getMaxColumns());
  }

  // --- ヘッダー書き込み ---
  if (numCols > 1) {
    sheet.getRange(1, 1, 4, numCols).setValues(headerRows);
  } else {
    sheet.getRange(1, 1, 4, 1).setValues(headerRows.map(row => [row[0]]));
    Logger.log('警告: 日付データがないため、A列ヘッダーのみ書き込み');
  }

  // --- ヘッダー書式 ---
  const writtenCols = Math.max(numCols, 1);
  const headerRange = sheet.getRange(1, 1, 4, writtenCols);
  headerRange.setFontWeight('bold')
             .setHorizontalAlignment('center')
             .setVerticalAlignment('middle');

  // --- ヘッダー列結合と背景色 ---
  allDates.forEach((d, i) => {
    const baseCol = cfg.logFromStartCol + i * cfg.logFromColsPerDay; // B, F, J...
    if (baseCol < 1) return;

    const dowNum = d.getDay();
    let bgColor = '#ffffff'; // デフォルト白

    const dateKey = normalizeDateKey_(d);
    if (eventsMap[dateKey]) {
      bgColor = '#f4cccc'; // 薄い赤 (公休)
    } else if (dowNum === 0) {
      bgColor = '#ea9999'; // 赤 (日曜)
    } else if (dowNum === 6) {
      bgColor = '#9fc5e8'; // 青 (土曜)
    }

    try {
      // ★★★ 列方向のみ結合 (行は結合しない) ★★★
      for (let r = 1; r <= 4; r++) {
         const range = sheet.getRange(r, baseCol, 1, cfg.logFromColsPerDay); // 1行 x 4列
         range.merge();
      }
      // 背景色は結合した範囲全体に設定
      sheet.getRange(1, baseCol, 4, cfg.logFromColsPerDay).setBackground(bgColor);

    } catch (e) {
      Logger.log(`ヘッダー結合/背景色エラー (列${baseCol}): ${e.message}`);
    }
  });

  // --- ライン名をA列に (5行目から4行ごと) ---
  if (lineNames.length > 0) {
    lineNames.forEach((lineName, index) => {
      const lineRow = 5 + index * rowsPerLine;
      sheet.getRange(lineRow, 1).setValue(lineName).setFontWeight('bold');
    });
  }

  // --- データ行の書式設定 ---
  if (allDates.length > 0 && lineNames.length > 0) {
     const dataStartRow = 5;
     // ★★★ 最終行を計算 ★★★
     const dataEndRow = 4 + lineNames.length * rowsPerLine; // 最後のラインの最後の空行まで
     const dataEndCol = 1 + allDates.length * cfg.logFromColsPerDay; // A列 + データ列の最後まで

     for(let i = 0; i < allDates.length; i++) {
        const baseCol = cfg.logFromStartCol + i * cfg.logFromColsPerDay; // B, F, J...
        if (baseCol < 1) continue;

        // 中央2列 (C, G, K... and D, H, L...) に背景色
        const middleCol1 = baseCol + 1; // C, G, K...
        const middleCol2 = baseCol + 2; // D, H, L...
        if (middleCol1 < dataEndCol) {
            sheet.getRange(dataStartRow, middleCol1, dataEndRow - dataStartRow + 1, 1).setBackground('#fff2cc'); // 薄い黄色
        }
        if (middleCol2 < dataEndCol) {
            sheet.getRange(dataStartRow, middleCol2, dataEndRow - dataStartRow + 1, 1).setBackground('#fff2cc'); // 薄い黄色
        }

        // 4列目 (E, I, M...) に右枠線
        const borderCol = baseCol + 3; // E, I, M...
        if (borderCol < dataEndCol) {
            sheet.getRange(dataStartRow, borderCol, dataEndRow - dataStartRow + 1, 1)
                 .setBorder(null, null, null, true, null, null, '#000000', SpreadsheetApp.BorderStyle.SOLID_MEDIUM); // 右枠線のみ太線
        }
     }

     // ★★★ 上枠線を追加 (9, 13, 17...) ★★★
     for (let i = 1; i < lineNames.length; i++) { // 2番目のラインから
         const borderRow = 5 + i * rowsPerLine; // 9, 13, 17...
         if (borderRow <= sheet.getMaxRows()) {
             // A列から最終列まで
             sheet.getRange(borderRow, 1, 1, dataEndCol)
                  .setBorder(true, null, null, null, null, null, '#000000', SpreadsheetApp.BorderStyle.SOLID); // 上枠線のみ
         }
     }
  }

  // --- 固定 ---
  sheet.setFrozenRows(4);
  sheet.setFrozenColumns(1);

  Logger.log("カレンダー初期化完了");
}

// データ配置（'calendar'シート出力用ヘルパー）

function placeDataOnCalendar_(sheet, allDates, lineNames, logData, cfg) { // cfg には CONFIG オブジェクト全体が渡される想定
  Logger.log("データ配置開始...");

  // データ構造化: dateKey -> line -> items[]
  const map = {};
  const allDateKeys = allDates.map(d => normalizeDateKey_(d));

  for (const r of logData) {
    const dKey = normalizeDateKey_(r.day);
    const line = String(r.line || '').trim();

    if (!dKey || !line) continue;
    if (!allDateKeys.includes(dKey)) continue; // 範囲外の日付は除外

    if (!map[dKey]) map[dKey] = {};
    if (!map[dKey][line]) map[dKey][line] = [];

    map[dKey][line].push({
      code: String(r.code || '').trim(),
      productName: String(r.productname || '').trim(),
      cell: Number(r.cell || 0)
    });
  }

  // --- データ書き込み ---
  let totalWritten = 0;
  const rowsPerLine = 1 + 3; // ライン名1行 + 空行3行

  allDateKeys.forEach((dKey, dateIndex) => {
    const baseCol = cfg.logFromStartCol + dateIndex * cfg.logFromColsPerDay; // B, F, J...
    if (baseCol < 1) return;

    lineNames.forEach((line, lineIndex) => {
      // ★★★ 書き込み開始行を計算: 5, 9, 13 ... ★★★
      const rowTop = 5 + lineIndex * rowsPerLine;

      const items = (map[dKey] && map[dKey][line]) ? map[dKey][line] : [];
      if (items.length === 0) return;

      // 行数確保 (最大アイテム数分は確保されているはずだが念のため)
      const needRows = rowTop + Math.max(items.length, cfg.logFromMaxItems); // 最大アイテム数までの行を考慮
      try {
        if (sheet.getMaxRows() < needRows) {
           sheet.insertRowsAfter(sheet.getMaxRows(), needRows - sheet.getMaxRows());
           LOG.warn(`行を追加挿入しました (Row: ${rowTop}, Need: ${needRows})`);
        }
      } catch (e) {
        LOG.error(`行挿入エラー (行${rowTop}): ${e.message}`);
        return; // このライン/日付の書き込みはスキップ
      }

      // データ配列作成 (最大アイテム数を超える場合は切り捨てる)
      const itemsToWrite = items.slice(0, cfg.logFromMaxItems);
      const rowData = []; // B, C, D列用
      const formulaData = []; // E列用

      itemsToWrite.forEach(it => {
        // B, C, D列
        rowData.push([it.code, it.productName, it.cell]);
        // E列 (計算式)
        formulaData.push(['=IFERROR((${cellCell} / VLOOKUP(${codeCell}, calculation!$B:$L, 6, FALSE) / 60), "")']);
      });

      // 一括書き込み
      if (itemsToWrite.length > 0) {
        try {
          // B,C,D列 (3列)
          sheet.getRange(rowTop, baseCol, itemsToWrite.length, 3).setValues(rowData);

          // E列 (1列)
          sheet.getRange(rowTop, baseCol + 3, itemsToWrite.length, 1).setFormulasR1C1(formulaData);

          totalWritten += itemsToWrite.length;
        } catch (e) {
          LOG.error(`データ書き込みエラー (行${rowTop}, 列${baseCol}): ${e.message}\n${e.stack}`);
        }
      }
    });
  });

  Logger.log(`データ配置完了: ${totalWritten}件`);
}