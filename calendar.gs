const ODD_ROW_COLOR = '#e6f4ea'; // 薄い緑

// ========== ユーティリティ ==========

/**
 * 処理対象月とその範囲、シート名を取得
 */
function _getMonthsToProcess() {
  const today = new Date();
  const ranges = [];
  for (let i = 0; i <= CONFIG.monthsAhead; i++) {
    const targetMonth = new Date(today.getFullYear(), today.getMonth() + i, 1);
    const year = targetMonth.getFullYear();
    const month = targetMonth.getMonth() + 1; // 1-12
    const startDate = new Date(year, month - 1, 1);
    const endDate = new Date(year, month, 0); // 月末日
    const monthStr = month < 10 ? '0' + month : '' + month; // ゼロ埋め
    const sheetName = year + '/' + monthStr; // yyyy/mm 形式
    ranges.push({ sheetName, start: startDate, end: endDate });
  }
  return ranges;
}

/**
 * Dateオブジェクト配列から重複を除去しソート
 */
function _getUniqueSortedDates(dates) {
  if (!dates || dates.length === 0) return [];
  const uniqueTimestamps = {};
  dates.forEach(date => { uniqueTimestamps[date.getTime()] = date; });
  const uniqueDates = Object.values(uniqueTimestamps);
  uniqueDates.sort((a, b) => a.getTime() - b.getTime()); // 日付昇順
  return uniqueDates;
}

// ========== メイン処理 (手動実行用) ==========

/**
 * メイン関数: カレンダー更新、在庫計算、書式設定、CSV出力を実行
 */
function updateCalendar() { // 旧: exportCalendarToSheetManual
  const monthlyRanges = _getMonthsToProcess();
  const ss = SpreadsheetApp.openById(CONFIG.targetSpreadsheetId);
  const tz = ss.getSpreadsheetTimeZone(); // タイムゾーン取得

  Logger.log('処理対象シート(yyyy/mm): ' + monthlyRanges.map(r => r.sheetName).join(', '));

  const today = new Date(); today.setHours(0, 0, 0, 0);
  let allWorkingDays = []; // 全期間の稼働日(Date)を収集

  // --- 1) カレンダー描画ループ ---
  Logger.log('\n--- 1. カレンダー描画開始 ---');
  for (let i = 0; i < monthlyRanges.length; i++) {
    const range = monthlyRanges[i];
    const isFirstSheet = (i === 0);
    const currentSheet = ss.getSheetByName(range.sheetName);

    if (!currentSheet) {
      Logger.log(`!!! スキップ: シート "${range.sheetName}" が見つかりません。`);
      continue;
    }
    Logger.log(`-> 処理中: ${range.sheetName}`);

    // カレンダー描画処理を実行し、稼働日リスト(Date[])を取得
    const { workingDays } = _drawCalendar(currentSheet, range.start, range.end, isFirstSheet, today, tz);

    if (workingDays && workingDays.length > 0) {
      allWorkingDays = allWorkingDays.concat(workingDays);
    }
    Logger.log(`${range.sheetName} のカレンダー描画完了。`);
  }

  // --- 2) 在庫予測計算 ---
  Logger.log('\n--- 2. 在庫予測計算 開始 ---');
  try {
    calculateInventory(); // 旧: processMonthlySheets
    Logger.log('✓ 在庫予測計算 完了');
  } catch (e) {
    Logger.log(`!!! 在庫予測計算エラー: ${e.message}\n${e.stack}`);
    SpreadsheetApp.getUi().alert('警告', `カレンダー描画は完了しましたが、在庫予測計算に失敗しました。\nエラー: ${e.message}`, SpreadsheetApp.getUi().ButtonSet.OK);
    // 続行
  }

  // --- 3) 奇数行の背景色設定 ---
  Logger.log('\n--- 3. 奇数行 背景色設定 開始 ---');
  monthlyRanges.forEach(range => {
    const currentSheet = ss.getSheetByName(range.sheetName);
    if (currentSheet) _formatOddRows(currentSheet);
  });
  Logger.log('✓ 奇数行 背景色設定 完了');

  // --- 4) 稼働日CSV出力 ---
  Logger.log('\n--- 4. 稼働日CSV出力 開始 ---');
  try {
    const uniqueSortedWorkingDays = _getUniqueSortedDates(allWorkingDays);
    _exportWorkdaysCsv(uniqueSortedWorkingDays, tz); // タイムゾーンを渡す
    Logger.log('✓ 稼働日CSV出力 完了');
  } catch (e) {
    Logger.log(`!!! CSV出力エラー: ${e.message}\n${e.stack}`);
    SpreadsheetApp.getUi().alert('警告', `シート更新は完了しましたが、CSV出力に失敗しました。\nエラー: ${e.message}`, SpreadsheetApp.getUi().ButtonSet.OK);
    return; // CSV出力失敗時はここで終了
  }

  Logger.log('\n--- 全処理 正常終了 ---');
  SpreadsheetApp.getUi().alert('完了', 'カレンダー更新、在庫予測計算、背景色設定、CSV出力が完了しました', SpreadsheetApp.getUi().ButtonSet.OK);
}

// ========== 背景色設定 ==========

/**
 * 指定シートの5行目以降、G列以降の奇数行に背景色を設定
 */
function _formatOddRows(sheet) { // 旧: applyOddRowFormatting
  try {
    const startRow = 5; // 5行目から
    const startCol = CONFIG.calendarStartCol; // G列
    const lastRow = sheet.getLastRow();
    const lastCol = sheet.getLastColumn();

    if (lastRow < startRow || lastCol < startCol) {
      Logger.log(`${sheet.getName()}: 背景色スキップ (データ範囲 ${startRow}行/${startCol}列 未満)`);
      return;
    }

    const numRows = lastRow - startRow + 1;
    const numCols = lastCol - startCol + 1;
    const range = sheet.getRange(startRow, startCol, numRows, numCols);

    // 新しい背景色の2次元配列を作成
    const newBackgrounds = Array.from({ length: numRows }, (_, r) => {
      const sheetRow = startRow + r; // シート上の行番号 (5, 6, 7...)
      // 行番号が奇数なら ODD_ROW_COLOR, 偶数なら null (デフォルト色)
      const rowColor = (sheetRow % 2 !== 0) ? ODD_ROW_COLOR : null;
      return Array(numCols).fill(rowColor);
    });

    range.setBackgrounds(newBackgrounds); // 一括適用
    Logger.log(`${sheet.getName()}: 奇数行 (${startRow}行目以降, G列以降) 背景色設定 完了`);

  } catch (e) {
    Logger.log(`!!! 背景色設定エラー (${sheet.getName()}): ${e.message}`);
  }
}

// ========== カレンダー描画 ==========

/**
 * 指定シートのG列以降にカレンダーヘッダを描画し、稼働日リストを返す
 */
function _drawCalendar(sheet, startDate, endDate, isFirstSheet, today, tz) { // 旧: exportCalendarToSheet
  let workingDaysForCsv = []; // 稼働日(Date)リスト

  try {
    // G列以降クリア
    Logger.log(`${sheet.getName()}: G列以降クリア開始`);
    const lastCol = sheet.getLastColumn();
    if (lastCol >= CONFIG.calendarStartCol) {
      sheet.getRange(1, CONFIG.calendarStartCol, sheet.getMaxRows(), lastCol - CONFIG.calendarStartCol + 1)
           .clear({ contentsOnly: true, formatOnly: false }); // 値のみクリア
      Logger.log(`${sheet.getName()}: G列～${lastCol}列 クリア完了`);
    }

    // カレンダーイベント取得
    const calendar = CalendarApp.getCalendarById(CONFIG.calendarId);
    const start = new Date(startDate); start.setHours(0,0,0,0);
    const end   = new Date(endDate);   end.setHours(23,59,59,999);
    const events = calendar.getEvents(start, end);

    // 全日付配列生成
    const dates = [];
    const current = new Date(start);
    while (current <= end) { dates.push(new Date(current)); current.setDate(current.getDate() + 1); }
    Logger.log(`${sheet.getName()}: ${dates.length}日分のカレンダー生成`);

    // 公休判定
    const holidayIndices = new Set(); // Setで重複を管理
    dates.forEach((date, idx) => {
      const dateStart = new Date(date); dateStart.setHours(0,0,0,0);
      const dateEnd   = new Date(date); dateEnd.setHours(23,59,59,999);
      events.forEach(event => {
        if (event.getTitle().includes('公休')) {
          const es = event.getStartTime(); const ee = event.getEndTime();
          const isAllDay = event.isAllDayEvent();
          let hit = false;
          if (isAllDay) {
            const eEndDate = new Date(ee); eEndDate.setDate(eEndDate.getDate() - 1); eEndDate.setHours(23,59,59,999);
            hit = (es <= dateEnd && eEndDate >= dateStart);
          } else { hit = (es <= dateEnd && ee >= dateStart); }
          if (hit) holidayIndices.add(idx); // Setに追加
        }
      });
    });
    Logger.log(`${sheet.getName()}: 公休日数: ${holidayIndices.size}`);

    // 公休以外の日付(稼働日)を抽出
    const filteredDates = dates.filter((_, idx) => !holidayIndices.has(idx));
    if (filteredDates.length === 0) {
      Logger.log(`${sheet.getName()}: 公休除外後に稼働日なし → 描画スキップ`);
      return { workingDays: [] }; // 稼働日リストは空
    }
    workingDaysForCsv = filteredDates; // CSV用リスト(Date[])
    Logger.log(`${sheet.getName()}: 公休除外後の日数: ${filteredDates.length}`);

    // 今日の列インデックス検索 (最初のシートのみ)
    let todayColIndex = -1; // 0-based index relative to filteredDates
    if (isFirstSheet) {
      todayColIndex = filteredDates.findIndex(d =>
        d.getFullYear() === today.getFullYear() &&
        d.getMonth()    === today.getMonth()    &&
        d.getDate()     === today.getDate()
      );
      Logger.log(`${sheet.getName()}` + (todayColIndex !== -1
        ? `: 今日列検出 → ${todayColIndex + CONFIG.calendarStartCol}列目`
        : ': 今日が範囲外または公休'));
    }

    // ヘッダー行データ作成 (yyyy/MM, 日, 曜日)
    const monthRow = [], dateRow = [], dayRow = [];
    let lastMonth = '', monthStartColIdx = 0; const monthRanges = []; // 月ごとの範囲情報
    filteredDates.forEach((d, idx) => {
      const m = Utilities.formatDate(d, tz, 'yyyy/MM'); // タイムゾーン指定
      if (m !== lastMonth) {
        if (lastMonth !== '') monthRanges.push({ startIdx: monthStartColIdx, endIdx: idx - 1, month: lastMonth });
        lastMonth = m; monthStartColIdx = idx;
      }
      monthRow.push(m);
      dateRow.push(d.getDate());
      dayRow.push(['日','月','火','水','木','金','土'][d.getDay()]);
    });
    monthRanges.push({ startIdx: monthStartColIdx, endIdx: filteredDates.length - 1, month: lastMonth });

    // ヘッダー書き込み (3行分)
    if (filteredDates.length > 0) {
      sheet.getRange(1, CONFIG.calendarStartCol, 3, filteredDates.length)
           .setValues([monthRow, dateRow, dayRow]);
      Logger.log(`${sheet.getName()}: ヘッダー3行 書き込み完了`);
    }

    // 書式設定
    monthRanges.forEach(r => { // 月ヘッダー結合と書式
      const startCol = r.startIdx + CONFIG.calendarStartCol;
      const endCol   = r.endIdx   + CONFIG.calendarStartCol;
      if (endCol >= startCol) {
        if (endCol > startCol) sheet.getRange(1, startCol, 1, endCol - startCol + 1).merge();
        sheet.getRange(1, startCol).setBackground('#4285f4').setFontColor('#fff').setFontWeight('bold').setHorizontalAlignment('center');
      }
    });
    if (filteredDates.length > 0) {
      sheet.getRange(2, CONFIG.calendarStartCol, 1, filteredDates.length).setBackground('#6fa8dc').setFontColor('#fff').setFontWeight('bold').setHorizontalAlignment('center'); // 日付行
      filteredDates.forEach((d, i) => { // 曜日行
        const cell = sheet.getRange(3, i + CONFIG.calendarStartCol);
        const dow = d.getDay();
        const bgColor = dow === 0 ? '#ea9999' : dow === 6 ? '#9fc5e8' : '#6fa8dc';
        cell.setBackground(bgColor).setFontColor('#fff').setFontWeight('bold').setHorizontalAlignment('center');
      });
    }

    // 列幅・行高・固定
    const endColNum = filteredDates.length + CONFIG.calendarStartCol - 1;
    if (endColNum >= CONFIG.calendarStartCol) {
      sheet.setColumnWidths(CONFIG.calendarStartCol, endColNum - CONFIG.calendarStartCol + 1, 30); // 幅30px
      sheet.setRowHeights(1, 3, 25); // 1-3行目 高さ25px
      sheet.setFrozenRows(3);
      sheet.setFrozenColumns(CONFIG.calendarStartCol - 1); // F列まで固定
      Logger.log(`${sheet.getName()}: 書式・固定設定 完了`);
    }

    // 今日列の4行目に "=C4" を設定（最初のシートのみ）
    if (todayColIndex !== -1) {
      try {
        const targetCol = todayColIndex + CONFIG.calendarStartCol;
        const targetRow = 4; // C4セルを参照
        const cell = sheet.getRange(targetRow, targetCol);
        const formula = '=C' + targetRow;
        cell.setFormula(formula);
        Logger.log(`${sheet.getName()}: ${cell.getA1Notation()} に数式 ${formula} を設定`);
      } catch (e) {
        Logger.log(`${sheet.getName()}: 今日セル数式エラー: ${e.message}`);
      }
    }

  } catch (error) {
    Logger.log(`!!! エラー (${sheet.getName()}): ${error.message}\n${error.stack}`);
  }

  // 稼働日リスト(Date[])を返す
  return { workingDays: workingDaysForCsv };
}


// ========== 稼働日CSV出力 ==========

/**
 * 稼働日リスト(Date[])をCSVとしてDriveに出力
 */
function _exportWorkdaysCsv(workingDays, tz) { // 旧: exportWorkingDaysToCsv
  if (!CONFIG.calendarOutputFolderId || CONFIG.calendarOutputFolderId === 'YOUR_FOLDER_ID_HERE') {
    throw new Error('CONFIG.calendarOutputFolderId が未設定です。');
  }
  if (!workingDays || workingDays.length === 0) {
    Logger.log('CSV出力スキップ: 稼働日データがありません。');
    return;
  }

  // Date[] -> "MM/dd"[]
  const formattedDates = workingDays.map(d => Utilities.formatDate(d, tz, 'MM/dd'));
  const csvContent = formattedDates.join(','); // カンマ区切り

  let outputFolder;
  try {
    outputFolder = DriveApp.getFolderById(CONFIG.calendarOutputFolderId);
  } catch (e) {
    throw new Error(`フォルダID "${CONFIG.calendarOutputFolderId}" が無効かアクセス権がありません: ${e.message}`);
  }

  // 既存ファイル削除 (上書き)
  const files = outputFolder.getFilesByName(CONFIG.calendarOutputFileName);
  while (files.hasNext()) {
    const f = files.next();
    Logger.log(`既存ファイル "${CONFIG.calendarOutputFileName}" (ID: ${f.getId()}) を削除`);
    f.setTrashed(true); // ゴミ箱へ
  }

  // 新規作成
  const newFile = outputFolder.createFile(CONFIG.calendarOutputFileName, csvContent, MimeType.CSV);
  Logger.log(`✓ 稼働日CSV作成: "${CONFIG.calendarOutputFileName}" (ID: ${newFile.getId()})`);
  Logger.log(`  場所: ${outputFolder.getName()} フォルダ`);
  Logger.log(`  内容(先頭): ${csvContent.substring(0, 50)}...`);
}


/**
 * ★ 修正版: 各月シートのG列以降に在庫予測数式を設定
 * G列: 偶数行=前月最終列+下セル参照 (例: ='2025/10'!AC6+G7), 奇数行=空欄
 * H列以降: 偶数行=計算式, 奇数行=空欄
 */
function calculateInventory() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = ss.getSheets();
  const numSheetsToProcess = Math.min(sheets.length, 4); // 最大4シート
  if (numSheetsToProcess === 0) { Logger.log("在庫予測: 処理対象シートなし"); return; }

  Logger.log(`在庫予測: 対象シート数 = ${numSheetsToProcess}`);
  const tz = ss.getSpreadsheetTimeZone();
  const today = new Date(); today.setHours(0,0,0,0);
  const todayDayStr = Utilities.formatDate(today, tz, "d"); // 今日の「日」文字列
  const START_ROW = 4; // データ開始行

  let prevSheetName = null;    // 前シート名 (G列数式用)
  let prevSheetLastCol = -1; // 前シート最終列番号 (G列数式用)

  for (let i = 0; i < numSheetsToProcess; i++) {
    const sheet = sheets[i];
    const sheetName = sheet.getName();
    if (!/^\d{4}\/\d{2}$/.test(sheetName)) { // yyyy/mm 形式か？
      Logger.log(`在庫予測スキップ: ${sheetName} (形式不正)`);
      prevSheetName = null; prevSheetLastCol = -1; // 次への参照を切る
      continue;
    }

    const lastCol = sheet.getLastColumn();
    const lastRowSheet = sheet.getLastRow();
    Logger.log(`--- 在庫予測: ${sheetName} 開始 (LCol:${lastCol}, LRow:${lastRowSheet}) ---`);

    // C列の最終データ行を特定
    let lastDataRowInC = START_ROW - 1;
    if (lastRowSheet >= START_ROW) {
      const cValues = sheet.getRange(START_ROW, 3, lastRowSheet - START_ROW + 1, 1).getValues();
      for (let r = cValues.length - 1; r >= 0; r--) {
        if (cValues[r][0] !== "") { lastDataRowInC = START_ROW + r; break; }
      }
    }
    Logger.log(`${sheetName}: C列最終データ行 = ${lastDataRowInC}`);
    if (lastDataRowInC < START_ROW) {
      Logger.log(`${sheetName}: C列データなし → 在庫予測スキップ`);
      prevSheetName = sheetName; prevSheetLastCol = lastCol; // 次へ情報渡し
      continue;
    }
    const numDataRows = lastDataRowInC - START_ROW + 1; // 処理行数

    // ====== （i>0）G 列に数式を設定 ======
    if (i > 0 && prevSheetName && prevSheetLastCol >= 1) {
      const gColRange = sheet.getRange(START_ROW, 7, numDataRows, 1); // G列
      const formulasG_A1 = []; // A1形式数式用配列

      // 前シート名をエスケープ
      const escapedPrevSheetName = prevSheetName.replace(/'/g, "''");
      
      for (let r = 0; r < numDataRows; r++) {
        const currentRow = START_ROW + r;
        let formula = "";
        if (currentRow % 2 === 0) { // 偶数行 (在庫行)
          // ★ 修正: 前月最終列(同じ行) + G列の1つ下のセル
          // 例: ='2025/10'!AC6+G7
          const prevCellA1 = `'${escapedPrevSheetName}'!${sheet.getRange(currentRow, prevSheetLastCol).getA1Notation()}`;
          const lowerCellA1 = sheet.getRange(currentRow + 1, 7).getA1Notation(); // G列の下のセル
          formula = `=${prevCellA1}+${lowerCellA1}`;
        } else { // 奇数行 (入庫行)
          // ★ 修正: 空欄にする
          formula = ""; 
        }
        formulasG_A1.push([formula]);
      }
      
      try {
          gColRange.setFormulas(formulasG_A1); // A1形式の数式を設定
          Logger.log(`${sheetName}: G${START_ROW}:G${lastDataRowInC} に数式を設定 (偶数行=前月+下セル, 奇数行=空欄)`);
      } catch (e) {
          Logger.log(`!!! ${sheetName}: G列への数式設定エラー: ${e.message}\n${e.stack}`);
      }
    } else if (i > 0) {
        Logger.log(`${sheetName}: G列への数式設定スキップ (前シート情報なし)`);
    } else {
        // シート1 (i=0) のG列は、_drawCalendar で今日の列に =C4 が設定される以外は空欄のまま
        Logger.log(`${sheetName}: シート1のためG列への繰越数式設定はスキップ`);
    }

    // ====== 数式を設定する開始列を決定 (H列以降 or 今日列以降) ======
    // ====== 数式を設定する開始列を決定 (H列以降 or 今日列以降 / 今日が公休なら過去の稼働日) ======
    let formulaStartCol = -1; // -1 = 設定しない
    if (i === 0) { // 最初のシート
      if (lastCol >= CONFIG.calendarStartCol) { // G列以降が存在する場合
        // 2行目(日付行)の値を取得 (G列から最終列まで)
        const dateHeaderRange = sheet.getRange(2, CONFIG.calendarStartCol, 1, lastCol - CONFIG.calendarStartCol + 1);
        const dateRowValues = dateHeaderRange.getDisplayValues()[0]; // 日付文字列の配列 ['1', '2', '3', '5', ...]
        const dateRowFormulas = dateHeaderRange.getFormulasR1C1()[0]; // 数式も取得 (もし日付が数式なら) - 必要に応じて

        let todayColIndexInHeader = -1; // G列以降のヘッダー内でのインデックス (0始まり)

        // --- まず今日の日付を探す ---
        todayColIndexInHeader = dateRowValues.findIndex(dayStr => (dayStr + '').trim() === todayDayStr);

        // --- 今日が見つからない場合（公休等）、今日より前の最も近い稼働日を検索 ---
        if (todayColIndexInHeader === -1) {
          Logger.log(`${sheetName}: 今日の日付(${todayDayStr})がヘッダーに見つかりません (公休の可能性)。過去の稼働日を探します...`);
          // G列から逆方向にループ
          for (let colOffset = dateRowValues.length - 1; colOffset >= 0; colOffset--) {
            const currentDayStr = (dateRowValues[colOffset] + '').trim();
            // 数字かどうかチェック (より堅牢にするなら正規表現など)
            if (/^\d+$/.test(currentDayStr)) {
               const currentDayNum = parseInt(currentDayStr, 10);
               // 今日の日付以下の稼働日が見つかったら採用
               if (currentDayNum <= today.getDate()) { // ここでは単純に「日」だけで比較（月が変わる場合は別途考慮が必要）
                  todayColIndexInHeader = colOffset;
                  Logger.log(`${sheetName}: 今日以前の最も近い稼働日を発見 → ${currentDayStr}日 (${CONFIG.calendarStartCol + colOffset}列目)`);
                  break;
               }
            }
          }
        }

        // --- 開始列を決定 ---
        if (todayColIndexInHeader !== -1) {
          formulaStartCol = CONFIG.calendarStartCol + todayColIndexInHeader; // G列 + オフセット
          // ★ もし開始列がG列なら、H列からにする (これは元のロジックを踏襲)
          if (formulaStartCol === CONFIG.calendarStartCol) {
              formulaStartCol = CONFIG.calendarStartCol + 1; // H列
              Logger.log(`${sheetName}: 開始列がG列のため、数式設定は H列(${formulaStartCol}) から開始`);
          } else {
               Logger.log(`${sheetName}: 数式設定は ${formulaStartCol}列 から開始`);
          }
        }
      }
      if (formulaStartCol === -1) Logger.log(`${sheetName}: 今日(${todayDayStr})以前の稼働日が見つかりません → 数式設定スキップ`);
      
    } else { // 2シート目以降
      if (lastCol >= 8) { formulaStartCol = 8; } // H列から開始
      if (formulaStartCol === -1) Logger.log(`${sheetName}: H列なし → 数式設定スキップ`);
      else Logger.log(`${sheetName}: H列(${formulaStartCol}) から数式設定`);
    }

    // ====== 数式設定 (H列以降 or 今日列以降) ======
    if (formulaStartCol > 0 && lastCol >= formulaStartCol) {
      const numFormulaCols = lastCol - formulaStartCol + 1; // 設定する列数
      const targetRange = sheet.getRange(START_ROW, formulaStartCol, numDataRows, numFormulaCols);
      
      // ★ 偶数行は数式文字列、奇数行は空文字列
      const valuesOrFormulas = Array.from({ length: numDataRows }, (_, r) => {
        const currentRow = START_ROW + r;
        if (currentRow % 2 !== 0) return Array(numFormulaCols).fill(""); // ★ 奇数行は空欄

        // 偶数行は数式文字列 (R1C1形式)
        return Array.from({ length: numFormulaCols }, (__, c) => {
          // =左セル-日割(D列)+入庫(次行同列)
          return `=R[0]C[-1]-N(R[0]C4)+N(R[1]C[0])`;
        });
      });

      try {
        // setValues で数式文字列と空文字列を一括書き込み
        targetRange.setValues(valuesOrFormulas); 
        Logger.log(`${sheetName}: ${targetRange.getA1Notation()} に数式/空欄 設定完了`);
      } catch (e) {
        Logger.log(`!!! ${sheetName}: 数式/空欄 設定エラー: ${e.message}\n${e.stack}`);
      }
    } else {
      Logger.log(`${sheetName}: 数式設定列なし`);
    }

    // 列幅設定 (G列～最終列)
    if (lastCol >= 7) {
      try { sheet.setColumnWidths(7, lastCol - 7 + 1, 70); Logger.log(`${sheetName}: G-${lastCol}列 幅=70px`); }
      catch (e) { Logger.log(`!!! ${sheetName}: 列幅設定エラー: ${e.message}`); }
    }

    // 次のループ用に情報を保持
    prevSheetName = sheetName;
    prevSheetLastCol = lastCol;

  } // end sheet loop

  Logger.log("在庫予測計算 正常終了");
}
