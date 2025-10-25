/**
 * スプレッドシートが編集されたときに実行される関数（インストール型トリガーで設定）
 * @param {GoogleAppsScript.Events.SheetsOnEdit} e - イベントオブジェクト
 */
function recordCalendarEdit(e) {
  const range = e.range;
  const sheet = range.getSheet();
  const sheetName = sheet.getName();
  const sheetIndex = sheet.getIndex();

  const logSheetName = 'log';

  // 1. 編集されたシートが 'log' シートの場合は何もしない
  if (sheetName === logSheetName) {
    return;
  }

  // 2. 編集されたシートが1～4番目以外の場合は何もしない
  if (sheetIndex < 1 || sheetIndex > 4) {
    return;
  }

  const logSheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(logSheetName);

  if (!logSheet) {
    Logger.log('ログシート "' + logSheetName + '" が見つかりません。');
    return;
  }

  const account = e.user ? e.user.getEmail() : 'unknown';
  const timestamp = new Date();

  const numRows = range.getNumRows();
  const numCols = range.getNumColumns();
  const values = range.getValues();

  const cellPositionsToDelete = new Set(); // セル位置ベースの削除用
  const entriesToAdd = [];

  const startRow = range.getRow();
  const startCol = range.getColumn();

  // 編集範囲をスキャン
  for (let r = 0; r < numRows; r++) {
    for (let c = 0; c < numCols; c++) {

      const editedRow = startRow + r;
      const editedCol = startCol + c;
      const editedValue = values[r][c];

      // セル位置を取得（例：D4）
      const cellPosition = getA1Notation(editedRow, editedCol);

      // A列とB列が結合セルの場合を考慮して値を取得
      const aValue = getMergedCellValue(sheet, editedRow, 1);
      const bValue = getMergedCellValue(sheet, editedRow, 2);

      // 日付取得ロジック
      let datePart1 = sheet.getRange(1, editedCol).getDisplayValue();
      const datePart2 = sheet.getRange(2, editedCol).getDisplayValue();

      if (String(datePart2).trim() === "") {
        Logger.log('2行目(日)がブランクのためスキップ (列: ' + editedCol + ')');
        continue;
      }

      if (String(datePart1).trim() === "") {
        for (let col = editedCol - 1; col >= 1; col--) {
          let prevDatePart1 = sheet.getRange(1, col).getDisplayValue();
          if (String(prevDatePart1).trim() !== "") {
            datePart1 = prevDatePart1;
            break;
          }
        }
      }

      if (String(datePart1).trim() === "") {
        Logger.log('1行目(月)の情報が見つからなかったためスキップ (列: ' + editedCol + ')');
        continue;
      }

      const dateString = String(datePart1) + "/" + String(datePart2);

      const isBlank = (editedValue === "" || editedValue === null || typeof editedValue === 'undefined');

      // このセル位置のログは削除対象
      cellPositionsToDelete.add(cellPosition);

      if (!isBlank) {
        // 空白でない場合は新規ログを追加
        const newId = Utilities.getUuid();
        entriesToAdd.push([
          account,      // Account (A列)
          aValue,       // code (B列) - 編集した行のA列の値
          bValue,       // productname (C列) - 編集した行のB列の値
          editedValue,  // cell (D列) - 編集した値
          dateString,   // day (E列) - yyyy/mm/dd
          newId,        // ID (F列)
          timestamp,    // Timestamp (G列)
          cellPosition  // CellPosition (H列) - セル位置（例：D4）
        ]);
      }
    }
  }

  if (cellPositionsToDelete.size === 0 && entriesToAdd.length === 0) {
    Logger.log('処理対象のログ追加・削除はありませんでした。');
    return;
  }

  // 既存ログのスキャンと削除（セル位置ベース）
  const logData = logSheet.getDataRange().getValues();
  const CELL_POSITION_COL_IDX = 7; // H列: CellPosition

  const rowsToDelete = [];

  for (let i = 0; i < logData.length; i++) {
    const row = logData[i];
    if (row.length <= CELL_POSITION_COL_IDX) {
      continue;
    }
    const logCellPosition = row[CELL_POSITION_COL_IDX];

    // セル位置が一致する場合は削除
    if (cellPositionsToDelete.has(logCellPosition)) {
      rowsToDelete.push(i + 1);
    }
  }

  // 既存ログの削除
  if (rowsToDelete.length > 0) {
    rowsToDelete.sort((a, b) => b - a);
    for (const rowNum of rowsToDelete) {
      logSheet.deleteRow(rowNum);
    }
  }

  // 新規ログの追加
  if (entriesToAdd.length > 0) {
    logSheet.getRange(
      logSheet.getLastRow() + 1,
      1,
      entriesToAdd.length,
      entriesToAdd[0].length
    ).setValues(entriesToAdd);
  }
}

/**
 * 行列番号からA1形式の表記を取得する関数
 * @param {number} row - 行番号
 * @param {number} col - 列番号
 * @return {string} A1形式の表記（例：D4）
 */
function getA1Notation(row, col) {
  let columnName = "";
  let tempCol = col;
  
  while (tempCol > 0) {
    const remainder = (tempCol - 1) % 26;
    columnName = String.fromCharCode(65 + remainder) + columnName;
    tempCol = Math.floor((tempCol - 1) / 26);
  }
  
  return columnName + row;
}

/**
 * 結合セルを考慮してセルの値を取得する関数
 * @param {GoogleAppsScript.Spreadsheet.Sheet} sheet - シート
 * @param {number} row - 行番号
 * @param {number} col - 列番号
 * @return {*} セルの値
 */
function getMergedCellValue(sheet, row, col) {
  const range = sheet.getRange(row, col);
  
  // セルが結合されているか確認
  if (range.isPartOfMerge()) {
    // 結合セルの場合、結合範囲全体を取得
    const mergedRange = range.getMergedRanges()[0];
    // 結合範囲の最初のセル（左上）の値を返す
    return mergedRange.getValue();
  } else {
    // 通常のセルの場合
    return range.getValue();
  }
}