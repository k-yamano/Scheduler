/**
 * logシートのデータをarchiveシートに移動し、logシートを初期化します。
 * (2行目から最終行までが対象)
 */
function archiveLogData() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const logSheet = ss.getSheetByName('log');
  const archiveSheet = ss.getSheetByName('archive');
  
  // --- 1. シートの存在チェック ---
  if (!logSheet) {
    Logger.log('エラー: logシートが見つかりません。'); 
    SpreadsheetApp.getUi().alert("エラー: 'log' という名前のシートが見つかりません。");
    return;
  }
  
  if (!archiveSheet) {
    Logger.log('エラー: archiveシートが見つかりません。'); 
    SpreadsheetApp.getUi().alert("エラー: 'archive' という名前のシートが見つかりません。\n(先に 'archive' という名前のシートを作成してください)");
    return;
  }

  // --- 2. logシートのデータ範囲を特定 ---
  const startRow = 2; // 1行目はヘッダー
  const lastRow = logSheet.getLastRow();

  if (lastRow < startRow) {
    Logger.log('logシートに処理対象データがありません。'); 
    SpreadsheetApp.getUi().alert("logシートにアーカイブするデータがありません。");
    return;
  }
  
  const numRows = lastRow - startRow + 1;
  const numCols = logSheet.getLastColumn(); // logシートのすべての列
  
  if (numCols === 0) {
    Logger.log('logシートに列がありません。');
    return;
  }

  try {
    // --- 3. logシートからデータを取得 ---
    const dataRange = logSheet.getRange(startRow, 1, numRows, numCols);
    const dataToArchive = dataRange.getValues();
    Logger.log(`${numRows} 行のデータを 'log' から取得しました。`);

    // --- 4. archiveシートの最終行にデータを追加 ---
    // (archiveシートの最終行 + 1 の位置から書き込む)
    archiveSheet.getRange(archiveSheet.getLastRow() + 1, 1, numRows, numCols).setValues(dataToArchive);
    Logger.log(`データを 'archive' シートに追加しました。`);

    // --- 5. logシートの2行目以降をクリア (内容のみ) ---
    dataRange.clearContent();
    Logger.log(`'log' シートの ${startRow} 行目以降をクリアしました。`);
    
    SpreadsheetApp.getUi().alert(`アーカイブ完了\n\n${numRows} 行のデータを 'log' から 'archive' に移動しました。`);

  } catch (e) {
    Logger.log("アーカイブ処理中にエラーが発生しました: " + e.message);
    SpreadsheetApp.getUi().alert("アーカイブ処理中にエラーが発生しました:\n" + e.message);
  }
}