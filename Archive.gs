// [Archive.gs の archiveLogData 関数 (source: 49-60) を以下に置き換え]

/**
 * ★ 修正版: logシートのデータをarchiveシートに「上書き」し、logシートを初期化します。
 * (ヘッダー含む全データを上書きし、logシートの2行目以降をクリア)
 */
function archiveLogData() { //
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const logSheet = ss.getSheetByName('log');
  const archiveSheet = ss.getSheetByName('archive'); //
  
  // --- 1. シートの存在チェック ---
  if (!logSheet) {
    Logger.log('エラー: logシートが見つかりません。');
    SpreadsheetApp.getUi().alert("エラー: 'log' という名前のシートが見つかりません。"); //
    return;
  }
  
  if (!archiveSheet) {
    Logger.log('エラー: archiveシートが見つかりません。');
    SpreadsheetApp.getUi().alert("エラー: 'archive' という名前のシートが見つかりません。"); //
    return;
  }

  // --- 2. logシートのデータ範囲を特定 (ヘッダー含む) ---
  const logLastRow = logSheet.getLastRow();
  if (logLastRow < 1) { // 1行目(ヘッダー)すらない
    Logger.log('logシートに処理対象データがありません。'); 
    SpreadsheetApp.getUi().alert("logシートにアーカイブするデータがありません。");
    return; //
  }
  
  const numCols = logSheet.getLastColumn();
  if (numCols === 0) { //
    Logger.log('logシートに列がありません。');
    return; //
  }

  try {
    // --- 3. logシートから「全データ」を取得 (ヘッダー含む) ---
    const dataRange = logSheet.getDataRange(); //
    const dataToArchive = dataRange.getValues(); //
    const numRows = dataToArchive.length;
    Logger.log(`${numRows} 行のデータを 'log' から取得しました。`);

    // --- 4. archiveシートを「クリア」 ---
    archiveSheet.clear({ contentsOnly: true });
    Logger.log(`'archive' シートをクリアしました。`);

    // --- 5. archiveシートの1行目から「上書き」 ---
    archiveSheet.getRange(1, 1, numRows, numCols).setValues(dataToArchive); //
    Logger.log(`データを 'archive' シートに「上書き」しました。`); //

    // --- 6. logシートの2行目以降をクリア (内容のみ) ---
    const startRowClear = 2; //
    if (logLastRow >= startRowClear) {
        logSheet.getRange(startRowClear, 1, logLastRow - startRowClear + 1, numCols).clearContent(); //
        Logger.log(`'log' シートの ${startRowClear} 行目以降をクリアしました。`);
    }
    
    SpreadsheetApp.getUi().alert(`アーカイブ完了 (上書き)\n\n${numRows} 行のデータを 'log' から 'archive' に上書きしました。`); //

  } catch (e) {
    Logger.log("アーカイブ処理中にエラーが発生しました: " + e.message);
    SpreadsheetApp.getUi().alert("アーカイブ処理中にエラーが発生しました:\n" + e.message); //
  }
}