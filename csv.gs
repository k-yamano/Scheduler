// メイン関数: log と calendar を同時出力
function exportSheets() {
  const SPREADSHEET_ID = '1g3ZeCFzexguuu6q3r7kS3tOHqq44JtDarnnwd8wpRhc';
  const SHEET_NAMES = ['log'];
  //const OUTPUT_DIR = 'dp_Scheduler/Input/Master';
  
  const results = [];
  
  for (const sheetName of SHEET_NAMES) {
    try {
      const spreadsheet = SpreadsheetApp.openById(SPREADSHEET_ID);
      const sheet = spreadsheet.getSheetByName(sheetName);
      
      if (!sheet) {
        Logger.log(`⚠ シート "${sheetName}" が見つかりません`);
        continue;
      }
      
      const range = sheet.getDataRange();
      const values = range.getValues();
      const csv = convertToCSV(values);
      const folder = getOrCreateFolder(OUTPUT_DIR);
      const fileName = `${sheetName}.csv`;
      
      // 既存ファイルを削除
      const existingFiles = folder.getFilesByName(fileName);
      while (existingFiles.hasNext()) {
        existingFiles.next().setTrashed(true);
      }
      
      // 新しいファイルを作成
      const file = folder.createFile(fileName, csv, MimeType.CSV);
      
      results.push({
        sheet: sheetName,
        success: true,
        fileName: file.getName(),
        rows: values.length
      });
      
      Logger.log(`✓ ${sheetName}: ${values.length}行を出力`);
      
    } catch (error) {
      results.push({
        sheet: sheetName,
        success: false,
        error: error.message
      });
      Logger.log(`✗ ${sheetName}: ${error.message}`);
    }
  }
  
  Logger.log('========================================');
  Logger.log('出力完了');
  Logger.log(`成功: ${results.filter(r => r.success).length}件`);
  Logger.log(`失敗: ${results.filter(r => !r.success).length}件`);
  
  return results;
}

// CSVフォーマットに変換する関数
function convertToCSV(data) {
  return data.map(row => 
    row.map(cell => {
      let value;
      
      // 日付オブジェクトの場合はフォーマット
      if (cell instanceof Date) {
        const month = cell.getMonth() + 1;
        const day = cell.getDate();
        value = `${month}/${day}`;
      } else {
        // セルの内容を文字列に変換
        value = String(cell);
      }
      
      // カンマ、改行、ダブルクォートが含まれる場合はエスケープ
      if (value.includes(',') || value.includes('\n') || value.includes('"')) {
        value = '"' + value.replace(/"/g, '""') + '"';
      }
      
      return value;
    }).join(',')
  ).join('\n');
}

// フォルダパスからフォルダを取得または作成
function getOrCreateFolder(folderPath) {
  const folders = folderPath.split('/');
  let currentFolder = DriveApp.getRootFolder();
  
  for (const folderName of folders) {
    if (!folderName) continue;
    
    const subFolders = currentFolder.getFoldersByName(folderName);
    if (subFolders.hasNext()) {
      currentFolder = subFolders.next();
    } else {
      currentFolder = currentFolder.createFolder(folderName);
      Logger.log(`フォルダを作成しました: ${folderName}`);
    }
  }
  
  return currentFolder;
}

// 単一シート出力（個別に出力したい場合のみ使用）
function exportSingleSheet(sheetName) {
  const SPREADSHEET_ID = '1g3ZeCFzexguuu6q3r7kS3tOHqq44JtDarnnwd8wpRhc';
  const OUTPUT_DIR = 'dp_Scheduler/Input/Master';
  
  try {
    const spreadsheet = SpreadsheetApp.openById(SPREADSHEET_ID);
    const sheet = spreadsheet.getSheetByName(sheetName);
    
    if (!sheet) {
      throw new Error(`シート "${sheetName}" が見つかりません`);
    }
    
    const range = sheet.getDataRange();
    const values = range.getValues();
    const csv = convertToCSV(values);
    const folder = getOrCreateFolder(OUTPUT_DIR);
    const fileName = `${sheetName}.csv`;
    
    const existingFiles = folder.getFilesByName(fileName);
    while (existingFiles.hasNext()) {
      existingFiles.next().setTrashed(true);
    }
    
    const file = folder.createFile(fileName, csv, MimeType.CSV);
    
    Logger.log(`✓ ${sheetName}: ${values.length}行を出力`);
    
    return {
      success: true,
      fileName: file.getName(),
      rows: values.length
    };
    
  } catch (error) {
    Logger.log(`✗ エラー: ${error.message}`);
    throw error;
  }
}