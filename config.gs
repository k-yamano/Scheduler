/*******************************************
 * config.gs
 * すべてのファイルから参照する共通設定 & ロガー
 * (設定をこのファイルに一本化)
 *******************************************/

// ===== 共通設定（一本化） =====
const CONFIG = {
  // --- システム全体 ---
  targetSpreadsheetId: SpreadsheetApp.getActive().getId(), // 現ブック
  TIMEZONE: 'Asia/Tokyo',

  // --- 1. カレンダー ('yyyy/mm' シート) 関連 (calendar.gs用) ---
  calendarId: 'c_1qdjjdd90422v8qcba65oumcso@group.calendar.google.com',
  calendarStartCol: 7,          // G列開始 (yyyy/mm シートの描画開始列)
  monthsAhead: 3,               // 今月 + 3ヶ月先まで
  calendarOutputFolderId: '13EoohP_R4zZXt5uMu_EgVR9ES8iwzBtc', // workday.csv を出力する先
  calendarOutputFileName: 'workday.csv',

  // --- 2. 'log' シートから 'calendar' シートへの出力設定 ---
  logFromStartCol: 2,           // B列開始
  logFromColsPerDay: 4,         // 1日あたり4列 (B,C,D,E)
  logFromMaxItems: 4,           // 1日1ラインあたりの最大行数

  // --- 3. バックアップ設定 ---
  BACKUP_BASE_PATH: 'dp_Scheduler/Output/Backup', // ルートフォルダからのパス
  MASTER_CSV_FILE_ID: '1qP30aCp4TrXmj4dS-9jn18g1zOVhLf91',
  CAPACITY_CSV_FILE_ID: '1mjbTBNOPMyFuuR288-0A-1PtXRcpyKK7',

  // --- 4. Python連携 関連 (csv.gs / logformtest.txt用) ---
  pythonInFolderId: 'YOUR_PYTHON_INPUT_FOLDER_ID', // ★ PythonがCSVを読み込むフォルダIDに変更してください
  SHEET_NAMES: {
    MAIN: 'main_sheet', // Python(1.py)が使うシート名
    LOG: 'log',
    CAL: 'calendar'     // Python(2.py)がカレンダーCSVとして読み込むシート名
  },

  // --- 5. 情報付与（Enrich）用マスタファイル ---
  // (logシートに情報を付与する際に使用)
  MASTER_FOLDER_ID: '13EoohP_R4zZXt5uMu_EgVR9ES8iwzBtc', // マスタ格納フォルダID
  
  MASTER_FILES: {
    FORMULATION: 'formulation.csv', // 配合マスタ
    MACHINE: 'machine.csv',         // 設備マスタ
    MATERIAL: 'material_master.csv' // 2.py で使われているファイル
  }
};

// ===== タイムゾーン共通化 =====
const TZ = CONFIG.TIMEZONE || Session.getScriptTimeZone() || 'Asia/Tokyo';

// ===== 統一ロガー（アプリログは絵文字なし）=====
const LOG = {
  info:  (m) => Logger.log(`[INFO] ${m}`),
  warn:  (m) => Logger.log(`[WARN] ${m}`),
  error: (m) => Logger.log(`[ERROR] ${m}`)
};