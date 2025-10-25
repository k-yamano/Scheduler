/*******************************************
 * config.gs
 * すべてのファイルから参照する共通設定 & ロガー
 *******************************************/

// ===== 共通設定（一本化） =====
const CONFIG = {
  // カレンダー & スプレッドシート
  calendarId: 'c_1qdjjdd90422v8qcba65oumcso@group.calendar.google.com',
  targetSpreadsheetId: SpreadsheetApp.getActive().getId(), // 現ブック

  // カレンダー描画関連
  START_COL: 7,          // G列開始（calendarロジックに合わせる）
  monthsAhead: 3,        // 今月 + 3ヶ月先まで

  // CSV 出力（workday.csv は calendar 側のみで出力）
  outputFolderId: '13EoohP_R4zZXt5uMu_EgVR9ES8iwzBtc',
  outputFileName: 'workday.csv',

  // 時刻
  TIMEZONE: 'Asia/Tokyo',
};

// ===== タイムゾーン共通化 =====
const TZ = CONFIG.TIMEZONE || Session.getScriptTimeZone() || 'Asia/Tokyo';

// ===== 統一ロガー（アプリログは絵文字なし）=====
const LOG = {
  info:  (m) => Logger.log(`[INFO] ${m}`),
  warn:  (m) => Logger.log(`[WARN] ${m}`),
  error: (m) => Logger.log(`[ERROR] ${m}`)
};

