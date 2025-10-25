/*******************************************
 * menu.gs
 * スプレッドシート用メニュー（ユーザー向け最適化・絵文字あり）
 *******************************************/

function onOpen() {
  const ui = SpreadsheetApp.getUi();

  const menu = ui.createMenu('📅 スケジューラー');

  // ── カレンダー管理 ─────────────────────────────────────
  const calendarMenu = ui.createMenu('📆 カレンダー管理')
    .addItem('🔄 カレンダー更新（全処理）', 'updateCalendar') // ← calendar 側のメイン
    .addItem('📝 カレンダーに書き込み', 'writeToCalendar');   // ← 任意の個別書込がある場合

  // ── データ更新 ─────────────────────────────────────────
  // ※ もし "calendar出力" が LogFrom 側の関数ならメニューから削除
  //   （workday出力の二重化防止のため）
  const dataMenu = ui.createMenu('📊 データ更新')
    .addItem('✅ logシート更新（マスタCSV）', 'updateLogFromMasterCSV');

  // ── バックアップ ───────────────────────────────────────
  const backupMenu = ui.createMenu('🧰 バックアップ')
    .addItem('💾 CSV保存（log → Drive）', 'backupLogToDrive')
    .addItem('⤴ CSV復元（Drive → log）', 'restoreLogFromBackupPrompt');

  menu
    .addSubMenu(calendarMenu)
    .addSeparator()
    .addSubMenu(dataMenu)
    .addSeparator()
    .addSubMenu(backupMenu)
    .addSeparator()
    .addItem('🔧 シートを月順にソート', 'sortSheetsByMonth')
    .addSeparator()
    .addItem('❓ 使い方ガイド', 'showUsageGuide')
    .addToUi();
}

function showUsageGuide() {
  const ui = SpreadsheetApp.getUi();
  const msg =
    '【📅 スケジューラー – 使い方】\n\n' +
    '1) 📊 データ更新：\n' +
    '   ・「✅ logシート更新（マスタCSV）」で Drive 上のマスタCSVを取り込み\n\n' +
    '2) 📆 カレンダー管理：\n' +
    '   ・「🔄 カレンダー更新（全処理）」で一括更新（稼働日CSVの作成もここで実施）\n' +
    '   ・「📝 カレンダーに書き込み」で必要箇所だけ反映\n\n' +
    '3) 🧰 バックアップ：\n' +
    '   ・「💾 CSV保存（log → Drive）」で現状の log を退避\n' +
    '   ・「⤴ CSV復元（Drive → log）」で任意CSVを復元\n\n' +
    '4) その他：\n' +
    '   ・「🔧 シートを月順にソート」で「yyyy/mm」形式のシートを時系列に整列\n\n' +
    '※ ログは [INFO]/[WARN]/[ERROR] のテキスト基調（操作メニューのみ絵文字を使用）';
  ui.alert('使い方ガイド', msg, ui.ButtonSet.OK);
}
