/*******************************************
 * menu.gs
 * スプレッドシート用メニュー（outputToCalendar 追加版）
 *******************************************/

function onOpen() {
  const ui = SpreadsheetApp.getUi();
  const menu = ui.createMenu('📅 スケジューラー');

  // ── 1. データ処理（メイン） ────────────────────────────────
  const processMenu = ui.createMenu('⚙️ 1. データ処理')
     .addItem('✨ logシートにマスタ情報を付与', 'enrichLogSheetFromMasters'); // LogFrom.gs

  // ── 2. シート・カレンダー ───────────────────────────
  const viewMenu = ui.createMenu('🔄 2. シート・カレンダー')
    .addItem('🔄 カレンダー描画（全処理）', 'updateCalendar') // calendar.gs
    .addItem('📝 商品情報を "calendar" シートに出力', 'outputToCalendar') // ★★★ これを追加しました ★★★
    .addItem('🔧 シートを月順にソート', 'sortSheetsByMonth'); // LogFrom.gs

  // ── 3. Python連携 ──────────────────────────────────
  const pythonMenu = ui.createMenu('🐍 3. Python連携')
     .addItem('📤 Python入力CSV一括出力', 'exportAllSheetsForPython'); // csv.gs

  // ── 4. バックアップ ────────────────────────────────
  const backupMenu = ui.createMenu('🧰 4. バックアップ (日付別)')
    .addItem('💾 CSV保存（log → 日付フォルダ）', 'backupLogSheet') // csv.gs
    .addItem('⤴ CSV復元（日付フォルダ → log）', 'restoreLogFromBackupPrompt'); // csv.gs

  menu
    .addSubMenu(processMenu)
    .addSeparator()
    .addSubMenu(viewMenu) // ★ 「outputToCalendar」はこのサブメニューに含まれます
    .addSeparator()
    .addSubMenu(pythonMenu)
    .addSeparator()
    .addSubMenu(backupMenu)
    .addSeparator()
    .addItem('🗂️ logシートをアーカイブ', 'archiveLogData') // Archive.gs
    .addSeparator()
    .addItem('❓ 使い方ガイド', 'showUsageGuide')
    .addToUi();
}

/**
 * 使い方ガイド（outputToCalendar 追加版）
 */
function showUsageGuide() {
  const ui = SpreadsheetApp.getUi();
  const msg =
    '【📅 スケジューラー – 使い方】\n\n' +
    '--- ワークフロー ---\n\n' +
    '1) ユーザー入力：\n' +
    '   ・「yyyy/mm」シートに数量を入力します。\n' +
    '   ・（→ log.gs が logシートのA〜H列に自動記録します）\n\n' +
    '2) ⚙️ データ処理：\n' +
    '   ・「✨ logシートにマスタ情報を付与」を実行します。\n' +
    '   ・（→ logシートのI列以降に、マスタ情報(Recipe, line等)が追記されます）\n\n' +
    '3) 🐍 Python連携：\n' +
    '   ・「📤 Python入力CSV一括出力」を実行し、完成した log.csv を出力します。\n\n' +
    '------------------------\n\n' +
    '・🔄 シート・カレンダー：\n' +
    '   「🔄 カレンダー描画」で `yyyy/mm` シートの日付ヘッダーを更新します。\n' +
    '   「📝 商品情報を "calendar" シートに出力」で、`log` の内容を一覧表（"calendar"シート）にします。\n\n' + // ★ 説明を追加
    '・🧰 バックアップ：\n' +
    '   「log」シートを日付別フォルダ (yyyymmdd/log.csv) に保存・復元します。\n';
  ui.alert('使い方ガイド', msg, ui.ButtonSet.OK);
}