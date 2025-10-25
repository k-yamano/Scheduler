import os, re, glob, time, random, io
import numpy as np
import pandas as pd
from datetime import datetime

# --- 1. Google Colab認証（1回のみ） ---
print("=" * 60)
print("Google Colab - スプレッドシート更新プログラム")
print("=" * 60)

print("\n[1/9] 認証処理を開始します...")
from google.colab import auth
import gspread
from google.auth import default
from googleapiclient.discovery import build

# Google認証（この1回だけ）
auth.authenticate_user()
print("  ✓ Google認証完了")

# 認証情報取得
creds, _ = default()

# Google Sheets API接続
gc = gspread.authorize(creds)
SHEET_KEY = "1g3ZeCFzexguuu6q3r7kS3tOHqq44JtDarnnwd8wpRhc"
sh = gc.open_by_key(SHEET_KEY)
print(f"  ✓ スプレッドシート接続完了")

# Google Drive API接続（ファイル読み取り用）
drive_service = build('drive', 'v3', credentials=creds)
print(f"  ✓ Google Drive API接続完了")

# --- 2. Google Driveファイル検索関数 ---
def search_drive_files(query, service):
    """Google Drive APIでファイルを検索"""
    try:
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name, mimeType)',
            pageSize=10
        ).execute()
        return results.get('files', [])
    except Exception as e:
        print(f"    検索エラー: {e}")
        return []

def download_file(file_id, service):
    """ファイルをダウンロード"""
    try:
        request = service.files().get_media(fileId=file_id)
        file_content = io.BytesIO()
        from googleapiclient.http import MediaIoBaseDownload
        downloader = MediaIoBaseDownload(file_content, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        file_content.seek(0)
        return file_content
    except Exception as e:
        print(f" ダウンロードエラー: {e}")
        return None

# --- 3. ユーティリティ関数 ---
def _retry_google(func, *args, retries=5, **kwargs):
    """APIエラー時のリトライ処理"""
    for i in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            s = str(e)
            if "429" in s or "500" in s or "503" in s:
                wait = (2 ** i) + random.random()
                print(f"    ⏳ APIエラー、{wait:.1f}秒待機してリトライ...")
                time.sleep(wait)
                continue
            raise
    return func(*args, **kwargs)

def read_csv_flexible(file_obj):
    """複数エンコーディングを試行してCSV読み込み"""
    for enc in ["utf-8-sig", "cp932", "utf-8", "shift_jis"]:
        try:
            file_obj.seek(0)
            return pd.read_csv(file_obj, encoding=enc)
        except:
            pass
    try:
        file_obj.seek(0)
        return pd.read_csv(file_obj, sep="\t")
    except Exception as e:
        raise Exception(f"読込失敗: ({e})")

def parse_last_number(x):
    """文字列から最後の数値を抽出"""
    if pd.isna(x):
        return np.nan
    m = re.findall(r'\d+(?:\.\d+)?', str(x).replace(',', ''))
    return float(m[-1]) if m else np.nan

def pretty_num(x):
    """数値を整形（整数は小数点なし、小数は2桁）"""
    if pd.isna(x) or x == "":
        return ""
    try:
        f = float(x)
        return int(f) if f.is_integer() else round(f, 2)
    except:
        return x

# --- 4. ファイル探索 ---
print("\n[2/9] Google Driveからマスターファイルを探索中...")

files_found = {}

def find_drive_file(key, patterns):
    """パターンに一致するファイルを検索"""
    for pattern in patterns:
        # ファイル名で検索
        query = f"name contains '{pattern.replace('*', '')}' and trashed=false"
        results = search_drive_files(query, drive_service)

        if results:
            file = results[0]
            files_found[key] = file
            print(f"  ✓ {key:12s}: {file['name']}")
            return

    print(f"{key:12s}: 見つかりません")

find_drive_file("product", ['product.csv', '製品.csv', '製品.xlsx'])
find_drive_file("zaiko", ['zaiko.xlsx', '在庫.xlsx'])
find_drive_file("honpo", ['需要予測_本舗.csv', '本舗.csv'])
find_drive_file("sales", ['需要予測_販売.csv', '販売.csv'])
find_drive_file("workday", ['workday.csv', '稼働日.csv', '営業日.csv'])
find_drive_file("base", ['base_file.csv', 'base.csv'])

# --- 5. 稼働日数計算 ---
print("\n[3/9] 稼働日数を計算中...")

today = pd.Timestamp.now(tz='Asia/Tokyo')
FY_ORDER = ['4月','5月','6月','7月','8月','9月','10月','11月','12月','1月','2月','3月']
WINDOW_LABELS = [FY_ORDER[(FY_ORDER.index(f"{today.month}月") + k) % 12] for k in range(4)]
print(f"  対象月: {' → '.join(WINDOW_LABELS)}")

def get_workdays(file_info, year, month):
    """稼働日数を取得（ファイルまたは営業日計算）"""
    if not file_info:
        s = pd.Timestamp(year, month, 1)
        e = s + pd.offsets.MonthEnd(1)
        return float(len(pd.date_range(start=s, end=e, freq="B")))

    try:
        file_content = download_file(file_info['id'], drive_service)
        if not file_content:
            raise Exception("ダウンロード失敗")

        df = read_csv_flexible(file_content).fillna("")
        df.columns = [str(c).strip() for c in df.columns]
        cur_ym, cur_m = f"{year}-{month:02d}", f"{month}月"

        for mk in df.columns:
            if any(k in mk for k in ["年月","月"]):
                for wk in df.columns:
                    if any(k in wk for k in ["稼働日","営業日"]):
                        hit = df[df[mk].astype(str).str.contains(cur_ym)|df[mk].astype(str).str.contains(cur_m)]
                        if not hit.empty:
                            v = pd.to_numeric(hit[wk], errors='coerce').dropna()
                            if not v.empty:
                                return float(v.iloc[0])
    except:
        pass

    s = pd.Timestamp(year, month, 1)
    e = s + pd.offsets.MonthEnd(1)
    return float(len(pd.date_range(start=s, end=e, freq="B")))

workdays_map = {}
for k in range(4):
    dt = (today.tz_localize(None) + pd.DateOffset(months=k))
    label = WINDOW_LABELS[k]
    workdays_map[label] = get_workdays(files_found.get("workday"), dt.year, dt.month)
    print(f"  {dt.year}年{dt.month:02d}月 ({label}): {workdays_map[label]:.0f}日")

# --- 6. マスターデータ読み込み ---
print("\n[4/9] マスターデータを読み込み中...")

# 製品マスタ
prod_info = files_found.get("product")
if not prod_info:
    raise FileNotFoundError("製品マスタが見つかりません")

prod_content = download_file(prod_info['id'], drive_service)
if prod_info['name'].endswith(('.xlsx', '.xls')):
    df_prod = pd.read_excel(prod_content)
else:
    df_prod = read_csv_flexible(prod_content)

df_prod.columns = [str(c).strip() for c in df_prod.columns]
df_prod = df_prod.rename(columns={
    df_prod.columns[0]: "品番",
    df_prod.columns[1]: "商品名"
})

if len(df_prod.columns) >= 3:
    df_prod = df_prod.rename(columns={df_prod.columns[2]: "発注リードタイム"})
else:
    df_prod["発注リードタイム"] = 0

df_prod["発注リードタイム"] = pd.to_numeric(df_prod["発注リードタイム"], errors='coerce').fillna(0)
df_prod["品番"] = df_prod["品番"].astype(str).str.strip()
print(f"  ✓ 製品マスタ: {len(df_prod):,}件")

# 在庫データ
zaiko_info = files_found.get("zaiko")
if not zaiko_info:
    raise FileNotFoundError("在庫ファイルが見つかりません")

zaiko_content = download_file(zaiko_info['id'], drive_service)
df_zaiko_raw = pd.read_excel(zaiko_content, sheet_name=0, header=None)
df_zaiko = df_zaiko_raw.iloc[:, [1, 3]].copy()
df_zaiko.columns = ["品番", "在庫数量"]
df_zaiko["品番"] = df_zaiko["品番"].astype(str).str.strip()
df_zaiko["在庫数量"] = pd.to_numeric(df_zaiko["在庫数量"], errors='coerce').fillna(0)
df_zaiko = df_zaiko.groupby("品番", as_index=False)["在庫数量"].sum()
print(f"  ✓ 在庫データ: {len(df_zaiko):,}件")

# --- 7. 需要予測データ ---
print("\n[5/9] 需要予測データを処理中...")

def read_forecast(file_info, window_labels):
    """需要予測CSVを読み込み"""
    if not file_info:
        return pd.DataFrame({"品番": []})

    try:
        file_content = download_file(file_info['id'], drive_service)
        df = read_csv_flexible(file_content)
        df["品番"] = df["品番"].astype(str).str.strip()

        for m in window_labels:
            if m not in df.columns:
                df[m] = np.nan
            else:
                df[m] = df[m].apply(parse_last_number)

        return df[["品番"] + list(window_labels)]
    except:
        return pd.DataFrame({"品番": []})

df_need = pd.merge(
    read_forecast(files_found.get("honpo"), WINDOW_LABELS),
    read_forecast(files_found.get("sales"), WINDOW_LABELS),
    on="品番",
    how="outer",
    suffixes=("_本舗", "_販売")
)

for m in WINDOW_LABELS:
    df_need[m] = df_need.filter(regex=f"^{m}").sum(axis=1, skipna=True)
    wd = workdays_map[m]
    df_need[f"{m}日割"] = (df_need[m] / wd) if wd else np.nan

print(f"  ✓ 需要予測: {len(df_need):,}件")

# --- 8. 移動平均・安全在庫 ---
print("\n[6/9] 移動平均・安全在庫を計算中...")

base_info = files_found.get("base")
if not base_info:
    raise FileNotFoundError("Baseファイルが見つかりません")

base_content = download_file(base_info['id'], drive_service)
df_base = read_csv_flexible(base_content)

colA, colB, colS = df_base.columns[0], df_base.columns[1], df_base.columns[18]
df_base[colA] = pd.to_datetime(df_base[colA], errors="coerce")
df_base[colB] = df_base[colB].astype(str).str.strip()

start = (today - pd.DateOffset(months=3)).tz_localize(None)
df_base3 = df_base[df_base[colA] >= start]

df_ma = df_base3.groupby(colB, as_index=False)[colS].mean().rename(columns={colB: "品番", colS: "移動平均"})
df_std = df_base3.groupby(colB, as_index=False)[colS].std().rename(columns={colB: "品番", colS: "使用量標準偏差"}).fillna(0)
df_stats = pd.merge(df_ma, df_std, on="品番", how="left")

def calc_safety(std, lead, interval=7, factor=1.65):
    """安全在庫を計算"""
    import math
    if pd.isna(std) or pd.isna(lead) or std == 0:
        return 0
    return factor * std * math.sqrt(lead + interval)

print(f"  ✓ 統計データ: {len(df_stats):,}件")

# --- 9. Google Sheets出力 ---
print("\n[7/9] Google Sheetsへ出力中...")

def get_or_create_worksheet(spreadsheet, sheet_name, rows=1000, cols=20):
    """シートを取得またはコピー作成"""
    try:
        ws = spreadsheet.worksheet(sheet_name)
        print(f"    ✓ '{sheet_name}' (既存)")
        return ws
    except gspread.exceptions.WorksheetNotFound:
        try:
            template = spreadsheet.worksheet("format")
            new_ws = _retry_google(
                spreadsheet.duplicate_sheet,
                source_sheet_id=template.id,
                new_sheet_name=sheet_name
            )
            print(f"    ✓ '{sheet_name}' (formatからコピー)")
            return new_ws
        except gspread.exceptions.WorksheetNotFound:
            ws = _retry_google(spreadsheet.add_worksheet, title=sheet_name, rows=rows, cols=cols)
            print(f"    ✓ '{sheet_name}' (新規作成)")
            return ws

month_sheet_names = [(today + pd.DateOffset(months=k)).strftime("%Y/%m") for k in range(4)]
all_merge_requests = []

for k, sheet_name in enumerate(month_sheet_names):
    month_label = WINDOW_LABELS[k]
    col = f"{month_label}日割"

    # データ結合
    df_out = (df_prod
        .merge(df_zaiko, on="品番", how="left")
        .merge(df_need[["品番", col]], on="品番", how="left")
        .merge(df_stats, on="品番", how="left"))

    df_out = df_out.rename(columns={col: "日割"})
    df_out["安全在庫"] = df_out.apply(lambda r: calc_safety(r["使用量標準偏差"], r["発注リードタイム"]), axis=1)

    for c in ["在庫数量", "日割", "移動平均", "安全在庫"]:
        df_out[c] = df_out[c].map(pretty_num)

    df_out = df_out[["品番", "商品名", "在庫数量", "日割", "移動平均", "安全在庫"]]

    # シート準備
    required_rows = len(df_out) * 2 + 10
    ws = get_or_create_worksheet(sh, sheet_name, rows=required_rows, cols=20)

    # クリア
    max_rows_to_clear = max(required_rows, 500)
    try:
        _retry_google(ws.clear_basic_filter)
    except:
        pass

    _retry_google(ws.batch_clear, [f"A3:F{max_rows_to_clear}"])

    # データ準備（1行おきに空行）
    header_row = [df_out.columns.tolist()]
    data_rows_interleaved = []
    merge_requests_for_this_sheet = []

    start_row = 4
    num_cols = len(df_out.columns)
    df_out_filled = df_out.fillna("")

    if len(df_out) > 0:
        for i, row in enumerate(df_out_filled.values.tolist()):
            data_rows_interleaved.append(row)
            data_rows_interleaved.append([""] * num_cols)

            current_data_row_gspread = start_row + (i * 2)
            current_data_row_api = current_data_row_gspread - 1

            # セル結合リクエスト（A～F列）
            for j in range(num_cols):
                merge_requests_for_this_sheet.append({
                    "mergeCells": {
                        "range": {
                            "sheetId": ws.id,
                            "startRowIndex": current_data_row_api,
                            "endRowIndex": current_data_row_api + 2,
                            "startColumnIndex": j,
                            "endColumnIndex": j + 1
                        },
                        "mergeType": "MERGE_ALL"
                    }
                })

    # データ書き込み
    data_to_write = header_row + data_rows_interleaved

    if data_to_write:
        end_row = 3 + len(data_to_write) - 1
        write_range = f"A3:F{end_row}"

        _retry_google(
            ws.update,
            range_name=write_range,
            values=data_to_write,
            value_input_option='USER_ENTERED'
        )
        print(f"{len(df_out):,}行書き込み完了")

        all_merge_requests.extend(merge_requests_for_this_sheet)

# 一括セル結合
if all_merge_requests:
    print("\n[8/9] セル結合を実行中...")
    try:
        body = {"requests": all_merge_requests}
        _retry_google(sh.batch_update, body)
        print(f"  ✓ {len(all_merge_requests):,}件の結合完了")
    except Exception as e:
        print(f"結合エラー: {e}")

# --- 10. シート並べ替え ---
print("\n[9/9] シートを月順に並べ替え中...")

all_worksheets = _retry_google(sh.worksheets)
sheet_order = {}

for ws in all_worksheets:
    name = ws.title
    if re.match(r'^\d{4}/\d{2}$', name):
        try:
            date_obj = datetime.strptime(name, "%Y/%m")
            sheet_order[name] = date_obj
        except:
            pass

sorted_sheets = sorted(sheet_order.items(), key=lambda x: x[1])

for idx, (sheet_name, _) in enumerate(sorted_sheets):
    try:
        ws = sh.worksheet(sheet_name)
        _retry_google(ws.update_index, idx)
        print(f"  {sheet_name} → 位置 {idx + 1}")
    except Exception as e:
        print(f"{sheet_name}: {e}")

# --- 11. 完了 ---
print("\n" + "=" * 60)
print("処理が完了しました！")
print("=" * 60)
print("\n次のステップ:")
print("1. スプレッドシートを開く:")
print(f"   https://docs.google.com/spreadsheets/d/{SHEET_KEY}/")
print("\n2. メニュー「📅 カレンダー管理」→「🔄 カレンダー更新」を実行")
print("\n※ G列以降にカレンダー情報が追加されます")
print("=" * 60)