# ===============================================
# ワンセル起動スイッチ
#  - Driveマウント＆ROOT自動判定
#  - ① スケジューラ処理（先に実行）
#  - ② AIscheduler_YYYYMMDD.csv 生成
# ===============================================
import os
import re
import math
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Tuple, Dict, List, Optional

warnings.filterwarnings('ignore')

# ===== 0) Google Drive マウント（Colab） =====
try:
    from google.colab import drive  # noqa
    drive.mount('/content/drive', force_remount=False)
except Exception:
    pass  # Colab以外ならスキップ

# ===== 1) パス自動判定 =====
MYDRIVE_CANDIDATES = ["/content/drive/MyDrive", "/content/drive/My Drive"]
ROOT = next((p for p in MYDRIVE_CANDIDATES if os.path.exists(p)), "/content/drive/My Drive")

INPUT_DIR   = os.path.join(ROOT, "dp_Scheduler/Input/Master/")
OUTPUT_DIR  = os.path.join(ROOT, "dp_Scheduler/Output/")
LOG_FILE        = os.path.join(INPUT_DIR,  "log.csv")
CALENDAR_FILE   = os.path.join(INPUT_DIR,  "workday.csv")
MATERIAL_FILE   = os.path.join(INPUT_DIR,  "material_master.csv")
os.makedirs(OUTPUT_DIR, exist_ok=True)

TODAY_STR = datetime.now().strftime("%Y%m%d")

# ===== 2) 共通ユーティリティ =====
def safe_read_csv(path, **kwargs):
    if not os.path.exists(path):
        raise FileNotFoundError(f"ファイルが見つかりません: {path}")
    return pd.read_csv(path, **kwargs)

def pick_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
        low = [x.lower() for x in df.columns]
        if c.lower() in low:
            return df.columns[low.index(c.lower())]
    return None

# ===== 3) ① スケジューラ処理（あなたのコードを関数化し、冗長printは整理） =====
# 計画ロジック設定
STANDARD_LEAD_TIME = 4    # 標準仕込L/T（営業日）
DEFAULT_LEAD_TIME  = 3    # デフォL/T
SHORT_LEAD_TIME    = 1    # 短縮L/T（NR, LC, 工程3なし）
CALENDAR_START_YEAR= 2025
MAX_PRODUCTS_PER_BATCH = 4
FILTER_START_DATE = "2025-10-01"   # 必要に応じて変更
FILTER_END_DATE   = "2025-11-30"   # 必要に応じて変更

def get_working_days(calendar_file_path: str, start_year: int) -> Tuple[pd.DatetimeIndex, pd.Series]:
    df_calendar = safe_read_csv(calendar_file_path, header=None)
    calendar_dates = df_calendar.iloc[0].values
    working_days, last_month, current_year = [], 1, start_year
    pat = re.compile(r"(\d{1,2})/(\d{1,2})")
    for v in calendar_dates:
        m = pat.match(str(v))
        if m:
            month, day = map(int, m.groups())
            if month < last_month:
                current_year += 1
            working_days.append(f"{current_year}/{month:02d}/{day:02d}")
            last_month = month
    working_days_dt = pd.to_datetime(sorted(set(working_days)))
    if len(working_days_dt) == 0:
        raise ValueError("カレンダーから稼働日が取得できません")
    working_days_lookup = pd.Series(range(len(working_days_dt)), index=working_days_dt)
    return working_days_dt, working_days_lookup

def get_material_info(material_file_path: str) -> Tuple[Dict, Dict]:
    df_material = safe_read_csv(material_file_path)
    if '素地' not in df_material.columns or '油脂仕込み量１' not in df_material.columns:
        raise KeyError("material_master.csv に ['素地','油脂仕込み量１'] が必要です")
    df_u = df_material.drop_duplicates(subset=['素地']).copy()
    df_u['Maxbatchsize'] = pd.to_numeric(df_u['油脂仕込み量１'], errors='coerce').fillna(0)
    max_batch_dict = pd.Series(df_u['Maxbatchsize'].values, index=df_u['素地']).to_dict()

    lt_dict = {}
    for _, row in df_u.iterrows():
        recipe = row['素地']
        is_short = ('工程３' not in row) or pd.isna(row['工程３'])
        lt_dict[recipe] = SHORT_LEAD_TIME if (recipe in ['NR','LC'] or is_short) else DEFAULT_LEAD_TIME
    return max_batch_dict, lt_dict

def get_prep_day_by_index(
    filling_date: pd.Timestamp,
    lead_time_days: int,
    working_days_list: pd.DatetimeIndex,
    working_days_lookup: pd.Series
) -> Optional[pd.Timestamp]:
    if pd.isna(filling_date):
        return None
    idx = working_days_lookup.get(filling_date)
    if idx is None:
        return None
    target_idx = idx - lead_time_days
    return None if target_idx < 0 else working_days_list[target_idx]

def consolidate_batches_advanced(df_schedulable: pd.DataFrame) -> pd.DataFrame:
    if df_schedulable.empty:
        return df_schedulable
    rows = []
    for recipe, group in df_schedulable.groupby('Recipe', sort=False):
        group = group.sort_values(by=['最終仕込デッドライン', '標準仕込希望日']).reset_index(drop=True)
        max_capacity = group['釜最大容量'].iloc[0]
        # 製品キュー
        q = [{
            'code': r['code'],
            'name': r['productname'],
            'cell': r['cell'],
            'fill_date': r['充填日'],
            'amount': r['必要素地量'],
            'deadline': r['最終仕込デッドライン'],
            'preferred': r['標準仕込希望日']
        } for _, r in group.iterrows()]

        while q:
            current, cur_amount = [], 0.0
            i = 0
            while i < len(q) and len(current) < MAX_PRODUCTS_PER_BATCH:
                prod = q[i]
                remain = max_capacity - cur_amount
                if remain <= 0:
                    break
                if prod['amount'] <= remain:
                    cur_amount += prod['amount']
                    current.append({**prod, 'is_partial': False})
                    q.pop(i)
                else:
                    absorbed = remain
                    cur_amount += absorbed
                    current.append({
                        **prod,
                        'amount': absorbed,
                        'is_partial': True,
                        'original_amount': prod['amount']
                    })
                    prod['amount'] -= absorbed
                    break  # 次バッチへ

            if current:
                base = current[0]
                row = {
                    'Recipe': recipe,
                    '充填日': base['fill_date'],
                    '標準仕込希望日': q[0]['preferred'] if q else base['fill_date'],
                    '最終仕込デッドライン': q[0]['deadline'] if q else base['fill_date'],
                    '必要素地量': cur_amount,
                    '釜最大容量': max_capacity,
                    '余剰液量': max_capacity - cur_amount,
                    '統合製品数': len(current),
                    '統合フラグ': '統合済' if len(current) > 1 else '単独',
                    '仕込回数削減': len(current) - 1
                }
                for idx in range(MAX_PRODUCTS_PER_BATCH):
                    num = idx + 1
                    if idx < len(current):
                        p = current[idx]
                        row[f'製品({num})_コード'] = p['code']
                        row[f'製品({num})_商品名'] = p['name']
                        row[f'製品({num})_個数'] = p['cell']
                        row[f'製品({num})_充填日'] = p['fill_date'].strftime('%Y-%m-%d')
                        row[f'製品({num})_素地量'] = round(p['amount'], 2)
                        row[f'製品({num})_状態'] = '部分' if p.get('is_partial', False) else '全量'
                    else:
                        row[f'製品({num})_コード'] = ''
                        row[f'製品({num})_商品名'] = ''
                        row[f'製品({num})_個数'] = ''
                        row[f'製品({num})_充填日'] = ''
                        row[f'製品({num})_素地量'] = ''
                        row[f'製品({num})_状態'] = ''
                rows.append(row)
    return pd.DataFrame(rows)

def run_scheduler():
    # ロード
    working_days_dt, working_days_lookup = get_working_days(CALENDAR_FILE, CALENDAR_START_YEAR)
    max_batch_dict, lt_dict = get_material_info(MATERIAL_FILE)
    df_log = safe_read_csv(LOG_FILE)
    required_cols = ['day','Recipe','batchsize','code','productname','cell']
    if not all(c in df_log.columns for c in required_cols):
        raise KeyError("log.csv に必要列が不足しています: " + str(required_cols))

    df_plan = df_log[required_cols].copy()
    df_plan['充填日'] = pd.to_datetime(df_plan['day'], errors='coerce')
    df_plan['必要素地量'] = pd.to_numeric(df_plan['batchsize'], errors='coerce')
    # 期間フィルタ
    df_plan = df_plan[
        (df_plan['充填日'] >= pd.to_datetime(FILTER_START_DATE)) &
        (df_plan['充填日'] <= pd.to_datetime(FILTER_END_DATE))
    ]
    df_plan = df_plan.dropna(subset=['充填日','Recipe','必要素地量'])
    df_plan = df_plan[df_plan['必要素地量'] > 0]

    if not df_plan.empty:
        df_plan['L/T'] = df_plan['Recipe'].map(lt_dict).fillna(DEFAULT_LEAD_TIME)
        df_plan['釜最大容量'] = df_plan['Recipe'].map(max_batch_dict).fillna(0)
        df_plan['code'] = pd.to_numeric(df_plan['code'], errors='coerce').fillna(0).astype(int)
        df_plan['cell'] = pd.to_numeric(df_plan['cell'], errors='coerce').fillna(0).astype(int)
        df_plan['標準仕込希望日'] = df_plan['充填日'].apply(
            lambda d: get_prep_day_by_index(d, STANDARD_LEAD_TIME, working_days_dt, working_days_lookup)
        )
        df_plan['最終仕込デッドライン'] = df_plan.apply(
            lambda r: get_prep_day_by_index(r['充填日'], int(r['L/T']), working_days_dt, working_days_lookup),
            axis=1
        )

    # 出力
    path_schedulable = os.path.join(OUTPUT_DIR, f'scheduler_list_{TODAY_STR}.csv')
    path_shortage    = os.path.join(OUTPUT_DIR, f'scheduler_shortage_{TODAY_STR}.csv')

    if '最終仕込デッドライン' in df_plan.columns and not df_plan.empty:
        df_schedulable = df_plan.dropna(subset=['最終仕込デッドライン']).sort_values(
            by=['最終仕込デッドライン','標準仕込希望日','Recipe']
        )
    else:
        df_schedulable = pd.DataFrame()

   # ==== run_scheduler の「df_out を書き出す直前」差し替えパッチ ====
def _safe_write_scheduler_csv(df_schedulable, output_dir, today_str):
    path_schedulable = os.path.join(output_dir, f'scheduler_list_{today_str}.csv')

    if df_schedulable.empty:
        print("ℹ️ スケジュール可能タスクなし（scheduler_list 出力スキップ）")
        return

    df_out = df_schedulable.copy()
    # 日付列の整形
    for col in ['充填日','標準仕込希望日','最終仕込デッドライン']:
        if col in df_out.columns:
            df_out[col] = pd.to_datetime(df_out[col], errors='coerce').dt.strftime('%Y-%m-%d')

    # 期待列（無いものは空列で補完）
    output_cols = [
        'Recipe','充填日','標準仕込希望日','最終仕込デッドライン',
        '必要素地量','釜最大容量','余剰液量','統合フラグ','統合製品数','仕込回数削減',
        '製品(1)_コード','製品(1)_商品名','製品(1)_個数','製品(1)_充填日','製品(1)_素地量','製品(1)_状態',
        '製品(2)_コード','製品(2)_商品名','製品(2)_個数','製品(2)_充填日','製品(2)_素地量','製品(2)_状態',
        '製品(3)_コード','製品(3)_商品名','製品(3)_個数','製品(3)_充填日','製品(3)_素地量','製品(3)_状態',
        '製品(4)_コード','製品(4)_商品名','製品(4)_個数','製品(4)_充填日','製品(4)_素地量','製品(4)_状態',
        '製品リスト'
    ]
    for c in output_cols:
        if c not in df_out.columns:
            df_out[c] = ""

    # 並べ替えて保存
    df_out = df_out[output_cols]
    df_out.to_csv(path_schedulable, index=False, encoding='utf-8-sig')
    print(f"✅ 出力: {path_schedulable} ({len(df_out)} 件)")

# --- run_scheduler を上書き：最後の保存部だけ上の関数を呼ぶ ---
def run_scheduler():
    working_days_dt, working_days_lookup = get_working_days(CALENDAR_FILE, CALENDAR_START_YEAR)
    max_batch_dict, lt_dict = get_material_info(MATERIAL_FILE)
    df_log = safe_read_csv(LOG_FILE)

    required_cols = ['day','Recipe','batchsize','code','productname','cell']
    if not all(c in df_log.columns for c in required_cols):
        raise KeyError("log.csv に必要列が不足しています: " + str(required_cols))

    df_plan = df_log[required_cols].copy()
    df_plan['充填日'] = pd.to_datetime(df_plan['day'], errors='coerce')
    df_plan['必要素地量'] = pd.to_numeric(df_plan['batchsize'], errors='coerce')

    df_plan = df_plan[
        (df_plan['充填日'] >= pd.to_datetime(FILTER_START_DATE)) &
        (df_plan['充填日'] <= pd.to_datetime(FILTER_END_DATE))
    ].dropna(subset=['充填日','Recipe','必要素地量'])
    df_plan = df_plan[df_plan['必要素地量'] > 0]

    if not df_plan.empty:
        df_plan['L/T'] = df_plan['Recipe'].map(lt_dict).fillna(DEFAULT_LEAD_TIME)
        df_plan['釜最大容量'] = df_plan['Recipe'].map(max_batch_dict).fillna(0)
        df_plan['code'] = pd.to_numeric(df_plan['code'], errors='coerce').fillna(0).astype(int)
        df_plan['cell'] = pd.to_numeric(df_plan['cell'], errors='coerce').fillna(0).astype(int)
        df_plan['標準仕込希望日'] = df_plan['充填日'].apply(
            lambda d: get_prep_day_by_index(d, STANDARD_LEAD_TIME, working_days_dt, working_days_lookup)
        )
        df_plan['最終仕込デッドライン'] = df_plan.apply(
            lambda r: get_prep_day_by_index(r['充填日'], int(r['L/T']), working_days_dt, working_days_lookup),
            axis=1
        )

    # 可スケジュール
    if '最終仕込デッドライン' in df_plan.columns and not df_plan.empty:
        df_schedulable = df_plan.dropna(subset=['最終仕込デッドライン']).sort_values(
            by=['最終仕込デッドライン','標準仕込希望日','Recipe']
        )
    else:
        df_schedulable = pd.DataFrame()

    if not df_schedulable.empty:
        df_schedulable = consolidate_batches_advanced(df_schedulable)
    else:
        print("ℹ️ スケジュール可能タスクなし（統合処理スキップ）")

    # 保存（不足列は空で補完）
    _safe_write_scheduler_csv(df_schedulable, OUTPUT_DIR, TODAY_STR)

    # 不足タスクも従来どおり（必要ならこの下を略）
    path_shortage = os.path.join(OUTPUT_DIR, f'scheduler_shortage_{TODAY_STR}.csv')
    if '最終仕込デッドライン' in df_plan.columns:
        df_shortage = df_plan[df_plan['最終仕込デッドライン'].isna()].copy()
    else:
        df_shortage = pd.DataFrame()
    if not df_shortage.empty:
        df_shortage['理由'] = df_shortage.apply(
            lambda r: ("充填日が稼働日カレンダーにない" if r['充填日'] not in working_days_lookup.index
                       else "リードタイムがカレンダー範囲外"),
            axis=1
        )
        df_shortage['充填日'] = df_shortage['充填日'].dt.strftime('%Y-%m-%d')
        df_shortage[['Recipe','充填日','必要素地量','code','productname','cell','理由']].to_csv(
            path_shortage, index=False, encoding='utf-8-sig'
        )
        print(f"✅ 出力: {path_shortage} ({len(df_shortage)} 件)")
    else:
        print("ℹ️ 不足タスクなし（shortage 出力スキップ）")

# ===== 4) ② AIscheduler_YYYYMMDD.csv 生成（先ほどの仕様） =====
def run_ai_formatter():
    df_log = safe_read_csv(LOG_FILE)
    df_mat = safe_read_csv(MATERIAL_FILE)

    RECIPE_CANDIDATES     = ["Recipe","素地","recipe","item_name","recipe_name"]
    DEADLINE_CANDIDATES   = ["最終仕込デッドライン","最終仕込デットライン","deadline","production_deadline","最終仕込"]
    ITEM_CODE_CANDIDATES  = ["item_code","コード","code"]
    MAXCAP_CANDIDATES     = ["釜最大容量","油脂仕込み量１","Maxbatchsize","max_batch_size"]

    RECIPE_COL     = pick_col(df_log, RECIPE_CANDIDATES) or df_log.columns[0]
    DEADLINE_COL   = pick_col(df_log, DEADLINE_CANDIDATES)
    ITEM_CODE_COL  = pick_col(df_mat, ITEM_CODE_CANDIDATES) or "item_code"
    RECIPE_KEY_MAT = pick_col(df_mat, RECIPE_CANDIDATES)
    MAXCAP_COL     = pick_col(df_mat, MAXCAP_CANDIDATES)

    df = df_log.copy()
    if DEADLINE_COL:
        df[DEADLINE_COL] = pd.to_datetime(df[DEADLINE_COL], errors="coerce")
    else:
        DEADLINE_COL = "production_deadline"
        df[DEADLINE_COL] = pd.NaT

    # item_code付与
    if RECIPE_KEY_MAT and ITEM_CODE_COL in df_mat.columns:
        df = df.merge(
            df_mat[[RECIPE_KEY_MAT, ITEM_CODE_COL]].drop_duplicates(),
            how="left", left_on=RECIPE_COL, right_on=RECIPE_KEY_MAT
        ).drop(columns=[c for c in [RECIPE_KEY_MAT] if c != RECIPE_COL], errors="ignore")
    else:
        df[ITEM_CODE_COL] = np.nan

    # 釜最大容量付与
    if RECIPE_KEY_MAT and MAXCAP_COL and MAXCAP_COL in df_mat.columns:
        df = df.merge(
            df_mat[[RECIPE_KEY_MAT, MAXCAP_COL]].drop_duplicates(),
            how="left", left_on=RECIPE_COL, right_on=RECIPE_KEY_MAT
        ).drop(columns=[c for c in [RECIPE_KEY_MAT] if c != RECIPE_COL], errors="ignore")
        if MAXCAP_COL != "釜最大容量":
            df.rename(columns={MAXCAP_COL: "釜最大容量"}, inplace=True)
    else:
        if "釜最大容量" not in df.columns:
            df["釜最大容量"] = np.nan

    # lot_no（Recipeごとに01〜）
    df["_idx"] = np.arange(len(df))
    df = df.sort_values([RECIPE_COL, DEADLINE_COL, "_idx"], na_position="last")
    df["serial_within_recipe"] = df.groupby(RECIPE_COL).cumcount() + 1
    df["lot_no"] = df[RECIPE_COL].astype(str) + df["serial_within_recipe"].astype(str).str.zfill(2)

    # 仕込/PH ブロック
    block1 = pd.DataFrame({
        "id": "",
        "lot_no": df["lot_no"],
        "item_code": df[ITEM_CODE_COL] if ITEM_CODE_COL in df.columns else "",
        "item_name": df[RECIPE_COL],
        "process_code": 1,
        "process_name": "仕込/PH",
        "num": 2,
        "prep_amount": df["釜最大容量"],
        "production_deadline": df[DEADLINE_COL].dt.strftime("%Y-%m-%d") if pd.api.types.is_datetime64_any_dtype(df[DEADLINE_COL]) else df[DEADLINE_COL],
        "before_arrange_ids": ""
    })
    block1["arrange_data_type"] = 0
    block1["arrange_status"]    = 0

    # 配合/充填 ブロック（コピーして変更）
    block2 = block1.copy()
    block2["process_code"]   = 2
    block2["process_name"]   = "配合/充填"
    block2["num"]            = 1
    block2["before_arrange_ids"] = [str(x) for x in range(1, 2*len(block2), 2)]  # 1,3,5,...

    # 結合＆ID採番
    out = pd.concat([block1, block2], ignore_index=True)
    out["id"] = (np.arange(len(out)) + 1).astype(int)

    col_order = [
        "id","lot_no","item_code","item_name",
        "process_code","process_name","num","prep_amount",
        "production_deadline","before_arrange_ids",
        "arrange_data_type","arrange_status"
    ]
    for c in col_order:
        if c not in out.columns:
            out[c] = ""
    out = out[col_order]

    path_ai = os.path.join(OUTPUT_DIR, f"AIscheduler_{TODAY_STR}.csv")
    out.to_csv(path_ai, index=False, encoding="utf-8-sig")
    print(f"✅ 出力: {path_ai} ({len(out)}行)")

# ===== 5) --- 実 行 -------------------------------------------------
# デフォルトは両方実行。片方だけにしたい場合は、下の行をコメントアウトしてください。
run_scheduler()      # ← スケジューラ（scheduler_list_YYYYMMDD.csv / scheduler_shortage_YYYYMMDD.csv）
run_ai_formatter()   # ← AIscheduler_YYYYMMDD.csv

# ---------------------------------------------------------------
# ここまで。必要に応じて FILTER_START_DATE / FILTER_END_DATE などを上で調整してください。
