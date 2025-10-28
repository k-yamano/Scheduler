import pandas as pd
import numpy as np
import re
import math
from datetime import timedelta, datetime
import os
from typing import Tuple, Dict, List, Optional
import warnings

# 警告を非表示
warnings.filterwarnings('ignore')

# ファイルパス
INPUT_DIR        = '/content/drive/My Drive/dp_Scheduler/Input/Master/'
OUTPUT_DIR       = '/content/drive/My Drive/dp_Scheduler/Output/'
LOG_FILE         = os.path.join(INPUT_DIR, 'log.csv')
CALENDAR_FILE    = os.path.join(INPUT_DIR, 'workday.csv')
MATERIAL_FILE    = os.path.join(INPUT_DIR, 'material_master.csv')

# 計画ロジック設定
STANDARD_LEAD_TIME = 4  # 標準仕込リードタイム（営業日）
DEFAULT_LEAD_TIME  = 3  # デフォルトの素地L/T（営業日）
SHORT_LEAD_TIME    = 1  # 短縮L/T（NR, LC, 工程3なし）
CALENDAR_START_YEAR= 2025 # カレンダーの開始年（M/D形式のファイル用）
MAX_PRODUCTS_PER_BATCH = 4  # 1バッチあたり最大製品数

# 充填需要のフィルタ期間
FILTER_START_DATE = "2025-10-01"
FILTER_END_DATE   = "2025-11-30"

# --- ヘルパー関数群 ---

def get_working_days(calendar_file_path: str, start_year: int) -> Tuple[pd.DatetimeIndex, pd.Series]:
    """カレンダーCSV(横持ち)を読み込み、稼働日リストとルックアップ辞書を返す"""
    try:
        df_calendar = pd.read_csv(calendar_file_path, header=None)
    except FileNotFoundError:
        print(f"エラー: カレンダーファイル '{calendar_file_path}' が見つかりません。")
        raise

    calendar_dates = df_calendar.iloc[0].values
    working_days  = []
    date_pattern  = re.compile(r"(\d{1,2})/(\d{1,2})")
    current_year  = start_year
    last_month    = 1

    for date_val in calendar_dates:
        match = date_pattern.match(str(date_val))
        if match:
            month, day = map(int, match.groups())
            if month < last_month:
                current_year += 1
            working_days.append(f"{current_year}/{month:02d}/{day:02d}")
            last_month = month

    working_days_dt     = pd.to_datetime(sorted(list(set(working_days))))
    working_days_lookup = pd.Series(range(len(working_days_dt)), index=working_days_dt)

    if len(working_days_dt) == 0:
        raise ValueError("カレンダーから稼働日リストが取得できませんでした。")
    print(f"--- 稼働日リスト作成完了 (全 {len(working_days_dt)} 日) ---")
    return working_days_dt, working_days_lookup

def get_material_info(material_file_path: str) -> Tuple[Dict, Dict]:
    """マテリアルマスタから釜容量とリードタイム辞書を返す"""
    try:
        df_material = pd.read_csv(material_file_path)
    except FileNotFoundError:
        print(f"エラー: マテリアルマスタ '{material_file_path}' が見つかりません。")
        raise

    if '素地' not in df_material.columns or '油脂仕込み量１' not in df_material.columns:
        raise KeyError(f"マテリアルマスタに必要な列がありません。")

    df_unique = df_material.drop_duplicates(subset=['素地']).copy()
    df_unique['Maxbatchsize'] = pd.to_numeric(df_unique['油脂仕込み量１'], errors='coerce').fillna(0)
    max_batch_dict = pd.Series(df_unique['Maxbatchsize'].values, index=df_unique['素地']).to_dict()

    lt_dict = {}
    for _, row in df_unique.iterrows():
        recipe = row['素地']
        is_short = '工程３' not in row or pd.isna(row['工程３'])
        if recipe in ['NR', 'LC'] or is_short:
            lt_dict[recipe] = SHORT_LEAD_TIME
        else:
            lt_dict[recipe] = DEFAULT_LEAD_TIME

    print(f"--- 釜容量・リードタイムマスタ作成完了 ---")
    return max_batch_dict, lt_dict

def get_prep_day_by_index(
    filling_date: pd.Timestamp,
    lead_time_days: int,
    working_days_list: pd.DatetimeIndex,
    working_days_lookup: pd.Series
) -> Optional[pd.Timestamp]:
    """充填日から N 営業日前の仕込日を計算"""
    if pd.isna(filling_date):
        return None
    idx = working_days_lookup.get(filling_date)
    if idx is None:
        return None
    target_idx = idx - lead_time_days
    if target_idx < 0:
        return None
    return working_days_list[target_idx]

def consolidate_batches_advanced(df_schedulable: pd.DataFrame) -> pd.DataFrame:
    """
    改善版バッチ統合ロジック
    - 同一Recipeで釜容量を最大活用
    - 部分吸収の残量を適切に管理
    - 重複仕込を防止
    """
    if df_schedulable.empty:
        return df_schedulable

    print("\n--- 改善版バッチ統合処理開始 ---")
    consolidated_rows = []

    for recipe, group in df_schedulable.groupby('Recipe', sort=False):
        group = group.sort_values(by=['最終仕込デッドライン', '標準仕込希望日']).reset_index(drop=True)
        max_capacity = group['釜最大容量'].iloc[0]

        # 製品リストを作成(部分吸収管理用)
        product_queue = []
        for idx, row in group.iterrows():
            product_queue.append({
                'code': row['code'],
                'name': row['productname'],
                'cell': row['cell'],
                'fill_date': row['充填日'],
                'amount': row['必要素地量'],
                'deadline': row['最終仕込デッドライン'],
                'preferred': row['標準仕込希望日']
            })

        # バッチ統合処理
        while product_queue:
            current_batch = []
            current_amount = 0.0
            
            i = 0
            while i < len(product_queue) and len(current_batch) < MAX_PRODUCTS_PER_BATCH:
                product = product_queue[i]
                remaining_capacity = max_capacity - current_amount

                if remaining_capacity <= 0:
                    break  # 釜が満杯

                if product['amount'] <= remaining_capacity:
                    # 全量吸収可能
                    current_amount += product['amount']
                    current_batch.append({
                        'code': product['code'],
                        'name': product['name'],
                        'cell': product['cell'],
                        'fill_date': product['fill_date'],
                        'amount': product['amount'],
                        'is_partial': False
                    })
                    product_queue.pop(i)  # キューから削除
                    print(f"  吸収: Recipe={recipe}, 製品{product['code']}を全量吸収 ({product['amount']:.2f})")
                else:
                    # 部分吸収
                    absorbed = remaining_capacity
                    current_amount += absorbed
                    current_batch.append({
                        'code': product['code'],
                        'name': product['name'],
                        'cell': product['cell'],
                        'fill_date': product['fill_date'],
                        'amount': absorbed,
                        'is_partial': True,
                        'original_amount': product['amount']
                    })
                    # 残量を更新してキューに残す
                    product['amount'] -= absorbed
                    print(f"  部分吸収: Recipe={recipe}, 製品{product['code']} ({absorbed:.2f}/{product['amount']+absorbed:.2f})")
                    break  # 釜満杯なので次のバッチへ
                
            # バッチ情報を保存
            if current_batch:
                # 代表行を作成(最初の製品の情報をベースに)
                base_product = current_batch[0]
                batch_row = {
                    'Recipe': recipe,
                    '充填日': base_product['fill_date'],
                    '標準仕込希望日': product_queue[0]['preferred'] if product_queue else base_product['fill_date'],
                    '最終仕込デッドライン': product_queue[0]['deadline'] if product_queue else base_product['fill_date'],
                    '必要素地量': current_amount,
                    '釜最大容量': max_capacity,
                    '余剰液量': max_capacity - current_amount,
                    '統合製品数': len(current_batch),
                    '統合フラグ': '統合済' if len(current_batch) > 1 else '単独',
                    '仕込回数削減': len(current_batch) - 1
                }

                # 製品詳細情報を追加
                for idx in range(MAX_PRODUCTS_PER_BATCH):
                    num = idx + 1
                    if idx < len(current_batch):
                        prod = current_batch[idx]
                        batch_row[f'製品({num})_コード'] = prod['code']
                        batch_row[f'製品({num})_商品名'] = prod['name']
                        batch_row[f'製品({num})_個数'] = prod['cell']
                        batch_row[f'製品({num})_充填日'] = prod['fill_date'].strftime('%Y-%m-%d')
                        batch_row[f'製品({num})_素地量'] = round(prod['amount'], 2)
                        batch_row[f'製品({num})_状態'] = '部分' if prod.get('is_partial', False) else '全量'
                    else:
                        batch_row[f'製品({num})_コード'] = ''
                        batch_row[f'製品({num})_商品名'] = ''
                        batch_row[f'製品({num})_個数'] = ''
                        batch_row[f'製品({num})_充填日'] = ''
                        batch_row[f'製品({num})_素地量'] = ''
                        batch_row[f'製品({num})_状態'] = ''

                # 製品リスト(サマリー)
                product_list_parts = []
                for prod in current_batch:
                    status = f"[部分:{prod['amount']:.1f}/{prod.get('original_amount', prod['amount']):.1f}]" if prod.get('is_partial', False) else f"[全量:{prod['amount']:.1f}]"
                    product_list_parts.append(
                        f"{prod['code']}:{prod['name']}({prod['cell']}個){status}"
                    )
                batch_row['製品リスト'] = ' | '.join(product_list_parts)

                consolidated_rows.append(batch_row)

    df_consolidated = pd.DataFrame(consolidated_rows)

    if not df_consolidated.empty:
        original_count = len(df_schedulable)
        consolidated_count = len(df_consolidated)
        total_reduction = df_consolidated['仕込回数削減'].sum()

        print(f"\n--- バッチ統合完了 ---")
        print(f"  元の仕込予定: {original_count}回")
        print(f"  統合後の仕込: {consolidated_count}回")
        print(f"  削減回数: {int(total_reduction)}回 ({total_reduction/original_count*100:.1f}%削減)")

    return df_consolidated

# --- メイン処理 ---
try:
    print("\n--- 処理開始 ---")

    # マスタ＆稼働日の読み込み
    working_days_dt, working_days_lookup = get_working_days(CALENDAR_FILE, CALENDAR_START_YEAR)
    max_batch_dict, lt_dict = get_material_info(MATERIAL_FILE)

    # 充填需要ログの読み込み
    df_log = pd.read_csv(LOG_FILE)
    print(f"--- 充填需要ログ読み込み完了 ({len(df_log)} 行) ---")

    required_cols = ['day','Recipe','batchsize','code','productname','cell']
    if not all(col in df_log.columns for col in required_cols):
        raise KeyError(f"ログファイルに不足列あり")

    df_plan = df_log[required_cols].copy()
    df_plan['充填日'] = pd.to_datetime(df_plan['day'], errors='coerce')
    df_plan['必要素地量'] = pd.to_numeric(df_plan['batchsize'], errors='coerce')

    # 指定範囲にフィルタ
    start_date = pd.to_datetime(FILTER_START_DATE)
    end_date = pd.to_datetime(FILTER_END_DATE)
    df_plan = df_plan[(df_plan['充填日'] >= start_date) & (df_plan['充填日'] <= end_date)]
    df_plan = df_plan.dropna(subset=['充填日','Recipe','必要素地量'])
    df_plan = df_plan[df_plan['必要素地量'] > 0]

    print(f"--- 需要ログ整形完了: {len(df_plan)} 件 ---")

    # L/Tと仕込日を計算
    if not df_plan.empty:
        df_plan['L/T'] = df_plan['Recipe'].map(lt_dict).fillna(DEFAULT_LEAD_TIME)
        df_plan['釜最大容量'] = df_plan['Recipe'].map(max_batch_dict).fillna(0)
        df_plan['code'] = pd.to_numeric(df_plan['code'], errors='coerce').fillna(0).astype(int)
        df_plan['cell'] = pd.to_numeric(df_plan['cell'], errors='coerce').fillna(0).astype(int)

        df_plan['標準仕込希望日'] = df_plan['充填日'].apply(
            lambda d: get_prep_day_by_index(d, STANDARD_LEAD_TIME, working_days_dt, working_days_lookup)
        )
        df_plan['最終仕込デッドライン'] = df_plan.apply(
            lambda row: get_prep_day_by_index(row['充填日'], int(row['L/T']), working_days_dt, working_days_lookup),
            axis=1
        )

    # スケジュール可能タスク
    if '最終仕込デッドライン' in df_plan.columns and not df_plan.empty:
        df_schedulable = df_plan.dropna(subset=['最終仕込デッドライン']).sort_values(
            by=['最終仕込デッドライン','標準仕込希望日','Recipe']
        )
    else:
        df_schedulable = pd.DataFrame()

    if not df_schedulable.empty:
        df_schedulable = consolidate_batches_advanced(df_schedulable)

    print("\n--- スケジュール可能タスク ---")
    if df_schedulable.empty:
        print("該当タスクなし")
        df_schedulable_to_csv = pd.DataFrame()
    else:
        df_out_schedulable = df_schedulable.copy()
        for col in ['充填日','標準仕込希望日','最終仕込デッドライン']:
            if col in df_out_schedulable.columns:
                df_out_schedulable[col] = pd.to_datetime(
                    df_out_schedulable[col], errors='coerce'
                ).dt.strftime('%Y-%m-%d')

        output_cols = [
            'Recipe','充填日','標準仕込希望日','最終仕込デッドライン',
            '必要素地量','釜最大容量','余剰液量','統合フラグ','統合製品数','仕込回数削減',
            '製品(1)_コード','製品(1)_商品名','製品(1)_個数','製品(1)_充填日','製品(1)_素地量','製品(1)_状態',
            '製品(2)_コード','製品(2)_商品名','製品(2)_個数','製品(2)_充填日','製品(2)_素地量','製品(2)_状態',
            '製品(3)_コード','製品(3)_商品名','製品(3)_個数','製品(3)_充填日','製品(3)_素地量','製品(3)_状態',
            '製品(4)_コード','製品(4)_商品名','製品(4)_個数','製品(4)_充填日','製品(4)_素地量','製品(4)_状態',
            '製品リスト'
        ]
        print(df_out_schedulable.head(10)[output_cols])
        df_schedulable_to_csv = df_out_schedulable[output_cols]

    # 不足タスク
    if '最終仕込デッドライン' in df_plan.columns:
        df_shortage = df_plan[df_plan['最終仕込デッドライン'].isna()].copy()
    else:
        df_shortage = pd.DataFrame()

    if not df_shortage.empty:
        df_shortage['理由'] = df_shortage.apply(
            lambda r: "充填日が稼働日カレンダーにない"
                      if r['充填日'] not in working_days_lookup.index
                      else "リードタイムがカレンダー範囲外",
            axis=1
        )
        df_shortage['充填日'] = df_shortage['充填日'].dt.strftime('%Y-%m-%d')
        df_shortage_to_csv = df_shortage[['Recipe','充填日','必要素地量','code','productname','cell','理由']]
    else:
        df_shortage_to_csv = pd.DataFrame()

    print("\n--- 計算不可タスク(不足リスト) ---")
    if df_shortage_to_csv.empty:
        print("該当タスクなし")
    else:
        print(df_shortage_to_csv.head(10).to_string(index=False))

    # 出力処理
    today = datetime.now()
    date_str = today.strftime('%Y%m%d')

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    path_schedulable = os.path.join(OUTPUT_DIR, f'scheduler_list_{date_str}.csv')
    path_shortage = os.path.join(OUTPUT_DIR, f'scheduler_shortage_{date_str}.csv')

    if not df_schedulable_to_csv.empty:
        df_schedulable_to_csv.to_csv(path_schedulable, index=False, encoding='utf-8-sig')
        print(f"\n出力完了: {path_schedulable} ({len(df_schedulable_to_csv)} 件)")
    else:
        print("スケジュール可能タスク出力対象なし")

    if not df_shortage_to_csv.empty:
        df_shortage_to_csv.to_csv(path_shortage, index=False, encoding='utf-8-sig')
        print(f"出力完了: {path_shortage} ({len(df_shortage_to_csv)} 件)")
    else:
        print("不足タスク出力対象なし")

    print("\n--- 全処理完了 ---")

except Exception as e:
    print("\n--- エラーが発生しました ---")
    print(e)
    import traceback
    traceback.print_exc()