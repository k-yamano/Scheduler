import pandas as pd
import numpy as np
import re
import math
from datetime import timedelta, datetime
import os
from typing import Tuple, Dict, List, Optional
import warnings

from google.colab import drive
drive.mount('/content/drive')

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

# --- 2. ヘルパー関数群 ---

def get_working_days(calendar_file_path: str, start_year: int) -> Tuple[pd.DatetimeIndex, pd.Series]:
    """
    カレンダーCSV(横持ち)を読み込み、稼働日リストとルックアップ辞書を返す。
    """
    try:
        df_calendar = pd.read_csv(calendar_file_path, header=None)
    except FileNotFoundError:
        print(f"エラー: カレンダーファイル '{calendar_file_path}' が見つかりません。")
        raise

    calendar_dates = df_calendar.iloc[0].values
    working_days  = []
    date_pattern  = re.compile(r"(\d{1,2})/(\d{1,2})")

    current_year  = start_year
    last_month    = 1 # 年またぎ検出用

    for date_val in calendar_dates:
        match = date_pattern.match(str(date_val))
        if match:
            month, day = map(int, match.groups())

            # 月が巻き戻ったら（例: 12月 -> 1月）、年をインクリメント
            if month < last_month:
                current_year += 1

            working_days.append(f"{current_year}/{month:02d}/{day:02d}")
            last_month = month

    working_days_dt     = pd.to_datetime(sorted(list(set(working_days))))
    working_days_lookup = pd.Series(range(len(working_days_dt)), index=working_days_dt)

    if len(working_days_dt) == 0:
        raise ValueError("カレンダーから稼働日リストが取得できませんでした。")
    print(f"--- 稼働日リスト作成完了 (全 {len(working_days_dt)} 日, {working_days_dt.min().year}〜{working_days_dt.max().year}年) ---")
    return working_days_dt, working_days_lookup

def get_material_info(material_file_path: str) -> Tuple[Dict, Dict]:
    """
    マテリアルマスタCSVから「釜最大容量」と「リードタイム(LT)」辞書を返す
    """
    try:
        df_material = pd.read_csv(material_file_path)
    except FileNotFoundError:
        print(f"エラー: マテリアルマスタ '{material_file_path}' が見つかりません。")
        raise

    if '素地' not in df_material.columns or '油脂仕込み量１' not in df_material.columns:
        raise KeyError(f"マテリアルマスタに必要な列 ('素地', '油脂仕込み量１') がありません。")

    # SettingWithCopyWarning を回避
    df_unique = df_material.drop_duplicates(subset=['素地']).copy()

    df_unique['Maxbatchsize'] = pd.to_numeric(df_unique['油脂仕込み量１'], errors='coerce').fillna(0)
    max_batch_dict = pd.Series(df_unique['Maxbatchsize'].values, index=df_unique['素地']).to_dict()

    lt_dict = {}
    for _, row in df_unique.iterrows():
        recipe    = row['素地']
        is_short  = '工程３' not in row or pd.isna(row['工程３'])

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
    仕込回数最小化に特化した統合ロジック
    - 同一Recipeで釜容量を最大限活用
    - 余剰がある場合は次の製品を部分的にでも吸収
    - 最大4製品まで統合可能
    """
    if df_schedulable.empty:
        return df_schedulable
    
    print("\n--- 仕込回数最小化バッチ統合処理開始 ---")
    
    consolidated_rows = []
    
    for recipe, group in df_schedulable.groupby('Recipe', sort=False):
        group = group.sort_values(by=['最終仕込デッドライン', '標準仕込希望日']).reset_index(drop=True)
        max_capacity = group['釜最大容量'].iloc[0]
        
        processed = set()  # 処理済みインデックス
        
        i = 0
        while i < len(group):
            if i in processed:
                i += 1
                continue
                
            current_row = group.iloc[i].copy()
            current_amount = current_row['必要素地量']
            merged_products = [{
                'code': current_row['code'],
                'name': current_row['productname'],
                'cell': current_row['cell'],
                'fill_date': current_row['充填日'],
                'amount': current_row['必要素地量'],
                'is_partial': False
            }]
            processed.add(i)
            
            # 次の製品を最大4製品まで吸収
            j = i + 1
            while j < len(group) and len(merged_products) < MAX_PRODUCTS_PER_BATCH:
                if j in processed:
                    j += 1
                    continue
                    
                next_row = group.iloc[j]
                next_amount = next_row['必要素地量']
                remaining_capacity = max_capacity - current_amount
                
                if remaining_capacity <= 0:
                    break  # 釜が満杯
                
                if next_amount <= remaining_capacity:
                    # 全量吸収可能
                    current_amount += next_amount
                    merged_products.append({
                        'code': next_row['code'],
                        'name': next_row['productname'],
                        'cell': next_row['cell'],
                        'fill_date': next_row['充填日'],
                        'amount': next_amount,
                        'is_partial': False
                    })
                    processed.add(j)
                    print(f"  吸収: Recipe={recipe}, 製品{next_row['code']}を全量吸収 ({next_amount:.2f})")
                elif remaining_capacity > 0 and len(merged_products) < MAX_PRODUCTS_PER_BATCH:
                    # 部分吸収（釜の余剰分だけ）
                    absorbed_amount = remaining_capacity
                    current_amount += absorbed_amount
                    merged_products.append({
                        'code': next_row['code'],
                        'name': next_row['productname'],
                        'cell': next_row['cell'],
                        'fill_date': next_row['充填日'],
                        'amount': absorbed_amount,
                        'is_partial': True,
                        'original_amount': next_amount
                    })
                    
                    # 残りを新しい行として保持（次の仕込み対象）
                    remaining_row = next_row.copy()
                    remaining_row['必要素地量'] = next_amount - absorbed_amount
                    
                    print(f"  部分吸収: Recipe={recipe}, 製品{next_row['code']}を部分吸収 ({absorbed_amount:.2f}/{next_amount:.2f})")
                    
                    # 残りを次の処理対象として追加
                    group = pd.concat([group.iloc[:j+1], pd.DataFrame([remaining_row]), group.iloc[j+1:]], ignore_index=True)
                    
                    break  # 釜満杯のため次のバッチへ
                
                j += 1
            
            # 統合結果を保存
            current_row['必要素地量'] = current_amount
            current_row['余剰液量'] = max_capacity - current_amount
            current_row['統合製品数'] = len(merged_products)
            current_row['統合フラグ'] = '統合済' if len(merged_products) > 1 else '単独'
            current_row['仕込回数削減'] = len(merged_products) - 1
            
            # 製品①〜④の個別情報を出力（丸数字ではなく括弧数字を使用）
            for idx in range(MAX_PRODUCTS_PER_BATCH):
                num = idx + 1
                if idx < len(merged_products):
                    prod = merged_products[idx]
                    current_row[f'製品({num})_コード'] = prod['code']
                    current_row[f'製品({num})_商品名'] = prod['name']
                    current_row[f'製品({num})_個数'] = prod['cell']
                    current_row[f'製品({num})_充填日'] = prod['fill_date'].strftime('%Y-%m-%d')
                    current_row[f'製品({num})_素地量'] = round(prod['amount'], 2)
                    current_row[f'製品({num})_状態'] = '部分' if prod.get('is_partial', False) else '全量'
                else:
                    current_row[f'製品({num})_コード'] = ''
                    current_row[f'製品({num})_商品名'] = ''
                    current_row[f'製品({num})_個数'] = ''
                    current_row[f'製品({num})_充填日'] = ''
                    current_row[f'製品({num})_素地量'] = ''
                    current_row[f'製品({num})_状態'] = ''
            
            # 製品リスト（サマリー用）
            product_list_parts = []
            for prod in merged_products:
                status = f"[部分:{prod['amount']:.1f}/{prod['original_amount']:.1f}]" if prod.get('is_partial', False) else f"[全量:{prod['amount']:.1f}]"
                product_list_parts.append(
                    f"{prod['code']}:{prod['name']}({prod['cell']}個){status}"
                )
            current_row['製品リスト'] = ' | '.join(product_list_parts)
            
            consolidated_rows.append(current_row)
            i += 1
    
    df_consolidated = pd.DataFrame(consolidated_rows)
    
    original_count = len(df_schedulable)
    consolidated_count = len(df_consolidated)
    total_reduction = df_consolidated['仕込回数削減'].sum()
    
    print(f"--- バッチ統合完了 ---")
    print(f"  元の仕込予定: {original_count}回")
    print(f"  統合後の仕込: {consolidated_count}回")
    print(f"  削減回数: {int(total_reduction)}回 ({total_reduction/original_count*100:.1f}%削減)")
    
    return df_consolidated

# --- 3. メイン処理 ---
try:
    print("\n--- 処理開始 ---")

    # 3-1. マスタ＆稼働日の読み込み
    working_days_dt, working_days_lookup = get_working_days(CALENDAR_FILE, CALENDAR_START_YEAR)
    max_batch_dict, lt_dict              = get_material_info(MATERIAL_FILE)

    # 3-2. 充填需要ログの読み込み
    df_log = pd.read_csv(LOG_FILE)
    print(f"--- 充填需要ログ読み込み完了 ({len(df_log)} 行) ---")

    required_cols = ['day','Recipe','batchsize','code','productname','cell']
    if not all(col in df_log.columns for col in required_cols):
        raise KeyError(f"ログファイルに不足列あり: {required_cols} を確認してください。")

    df_plan = df_log[required_cols].copy()
    df_plan['充填日']       = pd.to_datetime(df_plan['day'],       errors='coerce')
    df_plan['必要素地量'] = pd.to_numeric(df_plan['batchsize'], errors='coerce')

    # 3-3. 指定範囲にフィルタ
    start_date = pd.to_datetime(FILTER_START_DATE)
    end_date   = pd.to_datetime(FILTER_END_DATE)

    df_plan    = df_plan[(df_plan['充填日'] >= start_date) & (df_plan['充填日'] <= end_date)]
    df_plan    = df_plan.dropna(subset=['充填日','Recipe','必要素地量'])
    df_plan    = df_plan[df_plan['必要素地量'] > 0]
    print(f"--- 需要ログ整形完了: {len(df_plan)} 件 ---")

    # --- Vectorization (ベクトル化処理) ---

    if not df_plan.empty:
        print("--- ベクトル化処理でL/Tと仕込日を計算中 ---")
        # 1. マスタ情報を .map で一括付与
        df_plan['L/T']       = df_plan['Recipe'].map(lt_dict).fillna(DEFAULT_LEAD_TIME)
        df_plan['釜最大容量']  = df_plan['Recipe'].map(max_batch_dict).fillna(0)
        df_plan['code']      = pd.to_numeric(df_plan['code'], errors='coerce').fillna(0).astype(int)
        df_plan['cell']      = pd.to_numeric(df_plan['cell'], errors='coerce').fillna(0).astype(int)

        # 2. 仕込日を .apply で一括計算
        # 2a. 標準仕込希望日
        df_plan['標準仕込希望日'] = df_plan['充填日'].apply(
            lambda d: get_prep_day_by_index(d, STANDARD_LEAD_TIME, working_days_dt, working_days_lookup)
        )

        # 2b. 最終仕込デッドライン
        df_plan['最終仕込デッドライン'] = df_plan.apply(
            lambda row: get_prep_day_by_index(row['充填日'], int(row['L/T']), working_days_dt, working_days_lookup),
            axis=1
        )

    # 4-1. スケジュール可能タスク
    df_schedulable = df_plan.dropna(subset=['最終仕込デッドライン']).sort_values(
        by=['最終仕込デッドライン','標準仕込希望日','Recipe']
    )

    # バッチ統合処理を実行（仕込回数最小化特化版）
    if not df_schedulable.empty:
        df_schedulable = consolidate_batches_advanced(df_schedulable)

    print("\n--- スケジュール可能タスク ---")
    if df_schedulable.empty:
        print("該当タスクなし")
    else:
        # 出力前に日付形式をYYYY-MM-DDにフォーマット
        df_out_schedulable = df_schedulable.copy()
        df_out_schedulable['充填日']              = df_out_schedulable['充填日'].dt.strftime('%Y-%m-%d')
        df_out_schedulable['標準仕込希望日']      = df_out_schedulable['標準仕込希望日'].dt.strftime('%Y-%m-%d')
        df_out_schedulable['最終仕込デッドライン'] = df_out_schedulable['最終仕込デッドライン'].dt.strftime('%Y-%m-%d')

        output_cols = [
            'Recipe','充填日','標準仕込希望日','最終仕込デッドライン',
            '必要素地量','釜最大容量','余剰液量','統合フラグ','統合製品数','仕込回数削減',
            '製品(1)_コード','製品(1)_商品名','製品(1)_個数','製品(1)_充填日','製品(1)_素地量','製品(1)_状態',
            '製品(2)_コード','製品(2)_商品名','製品(2)_個数','製品(2)_充填日','製品(2)_素地量','製品(2)_状態',
            '製品(3)_コード','製品(3)_商品名','製品(3)_個数','製品(3)_充填日','製品(3)_素地量','製品(3)_状態',
            '製品(4)_コード','製品(4)_商品名','製品(4)_個数','製品(4)_充填日','製品(4)_素地量','製品(4)_状態',
            '製品リスト'
        ]
        print(df_out_schedulable[output_cols].head(10).to_string(index=False))
        df_schedulable_to_csv = df_out_schedulable[output_cols]

    # 4-2. 不足／計算不可タスク
    df_shortage = df_plan[df_plan['最終仕込デッドライン'].isna()].copy()

    if not df_shortage.empty:
        df_shortage['理由'] = df_shortage.apply(
            lambda r: "充填日が稼働日カレンダーにない"
                      if r['充填日'] not in working_days_lookup.index
                      else "リードタイムがカレンダー範囲外",
            axis=1
        )
        # 充填日をフォーマット
        df_shortage['充填日'] = df_shortage['充填日'].dt.strftime('%Y-%m-%d')
        df_shortage_to_csv = df_shortage[['Recipe','充填日','必要素地量','code','productname','cell','理由']]
    else:
        df_shortage_to_csv = pd.DataFrame()

    print("\n--- 計算不可タスク（不足リスト） ---")
    if df_shortage_to_csv.empty:
        print("該当タスクなし")
    else:
        print(df_shortage_to_csv.head(10).to_string(index=False))

    # --- 5. 出力処理 ---
    today     = datetime.now()
    date_str  = today.strftime('%Y%m%d')

    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        print(f"作成しました: 出力フォルダ {OUTPUT_DIR}")

    path_schedulable = os.path.join(OUTPUT_DIR, f'scheduler_list_{date_str}.csv')
    path_shortage    = os.path.join(OUTPUT_DIR, f'scheduler_shortage_{date_str}.csv')

    if not df_schedulable_to_csv.empty:
        df_schedulable_to_csv.to_csv(path_schedulable, index=False, encoding='utf-8-sig')
        print(f"\n出力完了: {path_schedulable} （{len(df_schedulable_to_csv)} 件）")
        
        # サマリー情報
        total_batches = len(df_schedulable_to_csv)
        consolidated_batches = len(df_schedulable_to_csv[df_schedulable_to_csv['統合フラグ'] == '統合済'])
        print(f"  - 単独仕込: {total_batches - consolidated_batches}件")
        print(f"  - 統合仕込: {consolidated_batches}件")
    else:
        print("スケジュール可能タスク出力対象なし")

    if not df_shortage_to_csv.empty:
        df_shortage_to_csv.to_csv(path_shortage, index=False, encoding='utf-8-sig')
        print(f"出力完了: {path_shortage} （{len(df_shortage_to_csv)} 件）")
    else:
        print("不足タスク出力対象なし")

    print("\n--- 全処理完了 ---")

except Exception as e:
    print("\n--- エラーが発生しました ---")
    print(e)
    import traceback
    traceback.print_exc()