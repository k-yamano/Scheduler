import pandas as pd
import numpy as np
import re
import math
from datetime import timedelta
import os

# --- 1. Google Driveへの接続 ---
from google.colab import drive
try:
    drive.mount('/content/drive', force_remount=True)
    print("Google Driveのマウント完了。")
except Exception as e:
    print(f"Google Driveのマウントに失敗しました: {e}")
    raise

# --- 2. 定数定義 ---
INPUT_DIR = '/content/drive/My Drive/dp_Scheduler/Input/Master/'
LOG_FILE = os.path.join(INPUT_DIR, 'log.csv')
CALENDAR_FILE = os.path.join(INPUT_DIR, 'calendar.csv')
MATERIAL_MASTER_FILE = os.path.join(INPUT_DIR, 'material_master.csv') # (material_master (2).csv (ソース 43) から変更)
OUTPUT_DIR = '/content/drive/My Drive/dp_Scheduler/Output/'
STANDARD_LEAD_TIME = 4

# --- 3. ヘルパー関数群 ---

def get_working_days(calendar_file_path):
    """
    カレンダーCSV を読み込み、稼働日リストとルックアップ辞書を返す
    （ヘッダー行から日付を抽出し、公休情報は無視）
    """
    try:
        # ヘッダー行のみ読み込み
        df_calendar = pd.read_csv(calendar_file_path, header=0, nrows=0, encoding='utf-8-sig')
    except FileNotFoundError:
        print(f"エラー: カレンダーファイル '{calendar_file_path}' が見つかりません。")
        raise
    except Exception as e:
        print(f"カレンダーファイル読み込みエラー: {e}")
        # エンコーディングエラーの可能性を考慮して Shift-JIS (CP932) で再試行
        try:
            print("再試行: エンコーディングを 'cp932' に変更します。")
            df_calendar = pd.read_csv(calendar_file_path, header=0, nrows=0, encoding='cp932')
        except Exception as e_retry:
            print(f"再試行失敗: {e_retry}")
            raise

    # カラム名から日付を抽出（'10/25'形式のみ）
    calendar_dates = []
    date_pattern = re.compile(r"^(\d{1,2})/(\d{1,2})$")

    for col in df_calendar.columns:
        col_str = str(col).strip()
        if date_pattern.match(col_str):
            calendar_dates.append(col_str)

    # 日付を年付きフォーマットに変換
    working_days = []
    current_year = 2025

    for i, date_str in enumerate(calendar_dates):
        match = date_pattern.match(date_str)
        if match:
            month, day = map(int, match.groups())

            # 年またぎの処理（12月→1月）
            if i > 0 and month == 1:
                prev_match = date_pattern.match(calendar_dates[i-1])
                if prev_match:
                    prev_month = int(prev_match.group(1))
                    if prev_month == 12:
                        current_year = 2026

            working_days.append(f"{current_year}/{month:02d}/{day:02d}")

    if len(working_days) == 0:
        raise ValueError(f"カレンダーファイル '{calendar_file_path}' のヘッダーから日付形式 (例: '10/25') が読み取れませんでした。")

    working_days_dt = pd.to_datetime(sorted(list(set(working_days))))
    working_days_lookup = pd.Series(range(len(working_days_dt)), index=working_days_dt)

    print(f"--- 稼働日リスト作成完了 (全 {len(working_days_dt)} 日) ---")
    return working_days_dt, working_days_lookup

def parse_day_column(day_str):
    """
    day列（'10/28'や'2025/11/7'形式）をdatetimeオブジェクトに変換
    """
    if pd.isna(day_str):
        return pd.NaT

    day_str_cleaned = str(day_str).strip()

    # 形式1: '10/28' または '11/7' (年なし)
    date_pattern_md = re.compile(r"^(\d{1,2})/(\d{1,2})$")
    match_md = date_pattern_md.match(day_str_cleaned)

    if match_md:
        month, day = map(int, match_md.groups())
        # 1月は2026年、それ以外は2025年
        year = 2026 if month == 1 else 2025
        try:
            return pd.to_datetime(f"{year}/{month:02d}/{day:02d}")
        except:
            return pd.NaT

    # 形式2: '2025/11/7' (年あり) またはその他の標準形式
    # (注: 'YYYY/MM/DD' を 'MM/DD/YYYY' と誤認しないよう dayfirst=False を推奨)
    return pd.to_datetime(day_str_cleaned, errors='coerce', dayfirst=False)

def get_material_info(material_file_path):
    """ マテリアルマスタCSVから釜容量とLT辞書を返す """
    try:
        df_material = pd.read_csv(material_file_path)
    except FileNotFoundError:
        print(f"エラー: マテリアルマスタ '{material_file_path}' が見つかりません。")
        raise
    except Exception as e:
        print(f"マテリアルマスタ読み込みエラー: {e}")
        # エンコーディングエラーの可能性を考慮して Shift-JIS (CP932) で再試行
        try:
            print("再試行: エンコーディングを 'cp932' に変更します。")
            df_material = pd.read_csv(material_file_path, encoding='cp932')
        except Exception as e_retry:
            print(f"再試行失敗: {e_retry}")
            raise

    if '素地' not in df_material.columns or '油脂仕込み量１' not in df_material.columns:
        raise KeyError(f"'{material_file_path}' に '素地' または '油脂仕込み量１' 列がありません。")

    df_material['油脂仕込み量１'] = pd.to_numeric(df_material['油脂仕込み量１'], errors='coerce').fillna(0)
    df_material_unique = df_material.drop_duplicates(subset=['素地'])
    max_batch_dict = pd.Series(
        df_material_unique['油脂仕込み量１'].values,
        index=df_material_unique['素地']
    ).to_dict()

    lt_dict = {}
    for _, row in df_material_unique.iterrows():
        recipe = row['素地']
        is_short_process = pd.isna(row.get('工程３'))
        if recipe in ['NR', 'LC'] or is_short_process:
             lt_dict[recipe] = 1 # LT=1
        else:
             lt_dict[recipe] = 3 # LT=3

    print(f"--- 釜容量・リードタイムマスタ作成完了 ---")
    return max_batch_dict, lt_dict

def get_prep_day_by_index(filling_date, lead_time_days, working_days_list, working_days_lookup):
    """ 充填日からN営業日前の仕込日を計算する """
    if pd.isna(filling_date) or filling_date not in working_days_lookup:
        return None
    filling_date_index = working_days_lookup.get(filling_date)
    prep_date_index = filling_date_index - lead_time_days
    if prep_date_index < 0: return None
    return working_days_list[prep_date_index]

# --- 4. メイン実行ロジック ---
try:
    print("\n--- 処理開始 ---")

    # 4-1. マスタと稼働日の準備
    working_days_dt, working_days_lookup = get_working_days(CALENDAR_FILE)
    max_batch_dict, lt_dict = get_material_info(MATERIAL_MASTER_FILE)

    # 4-2. 充填需要ログ (log.csv) の読み込み
    try:
        df_plan = pd.read_csv(LOG_FILE)
        print(f"--- 充填需要ログ (log) 読み込み完了 ---")
    except FileNotFoundError:
        print(f"エラー: ログファイル '{LOG_FILE}' が見つかりません。")
        raise
    except Exception as e:
        print(f"ログファイル読み込みエラー: {e}")
        # エンコーディングエラーの可能性を考慮して Shift-JIS (CP932) で再試行
        try:
            print("再試行: エンコーディングを 'cp932' に変更します。")
            df_plan = pd.read_csv(LOG_FILE, encoding='cp932')
        except Exception as e_retry:
            print(f"再試行失敗: {e_retry}")
            raise

    required_cols = ['day', 'Recipe', 'batchsize', 'code', 'productname', 'cell']
    if not all(col in df_plan.columns for col in required_cols):
         raise KeyError(f"'{LOG_FILE}' に {required_cols} 列が不足しています。")

    # 'day'列を新しい日付パース関数で処理
    df_plan['充填日'] = df_plan['day'].apply(parse_day_column)
    df_plan['必要素地量'] = pd.to_numeric(df_plan['batchsize'], errors='coerce')

    # デバッグ: 日付変換結果を確認
    print(f"--- day列のパース結果 (先頭5件) ---")
    print(df_plan[['day', '充填日']].head())

    # 10月・11月のデータのみにフィルタリング
    start_date = pd.to_datetime("2025-10-01")
    end_date = pd.to_datetime("2025-11-30")

    df_plan = df_plan[(df_plan['充填日'] >= start_date) & (df_plan['充填日'] <= end_date)]
    df_plan = df_plan.dropna(subset=['充填日', 'Recipe', '必要素地量'])
    df_plan = df_plan[df_plan['必要素地量'] > 0]

    print(f"--- 充填需要ログ (log) の整形完了: {len(df_plan)} 件のタスクを抽出 ---")

    # 4-3. 仕込デッドラインの計算
    demand_list = []
    for _, row in df_plan.iterrows():
        recipe = row['Recipe']
        filling_date = row['充填日']
        lt = lt_dict.get(recipe, 3)
        std_prep_day = get_prep_day_by_index(filling_date, STANDARD_LEAD_TIME, working_days_dt, working_days_lookup)
        final_deadline = get_prep_day_by_index(filling_date, lt, working_days_dt, working_days_lookup)
        max_batch = max_batch_dict.get(recipe, 0)

        demand_list.append({
            "Recipe": recipe,
            "充填日": filling_date.strftime('%Y-%m-%d'),
            "標準仕込希望日": std_prep_day.strftime('%Y-%m-%d') if pd.notna(std_prep_day) else None,
            "最終仕込デッドライン": final_deadline.strftime('%Y-%m-%d') if pd.notna(final_deadline) else None,
            "必要素地量": row['必要素地量'],
            "釜最大容量": max_batch,
            "code": int(row['code']) if pd.notna(row['code']) else 0,
            "productname": row['productname'],
            "cell": int(row['cell']) if pd.notna(row['cell']) else 0,
        })

    if not demand_list:
        print("警告: 10月・11月の処理対象タスクが0件です。logシートの日付を確認してください。")
        df_demand_full = pd.DataFrame(columns=[
            "Recipe", "充填日", "標準仕込希望日", "最終仕込デッドライン",
            "必要素地量", "釜最大容量", "code", "productname", "cell"
        ])
    else:
        df_demand_full = pd.DataFrame(demand_list)

    # 4-4. 出力1：AIスケジューラ用 需要リスト
    df_demand_schedulable = df_demand_full.dropna(
        subset=['最終仕込デッドライン']
    ).sort_values(by=['最終仕込デッドライン', '標準仕込希望日', 'Recipe'])

    # 4-5. 出力2：不足（計算不可）リスト
    df_shortage = df_demand_full[
        df_demand_full['最終仕込デッドライン'].isna()
    ].copy()

    if not df_shortage.empty:
        df_shortage['理由'] = df_shortage.apply(
            lambda row: "充填日が稼働日カレンダーにない"
                        if pd.to_datetime(row['充填日']) not in working_days_lookup.index
                        else "リードタイムがカレンダー範囲外",
            axis=1
        )
        df_shortage = df_shortage[['Recipe', '充填日', '必要素地量', 'code', 'productname', 'cell', '理由']]
    else:
        df_shortage = pd.DataFrame(columns=['Recipe', '充填日', '必要素地量', 'code', 'productname', 'cell', '理由'])

    # 4-6. Google DriveへのCSV出力
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"作成しました: 出力フォルダ {OUTPUT_DIR}")

    output_path_demand = os.path.join(OUTPUT_DIR, 'ai_scheduler_demand_list.csv')
    output_path_shortage = os.path.join(OUTPUT_DIR, 'shortage_list.csv')

    df_demand_schedulable.to_csv(output_path_demand, index=False, encoding='utf-8-sig')
    df_shortage.to_csv(output_path_shortage, index=False, encoding='utf-8-sig')

    print("\n--- 処理完了 ---")
    print(f"ファイルがGoogle Driveに出力されました:")
    print(f" 1. 需要リスト: {output_path_demand} ({len(df_demand_schedulable)} 件)")
    print(f" 2. 不足リスト: {output_path_shortage} ({len(df_shortage)} 件)")

except Exception as e:
    print(f"\n--- エラーが発生しました ---")
    print(e)
    import traceback
    traceback.print_exc()