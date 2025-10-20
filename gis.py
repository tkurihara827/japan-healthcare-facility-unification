#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 20 14:42:53 2025

@author: tkurihara827
"""

#gis.py
#gis情報を追加したファイルを出力

#########
# 外部ジオコーディング処理のフェーズ
# 国土交通省の基盤地図情報または東京大学空間情報科学研究センターのCSV Geocoding Service等の
# 公的な空間情報サービスを利用し、address.csvを用いて世界測地系（WGS84またはJGD2011）の経度・緯度データを取得する。
# 得られた結果は address_out.csvというファイル名とし、MERGE_DIRに「分析者自身が」格納すること。
#########

# ==============================================================================
# 1. 設定の外部化と作業ディレクトリの設定
# ==============================================================================

# ライブラリのインポート
import os # オペレーティングシステムレベルの機能、特にファイルパス操作のために利用
import pandas as pd # 構造化データ処理のための標準的なライブラリ（データフレーム操作）
import numpy as np # 大規模な数値計算と配列操作の最適化（ベクトル化演算の基盤）

# 外部化された設定変数
# データパイプラインの再現性と保守性を確保するため、パラメータを一元管理する
# 作業ディレクトリ名（例: 'test2'）
WORKDIR_NAME = 'r512'
# 処理対象データの時間的コホートを識別するキー（例: 和暦5年12月）
DATA_MONTH = 'r0512'

# ベースディレクトリ指定
# データソースと処理結果の格納場所を絶対パスで定義
BASE_DIR = f'/Users/tkurihara/Desktop/{WORKDIR_NAME}'

# 作業ディレクトリの変更
try:
    # 処理中の相対パス参照を確実にするためのカレントディレクトリ設定
    os.chdir(BASE_DIR)
    print(f"作業ディレクトリ: {os.getcwd()}")
except FileNotFoundError:
    # 処理の続行可能性を判断し、エラーログを出力
    print(f"エラー: 指定されたディレクトリ '{BASE_DIR}' が見つかりません。パスを確認してください。")
    # 処理を続行できない場合は exit() などで停止する

# --- 派生パスの定義 ---
# os.path.joinを使用して、BASE_DIRと後続のディレクトリを安全に結合
# --------------------------------------------------------------------------------------
PROC_DIR = DATA_MONTH
MERGE_DIR = DATA_MONTH + 'merge'
# 中間および最終的な統合データセットのファイルパス
TOTAL2_FILE = os.path.join(MERGE_DIR, 'total2.xlsx')
TOTAL3_FILE = os.path.join(MERGE_DIR, 'total3.xlsx')
# ジオコーディング処理に用いる外部連携ファイルのパス
ADDRESS_CSV = os.path.join(MERGE_DIR, 'address.csv')
ADDRESS_OUT_CSV = os.path.join(MERGE_DIR, 'address_out.csv')
# --------------------------------------------------------------------------------------



df5 = pd.read_excel(TOTAL2_FILE)


# 経度・緯度を入手後、改めてADDRESS_OUT_CSV を使用
try:
    # ジオコーディング結果のCSVファイルを読み込み
    df6=pd.read_csv(ADDRESS_OUT_CSV) 
    # データフレームのインデックスをリセットし、整合性を確保
    df5 = df5.reset_index(drop=True)
    df6 = df6.reset_index(drop=True)
    # 空間情報（経度: fX, 緯度: fY）を元のデータセットに結合
    df5['lon'] = df6['fX']
    df5['lat'] = df6['fY']
    
except FileNotFoundError:
    # 外部処理の失敗やファイル欠損時のデータ完全性を確保
    print("緯度経度ファイルが見つかりません。緯度経度カラムはNaNになります。")
    df5['lon'] = np.nan
    df5['lat'] = np.nan

# 最終処理済みデータセットの保存
df5.to_excel(TOTAL3_FILE, index=False)



