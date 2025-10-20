#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 17 18:13:18 2025

@author: tkurihara827
"""

#append.py
#都道府県データの統合と、GIS情報を入手するためのファイルを出力
# ==============================================================================
# 1. 設定の外部化と作業ディレクトリの設定
# ==============================================================================

# ライブラリのインポート
import os # オペレーティングシステムレベルの機能、特にファイルパス操作のために利用
import pandas as pd # 構造化データ処理のための標準的なライブラリ（データフレーム操作）
import glob # 指定されたパターンにマッチするファイルパスリストを取得（データ統合に利用）
from difflib import SequenceMatcher # **文字列類似度計算アルゴリズム (Ratcliff/Obershelpアルゴリズム)** を利用

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
RAW_DIR = os.path.join(DATA_MONTH + 'raw')
PROC_DIR = DATA_MONTH
MERGE_DIR = DATA_MONTH + 'merge'
# 中間および最終的な統合データセットのファイルパス
TOTAL_FILE = os.path.join(MERGE_DIR, 'total.xlsx')
TOTAL2_FILE = os.path.join(MERGE_DIR, 'total2.xlsx')
# ジオコーディング処理に用いる外部連携ファイルのパス
ADDRESS_CSV = os.path.join(MERGE_DIR, 'address.csv')
ADDRESS_OUT_CSV = os.path.join(MERGE_DIR, 'address_out.csv')
# --------------------------------------------------------------------------------------


#汎用関数
# 縦方向のデータ繰り越し処理（特に、ヘッダ行を持たない非定型データ構造からの情報抽出に必須）
def move(n,x,m,v1,v2,w):
    # データフレーム内の隣接する行間（row_num: m）で同一のエンティティID（"id"）を共有し、
    # ターゲットカラム（v1）がプレースホルダ（"*"）である場合に、
    # ソースカラム（v2）のデータを繰り越す処理。
    for i in range(0,n):
        if x.at[i+m,"id"] == x.at[i,"id"] and x.at[i,v1] == "*" and w in x.at[i+m,v2]:
            x.at[i, v1] = x.at[i+m,v2]
            
# 特定の条件（w）を満たす行をデータセットから除外（データのクレンジングと対象エンティティの絞り込み）
def rdel(x,v,w):
    x.drop(x[x[v].str.contains(w)].index, inplace = True)

# 文字列置換の汎用化（データ品質向上と統一化のためのサニタイズ処理）
def rep(x,var,w1,w2):
    # Pandasのベクトル化されたstrアクセサを利用し、カラム全体に対する置換を高速に実行
    x[var] = x[var].str.replace(w1, w2)


# --- データ前処理のための初期設定 ---
# データの読み込み時にスキップする行数（ヘッダ・メタデータ行の排除）
pre0 = [i for i in range(0,11)]
# データフレームの初期カラム名（主要属性）
pre1 = [
        "id", 
        "code", 
        "name", 
        "address", 
        "tell", 
        "establisher", 
        "owner", 
        "register", 
        "category", 
        "type" 
        ]
# 繰り越し処理により値を格納するためのターゲットカラム（補助属性）
pre2 = [
        "type1",
        "type2",
        "type3",
        "type4",
        "n_tenu", 
        "n_tenu_dr", 
        "n_tenu_den", 
        "n_ntenu", 
        "n_ntenu_dr", 
        "n_ntenu_den",
        "reason", 
        "start",
        'c1',
        'c2',
        'c3',
        'c4',
        'c5',
        'c6',
        'c7'
        ]


# --- 各都道府県データの下処理 out df1 ---
# データ前処理済みファイルを格納するためのディレクトリを再帰的に作成
os.makedirs(PROC_DIR, exist_ok=True)


for m in range(1,49):
    try:
        # 都道府県別（1〜48）の生データファイル（Excel形式）のバッチ処理
        df1 = pd.read_excel(
        f"{RAW_DIR}/{m}.xlsx", 
        skiprows = pre0, 
        header = None,
        names = pre1
        )
    except FileNotFoundError:
        # ファイルが存在しない場合（例: 存在しない都道府県番号）は処理をスキップ
        continue
    
    k=len(df1)
    
    for i in pre2:
        # 補助属性カラムを初期プレースホルダで初期化
        df1[i] = "*"
    
    # 欠損値の補完：同一エンティティ内での情報欠落を前方の非欠損値で埋める（FFILL: Forward Fill）
    df1.fillna(method="ffill", inplace = True)
    
    # データの粒度統一：特定のカテゴリ名を短縮形に統一（例: 総合病院 -> 総合）
    rep(df1,"type","総合病院", "総合")
    
    # move関数を用いた縦方向データ繰り越し処理のパラメータ設定
    move_params = [
        (k-1, 1, "n_tenu", "tell", "常"), 
        (k-2, 2, "n_tenu_dr", "tell", "医"),
        (k-3, 3, "n_tenu_den", "tell", "歯"), 
        (k-4, 4, "n_ntenu", "tell", "非"),
        (k-5, 5, "n_ntenu_dr", "tell", "医"), 
        (k-6, 6, "n_ntenu_den", "tell", "歯"),
        (k-1, 1, "reason", "register", ""), 
        (k-1, 1, "start", "reason", ""),
        (k-1, 1, "type1", "type", ""), 
        (k-2, 2, "type2", "type", ""),
        (k-3, 3, "type3", "type", ""), 
        (k-4, 4, "type4", "type", ""),
        (k,   0, "c1", "category", ""), 
        (k-1, 1, "c2", "category", ""),
        (k-2, 2, "c3", "category", ""), 
        (k-3, 3, "c4", "category", ""),
        (k-4, 4, "c5", "category", ""), 
        (k-5, 5, "c6", "category", ""),
        (k-6, 6, "c7", "category", "")
        ]
    
    # 定義されたパラメータに基づく繰り越し関数 move() の実行
    for idx, row_num, target_col, src_col, val in move_params:
        move(idx, df1, row_num, target_col, src_col, val)

    # 無効なステータスを持つエンティティ（例: 休止、現存）を除外
    for i in ["現存","休止"]:
        rdel(df1,"type",i)
    # 不必要な情報（例: 常勤関連情報）を含む行を除外
    rdel(df1,"tell","常")
    
    # 都道府県コードの整合性確保（コード48は北海道の重複を回避するための暫定措置と推測）
    df1["pref"] = m if m < 48 else 1
    
    # 前処理済みデータセットを中間ファイルとして保存
    df1.to_excel(f'{PROC_DIR}/{DATA_MONTH}_{m}.xlsx', index=False)


# --- データのマージ＆編集 out df2 ---
# 統合済みデータセットを格納するディレクトリを作成
os.makedirs(MERGE_DIR, exist_ok=True)


# 全都道府県の前処理済みファイルを収集
files = glob.glob(f'{PROC_DIR}/*.xlsx')
data_list = []
for i in files:
    # 各ファイルを読み込みリストに格納
    data_list.append(pd.read_excel(i))

# 垂直方向へのデータ結合（全都道府県データの統合）
df2 = pd.concat(data_list, axis=0, sort=False)


# 不要なメタデータカラムの削除と、エンティティID及び住所に基づくソーティング
df2.drop(columns=['Unnamed: 0','id', 'tell'], inplace = True, errors='ignore')
df2.sort_values(by = ["pref", "address"], inplace = True)
df2["post"]=df2["address"] # 郵便番号抽出のための住所カラムの複製


# 登録理由に関するデータクレンジング：一意なエンティティ識別のための不必要な変動要因を除外
for i in ["その他","移動","新規","交代","移転","組織変更","開設者変","更新"]:
    rdel(df2,"register",i)


# 複数カラムに分散した診療科情報を単一の複合カラムに結合（データ集約）
df2['c']=df2['c1'].astype(str)+'/'+df2['c2'].astype(str)+'/'+df2['c3'].astype(str)+'/'+df2['c4'].astype(str)+'/'+df2['c5'].astype(str)+'/'+df2['c6'].astype(str)+'/'+df2['c7'].astype(str)
df2.drop(columns=['c1','c2', 'c3','c4','c5', 'c6', 'c7'], inplace = True)

# 結合後の文字列サニタイズ処理

rep_df2=[
    ('\*', ''),
    ('/*', ''),
    ('　', '/'), # 全角スペースをセパレータ（/）に統一
    ('//', '/'), # 重複するセパレータを単一化
    (' ', '') # 半角スペースの除去
    ]

for old, new in rep_df2:
    rep(df2,'c',old, new)


# 病床数情報（例: :1, :2）を区別するためのフォーマット変換
for i in range(1,10):
    rep(df2,'c','/{0}'.format(i),':{0}'.format(i))


# 和暦の日付フィールド（登録日、開設日）の標準化とサニタイズ
for i in ['register','start']:
    rep(df2,i,' ', '')
    rep(df2,i,'元', '1') # 和暦元号初年を表す「元」を「1」に置換
    rep(df2,i,'令', '令.') # 和暦元号の文字と年数の間に区切り文字を挿入
    rep(df2,i,'平', '平.')
    rep(df2,i,'昭', '昭.')
    # 日付のゼロパディング（例: .1. -> .01.）を実行し、日付解析のロバスト性を向上
    for j in range(1,10):
        rep(df2,i,'.{0}.'.format(j),'.0{0}.'.format(j))
    for j in range(1,10):
        rep(df2,i,'.{0}.'.format(j),'.0{0}.'.format(j))


# 施設種別情報を単一の複合カラムに結合
df2["type_status"]=df2["type"].astype(str)+'/'+df2["type1"].astype(str)+'/'+df2["type2"].astype(str)+'/'+df2["type3"].astype(str)+'/'+df2["type4"].astype(str)
df2.drop(columns=["type","type1","type2","type3", "type4"], inplace = True)
rep(df2,'type_status','/*', '')

# 和暦（昭, 平, 令）を含む理由フィールドをプレースホルダ（'0'）に設定（日付フィールドとの競合回避）
wareki = ['昭', '平', '令']
r_check = df2['reason'].astype(str).str.contains('|'.join(wareki), na=False) 
df2.loc[r_check, 'reason'] = '0' 


# 理由情報を複数のダミー変数カラムに展開
r_var = ["r_other","r_move","r_new","r_exch", "r_org", "r_estach","r_update"]

for i in r_var:
    df2[i]=df2["reason"]

# 新しい一意のエンティティIDを連番で割り当て
df2['id'] = range(1, len(df2.index) + 1)

# カラム順序の再定義（データセットの構造化）
df2 = df2[[
    'id',
    'pref',
    'post',
    'address',
    "name",
    "establisher",
    "owner",
    "register",
    "start",
    "type_status",
    "n_tenu",
    "n_tenu_dr",
    "n_tenu_den",
    "n_ntenu",
    "n_ntenu_dr",
    "n_ntenu_den",
    "r_other",
    "r_move",
    "r_new",
    "r_exch",
    "r_org",
    "r_estach",
    "r_update",
    'c'
    ]]

# 常勤・非常勤医師/歯科医師数カラムのクレンジングと数値化準備
cols = [
        "n_tenu", 
        "n_tenu_dr", 
        "n_tenu_den", 
        "n_ntenu", 
        "n_ntenu_dr", 
        "n_ntenu_den"
        ]
replace_pairs = [
    ('　',''), 
    (' ',''),
    ('常勤',''), 
    ('非',''), 
    ('医',''), 
    ('歯',''),
    ('(',''), 
    (')',''), 
    (':',''),
    ('*','0') # 欠損値をゼロ（0）として扱う
]

for col in cols:
    for old, new in replace_pairs:
        rep(df2, col, old, new)


# 理由情報をバイナリダミー変数に変換するためのマッピング辞書
rep_dict = {
    "r_other":  ["その他"], 
    "r_move":   ["移動","移転","所在地変","所変"],
    "r_new":    ["新規"], 
    "r_exch":   ["交代","継承"],
    "r_org":    ["組織変更"], 
    "r_estach": ["開設者変更","開設者変","開変","開設変更"],
    "r_update": ["更新"]
}

# 対象となる理由フラグを「1」に設定
for col, values in rep_dict.items():
    for val in values:
        rep(df2, col, val, "1")

# 理由情報に含まれるすべてのカテゴリリスト（網羅性の確保）
r_list = [
    "その他",
    "移動",
    "移転",
    "所在地変",
    "所変",
    "新規",
    "交代",
    "継承", 
    "組織変更",
    "開設者変更",
    "開設者変",
    "開変",
    "開設変更",
    "更新"
    ]

# 対象外の理由フラグを「0」に設定（排他的なダミー変数作成）
rep_settings = {
    "r_other":  {"除外": ["その他"]}, 
    "r_move":   {"除外": ["移動","移転","所在地変","所変"]},
    "r_new":    {"除外": ["新規"]}, 
    "r_exch":   {"除外": ["交代","継承"]},
    "r_org":    {"除外": ["組織変更"]}, 
    "r_estach": {"除外": ["開設者変","開設者変更","開変","開設変更"]},
    "r_update": {"除外": ["更新"]}
}

for col, params in rep_settings.items():
    exclude = set(params["除外"])
    # r_listから除外リストにない要素を選び出し、'0'に置換
    for i in sorted(set(r_list) - exclude, key=r_list.index):
        rep(df2, col, i, "0")


# 理由ダミー変数カラムを最終的に数値型（整数）に変換
for col, params in rep_settings.items():
    # 非数値データはNaNに変換し、NaNを0で補完（欠損データの一貫性確保）
    df2[col] = pd.to_numeric(df2[col], errors='coerce').fillna(0).astype(int)


# 住所情報の構造化：郵便番号と住所文字列の分離
df2['post'] = df2['post'].str[1:9] # 郵便番号部分（9桁目まで）を抽出
df2['address'] = df2['address'].str[9:] # 9桁目以降を住所として抽出

# 郵便番号の区切り文字の標準化
rep(df2,"post","ー", "-")

# 複合カラム内の各要素をセット型（Set）に変換（順序を問わない一意な属性の集合として扱う）
# この処理は、後の類似度計算のための準備段階であり、計算効率のボトルネックとなりうる
df2['c'] = df2['c'].apply(lambda s: set(x.strip() for x in str(s).split('/')))
df2['type_status'] = df2['type_status'].apply(lambda s: set(x.strip() for x in str(s).split('/')))


# 西暦変換ロジックの実装と年月日への分割（データ解析における時系列情報の標準化）
for i in ["r_year", "r_month", "r_day","s_year", "s_month", "s_day"]:
    # 年月日カラムを初期値0で初期化
    df2[i]=0


# 和暦から西暦への変換処理（Pythonループによる行ごとの非ベクトル化処理）
# 処理速度の最適化が必要なボトルネックの一つ（学術的な妥当性と計算負荷のトレードオフ）
for i in df2.index:
    try:
        reg = str(df2['register'].loc[i])
        if "昭" in reg:
            # 昭和年を西暦に変換
            df2.loc[i, "r_year"] = 1925+int(reg[2:4])
        elif "平" in reg:
            # 平成年を西暦に変換
            df2.loc[i, "r_year"] = 2000+int(reg[2:4])-12
        elif "令" in reg:
            # 令和年を西暦に変換
            df2.loc[i, "r_year"] = 2020+int(reg[2:4])-2
            
        # 月と日の抽出
        df2.loc[i, "r_month"] = int(reg[5:7])
        df2.loc[i, "r_day"] = int(reg[8:])
    except (ValueError, IndexError, TypeError):
        # データ型変換やインデックスエラー時のロバストなエラー処理
        pass

for i in df2.index:
    try:
        start = str(df2['start'].loc[i])
        if "昭" in start:
            df2.loc[i, "s_year"] = 1925+int(start[2:4])
        elif "平" in start:
            df2.loc[i, "s_year"] = 2000+int(start[2:4])-12
        elif "令" in start:
            df2.loc[i, "s_year"] = 2020+int(start[2:4])-2
            
        df2.loc[i, "s_month"] = int(start[5:7])
        df2.loc[i, "s_day"] = int(start[8:])
    except (ValueError, IndexError, TypeError):
        pass


# 処理済みである元の和暦カラムを削除
df2.drop(columns=['register', 'start'], inplace=True)

# 施設種別フラグカラムの初期化
for i in ["defunct", "yoryo", "clinic","hospital", "sougou", "locsup", "tokutei"]:
    df2[i]=""

# 施設種別フラグの定義
flag_dict = {
    "defunct":  "休止", 
    "yoryo":    "療養病床", 
    "clinic":   "診療所",
    "hospital": "病院", 
    "sougou":   "総合", 
    "locsup":   "地域支援",
    "tokutei":  "特定機能"
}

# 施設種別フラグの作成（apply()による非効率なループ処理）
# 各エンティティの type_status がキーワードを含むかを判定し、バイナリフラグを立てる
for col, keyword in flag_dict.items():
    df2[col] = df2["type_status"].apply(lambda s: 1 if keyword in s else 0)


# 医師数/歯科医師数カラムを数値型に変換（分析のためのデータ型最終調整）
for col in cols:
    df2[col] = pd.to_numeric(df2[col], errors='coerce').fillna(0).astype(int)

# 中間統合データセットの保存（次の処理ステップへの入力）
df2.to_excel(TOTAL_FILE, index=False)

# ------------------------------------------------------------------------------

# df8の処理（診療科属性の高度な抽出とフラグ作成）

# ------------------------------------------------------------------------------

df3 = pd.read_excel(TOTAL_FILE)

# df8の'c'（診療科複合カラム）を明示的に文字列型に変換
df3['c'] = df3['c'].astype(str)

# 診療科情報文字列のサニタイズ（全角・半角統一、区切り文字統一、不要文字除去）
replace_pairs = [
    ('{',''), 
    ('}',''), 
    (" ", ""), 
    ("'", ""),
    ("、", ","), 
    ("・", ","), 
    ("/", ","),
    # 全角数字の半角変換
    ("０","0"), 
    ("１","1"), 
    ("２","2"), 
    ("３","3"), 
    ("４","4"),
    ("５","5"), 
    ("６","6"), 
    ("７","7"), 
    ("８","8"), 
    ("９","9")
]

for old, new in replace_pairs:
    rep(df3, 'c', old, new)
    
# 病床数を含む「科目名:数字」のパターンの抽出
# 正規表現 r'[^,]+:\d+' により、カンマ区切りでコロンと数字を含む部分（例: 一般:52）を抽出
df3["bed"] = df3["c"].str.findall(r'[^,]+:\d+')
# 数字のみの抽出（病床数の集計に利用）
df3["n_bed"] = df3["c"].str.findall(r'[0-9]+')

# 抽出された病床数リストの合計値を計算するカスタム関数
def sum_bed(x):
    try:
        if x is None:
            return 0
        # リスト内包表記とisdigit()による要素検証を経て、リスト内の文字列数字を合計（ロバストな集計）
        return sum(int(i) for i in x if i.strip().isdigit())
    except Exception:
        # エラー発生時はゼロ（0）を返す
        return 0

# sum_bed関数を適用し、病床数の合計を算出（apply()による非効率なループ処理）
df3['n_bed_sum'] = df3['n_bed'].apply(sum_bed)

# 診療科名から数字とコロンを削除（純粋な科目名抽出のためのクレンジング）
reppairs = [
    (":",""),
    ("0",""), 
    ("1",""), 
    ("2",""), 
    ("3",""), 
    ("4",""),
    ("5",""), 
    ("6",""), 
    ("7",""), 
    ("8",""), 
    ("9",""),
]

for old, new in reppairs:
    rep(df3, 'c', old, new)


# 診療科区分（リファレンス辞書）
# 63の診療科に対する公式名と内部識別子（英語名）のマッピング
cref = {
    1: ["internalmedicine", "内科"], 
    2: ["psychosomaticmedicine", "心療内科"], 
    3: ["psychiatry", "精神科"], 
    4: ["neurology", "神経科"], 
    5: ["pulmonology", "呼吸器科"], 
    6: ["gastroenterology", "消化器科"], 
    7: ["cardiology", "循環器科"], 
    8: ["allergy", "アレルギー科"], 
    9: ["rheumatology", "リウマチ科"], 
    10: ["pediatrics", "小児科"], 
    11: ["surgery", "外科"], 
    12: ["orthopedicsurgery", "整形外科"], 
    13: ["plasticsurgery", "形成外科"], 
    14: ["cosmeticsurgery", "美容外科"], 
    15: ["neurosurgery", "脳神経外科"], 
    16: ["thoracicsurgery", "呼吸器外科"], 
    17: ["cardiovasculursurgery", "心臓血管外科"], 
    18: ["pediatricsurgery", "小児外科"], 
    19: ["dermatologyurology", "皮膚泌尿器科"], 
    20: ["venereology", "性病科"], 
    21: ["colorectalsurgery", "肛門科"], 
    22: ["obstetricsgynecology", "産婦人科"], 
    23: ["ophthalmology", "眼科"], 
    24: ["otorhinolaryngology", "耳鼻咽喉科"], 
    25: ["esophagogastricsurgery", "気管食道科"], 
    26: ["rehabilitation", "ﾘﾊﾋﾞﾘﾃｰｼｮﾝ科"], 
    27: ["radiology", "放射線科"], 
    28: ["neurologyinternal", "神経内科"], 
    29: ["gastroenterologyinternal", "胃腸科"], 
    30: ["dermatology", "皮膚科"], 
    31: ["urology", "泌尿器科"], 
    32: ["obstetrics", "産科"], 
    33: ["gynecology", "婦人科"], 
    34: ["pulmonologyinternal", "呼吸器内科"], 
    35: ["cardiologyinternal", "循環器内科"], 
    36: ["dentistry", "歯科"], 
    37: ["orthodontics", "歯科矯正科"], 
    38: ["pedodontics", "小児歯科"], 
    39: ["oralmaxillofacialsurgery", "歯科口腔外科"], 
    40: ["diabetology", "糖尿病科"], 
    41: ["nephrology", "腎臓内科"], 
    42: ["renaltransplantation", "腎移植科"], 
    43: ["hemodialysis", "血液透析科"], 
    44: ["metabolism", "代謝内科"], 
    45: ["endocrinology", "内分泌内科"], 
    46: ["emergencymedicine", "救急医学科"], 
    47: ["hematology", "血液科"], 
    48: ["hematologyinternal", "血液内科"], 
    49: ["anesthesiology", "麻酔科"], 
    50: ["gastroenterologyinternal2", "消化器内科"], 
    51: ["gastrointestinalsurgery", "消化器外科"], 
    52: ["hepatobilipancsurgery", "肝胆膵外科"], 
    53: ["diabetologyinternal", "糖尿内科"], 
    54: ["colorectalsurgeryinternal", "大腸肛門科"], 
    55: ["ophthalmicplasticorbitalsurgery", "眼形成眼窩外科"], 
    56: ["endocrinologyinfertility", "不妊内分泌科"], 
    57: ["rheumatologycollagendisease", "膠原病ﾘｳﾏﾁ内科"], 
    58: ["stroke", "脳卒中科"], 
    59: ["oncology", "腫瘍治療科"], 
    60: ["generalmedicine", "総合診療科"], 
    61: ["breastthyroidsurgery", "乳腺甲状腺外科"], 
    62: ["neonatology", "新生児科"], 
    63: ["pediatriccardiology", "小児循環器科"]
}


for k, v in cref.items():
    df3[v[0]] = 0

# 類似度計算関数 SequenceMatcher (Ratcliff/Obershelpアルゴリズム) のラッパー
# 文字列間の類似度を浮動小数点数（0.0〜1.0）で定量化する
def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

# 診療科文字列をカンマ区切りで安全に分割する関数（括弧内のカンマを無視するなどの高度な処理を暗示）
def split_subjects(s):
    s = str(s).replace('，', ',')
    # 先頭のダミーカンマを除去してから分割
    if s.startswith(','):
        s = s[1:]
    parts = [p.strip() for p in s.split(',') if p.strip()]
    return parts

# 類似度計算による診療科マッチングのメイン処理（Pythonループによる逐次処理）
# O(N*M)の計算複雑性を持ち、大規模データ処理における主要なボトルネック（N: 行数, M: 診療科数）
for i, row in df3.iterrows():
    # cカラムからダミーカンマを除去して分割
    subjects = split_subjects(row['c'])
    for sub in subjects:
        # 最大スコアを初期化
        best_score = 0
        # 最大スコアと同点だったキー（k）を格納するリスト
        best_k_list = []
        
        for k, v in cref.items():
            # 入力文字列とリファレンス診療科名との類似度を計算
            score = similar(sub, v[1])
            
            # (1) 新しいスコアが現在の最大スコアより大きい場合
            if score > best_score:
                best_score = score
                # リストをリセットし、新しいベストキーを追加
                best_k_list = [k]
            
            # (2) 新しいスコアが現在の最大スコアと同点の場合（多重マッチの許容）
            elif score == best_score and score > 0:
                # リストに同点キーを追加
                best_k_list.append(k)
        
        # (3) 類似度が閾値（>0）を超え、該当するキーがある場合にフラグを立てる
        if best_score > 0:
            for best_k in best_k_list:
                # 該当する診療科のバイナリフラグ（ダミー変数）を1に設定
                df3.at[i, cref[best_k][0]] = 1


# 都道府県コードから正式名称へのマッピング辞書
pref_dict = {
    1: "北海道", 
    2: "青森県", 
    3: "岩手県", 
    4: "宮城県", 
    5: "秋田県", 
    6: "山形県", 
    7: "福島県", 
    8: "茨城県", 
    9: "栃木県", 
    10: "群馬県", 
    11: "埼玉県", 
    12: "千葉県", 
    13: "東京都", 
    14: "神奈川県", 
    15: "新潟県", 
    16: "富山県", 
    17: "石川県", 
    18: "福井県", 
    19: "山梨県", 
    20: "長野県", 
    21: "岐阜県", 
    22: "静岡県", 
    23: "愛知県", 
    24: "三重県", 
    25: "滋賀県", 
    26: "京都府", 
    27: "大阪府", 
    28: "兵庫県", 
    29: "奈良県", 
    30: "和歌山県", 
    31: "鳥取県", 
    32: "島根県", 
    33: "岡山県", 
    34: "広島県", 
    35: "山口県", 
    36: "徳島県", 
    37: "香川県", 
    38: "愛媛県", 
    39: "高知県", 
    40: "福岡県", 
    41: "佐賀県", 
    42: "長崎県", 
    43: "熊本県", 
    44: "大分県", 
    45: "宮崎県", 
    46: "鹿児島県", 
    47: "沖縄県", 
    48: "北海道"
}

# 住所カラムの正規化：都道府県コードを正式名称に変換し、住所文字列と結合
df3['pref'] = df3['pref'].astype(int)
df3['address'] = df3['pref'].map(pref_dict) + df3['address'].fillna('')

# 最終データセットを中間ファイルとして保存
df3.to_excel(TOTAL2_FILE, index=False)



#世界測地系の経度・緯度を出力するための準備（外部ジオコーディングAPI連携のインターフェース）
# ジオコーディングに必要なID、郵便番号、住所のサブセットを抽出
df4=df3[['id','post','address']]
# 外部サービスで利用するためのCSV形式で出力（BOM付きUTF-8エンコーディングを指定）
df4.to_csv(ADDRESS_CSV, index=False, header=False, encoding="utf-8-sig")


