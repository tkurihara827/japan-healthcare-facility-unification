# japan-healthcare-facility-unification: コード内容別医療機関一覧表から全国版データを作成
Pipeline to unify prefectural **Japanese healthcare facility** registry data into a national, analysis-ready dataset. Features robust data shifting, custom calendar conversion, and **string similarity matching** for 63 medical subjects. **This repository is for Japanese speakers.**

このリポジトリは、厚生労働省や各地方局から公開されている**都道府県別の医療機関データ（コード内容別一覧表）**を、統計分析やGIS分析に利用可能な**全国統合データセット**として作成するためのデータクレンジングおよび統合パイプライン（Pythonスクリプト）です。

本コードは、科学研究費プロジェクト「医療施設配置の決定要因分析と評価：投票システムの応用可能性」（若手研究；21K13317；研究代表者：栗原）の成果として共有されます。

---
**1. 動作環境と準備**

**1-1. ライブラリのインストール**

Python 3.8以上が必要です。以下の依存ライブラリをインストールしてください。

```bash
pip install -r requirements.txt
```

**1-2. データソースの準備（手動作業必須）**

本プログラムは、地方厚生局から公開されている「医科」または「医科・歯科」一覧のExcelファイルを前提としています。

Step 1	データソースの確認: 各地方厚生局のウェブサイトから、対象月のExcelファイルをすべてダウンロードしてください。

Step 2	作業ディレクトリの設定:作業ルートディレクトリ（BASE_DIRに設定するパス）をローカルに準備します。

Step 3　プログラムで使用する設定変数に基づき、ルート内に以下のディレクトリを作成します。

	    r0512raw   # 生データ格納用 （例: 令和5年12月データ）
      
	    ＊プログラムで自動生成されるので下記の設定は必要なし
      
	      r0512      # 中間処理データ格納用
        
	      r0512merge # 最終/連携データ格納用
        
Step 4　ファイル名の連番化（重要）:

	ダウンロードしたExcelファイルをすべて**r0512raw内に格納し、都道府県番号をベースに1.xlsx, 2.xlsx, 3.xlsx, ...**と連番にしてください。
	【注意】北海道は2つファイルに分かれているため、便宜上、2つ目のファイルを*48.xlsx**としてください。


1-3. パスの編集（必須）
・プログラムファイル run_pipeline.py の冒頭にある**BASE_DIR**変数を、必ずご自身の作業ルートディレクトリの絶対パスに書き換えてください。
	•	・例: BASE_DIR = '/Users/YourName/Desktop/your-project-folder'
	•	・【注意】 パス区切り文字は**スラッシュ（/）**を使用し、末尾にスラッシュは付けないでください。


2. プログラム実行手順（2ステップ）

全処理は、①データクレンジング・統合、②GISデータ結合の2段階に分けて実行します。

Step 1: データクレンジングと全国統合
run_pipeline.pyは、非定型データ処理、和暦変換、診療科目分類を含む、複雑なクレンジングと全国データの統合を行います。

```bash
python run_pipeline.py

【出力されるファイル】
	•	r0512merge/total.xlsx
	•	r0512merge/address.csv （外部ジオコーディングサービス連携用）

Step 2: 外部ジオコーディング処理
	1	Step 1で出力された address.csv を、外部のGISサービス（例: ジオコーディングAPI）に入力します。
	2	得られた緯度（lon/fX）と経度（lat/fY）を含む結果ファイルを**r0512merge/address_out.csv**として配置します。

Step 3: GISデータ結合と最終ファイル作成
ジオコーディング結果ファイル配置後、merge_geocode.pyを実行して最終データセットを完成させます。

```bash
python merge_geocode.py
【最終ファイル】
	•	r0512merge/total2.xlsx （緯度経度結合済みの最終分析用データ）


3. 執筆者情報と技術的な注意点

3-1. 執筆者からのメッセージ
	•	研究代表者: 栗原 崇 (Takashi Kurihara)
	•	所属・職位: 東海大学 准教授; 経済学博士 (PhD in Economics)
	•	専門分野: 選好理論、社会的選択理論、応用計量経済学
	•	公式サイト: https://sites.google.com/view/takashikurihara/

3-2. データ処理の工夫点と注意点
	•	非定型データ処理: move関数による、複数行にわたる施設情報の縦方向データ繰り越し処理。
	•	診療科目分類: 文字列類似度計算アルゴリズム (SequenceMatcher) を用いて、表記ゆれに対応した63科目への分類を試行。
	•	【注意】執筆者の専門はエンジニアリングではないため、性能改善やロジックに関するご提案をいただけると大変助かります。

4. ライセンスと引用

本コードは、オープンソースソフトウェアとしてMIT Licenseのもと公開されています。
本研究手法を学術論文で引用する際は、本リポジトリのDOIを参照してください。
	•	リポジトリ名: japan-healthcare-facility-unification
	•	DOI（Zenodo等で取得後追記）: [DOIをここに追加]
