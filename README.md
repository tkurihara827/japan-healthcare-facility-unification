# japan-healthcare-facility-unification: 地方局が公表するコード内容別医療機関一覧表から全国版データを作成
Pipeline to unify prefectural **Japanese healthcare facility** registry data into a national, analysis-ready dataset. Features robust data shifting, custom calendar conversion, and **string similarity matching** for 63 medical subjects. **This repository is for Japanese speakers.**

このリポジトリは、厚生労働省や各地方局から公開されている**都道府県別の医療機関データ（コード内容別医療機関一覧表）**を、統計分析やGIS分析に利用可能な**全国統合データセット**として作成するためのデータクレンジングおよび統合パイプライン（Pythonスクリプト）です。本コードは、科学研究費プロジェクト「医療施設配置の決定要因分析と評価：投票システムの応用可能性」（若手研究；21K13317；研究代表者：栗原崇）の成果として共有します。

---

**1. クイックスタート（ビギナーズ向け）**

Step 1. **コード内容別医療機関一覧表**が厚生労働省の各地方局に一般公開されているので、「医科」または「医科・歯科」一覧のエクセルファイルをダウンロードする。それぞれの地方局を別々に探すことになるので、下記のリンクを使用（リンク切れの可能性あり）。

- [北海道](https://kouseikyoku.mhlw.go.jp/hokkaido/gyomu/gyomu/hoken_kikan/code_ichiran.html)・[東北](https://kouseikyoku.mhlw.go.jp/tohoku/gyomu/gyomu/hoken_kikan/itiran.html)・[関東信越](https://kouseikyoku.mhlw.go.jp/kantoshinetsu/chousa/shitei.html)・[東海北陸](https://kouseikyoku.mhlw.go.jp/tokaihokuriku/gyomu/gyomu/hoken_kikan/shitei.html)・[近畿](https://kouseikyoku.mhlw.go.jp/kinki/tyousa/shinkishitei.html)・[中国四国](https://kouseikyoku.mhlw.go.jp/chugokushikoku/chousaka/iryoukikanshitei.html)・[四国](https://kouseikyoku.mhlw.go.jp/shikoku/gyomu/gyomu/hoken_kikan/shitei/index.html)・[九州](https://kouseikyoku.mhlw.go.jp/kyushu/gyomu/gyomu/hoken_kikan/index_00006.html)

Step 2. 作業場所となるディレクトリの名前を仮に**A**とする。

Step 3. ダウンロードしたデータが令和5年12月のデータだとすると、**A**の中に「**r0512raw**」というディレクトリを用意し、そこにダウンロードしたデータをすべて格納する。

Step 4. 都道府県番号[**都道府県番号**](https://www.mhlw.go.jp/topics/2007/07/dl/tp0727-1d.pdf)をファイル名にする（例：1.xlsx, 2.xlsx,...）。**北海道が2つのファイルとなるので、便宜上2つ目のファイル名を48とする**。

Step 5. Pythonの環境と整える。おすすめは、**Anaconda**をインストールし、そのなかの**Spyder**を使用する。

Step 6. **Spyder**を起動したら、**append.py**と**gis.py**を開く。

Step 7. 両方のスクリプトに、**WORKDIR_NAME**と**DATA_MONTH**と**BASE_DIR**を指定する場所があるので、下記のように指定する。
```bash
WORKDIR_NAME = 'A'
DATA_MONTH = 'r0512'
BASE_DIR = f'/Users/Aの直前までのパス/{WORKDIR_NAME}'　#Aまでのパスは自身で設定する
```
Step 8. **append.py**を実行すると、**A**のなかに2つのディレクトリ「**r0512** と **r0512merge**」が生成され、各都道府県の結合前データ（**r0512/r0512_n.xlsx**, n=1,2,...,48）、全国版編集前データ（**r0512merge/total.xlsx**）、全国版データ（**r0512merge/total2.xlsx**）、世界測地系緯度経度を外部取得するためのデータ（**r0512merge/address.csv**）が生成される。

**生成される変数は、varlist.txtを参照**

Step 9. 東京大学空間情報科学研究センターが提供する[**CSV Geocoding Service**](https://geocode.csis.u-tokyo.ac.jp/geocode-cgi/geocode.cgi?action=start)を用いて、**address.csv**に世界測地系緯度経度を追加し、**address_out.csv**というファイル名で**r0512merge**に保存する。

Step 10. 最後に**gis.py**を実行すると、**total2.xlsx**と**address_out.csv**がマージされ、GISソフトに使用可能なデータ（**total3.xlsx**）が**r0512merge**に生成される。

---

**2. 補足説明（主に、Spyderを使用しない方向け）**

・事前にクイックスタートに記載した手動作業を済ませ、プログラム実行の際に以下の補足説明を参考にしてください。

・Python 3.8以上が必要です。以下の依存ライブラリをインストールしてください。

```bash
pip install -r requirements.txt
```

・非定型データ処理、和暦変換、診療科目分類を含む、複雑なクレンジングと全国データの統合を行います。

```bash
python append.py
```

・**address.csv** を、外部のGISサービスに入力します。

・得られた緯度（lon/fX）と経度（lat/fY）を含む結果ファイルを**r0512merge/address_out.csv**として配置します。

・ジオコーディング結果ファイル配置後、**gis.py**を実行して最終データセットを完成させます。

```bash
python gis.py
```

・出力されるファイルはクイックスタートの説明と同じです。

---

**3. データ処理の工夫点と注意点**

•非定型データ処理: move関数による、複数行にわたる施設情報の縦方向データ繰り越し処理。

•診療科目分類: 文字列類似度計算アルゴリズム (**SequenceMatcher**) を用いて、表記ゆれに対応した63科目への分類を試行。医療機関の診療科目の回答が自由回答であるため、厚生労働省が定める63の診療科目名の正式名称を基準に、その中で類似度が最も高い診療科目（複数可、すなわちargmaxにしている）のダミー変数が1となるよう設定。さらに、63科目との類似度が0である場合は、すべての診療科目ダミー変数の値が0となるよう設定。

•上記の診療科目分類では、自由回答の科目ひとつずつについて、63科目との類似度を計算しており、同じ文字列が登場しても同様の計算を行なっています。高速化するためにキャッシュを残すことも検討しましたが、**類似度判定基準を変更して使うユーザーがいる場合**、変更前のキャッシュに基づき判定してしまう可能性があり、かえってやり直す手間が増えるかもしれません。また、年に1回くらいの処理であれば、macbook pro 13 (2022年)で10分強くらい要する程度であるため、ミスを誘発しない設計にしました。

•執筆者の専門はエンジニアリングではないため、性能改善やロジックに関するご提案をいただけると大変助かります。

---

**4. 執筆者情報（2025.10.21時点）**

•指名: 栗原 崇 (クリハラ　タカシ)

•所属・職位: 東海大学 准教授; 博士（経済学）

•専門分野: 選好理論、社会的選択理論、応用計量経済学

•公式サイト: [https://sites.google.com/view/takashikurihara/]

---

**5. ライセンスと引用**

本コードは、オープンソースソフトウェアとして**MIT License**のもと公開されています。本研究手法を学術論文で引用する際は、本リポジトリのDOIを参照してください。

**リポジトリ名**: japan-healthcare-facility-unification

**Citation (DOI)** : Takashi Kurihara. (2025). tkurihara827/japan-healthcare-facility-unification: v1.0.0: Initial Data Pipeline Release (v1.0.0). Zenodo. [https://doi.org/10.5281/zenodo.17404341]
