# J-Quants dbt Pipeline

J-Quants API V2 を使用した日本株データパイプライン。  
Python でデータを取得し、dbt + DuckDB でディメンショナルモデル（スタースキーマ）を構築します。

## 特徴

- **J-Quants API V2** - API Key 認証でシンプルに利用
- **DuckDB** - 高速な組み込みデータベース
- **dbt** - SQL ベースのデータ変換
- **Dev Container** - 環境構築不要で即開発可能

## クイックスタート

### 1. API Key の取得

[J-Quants](https://jpx-jquants.com/) でアカウントを作成し、ダッシュボードから API Key を取得してください。

### 2. 環境設定

```bash
cp env.sample .env
```

`.env` を編集:
```
JQUANTS_API_KEY=your_api_key_here
```

### 3. 仮想環境の有効化

```bash
source .venv/bin/activate
```

### 4. データ取得

```bash
# 全データ取得（上場銘柄 + 株価）
python -m jquants_pipeline.cli

# 上場銘柄のみ
python -m jquants_pipeline.cli --listed-only

# 株価のみ（日数・週指定）
python -m jquants_pipeline.cli --prices-only --days 14 --weeks-ago 13
```

### 5. dbt 実行

```bash
cd dbt_project

# モデルを実行（raw → staging → marts へ変換）
dbt run --profiles-dir .

# データ品質テストを実行
dbt test --profiles-dir .
```

> `--profiles-dir .` はデータベース接続設定（`profiles.yml`）をカレントディレクトリから読み込むオプションです。  
> これにより、プロジェクト内の設定ファイルで DuckDB（`data/jquants.duckdb`）に接続します。

### 6. データ確認

Python を使って DuckDB の中身を確認できます：

```bash
# テーブル一覧
python -c "import duckdb; print(duckdb.connect('data/jquants.duckdb').execute('SHOW ALL TABLES').df())"

# 上場銘柄データ
python -c "import duckdb; print(duckdb.connect('data/jquants.duckdb').execute('SELECT * FROM raw.listed_info LIMIT 10').df())"

# 株価データ
python -c "import duckdb; print(duckdb.connect('data/jquants.duckdb').execute('SELECT * FROM raw.stock_prices LIMIT 10').df())"

# dbt で変換後のデータ（marts）
python -c "import duckdb; print(duckdb.connect('data/jquants.duckdb').execute('SELECT * FROM main_marts.dim_company LIMIT 10').df())"
```

## Free プランの制限

[公式ドキュメント](https://jpx-jquants.com/ja/spec/data-spec)より:

| データ | 取得可能期間 |
|--------|-------------|
| 上場銘柄一覧 | 12週間前〜2年12週間前 |
| 株価四本値 | 12週間前〜2年12週間前 |

> **注意**: 直近12週間のデータは Free プランでは取得できません。

## アーキテクチャ

```
J-Quants API V2 → Python (Extract) → DuckDB (Raw) → dbt → Dimensional Model
```

### ディメンショナルモデル

| テーブル | 種類 | 説明 |
|----------|------|------|
| `dim_company` | Dimension | 会社マスタ（銘柄コード、会社名、セクター、市場区分） |
| `dim_date` | Dimension | 日付マスタ（年月日、曜日、会計年度） |
| `fct_stock_prices` | Fact | 株価ファクト（OHLCV、調整後終値） |

## プロジェクト構成

```
jquants-dbt-pipeline/
├── src/jquants_pipeline/       # Python パッケージ
│   ├── __init__.py             # 公開 API
│   ├── client.py               # J-Quants API クライアント
│   └── cli.py                  # CLI & データ保存
├── dbt_project/                # dbt プロジェクト
│   ├── models/
│   │   ├── staging/            # Staging モデル
│   │   └── marts/              # Dimension / Fact テーブル
│   └── profiles.yml
├── data/                       # DuckDB ファイル
├── .devcontainer/              # Dev Container 設定
├── env.sample                  # 環境変数サンプル
└── pyproject.toml
```

## ライブラリとして使用

```python
from jquants_pipeline import JQuantsClient

# 環境変数から API Key を読み込み
client = JQuantsClient.from_env()

# または直接指定
client = JQuantsClient(api_key="your_api_key")

# 上場銘柄一覧
df_listed = client.get_listed_info()

# 株価データ
from datetime import date
df_prices = client.get_stock_prices(
    start_date=date(2025, 1, 6),
    end_date=date(2025, 1, 10),
)
```

## CLI オプション

```bash
python -m jquants_pipeline.cli --help
```

| オプション | 説明 | デフォルト |
|-----------|------|-----------|
| `--days` | 取得日数 | 7 |
| `--weeks-ago` | 何週間前から取得するか | 12 (Free プランの最小値) |
| `--listed-only` | 上場銘柄のみ取得 | - |
| `--prices-only` | 株価のみ取得 | - |

## 開発環境

### Dev Container（推奨）

1. Docker Desktop をインストール
2. VS Code / Cursor で開く
3. `Dev Containers: Reopen in Container` を実行

### ローカル開発

```bash
# 依存関係インストール
uv sync

# 仮想環境を有効化
source .venv/bin/activate

# リント
ruff check src/
ruff format src/
```

### 仮想環境について

このプロジェクトでは Python の仮想環境（`.venv`）を使用しています。

仮想環境は、プロジェクトごとに独立した Python 環境を作る仕組みです。

```
パソコン
├── プロジェクトA/.venv → pandas 1.5, numpy 1.24
├── プロジェクトB/.venv → pandas 2.0, numpy 2.0
└── このプロジェクト/.venv → dbt, duckdb, requests
```

## ライセンス

MIT
