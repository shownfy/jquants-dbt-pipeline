"""J-Quants データ取得 CLI.

コマンドラインからデータを取得して DuckDB に保存するスクリプト。

Usage:
    # 全データ取得（上場銘柄 + 株価）
    uv run python -m jquants_pipeline.cli

    # 上場銘柄のみ
    uv run python -m jquants_pipeline.cli --listed-only

    # 株価のみ（14日間、13週間前から）
    uv run python -m jquants_pipeline.cli --prices-only --days 14 --weeks-ago 13
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

from jquants_pipeline.client import JQuantsClient

# =============================================================================
# 設定
# =============================================================================

PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "jquants.duckdb"


@dataclass
class ExtractionConfig:
    """データ取得設定.

    Attributes:
        days: 取得日数
        weeks_ago: 何週間前から取得するか（Free プランは12以上）
    """

    days: int = 7
    weeks_ago: int = 12  # Free プランの最小値

    @property
    def start_date(self) -> date:
        """取得開始日."""
        return self.end_date - timedelta(days=self.days)

    @property
    def end_date(self) -> date:
        """取得終了日（weeks_ago 週間前）."""
        return date.today() - timedelta(weeks=self.weeks_ago)


# =============================================================================
# データ保存
# =============================================================================


class DuckDBStorage:
    """DuckDB へのデータ保存を担当するクラス."""

    def __init__(self, db_path: Path = DB_PATH) -> None:
        """ストレージを初期化する.

        Args:
            db_path: DuckDB ファイルのパス
        """
        self.db_path = db_path
        self._ensure_directory()

    def _ensure_directory(self) -> None:
        """データディレクトリを作成する."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def save(
        self,
        df: pd.DataFrame,
        table_name: str,
        *,
        schema: str = "raw",
        replace: bool = True,
    ) -> int:
        """DataFrame を DuckDB に保存する.

        Args:
            df: 保存する DataFrame
            table_name: テーブル名
            schema: スキーマ名
            replace: True なら既存テーブルを置換、False なら追加

        Returns:
            保存した行数
        """
        full_table = f"{schema}.{table_name}"

        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

            if replace:
                conn.execute(f"DROP TABLE IF EXISTS {full_table}")
                conn.execute(f"CREATE TABLE {full_table} AS SELECT * FROM df")
            else:
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {full_table} AS
                    SELECT * FROM df WHERE 1=0
                """)
                conn.execute(f"INSERT INTO {full_table} SELECT * FROM df")

            row_count: int = conn.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]

        return row_count


# =============================================================================
# 抽出処理
# =============================================================================


def extract_listed_info(client: JQuantsClient, storage: DuckDBStorage) -> pd.DataFrame:
    """上場銘柄情報を取得して保存する."""
    print("Fetching listed info...")
    df = client.get_listed_info()
    count = storage.save(df, "listed_info")
    print(f"✓ raw.listed_info: {count:,} rows saved")
    return df


def extract_stock_prices(
    client: JQuantsClient,
    storage: DuckDBStorage,
    config: ExtractionConfig,
) -> pd.DataFrame:
    """株価データを取得して保存する."""
    print(f"Fetching stock prices ({config.start_date} to {config.end_date})...")
    df = client.get_stock_prices(config.start_date, config.end_date)

    if df.empty:
        print("⚠ No stock price data retrieved. Skipping save.")
        return df

    count = storage.save(df, "stock_prices")
    print(f"✓ raw.stock_prices: {count:,} rows saved")
    return df


def run_extraction(
    *,
    listed: bool = True,
    prices: bool = True,
    config: ExtractionConfig | None = None,
) -> None:
    """データ抽出を実行する.

    Args:
        listed: 上場銘柄を取得するか
        prices: 株価を取得するか
        config: 取得設定
    """
    config = config or ExtractionConfig()
    client = JQuantsClient.from_env()
    storage = DuckDBStorage()

    print("=" * 50)
    print("J-Quants Data Extraction")
    print("=" * 50)
    print("Note: Free プランは12週間前〜2年12週間前のデータのみ取得可能")
    print("=" * 50)

    if listed:
        extract_listed_info(client, storage)

    if prices:
        extract_stock_prices(client, storage, config)

    print("=" * 50)
    print("Extraction completed!")
    print(f"Database: {storage.db_path}")
    print("=" * 50)


# =============================================================================
# CLI
# =============================================================================


def parse_args() -> argparse.Namespace:
    """コマンドライン引数をパースする."""
    parser = argparse.ArgumentParser(
        description="J-Quants データ取得",
        epilog="Note: Free プランは12週間前〜2年12週間前のデータのみ取得可能です。",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="株価データの取得日数（デフォルト: 7）",
    )
    parser.add_argument(
        "--weeks-ago",
        type=int,
        default=12,
        help="何週間前から取得するか（デフォルト: 12、Free プランの最小値）",
    )
    parser.add_argument(
        "--listed-only",
        action="store_true",
        help="上場銘柄情報のみ取得",
    )
    parser.add_argument(
        "--prices-only",
        action="store_true",
        help="株価データのみ取得",
    )
    return parser.parse_args()


def main() -> None:
    """CLI エントリーポイント."""
    args = parse_args()
    config = ExtractionConfig(days=args.days, weeks_ago=args.weeks_ago)

    run_extraction(
        listed=not args.prices_only,
        prices=not args.listed_only,
        config=config,
    )


if __name__ == "__main__":
    main()
