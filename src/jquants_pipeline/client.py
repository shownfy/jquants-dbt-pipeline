"""J-Quants API V2 クライアント.

J-Quants API V2 を使用して株式データを取得するクライアントモジュール。
API Key は https://jpx-jquants.com/ のダッシュボードから取得できます。

Free プランの制限:
    - 上場銘柄一覧: 12週間前〜2年12週間前
    - 株価四本値: 12週間前〜2年12週間前
    - 直近12週間のデータは取得不可

Example:
    >>> from jquants_pipeline import JQuantsClient
    >>> client = JQuantsClient(api_key="your_api_key")
    >>> df = client.get_listed_info()
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import TYPE_CHECKING

import pandas as pd
import requests
from dotenv import find_dotenv, load_dotenv

if TYPE_CHECKING:
    from collections.abc import Iterator

# =============================================================================
# 設定
# =============================================================================

API_BASE_URL = "https://api.jquants.com/v2"


@dataclass(frozen=True)
class RetryConfig:
    """リトライ設定."""

    max_retries: int = 3
    retry_delay: float = 3.0  # レートリミット時の待機秒数
    request_interval: float = 0.5  # リクエスト間の待機秒数


DEFAULT_RETRY_CONFIG = RetryConfig()


# =============================================================================
# 例外
# =============================================================================


class JQuantsError(Exception):
    """J-Quants API エラーの基底クラス."""


class AuthenticationError(JQuantsError):
    """API 認証エラー."""


class RateLimitError(JQuantsError):
    """レートリミットエラー."""


# =============================================================================
# クライアント
# =============================================================================


class JQuantsClient:
    """J-Quants API V2 クライアント.

    Attributes:
        api_key: J-Quants API Key
        retry_config: リトライ設定
    """

    def __init__(
        self,
        api_key: str,
        retry_config: RetryConfig = DEFAULT_RETRY_CONFIG,
    ) -> None:
        """クライアントを初期化する.

        Args:
            api_key: J-Quants API Key（ダッシュボードから取得）
            retry_config: リトライ設定
        """
        self.api_key = api_key
        self.retry_config = retry_config
        self._session = requests.Session()
        self._session.headers.update({"x-api-key": api_key})

    @classmethod
    def from_env(cls, env_var: str = "JQUANTS_API_KEY") -> JQuantsClient:
        """環境変数から API Key を読み込んでクライアントを作成する.

        Args:
            env_var: API Key を格納する環境変数名

        Returns:
            JQuantsClient インスタンス

        Raises:
            ValueError: 環境変数が設定されていない場合
        """
        load_dotenv(find_dotenv(usecwd=True), override=True)
        api_key = os.environ.get(env_var)

        if not api_key:
            raise ValueError(
                f"環境変数 {env_var} が設定されていません。\n"
                f".env ファイルに {env_var} を設定してください。\n"
                "API Key は https://jpx-jquants.com/ から取得できます。"
            )

        return cls(api_key=api_key)

    # -------------------------------------------------------------------------
    # 公開 API
    # -------------------------------------------------------------------------

    def get_listed_info(self) -> pd.DataFrame:
        """上場銘柄一覧を取得する.

        Returns:
            上場銘柄情報の DataFrame
                - Date: 日付
                - Code: 銘柄コード
                - CoName: 会社名
                - CoNameEn: 会社名（英語）
                - S17/S17Nm: 17業種コード/名
                - S33/S33Nm: 33業種コード/名
                - ScaleCat: 規模区分
                - Mkt/MktNm: 市場コード/名
        """
        data = self._get_all_pages("/equities/master")
        return pd.DataFrame(data)

    def get_stock_prices(
        self,
        start_date: date,
        end_date: date,
        *,
        verbose: bool = True,
    ) -> pd.DataFrame:
        """株価四本値を取得する.

        日付ごとに全銘柄のデータを取得します。
        Free プランでは12週間前〜2年12週間前のデータのみ取得可能です。

        Args:
            start_date: 取得開始日
            end_date: 取得終了日
            verbose: 進捗を表示するか

        Returns:
            株価データの DataFrame
        """
        all_data: list[dict] = []
        consecutive_empty = 0

        for current_date in self._date_range(start_date, end_date):
            try:
                data = self._get_all_pages(
                    "/equities/bars/daily",
                    params={"date": current_date.isoformat()},
                )

                if data:
                    all_data.extend(data)
                    consecutive_empty = 0
                    if verbose:
                        print(f"  {current_date}: {len(data):,} records")
                else:
                    consecutive_empty += 1
                    if verbose:
                        print(f"  {current_date}: no data")

            except requests.HTTPError as e:
                consecutive_empty += 1
                if verbose:
                    status = e.response.status_code if e.response else "?"
                    print(f"  {current_date}: skipped (HTTP {status})")

            # 10日連続でデータなしなら中断
            if consecutive_empty >= 10:
                if verbose:
                    print("  (10 consecutive days with no data, stopping)")
                break

            time.sleep(self.retry_config.request_interval)

        return pd.DataFrame(all_data)

    # -------------------------------------------------------------------------
    # 内部メソッド
    # -------------------------------------------------------------------------

    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        """GET リクエストを実行する（リトライ付き）."""
        url = f"{API_BASE_URL}{endpoint}"
        config = self.retry_config

        for attempt in range(config.max_retries):
            response = self._session.get(url, params=params)

            # 認証エラー
            if response.status_code in (401, 403):
                msg = self._extract_error_message(response)
                raise AuthenticationError(
                    f"API認証に失敗しました。API Key を確認してください。\n"
                    f"  ステータス: {response.status_code}\n"
                    f"  エラー: {msg}"
                )

            # レートリミット
            if response.status_code == 429:
                wait = config.retry_delay * (attempt + 1)
                print(f"    Rate limited, waiting {wait:.0f}s...")
                time.sleep(wait)
                continue

            response.raise_for_status()
            return response.json()

        # リトライ上限
        response.raise_for_status()
        return {}

    def _get_all_pages(self, endpoint: str, params: dict | None = None) -> list[dict]:
        """ページネーションを処理して全データを取得する."""
        params = dict(params) if params else {}
        result = self._get(endpoint, params)
        data = result.get("data", [])

        while "pagination_key" in result:
            params["pagination_key"] = result["pagination_key"]
            result = self._get(endpoint, params)
            data.extend(result.get("data", []))

        return data

    @staticmethod
    def _extract_error_message(response: requests.Response) -> str:
        """レスポンスからエラーメッセージを抽出する."""
        try:
            return response.json().get("message", response.text)
        except Exception:
            return response.text

    @staticmethod
    def _date_range(start: date, end: date) -> Iterator[date]:
        """日付範囲のイテレータを生成する."""
        current = start
        while current <= end:
            yield current
            current += timedelta(days=1)
