"""J-Quants データパイプライン.

J-Quants API V2 を使用して株式データを取得し、DuckDB に保存するパイプライン。

Example:
    >>> from jquants_pipeline import JQuantsClient
    >>> client = JQuantsClient.from_env()
    >>> df = client.get_listed_info()

CLI:
    >>> uv run python -m jquants_pipeline.cli
"""

__version__ = "0.1.0"

from jquants_pipeline.client import (
    AuthenticationError,
    JQuantsClient,
    JQuantsError,
    RateLimitError,
    RetryConfig,
)

__all__ = [
    "JQuantsClient",
    "JQuantsError",
    "AuthenticationError",
    "RateLimitError",
    "RetryConfig",
]
