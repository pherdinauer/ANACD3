"""Downloader module with multiple download strategies."""

from .manager import DownloadManager, run_plan
from .strategies import (
    StrategyBase, S1DynamicStrategy, S2SparseStrategy, S3CurlStrategy,
    S4ShortConnStrategy, S5TailFirstStrategy, DownloadResult
)

__all__ = [
    'DownloadManager',
    'run_plan',
    'StrategyBase',
    'S1DynamicStrategy',
    'S2SparseStrategy', 
    'S3CurlStrategy',
    'S4ShortConnStrategy',
    'S5TailFirstStrategy',
    'DownloadResult'
]

