# -*- coding: utf-8 -*-
"""
===================================
CryptoFetcher - 加密货币数据源
===================================

数据来源：Yahoo Finance（通过 yfinance 库）
特点：支持 BTC, ETH 等主流加密货币
定位：专门用于加密货币数据获取

关键策略：
1. 自动将加密货币代码转换为 yfinance 格式（如 BTC -> BTC-USD）
2. 处理加密货币的数据格式
3. 支持主流加密货币对
"""

import logging
import os
from datetime import datetime
from typing import Optional, List, Dict, Any

import pandas as pd
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from .base import BaseFetcher, DataFetchError, STANDARD_COLUMNS
from .realtime_types import UnifiedRealtimeQuote, RealtimeSource

logger = logging.getLogger(__name__)


class CryptoFetcher(BaseFetcher):
    """
    加密货币数据源实现

    优先级：5（专门用于加密货币）
    数据来源：Yahoo Finance

    支持的加密货币：
    - BTC: 比特币
    - ETH: 以太坊
    - 其他主流加密货币

    关键策略：
    - 自动转换代码格式为 USD 对
    - 处理加密货币数据特性
    - 失败后指数退避重试
    """

    name = "CryptoFetcher"
    priority = int(os.getenv("CRYPTO_PRIORITY", "5"))

    # 支持的加密货币映射
    CRYPTO_MAPPING = {
        'BTC': 'BTC-USD',
        'ETH': 'ETH-USD',
        'BNB': 'BNB-USD',
        'ADA': 'ADA-USD',
        'SOL': 'SOL-USD',
        'DOT': 'DOT-USD',
        'DOGE': 'DOGE-USD',
        'AVAX': 'AVAX-USD',
        'LTC': 'LTC-USD',
        'LINK': 'LINK-USD',
    }

    def __init__(self):
        """初始化 CryptoFetcher"""
        pass

    def _convert_crypto_code(self, crypto_code: str) -> str:
        """
        转换加密货币代码为 Yahoo Finance 格式

        Args:
            crypto_code: 加密货币代码，如 'BTC', 'ETH'

        Returns:
            Yahoo Finance 格式代码，如 'BTC-USD'

        Examples:
            >>> fetcher._convert_crypto_code('BTC')
            'BTC-USD'
            >>> fetcher._convert_crypto_code('ETH')
            'ETH-USD'
        """
        code = crypto_code.strip().upper()

        # 如果已经是完整格式，直接返回
        if '-USD' in code:
            return code

        # 从映射中查找
        if code in self.CRYPTO_MAPPING:
            return self.CRYPTO_MAPPING[code]

        # 默认添加 -USD 后缀
        logger.debug(f"转换加密货币代码: {crypto_code} -> {code}-USD")
        return f"{code}-USD"

    def _is_crypto_code(self, code: str) -> bool:
        """
        判断是否为加密货币代码

        Args:
            code: 代码

        Returns:
            bool: 是否为加密货币
        """
        code = code.strip().upper()
        return code in self.CRYPTO_MAPPING or len(code) <= 5  # 简单判断，加密货币代码通常较短

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((ConnectionError, TimeoutError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _fetch_raw_data(self, crypto_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        从 Yahoo Finance 获取加密货币原始数据

        Args:
            crypto_code: 加密货币代码，如 'BTC', 'ETH'
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            原始数据 DataFrame
        """
        try:
            import yfinance as yf
        except ImportError:
            raise DataFetchError("yfinance 库未安装，请运行: pip install yfinance")

        # 转换代码格式
        yf_code = self._convert_crypto_code(crypto_code)

        logger.debug(f"调用 yfinance.download({yf_code}, {start_date}, {end_date})")

        try:
            # 使用 yfinance 下载数据
            df = yf.download(
                tickers=yf_code,
                start=start_date,
                end=end_date,
                progress=False,  # 禁止进度条
                auto_adjust=True,  # 自动调整价格（复权）
                multi_level_index=True
            )

            # 筛选出 yf_code 的列，避免多只数据混淆
            if isinstance(df.columns, pd.MultiIndex) and len(df.columns) > 1:
                ticker_level = df.columns.get_level_values(1)
                mask = ticker_level == yf_code
                if mask.any():
                    df = df.loc[:, mask].copy()

            if df.empty:
                raise DataFetchError(f"Yahoo Finance 未查询到 {crypto_code} 的数据")

            return df

        except Exception as e:
            if isinstance(e, DataFetchError):
                raise
            raise DataFetchError(f"Yahoo Finance 获取加密货币数据失败: {e}") from e

    def _normalize_data(self, df: pd.DataFrame, crypto_code: str) -> pd.DataFrame:
        """
        标准化加密货币数据

        yfinance 返回的列名：
        Open, High, Low, Close, Volume（索引是日期）

        新版 yfinance 返回 MultiIndex 列名，如 ('Close', 'BTC-USD')
        需要先扁平化列名再进行处理

        需要映射到标准列名：
        date, open, high, low, close, volume, amount, pct_chg

        Args:
            df: 原始数据
            crypto_code: 加密货币代码

        Returns:
            标准化的 DataFrame
        """
        df = df.copy()

        # 处理 MultiIndex 列名（新版 yfinance）
        if isinstance(df.columns, pd.MultiIndex):
            # 扁平化列名，只保留第一级
            df.columns = df.columns.get_level_values(0)

        # 重置索引，将日期从索引转换为列
        df = df.reset_index()

        # 列名映射（yfinance 使用大写开头）
        column_mapping = {
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume',
            'Adj Close': 'adj_close',  # 复权收盘价
        }

        # 重命名列
        df = df.rename(columns=column_mapping)

        # 确保所有标准列都存在
        for col in STANDARD_COLUMNS:
            if col not in df.columns:
                if col == 'amount':
                    # 加密货币没有 amount，使用 volume 作为近似
                    df['amount'] = df.get('volume', 0)
                elif col == 'pct_chg':
                    # 计算涨跌幅
                    if 'close' in df.columns:
                        df['pct_chg'] = df['close'].pct_change() * 100
                    else:
                        df['pct_chg'] = 0.0
                else:
                    df[col] = 0.0  # 其他缺失列填充 0

        # 选择标准列
        df = df[STANDARD_COLUMNS]

        # 数据类型转换
        df['date'] = pd.to_datetime(df['date'])
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount', 'pct_chg']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        return df

    def get_realtime_quote(self, crypto_code: str) -> Optional[UnifiedRealtimeQuote]:
        """
        获取加密货币实时报价

        Args:
            crypto_code: 加密货币代码，如 'BTC', 'ETH'

        Returns:
            UnifiedRealtimeQuote: 实时报价数据
        """
        try:
            import yfinance as yf

            yf_code = self._convert_crypto_code(crypto_code)
            ticker = yf.Ticker(yf_code)

            # 获取最新信息
            info = ticker.info
            if not info:
                return None

            # 获取当前价格
            current_price = info.get('regularMarketPrice', info.get('currentPrice'))
            if current_price is None:
                return None

            # 获取前一天收盘价计算涨跌幅
            previous_close = info.get('regularMarketPreviousClose', info.get('previousClose', current_price))
            change = current_price - previous_close
            change_pct = (change / previous_close) * 100 if previous_close != 0 else 0

            # 获取成交量
            volume = info.get('regularMarketVolume', info.get('volume', 0))
            amount = volume * current_price if volume else 0

            return UnifiedRealtimeQuote(
                code=crypto_code,
                name=info.get('name', crypto_code),
                price=current_price,
                change_amount=change,
                change_pct=change_pct,
                volume=int(volume) if volume else None,
                amount=amount,
                high=info.get('dayHigh', current_price),
                low=info.get('dayLow', current_price),
                open_price=info.get('dayOpen', current_price),
                source=RealtimeSource.CRYPTO
            )

        except Exception as e:
            logger.error(f"获取 {crypto_code} 实时报价失败: {e}")
            return None</content>
<parameter name="filePath">d:\1\daily_stock_analysis\data_provider\crypto_fetcher.py