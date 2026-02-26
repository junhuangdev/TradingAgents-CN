#!/usr/bin/env python3
"""
BaoStock兼容工具层

为旧版 data_source_manager 提供同步接口，内部桥接到
tradingagents.dataflows.providers.china.baostock 的异步实现。
"""

from __future__ import annotations

import asyncio
import concurrent.futures
from typing import Any, Dict, Optional

import pandas as pd

from tradingagents.utils.logging_manager import get_logger

logger = get_logger("agents")


def _run_async(coro):
    """在同步上下文安全执行协程。"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    if not loop.is_running():
        return loop.run_until_complete(coro)

    # 当前线程已有运行中的事件循环，切换到子线程执行
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(lambda: asyncio.run(coro))
        return future.result()


class BaoStockCompatProvider:
    """BaoStock同步兼容适配器。"""

    def __init__(self):
        from .providers.china.baostock import get_baostock_provider as get_provider

        self._provider = get_provider()

    @property
    def connected(self) -> bool:
        return bool(getattr(self._provider, "connected", False))

    def get_stock_data(
        self, symbol: str, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> pd.DataFrame:
        """同步获取历史行情数据。"""
        if not self.connected:
            logger.warning("⚠️ BaoStock未连接，返回空数据")
            return pd.DataFrame()

        try:
            start = start_date or ""
            end = end_date or ""
            data = _run_async(self._provider.get_historical_data(symbol, start, end, period="daily"))
            if data is None:
                return pd.DataFrame()
            return data
        except Exception as exc:
            logger.error(f"❌ BaoStock兼容层获取历史数据失败: {exc}")
            return pd.DataFrame()

    def get_stock_info(self, symbol: str) -> Dict[str, Any]:
        """同步获取股票基本信息。"""
        if not self.connected:
            return {"symbol": symbol, "name": f"股票{symbol}", "source": "baostock"}

        try:
            info = _run_async(self._provider.get_stock_basic_info(symbol)) or {}
            return {
                "symbol": symbol,
                "name": info.get("name", f"股票{symbol}"),
                "industry": info.get("industry", "未知"),
                "area": info.get("area", "未知"),
                "market": info.get("market_info", {}).get("market_name", "未知"),
                "list_date": info.get("list_date", "未知"),
                "source": "baostock",
            }
        except Exception as exc:
            logger.error(f"❌ BaoStock兼容层获取股票信息失败: {exc}")
            return {"symbol": symbol, "name": f"股票{symbol}", "source": "baostock"}


_baostock_compat_provider: Optional[BaoStockCompatProvider] = None


def get_baostock_provider() -> BaoStockCompatProvider:
    """获取BaoStock兼容提供器实例。"""
    global _baostock_compat_provider
    if _baostock_compat_provider is None:
        _baostock_compat_provider = BaoStockCompatProvider()
    return _baostock_compat_provider

