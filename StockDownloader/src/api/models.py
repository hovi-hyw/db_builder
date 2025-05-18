# src/api/models.py
"""
此模块定义了API的数据模型，使用Pydantic进行数据验证和序列化。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

from datetime import date

from pydantic import BaseModel


class StockData(BaseModel):
    """
    股票数据模型。
    定义了股票日线数据的结构，用于API的输入和输出。

    Attributes:
        symbol (str): 股票代码。
        date (date): 日期。
        open (float): 开盘价。
        close (float): 收盘价。
        high (float): 最高价。
        low (float): 最低价。
        volume (float): 成交量。
        amount (float): 成交额。
    """
    symbol: str
    date: date
    open: float
    close: float
    high: float
    low: float
    volume: float
    amount: float


class IndexData(BaseModel):
    """
    指数数据模型。
    定义了指数日线数据的结构，用于API的输入和输出。

    Attributes:
        symbol (str): 指数代码。
        date (date): 日期。
        open (float): 开盘价。
        close (float): 收盘价。
        high (float): 最高价。
        low (float): 最低价。
        volume (float): 成交量。
        amount (float): 成交额。
    """
    symbol: str
    date: date
    open: float
    close: float
    high: float
    low: float
    volume: float
    amount: float


class MarketSummaryData(BaseModel):
    """
    市场分布统计数据模型。
    定义了市场分布统计数据的结构，用于API的输入和输出。

    Attributes:
        symbol (str): 指数代码。
        date (date): 日期。
        count_lt_neg8pct (float): 跌幅小于-8%的股票占比。
        count_neg8pct_to_neg5pct (float): 跌幅在-8%到-5%之间的股票占比。
        count_neg5pct_to_neg2pct (float): 跌幅在-5%到-2%之间的股票占比。
        count_neg2pct_to_2pct (float): 涨跌幅在-2%到2%之间的股票占比。
        count_2pct_to_5pct (float): 涨幅在2%到5%之间的股票占比。
        count_5pct_to_8pct (float): 涨幅在5%到8%之间的股票占比。
        count_gt_8pct (float): 涨幅大于8%的股票占比。
    """
    symbol: str
    date: date
    count_lt_neg8pct: float
    count_neg8pct_to_neg5pct: float
    count_neg5pct_to_neg2pct: float
    count_neg2pct_to_2pct: float
    count_2pct_to_5pct: float
    count_5pct_to_8pct: float
    count_gt_8pct: float
