# src/api/app.py
"""
此模块创建FastAPI应用程序实例，并注册API路由。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

from fastapi import FastAPI

from .endpoints import stock, index, market_summary

app = FastAPI()

app.include_router(stock.router, prefix="/api/v1")
app.include_router(index.router, prefix="/api/v1")
app.include_router(market_summary.router, prefix="/api/v1")
