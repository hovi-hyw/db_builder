# src/api/endpoints/market_summary.py
"""
此模块定义了市场分布统计数据相关的API端点。
使用FastAPI框架，提供了获取和更新市场分布统计数据的接口。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

from datetime import date
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy.orm import Session

from ..models import MarketSummaryData
from ...database.models.market_summary import StockMarketSummary
from ...database.session import get_db
from ...services.market_summary import MarketSummaryService

router = APIRouter()


@router.get("/market-summary/{symbol}/{date}", response_model=MarketSummaryData)
def get_market_summary(symbol: str, date: date, db: Session = Depends(get_db)):
    """
    获取指定指数指定日期的市场分布统计数据。

    Args:
        symbol (str): 指数代码。
        date (date): 日期。
        db (Session): 数据库会话。

    Returns:
        MarketSummaryData: 市场分布统计数据。

    Raises:
        HTTPException: 如果找不到市场分布统计数据，则抛出404异常。
    """
    market_summary = db.query(StockMarketSummary).filter(
        StockMarketSummary.symbol == symbol, 
        StockMarketSummary.date == date
    ).first()
    
    if market_summary is None:
        raise HTTPException(status_code=404, detail="Market summary data not found")
    
    return market_summary


@router.post("/market-summary/update", status_code=status.HTTP_200_OK)
def update_market_summary(db: Session = Depends(get_db)):
    """
    更新市场分布统计数据。
    从derived_index表获取最新的日期数据，并计算更新市场分布统计数据。

    Args:
        db (Session): 数据库会话。

    Returns:
        Response: 成功更新的响应。
    """
    try:
        MarketSummaryService.update_from_derived_index(db)
        return {"message": "Market summary data updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update market summary data: {str(e)}")


@router.post("/market-summary/update/{date}", status_code=status.HTTP_200_OK)
def update_market_summary_by_date(date: date, db: Session = Depends(get_db)):
    """
    更新指定日期的市场分布统计数据。

    Args:
        date (date): 日期。
        db (Session): 数据库会话。

    Returns:
        Response: 成功更新的响应。
    """
    try:
        MarketSummaryService.calculate_market_summary(db, date)
        return {"message": f"Market summary data for {date} updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to update market summary data: {str(e)}")