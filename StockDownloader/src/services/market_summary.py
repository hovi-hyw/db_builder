# src/services/market_summary.py
"""
此模块提供市场分布统计数据的计算和更新服务。
根据derived_stock表中的数据，计算不同涨跌幅区间的股票占比，
并将结果写入stock_market_summary表。
Authors: hovi.hyw & AI
Date: 2024-07-03
"""

from datetime import date
from typing import List, Dict, Tuple

from sqlalchemy import func, and_, or_
from sqlalchemy.orm import Session

from ..database.models.derived import DerivedStock, DerivedIndex
from ..database.models.market_summary import StockMarketSummary


class MarketSummaryService:
    """
    市场分布统计服务类。
    提供计算和更新市场分布统计数据的方法。
    """

    @staticmethod
    def get_index_mapping() -> Dict[str, List[str]]:
        """
        获取股票代码前缀到指数的映射关系。

        Returns:
            Dict[str, List[str]]: 指数代码到股票代码前缀列表的映射。
        """
        return {
            "899050": ["83", "87", "88"],  # 83/87/88开头归纳到899050
            "000698": ["688"],             # 688开头归纳到000698
            "399006": ["300", "301"],      # 300/301开头归纳到399006
            "000001": []                   # 其余全部归纳到000001
        }

    @staticmethod
    def get_change_ranges() -> List[Tuple[float, float, str]]:
        """
        获取涨跌幅区间定义。

        Returns:
            List[Tuple[float, float, str]]: 涨跌幅区间列表，每个元素为(下限, 上限, 字段名)。
        """
        return [
            (-float('inf'), -0.08, "count_lt_neg8pct"),
            (-0.08, -0.05, "count_neg8pct_to_neg5pct"),
            (-0.05, -0.02, "count_neg5pct_to_neg2pct"),
            (-0.02, 0.02, "count_neg2pct_to_2pct"),
            (0.02, 0.05, "count_2pct_to_5pct"),
            (0.05, 0.08, "count_5pct_to_8pct"),
            (0.08, float('inf'), "count_gt_8pct")
        ]

    @classmethod
    def calculate_market_summary(cls, db: Session, target_date: date) -> None:
        """
        计算指定日期的市场分布统计数据，并更新到数据库。

        Args:
            db (Session): 数据库会话。
            target_date (date): 目标日期。
        """
        # 获取指数映射关系
        index_mapping = cls.get_index_mapping()
        change_ranges = cls.get_change_ranges()
        
        # 获取当天有数据的指数列表
        index_symbols = db.query(DerivedIndex.symbol).filter(
            DerivedIndex.date == target_date,
            DerivedIndex.symbol.in_(list(index_mapping.keys()))
        ).all()
        
        index_symbols = [symbol[0] for symbol in index_symbols]
        
        # 如果没有指数数据，则不处理
        if not index_symbols:
            return
        
        # 处理每个指数
        for index_symbol in index_symbols:
            # 获取该指数对应的股票前缀列表
            prefixes = index_mapping[index_symbol]
            
            # 构建查询条件
            if prefixes:  # 如果有指定前缀
                conditions = [DerivedStock.symbol.startswith(prefix) for prefix in prefixes]
                prefix_condition = or_(*conditions)
            else:  # 000001 对应其他所有股票
                # 排除其他指数已经包含的前缀
                excluded_prefixes = []
                for other_index, other_prefixes in index_mapping.items():
                    if other_index != index_symbol:
                        excluded_prefixes.extend(other_prefixes)
                
                if excluded_prefixes:
                    conditions = [~DerivedStock.symbol.startswith(prefix) for prefix in excluded_prefixes]
                    prefix_condition = and_(*conditions)
                else:
                    prefix_condition = True
            
            # 计算各个区间的股票数量和总数
            total_stocks = db.query(func.count(DerivedStock.symbol)).filter(
                DerivedStock.date == target_date,
                prefix_condition,
                DerivedStock.real_change.isnot(None)
            ).scalar() or 0
            
            # 如果没有股票数据，则跳过
            if total_stocks == 0:
                continue
            
            # 计算各个区间的占比
            summary_data = {}
            for lower, upper, field_name in change_ranges:
                count = db.query(func.count(DerivedStock.symbol)).filter(
                    DerivedStock.date == target_date,
                    prefix_condition,
                    DerivedStock.real_change >= lower,  # 数据库中已经是小数形式的百分比值
                    DerivedStock.real_change < upper if upper != float('inf') else True
                ).scalar() or 0
                
                # 计算占比
                ratio = count / total_stocks
                summary_data[field_name] = ratio
            
            # 更新或创建记录
            market_summary = db.query(StockMarketSummary).filter(
                StockMarketSummary.symbol == index_symbol,
                StockMarketSummary.date == target_date
            ).first()
            
            if market_summary:
                # 更新现有记录
                for field, value in summary_data.items():
                    setattr(market_summary, field, value)
            else:
                # 创建新记录
                market_summary = StockMarketSummary(
                    symbol=index_symbol,
                    date=target_date,
                    **summary_data
                )
                db.add(market_summary)
            
            db.commit()

    @classmethod
    def update_from_derived_index(cls, db: Session) -> None:
        """
        从derived_index表获取最新的日期数据，并更新市场分布统计数据。

        Args:
            db (Session): 数据库会话。
        """
        import logging
        from datetime import datetime
        import time
        
        # 获取logger
        logger = logging.getLogger('daily_update')
        
        # 记录开始时间
        start_time = datetime.now()
        logger.info(f"开始更新市场分布统计数据，当前时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 使用子查询优化性能，避免获取所有日期数据到内存
        # 获取derived_index表中的所有日期
        dates_query = db.query(DerivedIndex.date).distinct().order_by(DerivedIndex.date).subquery()
        
        # 获取已经处理过的日期
        processed_dates_query = db.query(StockMarketSummary.date).distinct().subquery()
        
        # 使用SQL差集操作找出未处理的日期
        unprocessed_dates = db.query(dates_query.c.date).filter(
            ~dates_query.c.date.in_(db.query(processed_dates_query.c.date))
        ).all()
        unprocessed_dates = [d[0] for d in unprocessed_dates]
        
        total_dates = len(unprocessed_dates)
        if total_dates == 0:
            logger.info("市场分布统计数据已是最新，无需更新")
            return
            
        logger.info(f"共有 {total_dates} 个日期需要处理")
        
        # 处理每个未处理的日期
        for i, target_date in enumerate(unprocessed_dates):
            # 计算进度
            progress = (i + 1) / total_dates * 100
            
            # 每10个日期或最后一个日期显示一次进度
            if (i + 1) % 10 == 0 or i == 0 or i == total_dates - 1:
                # 计算已用时间和预估剩余时间
                elapsed_time = (datetime.now() - start_time).total_seconds()
                if i > 0:  # 避免除以零
                    estimated_total_time = elapsed_time / (i + 1) * total_dates
                    remaining_time = estimated_total_time - elapsed_time
                    logger.info(f"处理进度: {progress:.2f}% ({i+1}/{total_dates}), "
                                f"已用时间: {elapsed_time:.2f}秒, "
                                f"预估剩余时间: {remaining_time:.2f}秒, "
                                f"当前处理日期: {target_date}")
                else:
                    logger.info(f"处理进度: {progress:.2f}% ({i+1}/{total_dates}), "
                                f"当前处理日期: {target_date}")
            
            # 处理当前日期
            try:
                cls.calculate_market_summary(db, target_date)
                # 每处理10个日期提交一次事务，减少数据库锁定时间
                if (i + 1) % 10 == 0:
                    db.commit()
            except Exception as e:
                logger.error(f"处理日期 {target_date} 时出错: {str(e)}")
                # 继续处理下一个日期
                continue
        
        # 确保所有更改都已提交
        db.commit()
        
        # 记录结束时间和总耗时
        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds()
        logger.info(f"市场分布统计数据更新完成，总耗时: {total_time:.2f}秒, "
                    f"处理了 {total_dates} 个日期")



def main():
    """主函数，用于直接运行时更新市场分布统计数据。"""
    from ..database.session import SessionLocal
    
    # 创建数据库会话
    with SessionLocal() as db:
        # 调用更新方法
        MarketSummaryService.update_from_derived_index(db)


if __name__ == "__main__":
    # 当作为模块运行时，需要设置PYTHONPATH
    import os
    import sys
    # 将项目根目录添加到Python路径
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
    sys.path.insert(0, project_root)
    main()