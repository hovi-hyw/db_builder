# check_index_data.py
"""
检查数据库中的指数代码，并分析市场分布统计数据问题
"""

import sys
import logging
from datetime import date

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('index_checker')

# 导入必要的模块
try:
    from StockDownloader.src.database.session import SessionLocal
    from StockDownloader.src.database.models.derived import DerivedIndex, DerivedStock
    from StockDownloader.src.database.models.market_summary import StockMarketSummary
    from sqlalchemy import func, distinct, desc
    from StockDownloader.src.services.market_summary import MarketSummaryService
except ImportError as e:
    logger.error(f"导入模块失败: {e}")
    sys.exit(1)


def check_index_data():
    """检查数据库中的指数数据"""
    with SessionLocal() as db:
        # 获取指数映射关系
        index_mapping = MarketSummaryService.get_index_mapping()
        all_index_symbols = list(index_mapping.keys())
        
        logger.info("===== 指数代码统计 =====")
        for symbol in all_index_symbols:
            count = db.query(func.count(DerivedIndex.symbol)).filter(
                DerivedIndex.symbol == symbol
            ).scalar() or 0
            logger.info(f"指数 {symbol} 在数据库中有 {count} 条记录")
            
            # 如果有记录，检查日期范围
            if count > 0:
                earliest = db.query(DerivedIndex.date).filter(
                    DerivedIndex.symbol == symbol
                ).order_by(DerivedIndex.date).first()
                
                latest = db.query(DerivedIndex.date).filter(
                    DerivedIndex.symbol == symbol
                ).order_by(desc(DerivedIndex.date)).first()
                
                logger.info(f"  日期范围: {earliest[0]} 至 {latest[0]}")
        
        logger.info("\n===== 所有指数代码 =====")
        all_symbols = db.query(DerivedIndex.symbol).distinct().all()
        all_symbols = [s[0] for s in all_symbols]
        logger.info(f"数据库中共有 {len(all_symbols)} 个不同的指数代码")
        logger.info(f"前20个指数代码: {all_symbols[:20]}")
        
        # 检查是否有指数代码与配置的不同但格式相似
        logger.info("\n===== 检查相似指数代码 =====")
        for symbol in all_index_symbols:
            # 检查不同格式的可能性，例如前缀不同
            similar_symbols = []
            for s in all_symbols:
                # 检查数字部分是否相同
                if symbol[-6:] == s[-6:] and symbol != s:
                    similar_symbols.append(s)
                # 或者检查不带前缀的部分是否相同
                elif symbol.lstrip('0') == s.lstrip('0') and symbol != s:
                    similar_symbols.append(s)
            
            if similar_symbols:
                logger.info(f"指数 {symbol} 有相似代码: {similar_symbols}")
                
                # 检查这些相似代码的记录数
                for sim_symbol in similar_symbols:
                    count = db.query(func.count(DerivedIndex.symbol)).filter(
                        DerivedIndex.symbol == sim_symbol
                    ).scalar() or 0
                    logger.info(f"相似指数 {sim_symbol} 在数据库中有 {count} 条记录")
                    
                    # 如果有记录，检查日期范围
                    if count > 0:
                        earliest = db.query(DerivedIndex.date).filter(
                            DerivedIndex.symbol == sim_symbol
                        ).order_by(DerivedIndex.date).first()
                        
                        latest = db.query(DerivedIndex.date).filter(
                            DerivedIndex.symbol == sim_symbol
                        ).order_by(desc(DerivedIndex.date)).first()
                        
                        logger.info(f"  日期范围: {earliest[0]} 至 {latest[0]}")
        
        # 检查市场分布统计表中的数据
        logger.info("\n===== 市场分布统计表数据 =====")
        summary_symbols = db.query(StockMarketSummary.symbol).distinct().all()
        summary_symbols = [s[0] for s in summary_symbols]
        logger.info(f"市场分布统计表中共有 {len(summary_symbols)} 个不同的指数代码")
        logger.info(f"指数代码: {summary_symbols}")
        
        # 检查每个指数在市场分布统计表中的记录数
        for symbol in all_index_symbols:
            count = db.query(func.count(StockMarketSummary.symbol)).filter(
                StockMarketSummary.symbol == symbol
            ).scalar() or 0
            logger.info(f"指数 {symbol} 在市场分布统计表中有 {count} 条记录")
            
            # 如果有记录，检查日期范围
            if count > 0:
                earliest = db.query(StockMarketSummary.date).filter(
                    StockMarketSummary.symbol == symbol
                ).order_by(StockMarketSummary.date).first()
                
                latest = db.query(StockMarketSummary.date).filter(
                    StockMarketSummary.symbol == symbol
                ).order_by(desc(StockMarketSummary.date)).first()
                
                logger.info(f"  日期范围: {earliest[0]} 至 {latest[0]}")
        
        # 检查股票前缀匹配情况
        logger.info("\n===== 股票前缀匹配情况 =====")
        # 获取最新的日期
        latest_date = db.query(func.max(DerivedStock.date)).scalar()
        logger.info(f"最新股票数据日期: {latest_date}")
        
        for symbol, prefixes in index_mapping.items():
            logger.info(f"指数 {symbol} 对应前缀: {prefixes if prefixes else '默认(其他未分类股票)'}")
            
            if prefixes:  # 如果有指定前缀
                for prefix in prefixes:
                    # 检查最新日期的匹配情况
                    if latest_date:
                        count = db.query(func.count(DerivedStock.symbol)).filter(
                            DerivedStock.date == latest_date,
                            DerivedStock.symbol.like(f"{prefix}%")
                        ).scalar() or 0
                        logger.info(f"  前缀 {prefix} 在 {latest_date} 匹配到 {count} 支股票")
                        
                        # 如果有匹配的股票，显示一些样本
                        if count > 0:
                            sample_limit = min(5, count)
                            sample_stocks = db.query(DerivedStock.symbol).filter(
                                DerivedStock.date == latest_date,
                                DerivedStock.symbol.like(f"{prefix}%")
                            ).limit(sample_limit).all()
                            logger.info(f"  样本股票: {[s[0] for s in sample_stocks]}")


def main():
    """主函数"""
    try:
        logger.info("开始检查数据库中的指数数据")
        check_index_data()
        logger.info("检查完成")
    except Exception as e:
        logger.error(f"检查过程中出错: {e}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()