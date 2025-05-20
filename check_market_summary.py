# check_market_summary.py
"""
检查市场分布统计表中的数据，确认修复是否成功
"""

import sys
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('summary_checker')

# 导入必要的模块
try:
    from StockDownloader.src.database.session import SessionLocal
    from StockDownloader.src.database.models.market_summary import StockMarketSummary
    from sqlalchemy import func, distinct, desc
    from StockDownloader.src.services.market_summary import MarketSummaryService
except ImportError as e:
    logger.error(f"导入模块失败: {e}")
    sys.exit(1)


def check_market_summary():
    """检查市场分布统计表中的数据"""
    with SessionLocal() as db:
        # 获取指数映射关系
        index_mapping = MarketSummaryService.get_index_mapping()
        all_index_symbols = list(index_mapping.keys())
        
        logger.info("===== 市场分布统计表数据 =====")
        
        # 获取所有不同的指数代码
        summary_symbols = db.query(StockMarketSummary.symbol).distinct().all()
        summary_symbols = [s[0] for s in summary_symbols]
        logger.info(f"市场分布统计表中共有 {len(summary_symbols)} 个不同的指数代码")
        logger.info(f"指数代码: {summary_symbols}")
        
        # 检查每个配置的指数是否都有数据
        for symbol in all_index_symbols:
            count = db.query(func.count(StockMarketSummary.symbol)).filter(
                StockMarketSummary.symbol == symbol
            ).scalar() or 0
            
            if count > 0:
                logger.info(f"指数 {symbol} 在市场分布统计表中有 {count} 条记录")
                
                # 获取最新的一条记录
                latest = db.query(StockMarketSummary).filter(
                    StockMarketSummary.symbol == symbol
                ).order_by(desc(StockMarketSummary.date)).first()
                
                if latest:
                    logger.info(f"  最新记录日期: {latest.date}")
                    logger.info(f"  涨跌幅区间分布:")
                    logger.info(f"    跌幅<-8%: {latest.count_lt_neg8pct:.2%}")
                    logger.info(f"    -8%~-5%: {latest.count_neg8pct_to_neg5pct:.2%}")
                    logger.info(f"    -5%~-2%: {latest.count_neg5pct_to_neg2pct:.2%}")
                    logger.info(f"    -2%~2%: {latest.count_neg2pct_to_2pct:.2%}")
                    logger.info(f"    2%~5%: {latest.count_2pct_to_5pct:.2%}")
                    logger.info(f"    5%~8%: {latest.count_5pct_to_8pct:.2%}")
                    logger.info(f"    涨幅>8%: {latest.count_gt_8pct:.2%}")
            else:
                logger.warning(f"指数 {symbol} 在市场分布统计表中没有记录")
        
        # 检查是否所有配置的指数都有数据
        missing_indices = set(all_index_symbols) - set(summary_symbols)
        if missing_indices:
            logger.error(f"以下指数在市场分布统计表中没有数据: {missing_indices}")
        else:
            logger.info("所有配置的指数都已有数据，修复成功!")


def main():
    """主函数"""
    try:
        logger.info("开始检查市场分布统计表数据")
        check_market_summary()
        logger.info("检查完成")
    except Exception as e:
        logger.error(f"检查过程中出错: {e}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()