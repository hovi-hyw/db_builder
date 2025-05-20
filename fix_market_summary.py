# fix_market_summary.py
"""
修复市场分布统计数据问题，确保所有配置的指数都能被正确处理
"""

import sys
import logging
from datetime import date, datetime
import argparse

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger('market_summary_fix')

# 导入必要的模块
try:
    from StockDownloader.src.database.session import SessionLocal
    from StockDownloader.src.database.models.derived import DerivedIndex, DerivedStock
    from StockDownloader.src.database.models.market_summary import StockMarketSummary
    from sqlalchemy import func, and_, or_, desc
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


def fix_missing_indices():
    """修复缺失的指数数据"""
    with SessionLocal() as db:
        # 获取指数映射关系
        index_mapping = MarketSummaryService.get_index_mapping()
        all_index_symbols = list(index_mapping.keys())
        
        # 获取最新的日期
        latest_date = db.query(func.max(DerivedStock.date)).scalar()
        if not latest_date:
            logger.error("数据库中没有股票数据，无法修复")
            return
        
        logger.info(f"使用最新日期 {latest_date} 的数据进行修复")
        
        # 检查每个指数是否存在于derived_index表中
        for symbol in all_index_symbols:
            count = db.query(func.count(DerivedIndex.symbol)).filter(
                DerivedIndex.symbol == symbol
            ).scalar() or 0
            
            if count == 0:
                logger.warning(f"指数 {symbol} 在derived_index表中不存在，需要添加")
                
                # 获取该指数对应的股票前缀列表
                prefixes = index_mapping[symbol]
                
                # 如果是默认指数(000001)，跳过，因为它已经有数据
                if symbol == "000001" and not prefixes:
                    logger.info(f"指数 {symbol} 是默认指数，已有数据，跳过")
                    continue
                
                # 计算该指数的涨跌幅
                # 这里简化处理，使用对应股票的平均涨跌幅作为指数涨跌幅
                if prefixes:
                    # 构建查询条件
                    conditions = [DerivedStock.symbol.like(f"{prefix}%") for prefix in prefixes]
                    prefix_condition = or_(*conditions)
                    
                    # 计算平均涨跌幅
                    avg_change = db.query(func.avg(DerivedStock.real_change)).filter(
                        DerivedStock.date == latest_date,
                        prefix_condition,
                        DerivedStock.real_change.isnot(None)
                    ).scalar()
                    
                    if avg_change is not None:
                        # 创建指数记录
                        new_index = DerivedIndex(
                            symbol=symbol,
                            date=latest_date,
                            real_change=avg_change
                        )
                        db.add(new_index)
                        logger.info(f"为指数 {symbol} 创建记录，日期: {latest_date}, 涨跌幅: {avg_change}")
                    else:
                        logger.warning(f"无法计算指数 {symbol} 的涨跌幅，没有找到符合条件的股票")
        
        # 提交更改
        db.commit()
        logger.info("指数数据修复完成")


def recalculate_market_summary(target_date=None):
    """重新计算市场分布统计数据"""
    with SessionLocal() as db:
        if target_date:
            # 处理指定日期
            logger.info(f"重新计算 {target_date} 的市场分布统计数据")
            MarketSummaryService.calculate_market_summary(db, target_date)
        else:
            # 获取最新的日期
            latest_date = db.query(func.max(DerivedStock.date)).scalar()
            if not latest_date:
                logger.error("数据库中没有股票数据，无法重新计算")
                return
            
            logger.info(f"重新计算 {latest_date} 的市场分布统计数据")
            MarketSummaryService.calculate_market_summary(db, latest_date)
        
        logger.info("市场分布统计数据重新计算完成")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='修复市场分布统计数据问题')
    parser.add_argument('--check', action='store_true', help='检查数据库中的指数数据')
    parser.add_argument('--fix', action='store_true', help='修复缺失的指数数据')
    parser.add_argument('--recalculate', action='store_true', help='重新计算市场分布统计数据')
    parser.add_argument('--date', type=str, help='指定处理日期，格式为YYYY-MM-DD')
    args = parser.parse_args()
    
    try:
        if args.check:
            logger.info("开始检查数据库中的指数数据")
            check_index_data()
        
        if args.fix:
            logger.info("开始修复缺失的指数数据")
            fix_missing_indices()
        
        if args.recalculate:
            target_date = None
            if args.date:
                try:
                    target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
                except ValueError:
                    logger.error(f"日期格式错误: {args.date}，正确格式为YYYY-MM-DD")
                    return
            
            logger.info("开始重新计算市场分布统计数据")
            recalculate_market_summary(target_date)
        
        # 如果没有指定任何操作，则执行所有操作
        if not (args.check or args.fix or args.recalculate):
            logger.info("执行完整修复流程")
            check_index_data()
            fix_missing_indices()
            recalculate_market_summary()
        
        logger.info("修复完成")
    except Exception as e:
        logger.error(f"修复过程中出错: {e}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    main()