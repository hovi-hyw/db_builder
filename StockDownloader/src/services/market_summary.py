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
            "899050": ["bj"],                # 北证50指数，包含北交所股票（包括bj开头）
            "000688": ["sh688"],             # 科创板指数（代码为000688），688开头
            "399006": ["sz300", "sz301"],    # 创业板指数（代码为399006），300/301开头
            "000001": []                     # 其他未分类股票归到上证综指（代码为000001）
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
        import logging
        logger = logging.getLogger('market_summary')
        
        # 获取指数映射关系
        index_mapping = cls.get_index_mapping()
        change_ranges = cls.get_change_ranges()
        
        logger.info(f"开始处理日期 {target_date} 的市场分布统计数据")
        
        # 获取当天有数据的指数列表
        all_index_symbols = list(index_mapping.keys())
        logger.info(f"配置的指数代码: {all_index_symbols}")
        
        # 查询所有指数数据，不仅限于配置的指数，用于调试
        all_indices_in_db = db.query(DerivedIndex.symbol).filter(
            DerivedIndex.date == target_date
        ).all()
        all_indices_in_db = [symbol[0] for symbol in all_indices_in_db]
        logger.info(f"数据库中该日期的所有指数: {all_indices_in_db}")
        
        # 查询配置的指数
        index_symbols_query = db.query(DerivedIndex.symbol).filter(
            DerivedIndex.date == target_date,
            DerivedIndex.symbol.in_(all_index_symbols)
        )
        
        # 记录实际执行的SQL查询（调试用）
        logger.debug(f"指数查询SQL: {str(index_symbols_query.statement.compile(compile_kwargs={'literal_binds': True}))}")
        
        index_symbols = [symbol[0] for symbol in index_symbols_query.all()]
        
        # 检查哪些配置的指数没有找到
        missing_indices = set(all_index_symbols) - set(index_symbols)
        if missing_indices:
            logger.warning(f"以下指数在 {target_date} 没有数据: {missing_indices}")
            
            # 尝试为缺失的指数创建临时数据，以便能够处理所有配置的指数
            for missing_symbol in missing_indices:
                # 获取该指数对应的股票前缀列表
                prefixes = index_mapping[missing_symbol]
                logger.info(f"尝试为缺失的指数 {missing_symbol} 创建临时数据，对应前缀: {prefixes if prefixes else '默认(其他未分类股票)'}")
                
                # 检查该指数是否有对应的股票前缀
                if prefixes:
                    # 检查是否有匹配的股票数据
                    has_data = False
                    for prefix in prefixes:
                        count = db.query(func.count(DerivedStock.symbol)).filter(
                            DerivedStock.date == target_date,
                            DerivedStock.symbol.like(f"{prefix}%")
                        ).scalar() or 0
                        if count > 0:
                            has_data = True
                            break
                    
                    if not has_data:
                        logger.warning(f"指数 {missing_symbol} 对应的股票前缀 {prefixes} 在 {target_date} 没有数据，跳过处理")
                        continue
                
                # 将缺失的指数添加到处理列表中
                index_symbols.append(missing_symbol)
        
        # 如果没有指数数据，则不处理
        if not index_symbols:
            logger.warning(f"日期 {target_date} 没有找到指数数据，跳过处理")
            return
        
        logger.info(f"将处理 {len(index_symbols)} 个指数数据: {index_symbols}")
        
        # 处理每个指数
        for index_symbol in index_symbols:
            # 获取该指数对应的股票前缀列表
            prefixes = index_mapping[index_symbol]
            logger.info(f"处理指数 {index_symbol}，对应股票前缀: {prefixes if prefixes else '默认(其他未分类股票)'}")
            
            # 构建查询条件
            if prefixes:  # 如果有指定前缀
                # 使用LIKE操作符进行前缀匹配，确保能匹配到所有相关股票
                conditions = [DerivedStock.symbol.like(f"{prefix}%") for prefix in prefixes]
                prefix_condition = or_(*conditions)
                logger.debug(f"构建OR条件: {prefixes}")
                
                # 添加调试信息：检查是否能找到匹配的股票
                for prefix in prefixes:
                    sample_count = db.query(func.count(DerivedStock.symbol)).filter(
                        DerivedStock.date == target_date,
                        DerivedStock.symbol.like(f"{prefix}%")
                    ).scalar() or 0
                    logger.info(f"前缀 {prefix} 在 {target_date} 匹配到 {sample_count} 支股票")
                    
                    # 如果是科创板股票，添加更详细的日志
                    if prefix == "sh688":
                        logger.info(f"科创板股票匹配情况（指数代码: {index_symbol}）: 找到 {sample_count} 支股票")
                        # 获取一些样本股票，不管数量多少都显示一些
                        sample_limit = min(20, max(5, sample_count))
                        if sample_count > 0:
                            sample_stocks = db.query(DerivedStock.symbol).filter(
                                DerivedStock.date == target_date,
                                DerivedStock.symbol.like(f"{prefix}%")
                            ).limit(sample_limit).all()
                            logger.info(f"科创板股票样本: {[s[0] for s in sample_stocks]}")
                    
                    # 如果数量很少，输出具体股票代码以便调试
                    if 0 < sample_count < 10:
                        sample_stocks = db.query(DerivedStock.symbol).filter(
                            DerivedStock.date == target_date,
                            DerivedStock.symbol.like(f"{prefix}%")
                        ).all()
                        logger.debug(f"前缀 {prefix} 匹配的股票: {[s[0] for s in sample_stocks]}")
            else:  # 000001 对应其他所有股票
                # 排除其他指数已经包含的前缀
                excluded_prefixes = []
                for other_index, other_prefixes in index_mapping.items():
                    if other_index != index_symbol:
                        excluded_prefixes.extend(other_prefixes)
                
                logger.debug(f"排除前缀列表: {excluded_prefixes}")
                
                if excluded_prefixes:
                    # 修正：使用LIKE操作符构建条件，确保能正确排除所有相关股票
                    conditions = [~DerivedStock.symbol.like(f"{prefix}%") for prefix in excluded_prefixes]
                    prefix_condition = and_(*conditions)
                    logger.debug(f"构建AND条件: NOT IN {excluded_prefixes}")
                else:
                    prefix_condition = True
                    logger.debug("无需排除前缀，使用所有股票")
            
            # 计算各个区间的股票数量和总数
            try:
                # 获取符合条件的股票列表（用于调试）
                stock_symbols_query = db.query(DerivedStock.symbol).filter(
                    DerivedStock.date == target_date,
                    prefix_condition,
                    DerivedStock.real_change.isnot(None)
                )
                
                # 执行查询并获取总数
                total_stocks = stock_symbols_query.count()
                
                # 记录查询到的股票数量
                logger.info(f"指数 {index_symbol} 在 {target_date} 找到 {total_stocks} 支股票")
                
                # 如果股票数量很少，输出具体股票代码以便调试
                if 0 < total_stocks < 10:
                    stock_symbols = [s[0] for s in stock_symbols_query.all()]
                    logger.debug(f"股票列表: {stock_symbols}")
                
                # 如果没有股票数据，则跳过
                if total_stocks == 0:
                    logger.warning(f"指数 {index_symbol} 在 {target_date} 没有找到符合条件的股票数据，跳过处理")
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
                    logger.debug(f"区间 [{lower}, {upper}] 有 {count} 支股票，占比 {ratio:.4f}")
                
                # 更新或创建记录
                market_summary = db.query(StockMarketSummary).filter(
                    StockMarketSummary.symbol == index_symbol,
                    StockMarketSummary.date == target_date
                ).first()
                
                if market_summary:
                    # 更新现有记录
                    logger.info(f"更新指数 {index_symbol} 在 {target_date} 的市场分布统计数据")
                    for field, value in summary_data.items():
                        setattr(market_summary, field, value)
                else:
                    # 创建新记录
                    logger.info(f"创建指数 {index_symbol} 在 {target_date} 的市场分布统计数据")
                    market_summary = StockMarketSummary(
                        symbol=index_symbol,
                        date=target_date,
                        **summary_data
                    )
                    db.add(market_summary)
                
                db.commit()
                logger.info(f"指数 {index_symbol} 在 {target_date} 的市场分布统计数据处理完成")
                
            except Exception as e:
                logger.error(f"处理指数 {index_symbol} 在 {target_date} 的市场分布统计数据时出错: {str(e)}")
                db.rollback()  # 回滚事务
                continue  # 继续处理下一个指数

    @staticmethod
    def _process_date_batch(date_batch, process_id):
        """
        处理一批日期数据的工作函数，用于多进程并行处理。

        Args:
            date_batch (List[date]): 需要处理的日期批次。
            process_id (int): 进程ID，用于日志标识。

        Returns:
            int: 成功处理的日期数量。
        """
        import logging
        from datetime import datetime
        from ..database.session import SessionLocal
        
        # 创建数据库会话
        # 注意：每个进程必须创建自己的数据库会话
        with SessionLocal() as db:
            # 获取logger
            logger = logging.getLogger(f'daily_update_process_{process_id}')
            
            # 记录开始时间
            start_time = datetime.now()
            logger.info(f"进程 {process_id} 开始处理 {len(date_batch)} 个日期，当前时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            processed_count = 0
            total_in_batch = len(date_batch)
            
            # 处理批次中的每个日期
            for i, target_date in enumerate(date_batch):
                # 计算进度
                progress = (i + 1) / total_in_batch * 100
                
                # 每10个日期或第一个/最后一个日期显示一次进度
                if (i + 1) % 10 == 0 or i == 0 or i == total_in_batch - 1:
                    logger.info(f"进程 {process_id} 处理进度: {progress:.2f}% ({i+1}/{total_in_batch}), 当前处理日期: {target_date}")
                
                # 处理当前日期
                try:
                    MarketSummaryService.calculate_market_summary(db, target_date)
                    processed_count += 1
                    # 每处理10个日期提交一次事务，减少数据库锁定时间
                    if (i + 1) % 10 == 0:
                        db.commit()
                except Exception as e:
                    logger.error(f"进程 {process_id} 处理日期 {target_date} 时出错: {str(e)}")
                    # 继续处理下一个日期
                    continue
            
            # 确保所有更改都已提交
            db.commit()
            
            # 记录结束时间和总耗时
            end_time = datetime.now()
            total_time = (end_time - start_time).total_seconds()
            logger.info(f"进程 {process_id} 处理完成，总耗时: {total_time:.2f}秒, 成功处理了 {processed_count}/{total_in_batch} 个日期")
            
            return processed_count

    @classmethod
    def update_from_derived_index(cls, db: Session, num_processes=4) -> None:
        """
        从derived_index表获取最新的日期数据，并使用多进程并行更新市场分布统计数据。

        Args:
            db (Session): 数据库会话。
            num_processes (int, optional): 并行处理的进程数量。默认为4。
        """
        import logging
        from datetime import datetime
        import time
        import multiprocessing as mp
        from math import ceil
        
        # 获取logger
        logger = logging.getLogger('daily_update')
        
        # 记录开始时间
        start_time = datetime.now()
        logger.info(f"开始更新市场分布统计数据，当前时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # 检查derived_index表是否有数据
            index_count = db.query(func.count(DerivedIndex.symbol)).scalar() or 0
            if index_count == 0:
                logger.error("derived_index表中没有数据，无法进行市场分布统计")
                return
                
            logger.info(f"derived_index表中共有 {index_count} 条记录")
            
            # 使用子查询优化性能，避免获取所有日期数据到内存
            # 获取derived_index表中的所有日期
            dates_query = db.query(DerivedIndex.date).distinct().order_by(DerivedIndex.date).subquery()
            
            # 获取已经处理过的日期
            processed_dates_query = db.query(StockMarketSummary.date).distinct().subquery()
            
            # 使用SQL差集操作找出未处理的日期
            unprocessed_dates = db.query(dates_query.c.date).filter(
                ~dates_query.c.date.in_(db.query(processed_dates_query.c.date))
            ).all()
            
            if not unprocessed_dates:
                logger.info("市场分布统计数据已是最新，无需更新")
                return
                
            unprocessed_dates = [d[0] for d in unprocessed_dates]
            
            # 获取日期范围信息
            min_date = min(unprocessed_dates)
            max_date = max(unprocessed_dates)
            logger.info(f"未处理日期范围: {min_date} 至 {max_date}")
            
            total_dates = len(unprocessed_dates)
            logger.info(f"共有 {total_dates} 个日期需要处理，将使用 {num_processes} 个进程并行处理")
            
            # 如果日期数量少于进程数，则调整进程数
            if total_dates < num_processes:
                num_processes = total_dates
                logger.info(f"由于日期数量较少，调整为使用 {num_processes} 个进程")
            
            # 将日期分成多个批次
            batch_size = ceil(total_dates / num_processes)
            date_batches = [unprocessed_dates[i:i + batch_size] for i in range(0, total_dates, batch_size)]
            
            logger.info(f"已将 {total_dates} 个日期分成 {len(date_batches)} 个批次，每批次约 {batch_size} 个日期")
            
            # 创建进程池并启动多进程处理
            with mp.Pool(processes=num_processes) as pool:
                # 为每个批次分配一个进程ID，并提交任务
                process_args = [(date_batches[i], i+1) for i in range(len(date_batches))]
                logger.info("启动多进程处理...")
                results = pool.starmap(cls._process_date_batch, process_args)
            
            # 计算总共处理的日期数量
            total_processed = sum(results)
            
            # 记录结束时间和总耗时
            end_time = datetime.now()
            total_time = (end_time - start_time).total_seconds()
            logger.info(f"市场分布统计数据更新完成，总耗时: {total_time:.2f}秒, "
                        f"成功处理了 {total_processed}/{total_dates} 个日期")
        
        except Exception as e:
            logger.error(f"更新市场分布统计数据时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())



def main():
    """主函数，用于直接运行时更新市场分布统计数据。
    
    支持命令行参数：
    --processes N: 指定使用的进程数量，默认为4
    --date YYYY-MM-DD: 指定处理特定日期，不指定则处理所有未处理日期
    --debug: 启用调试日志
    """
    import argparse
    import logging
    import sys
    from datetime import datetime
    from ..database.session import SessionLocal
    
    # 配置日志
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # 创建格式化器
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    
    # 添加处理器到logger
    logger.addHandler(console_handler)
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='更新市场分布统计数据')
    parser.add_argument('--processes', type=int, default=4, help='并行处理的进程数量，默认为4')
    parser.add_argument('--date', type=str, help='指定处理特定日期，格式为YYYY-MM-DD')
    parser.add_argument('--debug', action='store_true', help='启用调试日志')
    args = parser.parse_args()
    
    # 如果启用调试模式，设置日志级别为DEBUG
    if args.debug:
        logger.setLevel(logging.DEBUG)
        console_handler.setLevel(logging.DEBUG)
        logging.getLogger('market_summary').setLevel(logging.DEBUG)
    
    # 创建数据库会话
    with SessionLocal() as db:
        try:
            # 如果指定了日期，则只处理该日期
            if args.date:
                try:
                    target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
                    logging.info(f"处理指定日期: {target_date}")
                    MarketSummaryService.calculate_market_summary(db, target_date)
                except ValueError:
                    logging.error(f"日期格式错误: {args.date}，正确格式为YYYY-MM-DD")
                    return
            else:
                # 处理所有未处理日期
                logging.info("处理所有未处理日期")
                MarketSummaryService.update_from_derived_index(db, num_processes=args.processes)
        except Exception as e:
            logging.error(f"处理过程中出错: {str(e)}")
        
        # 等待用户输入，防止窗口立即关闭
        input("\n处理完成，按Enter键退出...")


if __name__ == "__main__":
    # 当作为模块运行时，需要设置PYTHONPATH
    import os
    import sys
    # 将项目根目录添加到Python路径
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
    sys.path.insert(0, project_root)
    main()