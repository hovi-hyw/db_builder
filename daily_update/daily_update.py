import sys
import os
import time
import threading
from datetime import datetime, timedelta
import akshare as ak
from functools import wraps
import random
import logging
from dotenv import load_dotenv
import pandas as pd

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# 设置日志
log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../logs'))
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"daily_update_{datetime.now().strftime('%Y%m%d')}.log")

# 配置日志 - 只配置当前模块的logger，避免与StockDownloader的logger冲突
logger = logging.getLogger('daily_update')
logger.setLevel(logging.INFO)
# 清除已有的处理器，避免重复
if logger.handlers:
    for handler in logger.handlers:
        logger.removeHandler(handler)
# 添加处理器
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)

# 加载环境变量
env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../StockDownloader/.env'))
if os.path.exists(env_path):
    load_dotenv(env_path)
    logger.info(f"已加载环境变量文件: {env_path}")
    logger.info(f"数据库用户: {os.getenv('DB_USER')}")
else:
    logger.error(f"环境变量文件不存在: {env_path}")

# 导入StockDownloader模块
from StockDownloader.src.tasks.download_stock_task import download_all_stock_data
from StockDownloader.src.tasks.download_index_task import download_all_index_data
from StockDownloader.src.tasks.download_etf_task import download_all_etf_data
from StockDownloader.src.database.session import SessionLocal
from StockDownloader.src.services.market_summary import MarketSummaryService

def retry_with_delay(max_retries=3, initial_delay=60):
    """
    带重试和延迟的装饰器
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            delay = initial_delay
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries == max_retries:
                        raise e
                    logger.warning(f"操作失败，{delay}秒后重试: {str(e)}")
                    # 添加随机延迟，避免固定间隔
                    actual_delay = delay + random.randint(1, 30)
                    time.sleep(actual_delay)
                    # 增加下次重试的延迟时间
                    delay *= 2
            return None
        return wrapper
    return decorator

@retry_with_delay(max_retries=3, initial_delay=60)
def is_trading_day():
    """
    判断今天是否为交易日
    通过获取交易日历来判断
    """
    try:
        today = datetime.now().strftime('%Y%m%d')
        # 获取交易日历
        df = ak.tool_trade_date_hist_sina()
        # 将交易日历中的日期转换为字符串格式
        trade_dates = [d.strftime('%Y%m%d') for d in df['trade_date'].values]
        return today in trade_dates
    except Exception as e:
        logger.error(f"检查交易日时发生错误: {str(e)}")
        raise

@retry_with_delay(max_retries=3, initial_delay=166)  # 1分钟初始延迟
def update_stock_data():
    """
    更新股票数据，带重试机制
    """
    download_all_stock_data(update_only=True)

@retry_with_delay(max_retries=3, initial_delay=166)  # 1分钟初始延迟
def update_index_data():
    """
    更新指数数据，带重试机制
    """
    logger.info("开始执行指数数据更新函数...")
    
    # 获取当前指数数据的最新日期
    latest_index_date = get_latest_index_date()
    logger.info(f"当前指数数据库中最新日期: {latest_index_date}")
    
    # 获取最近的交易日
    from StockDownloader.src.utils.trading_calendar import get_latest_trading_day
    latest_trading_day = get_latest_trading_day()
    logger.info(f"最近的交易日: {latest_trading_day}")
    
    # 判断是否需要更新
    if latest_index_date and latest_index_date >= latest_trading_day:
        logger.info(f"指数数据已是最新 ({latest_index_date})，无需更新")
        return
    
    logger.info(f"指数数据需要更新，从 {latest_index_date} 更新到 {latest_trading_day}")
    
    # 调用下载函数并记录详细日志
    try:
        logger.info("调用download_all_index_data函数进行更新...")
        download_all_index_data(update_only=True)
        
        # 验证更新后的数据
        new_latest_date = get_latest_index_date()
        logger.info(f"更新后指数数据库中最新日期: {new_latest_date}")
        
        if new_latest_date and new_latest_date > latest_index_date:
            logger.info(f"指数数据成功更新，最新日期从 {latest_index_date} 更新到 {new_latest_date}")
        else:
            logger.warning(f"指数数据可能未成功更新，更新前日期: {latest_index_date}，更新后日期: {new_latest_date}")
    except Exception as e:
        logger.error(f"指数数据更新过程中发生错误: {str(e)}")
        raise

@retry_with_delay(max_retries=3, initial_delay=166)  # 1分钟初始延迟
def update_etf_data():
    """
    更新ETF数据，带重试机制
    """
    download_all_etf_data(update_only=True)

def run_daily_update(max_retries=3):
    """
    运行每日更新任务
    Args:
        max_retries (int): 最大重试次数
    """
    for attempt in range(max_retries):
        try:
            logger.info(f"开始执行每日更新任务... (尝试 {attempt + 1}/{max_retries})")
            
            # 检查各类数据是否需要更新
            stock_need_update = need_update_stock()
            index_need_update = need_update_index()
            etf_need_update = need_update_etf()
            
            # 如果都不需要更新，直接返回
            if not any([stock_need_update, index_need_update, etf_need_update]):
                logger.info("所有数据都是最新的，无需更新")
                return
            
            # 创建线程来更新股票数据（如果需要）
            stock_thread = None
            if stock_need_update:
                logger.info("开始更新股票数据...")
                stock_thread = threading.Thread(target=update_stock_data)
                stock_thread.start()
            
            # 串行更新指数和ETF数据
            if index_need_update:
                logger.info("开始更新指数数据...")
                start_time = datetime.now()
                logger.info(f"当前时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
                
                # 获取更新前的最新指数数据日期
                pre_update_date = get_latest_index_date()
                logger.info(f"更新前指数数据最新日期: {pre_update_date}")
                
                # 调用更新函数
                update_index_data()
                
                # 获取更新后的最新指数数据日期
                post_update_date = get_latest_index_date()
                logger.info(f"更新后指数数据最新日期: {post_update_date}")
                
                # 检查是否真正更新了数据
                if post_update_date and post_update_date > pre_update_date:
                    logger.info(f"指数数据成功更新，日期从 {pre_update_date} 更新到 {post_update_date}")
                else:
                    logger.warning(f"指数数据可能未成功更新，更新前日期: {pre_update_date}，更新后日期: {post_update_date}")
                
                end_time = datetime.now()
                elapsed_time = (end_time - start_time).total_seconds()
                logger.info(f"指数数据更新完成，耗时: {elapsed_time:.2f} 秒")
                logger.info("指数数据更新完成")
            
            if etf_need_update:
                logger.info("开始更新ETF数据...")
                update_etf_data()
                logger.info("ETF数据更新完成")
            
            # 等待股票数据更新完成（如果有）
            if stock_thread:
                stock_thread.join()
                logger.info("股票数据更新完成")
            
            # 更新市场分布统计数据
            logger.info("开始更新市场分布统计数据...")
            try:
                with SessionLocal() as db:
                    MarketSummaryService.update_from_derived_index(db)
                logger.info("市场分布统计数据更新完成")
            except Exception as e:
                logger.error(f"更新市场分布统计数据时发生错误: {str(e)}")
            
            logger.info("每日更新任务执行完成")
            return  # 如果所有更新都成功，直接返回
            
        except Exception as e:
            logger.error(f"更新任务失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 300  # 递增等待时间，从5分钟开始
                logger.info(f"等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                logger.error("达到最大重试次数，更新任务失败")
                raise  # 重试耗尽后抛出异常

def get_latest_stock_date():
    """
    获取股票数据库中最新的数据日期
    """
    from StockDownloader.src.database.session import engine
    from StockDownloader.src.database.models.stock import StockDailyData
    from StockDownloader.src.tasks.update_data_task import get_latest_date_from_db
    return get_latest_date_from_db(engine, StockDailyData)

def get_latest_index_date():
    """
    获取指数数据库中最新的数据日期
    """
    from StockDownloader.src.database.session import engine
    from StockDownloader.src.database.models.index import IndexDailyData
    from StockDownloader.src.tasks.update_data_task import get_latest_date_from_db
    return get_latest_date_from_db(engine, IndexDailyData)

def get_latest_etf_date():
    """
    获取ETF数据库中最新的数据日期
    """
    from StockDownloader.src.database.session import engine
    from StockDownloader.src.database.models.etf import ETFDailyData
    from StockDownloader.src.tasks.update_data_task import get_latest_date_from_db
    return get_latest_date_from_db(engine, ETFDailyData)

def get_next_trading_day(from_date):
    """
    获取指定日期之后的第一个交易日
    """
    try:
        # 获取交易日历
        df = ak.tool_trade_date_hist_sina()
        # 将日期列转换为日期对象
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
        # 获取大于指定日期的第一个交易日
        next_trading_days = df[df['trade_date'] > from_date]['trade_date']
        return next_trading_days.iloc[0] if not next_trading_days.empty else None
    except Exception as e:
        logger.error(f"获取下一个交易日时发生错误: {str(e)}")
        return None

def calculate_next_update_time(is_trading=False, should_update=True):
    """
    计算下一次更新时间
    Args:
        is_trading (bool): 是否为交易日
        should_update (bool): 是否需要更新数据
    """
    now = datetime.now()
    today = now.date()
    
    # 如果数据已是最新，直接返回下一个交易日的17点
    if not should_update:
        next_trading = get_next_trading_day(today)
        if next_trading:
            return datetime(next_trading.year, next_trading.month, next_trading.day, 17, 0)
        return now + timedelta(days=1)
    
    # 如果当前时间在17点之前
    if now.hour < 17:
        return datetime(today.year, today.month, today.day, 17, 0)
    # 如果当前时间在22点之后，获取下一个交易日的17点
    elif now.hour >= 22:
        next_trading = get_next_trading_day(today)
        if next_trading:
            return datetime(next_trading.year, next_trading.month, next_trading.day, 17, 0)
    # 如果是交易日且在17-22点之间，可以立即更新
    elif is_trading and 17 <= now.hour < 22:
        return now
    
    # 默认等到下一个交易日的17点
    next_trading = get_next_trading_day(today)
    if next_trading:
        return datetime(next_trading.year, next_trading.month, next_trading.day, 17, 0)
    return now + timedelta(days=1)  # 如果无法获取下一个交易日，默认等待24小时

def need_update_stock():
    """
    判断是否需要更新股票数据
    """
    latest_data_date = get_latest_stock_date()
    if not latest_data_date:
        return True
    
    # 获取最近的交易日
    from StockDownloader.src.utils.trading_calendar import get_latest_trading_day
    latest_trading_day = get_latest_trading_day()
    
    # 如果数据日期落后于最近的交易日，需要更新
    return latest_data_date < latest_trading_day

def need_update_index():
    """
    判断是否需要更新指数数据
    """
    latest_data_date = get_latest_index_date()
    if not latest_data_date:
        return True
    
    # 获取最近的交易日
    from StockDownloader.src.utils.trading_calendar import get_latest_trading_day
    latest_trading_day = get_latest_trading_day()
    
    # 如果数据日期落后于最近的交易日，需要更新
    return latest_data_date < latest_trading_day

def need_update_etf():
    """
    判断是否需要更新ETF数据
    """
    latest_data_date = get_latest_etf_date()
    if not latest_data_date:
        return True
    
    # 获取最近的交易日
    from StockDownloader.src.utils.trading_calendar import get_latest_trading_day
    latest_trading_day = get_latest_trading_day()
    
    # 如果数据日期落后于最近的交易日，需要更新
    return latest_data_date < latest_trading_day

def main():
    """
    主函数
    """
    try:
        while True:
            now = datetime.now()
            is_trade_day = is_trading_day()
            # 检查是否需要更新任何一种数据
            should_update = any([need_update_stock(), need_update_index(), need_update_etf()])
            
            if not should_update:
                logger.info("数据已是最新，无需更新")
                # 计算下一次更新时间，传入should_update=False
                next_update = calculate_next_update_time(is_trade_day, should_update=False)
            else:
                # 判断当前是否在合适的更新时间
                if is_trade_day:
                    # 交易日：如果在17-22点之间可以更新，否则等到17点
                    if 17 <= now.hour < 22:
                        logger.info("开始更新数据...")
                        run_daily_update()
                        next_update = calculate_next_update_time(is_trade_day, should_update=should_update)
                    else:
                        next_update = calculate_next_update_time(is_trade_day, should_update=should_update)
                        logger.info(f"当前时间不在更新时间范围内，等待到下一个更新时间")
                else:
                    # 非交易日：可以立即更新
                    logger.info("非交易日，开始更新数据...")
                    run_daily_update()
                    next_update = calculate_next_update_time(is_trade_day, should_update=should_update)
            
            # 计算等待时间
            wait_seconds = (next_update - now).total_seconds()
            logger.info(f"下次更新时间: {next_update.strftime('%Y-%m-%d %H:%M:%S')}, ")
            logger.info(f"等待时间: {wait_seconds/3600:.2f}小时")
            
            # 如果不是容器环境，更新一次后退出
            if os.environ.get('CONTAINER_ENV') != 'true':
                logger.info("非容器环境，更新完成后退出")
                break
                
            # 休眠到下一次更新时间
            time.sleep(wait_seconds)
            
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()