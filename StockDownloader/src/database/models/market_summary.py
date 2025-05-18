from sqlalchemy import Column, String, Date, Numeric, PrimaryKeyConstraint

from ..base import Base


class StockMarketSummary(Base):
    __tablename__ = "stock_market_summary"

    symbol = Column(String(10), nullable=False)  # 指数代码
    date = Column(Date, nullable=False)  # 日期
    count_lt_neg8pct = Column(Numeric(4, 3))  # 跌幅小于-8%的股票占比
    count_neg8pct_to_neg5pct = Column(Numeric(4, 3))  # 跌幅在-8%到-5%之间的股票占比
    count_neg5pct_to_neg2pct = Column(Numeric(4, 3))  # 跌幅在-5%到-2%之间的股票占比
    count_neg2pct_to_2pct = Column(Numeric(4, 3))  # 涨跌幅在-2%到2%之间的股票占比
    count_2pct_to_5pct = Column(Numeric(4, 3))  # 涨幅在2%到5%之间的股票占比
    count_5pct_to_8pct = Column(Numeric(4, 3))  # 涨幅在5%到8%之间的股票占比
    count_gt_8pct = Column(Numeric(4, 3))  # 涨幅大于8%的股票占比

    __table_args__ = (
        PrimaryKeyConstraint('symbol', 'date'),
    )

    def __repr__(self):
        return f"<StockMarketSummary(symbol={self.symbol}, date={self.date})>"