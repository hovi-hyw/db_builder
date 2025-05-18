--  pg_dump -U si -d stock_db -s -t stock_market_summary
CREATE TABLE public.stock_market_summary (
    symbol character varying(10) NOT NULL,
    date date NOT NULL,
    count_lt_neg8pct numeric(4,3),
    count_neg8pct_to_neg5pct numeric(4,3),
    count_neg5pct_to_neg2pct numeric(4,3),
    count_neg2pct_to_2pct numeric(4,3),
    count_2pct_to_5pct numeric(4,3),
    count_5pct_to_8pct numeric(4,3),
    count_gt_8pct numeric(4,3)
);


ALTER TABLE public.stock_market_summary OWNER TO si;

--
-- Name: stock_market_summary stock_market_summary_pkey; Type: CONSTRAINT; Schema: public; Owner: si
--

ALTER TABLE ONLY public.stock_market_summary
    ADD CONSTRAINT stock_market_summary_pkey PRIMARY KEY (symbol, date);

-- 下面是计算
-- 创建计算市场分布的函数   calculate_market_summary
CREATE OR REPLACE FUNCTION calculate_market_summary(target_date date, index_symbol varchar)
RETURNS TABLE (
    count_lt_neg8pct numeric(4,3),
    count_neg8pct_to_neg5pct numeric(4,3),
    count_neg5pct_to_neg2pct numeric(4,3),
    count_neg2pct_to_2pct numeric(4,3),
    count_2pct_to_5pct numeric(4,3),
    count_5pct_to_8pct numeric(4,3),
    count_gt_8pct numeric(4,3)
) AS $$
DECLARE
    total_count integer;
BEGIN
    -- 计算符合条件的股票总数
    SELECT COUNT(*) INTO total_count
    FROM derived_stock
    WHERE date = target_date
    AND (
        (index_symbol = '899050' AND symbol ~ '^8[378]') OR
        (index_symbol = '000698' AND symbol ~ '^688') OR
        (index_symbol = '399006' AND symbol ~ '^30[01]') OR
        (index_symbol = '000001' AND NOT (symbol ~ '^8[378]' OR symbol ~ '^688' OR symbol ~ '^30[01]'))
    );

    RETURN QUERY
    SELECT 
        ROUND(COUNT(CASE WHEN real_change < -0.08 THEN 1 END)::NUMERIC / GREATEST(total_count, 1), 3),
        ROUND(COUNT(CASE WHEN real_change >= -0.08 AND real_change < -0.05 THEN 1 END)::NUMERIC / GREATEST(total_count, 1), 3),
        ROUND(COUNT(CASE WHEN real_change >= -0.05 AND real_change < -0.02 THEN 1 END)::NUMERIC / GREATEST(total_count, 1), 3),
        ROUND(COUNT(CASE WHEN real_change >= -0.02 AND real_change < 0.02 THEN 1 END)::NUMERIC / GREATEST(total_count, 1), 3),
        ROUND(COUNT(CASE WHEN real_change >= 0.02 AND real_change < 0.05 THEN 1 END)::NUMERIC / GREATEST(total_count, 1), 3),
        ROUND(COUNT(CASE WHEN real_change >= 0.05 AND real_change < 0.08 THEN 1 END)::NUMERIC / GREATEST(total_count, 1), 3),
        ROUND(COUNT(CASE WHEN real_change >= 0.08 THEN 1 END)::NUMERIC / GREATEST(total_count, 1), 3)
    FROM derived_stock
    WHERE date = target_date
    AND (
        (index_symbol = '899050' AND symbol ~ '^8[378]') OR
        (index_symbol = '000698' AND symbol ~ '^688') OR
        (index_symbol = '399006' AND symbol ~ '^30[01]') OR
        (index_symbol = '000001' AND NOT (symbol ~ '^8[378]' OR symbol ~ '^688' OR symbol ~ '^30[01]'))
    );
END;
$$ LANGUAGE plpgsql;

-- 创建触发器函数 
CREATE OR REPLACE FUNCTION update_market_summary()
RETURNS TRIGGER AS $$
BEGIN
    -- 更新 899050
    INSERT INTO stock_market_summary (symbol, date, count_lt_neg8pct, count_neg8pct_to_neg5pct, 
        count_neg5pct_to_neg2pct, count_neg2pct_to_2pct, count_2pct_to_5pct, 
        count_5pct_to_8pct, count_gt_8pct)
    SELECT '899050', NEW.date, s.* FROM calculate_market_summary(NEW.date, '899050') s
    ON CONFLICT (symbol, date) DO UPDATE SET
        count_lt_neg8pct = EXCLUDED.count_lt_neg8pct,
        count_neg8pct_to_neg5pct = EXCLUDED.count_neg8pct_to_neg5pct,
        count_neg5pct_to_neg2pct = EXCLUDED.count_neg5pct_to_neg2pct,
        count_neg2pct_to_2pct = EXCLUDED.count_neg2pct_to_2pct,
        count_2pct_to_5pct = EXCLUDED.count_2pct_to_5pct,
        count_5pct_to_8pct = EXCLUDED.count_5pct_to_8pct,
        count_gt_8pct = EXCLUDED.count_gt_8pct;

    -- 更新 000698
    INSERT INTO stock_market_summary (symbol, date, count_lt_neg8pct, count_neg8pct_to_neg5pct, 
        count_neg5pct_to_neg2pct, count_neg2pct_to_2pct, count_2pct_to_5pct, 
        count_5pct_to_8pct, count_gt_8pct)
    SELECT '000698', NEW.date, s.* FROM calculate_market_summary(NEW.date, '000698') s
    ON CONFLICT (symbol, date) DO UPDATE SET
        count_lt_neg8pct = EXCLUDED.count_lt_neg8pct,
        count_neg8pct_to_neg5pct = EXCLUDED.count_neg8pct_to_neg5pct,
        count_neg5pct_to_neg2pct = EXCLUDED.count_neg5pct_to_neg2pct,
        count_neg2pct_to_2pct = EXCLUDED.count_neg2pct_to_2pct,
        count_2pct_to_5pct = EXCLUDED.count_2pct_to_5pct,
        count_5pct_to_8pct = EXCLUDED.count_5pct_to_8pct,
        count_gt_8pct = EXCLUDED.count_gt_8pct;

    -- 更新 399006
    INSERT INTO stock_market_summary (symbol, date, count_lt_neg8pct, count_neg8pct_to_neg5pct, 
        count_neg5pct_to_neg2pct, count_neg2pct_to_2pct, count_2pct_to_5pct, 
        count_5pct_to_8pct, count_gt_8pct)
    SELECT '399006', NEW.date, s.* FROM calculate_market_summary(NEW.date, '399006') s
    ON CONFLICT (symbol, date) DO UPDATE SET
        count_lt_neg8pct = EXCLUDED.count_lt_neg8pct,
        count_neg8pct_to_neg5pct = EXCLUDED.count_neg8pct_to_neg5pct,
        count_neg5pct_to_neg2pct = EXCLUDED.count_neg5pct_to_neg2pct,
        count_neg2pct_to_2pct = EXCLUDED.count_neg2pct_to_2pct,
        count_2pct_to_5pct = EXCLUDED.count_2pct_to_5pct,
        count_5pct_to_8pct = EXCLUDED.count_5pct_to_8pct,
        count_gt_8pct = EXCLUDED.count_gt_8pct;

    -- 更新 000001
    INSERT INTO stock_market_summary (symbol, date, count_lt_neg8pct, count_neg8pct_to_neg5pct, 
        count_neg5pct_to_neg2pct, count_neg2pct_to_2pct, count_2pct_to_5pct, 
        count_5pct_to_8pct, count_gt_8pct)
    SELECT '000001', NEW.date, s.* FROM calculate_market_summary(NEW.date, '000001') s
    ON CONFLICT (symbol, date) DO UPDATE SET
        count_lt_neg8pct = EXCLUDED.count_lt_neg8pct,
        count_neg8pct_to_neg5pct = EXCLUDED.count_neg8pct_to_neg5pct,
        count_neg5pct_to_neg2pct = EXCLUDED.count_neg5pct_to_neg2pct,
        count_neg2pct_to_2pct = EXCLUDED.count_neg2pct_to_2pct,
        count_2pct_to_5pct = EXCLUDED.count_2pct_to_5pct,
        count_5pct_to_8pct = EXCLUDED.count_5pct_to_8pct,
        count_gt_8pct = EXCLUDED.count_gt_8pct;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 创建触发器
CREATE TRIGGER update_market_summary_trigger
AFTER INSERT OR UPDATE ON derived_index
FOR EACH ROW
EXECUTE FUNCTION update_market_summary();

-- 创建数据刷新函数
CREATE OR REPLACE FUNCTION refresh_stock_market_summary()
RETURNS void AS $$
DECLARE
    r RECORD;
BEGIN
    -- 清空现有数据
    TRUNCATE TABLE stock_market_summary;
    
    -- 从derived_index表获取所有需要的symbol和date组合
    FOR r IN 
        SELECT DISTINCT date 
        FROM derived_index 
        WHERE symbol IN ('899050', '000698', '399006', '000001')
        ORDER BY date
    LOOP
        -- 对每个日期执行更新
        INSERT INTO stock_market_summary (symbol, date, count_lt_neg8pct, count_neg8pct_to_neg5pct, 
            count_neg5pct_to_neg2pct, count_neg2pct_to_2pct, count_2pct_to_5pct, 
            count_5pct_to_8pct, count_gt_8pct)
        SELECT '899050', r.date, s.* FROM calculate_market_summary(r.date, '899050') s;
        
        INSERT INTO stock_market_summary (symbol, date, count_lt_neg8pct, count_neg8pct_to_neg5pct, 
            count_neg5pct_to_neg2pct, count_neg2pct_to_2pct, count_2pct_to_5pct, 
            count_5pct_to_8pct, count_gt_8pct)
        SELECT '000698', r.date, s.* FROM calculate_market_summary(r.date, '000698') s;
        
        INSERT INTO stock_market_summary (symbol, date, count_lt_neg8pct, count_neg8pct_to_neg5pct, 
            count_neg5pct_to_neg2pct, count_neg2pct_to_2pct, count_2pct_to_5pct, 
            count_5pct_to_8pct, count_gt_8pct)
        SELECT '399006', r.date, s.* FROM calculate_market_summary(r.date, '399006') s;
        
        INSERT INTO stock_market_summary (symbol, date, count_lt_neg8pct, count_neg8pct_to_neg5pct, 
            count_neg5pct_to_neg2pct, count_neg2pct_to_2pct, count_2pct_to_5pct, 
            count_5pct_to_8pct, count_gt_8pct)
        SELECT '000001', r.date, s.* FROM calculate_market_summary(r.date, '000001') s;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


-- 这些SQL代码的功能说明：

-- 1. calculate_market_summary 函数：
   
--    - 接受日期和指数代码作为参数
--    - 根据不同的指数代码筛选对应的股票
--    - 计算不同涨跌幅区间的股票占比
-- 2. update_market_summary 触发器函数：
   
--    - 当 derived_index 表有数据变化时触发
--    - 更新四个指数（899050、000698、399006、000001）的市场分布数据
-- 3. update_market_summary_trigger 触发器：
   
--    - 监听 derived_index 表的插入和更新操作
--    - 触发 update_market_summary 函数
-- 4. refresh_stock_market_summary 函数：
   
--    - 用于初始化或重新计算所有历史数据
--    - 清空现有数据并重新计算所有日期的市场分布
-- 使用方法：

-- 1. 首先执行所有创建函数和触发器的SQL语句
-- 2. 要填充历史数据，执行：SELECT refresh_stock_market_summary();
-- 之后，每当 derived_index 表有新数据插入或更新时，触发器会自动更新 stock_market_summary 表的对应数据。