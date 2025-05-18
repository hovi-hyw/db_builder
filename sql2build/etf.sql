-- pg_dump -U si -d stock_db -s -t daily_etf  --查看表结构

-- daily_etf
CREATE TABLE public.daily_etf (
    symbol character varying NOT NULL,
    date date NOT NULL,
    open double precision,
    close double precision,
    high double precision,
    low double precision,
    volume bigint,
    amount double precision,
    amplitude double precision,
    change_rate double precision,
    change_amount double precision,
    turnover_rate double precision
);

ALTER TABLE public.daily_etf OWNER TO si;

ALTER TABLE ONLY public.daily_etf ADD CONSTRAINT daily_etf_pkey PRIMARY KEY (symbol, date);
    
-- etf_info
CREATE TABLE public.etf_info (
    symbol character varying NOT NULL,
    name character varying(100) NOT NULL
);

ALTER TABLE public.etf_info OWNER TO si;

ALTER TABLE ONLY public.etf_info ADD CONSTRAINT etf_info_pkey PRIMARY KEY (symbol);