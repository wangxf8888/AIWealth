-- 股票基本信息表
CREATE TABLE IF NOT EXISTS stock_basic (
    ts_code VARCHAR(20) PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(100) NOT NULL,
    area VARCHAR(20),
    industry VARCHAR(50),
    list_date DATE,
    delist_date DATE,
    is_active BOOLEAN DEFAULT true
);

-- 日线行情表
CREATE TABLE IF NOT EXISTS daily_kline (
    id SERIAL PRIMARY KEY,
    ts_code VARCHAR(20) NOT NULL REFERENCES stock_basic(ts_code),
    trade_date DATE NOT NULL,
    open NUMERIC(10,2),
    high NUMERIC(10,2),
    low NUMERIC(10,2),
    close NUMERIC(10,2),
    volume BIGINT,
    amount NUMERIC(20,2),
    turnover_rate NUMERIC(8,4),
    UNIQUE(ts_code, trade_date)
);

-- 指数行情表
CREATE TABLE IF NOT EXISTS index_kline (
    id SERIAL PRIMARY KEY,
    index_code VARCHAR(20) NOT NULL,
    index_name VARCHAR(50) NOT NULL,
    trade_date DATE NOT NULL,
    open NUMERIC(10,2),
    high NUMERIC(10,2),
    low NUMERIC(10,2),
    close NUMERIC(10,2),
    volume BIGINT,
    amount NUMERIC(20,2),
    UNIQUE(index_code, trade_date)
);

-- 板块成分股表
CREATE TABLE IF NOT EXISTS concept_stocks (
    id SERIAL PRIMARY KEY,
    concept_name VARCHAR(50) NOT NULL,
    ts_code VARCHAR(20) NOT NULL REFERENCES stock_basic(ts_code),
    weight NUMERIC(5,2),
    UNIQUE(concept_name, ts_code)
);

-- 创建索引（提升查询性能）
CREATE INDEX IF NOT EXISTS idx_daily_ts_code ON daily_kline(ts_code);
CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_kline(trade_date);
CREATE INDEX IF NOT EXISTS idx_index_code ON index_kline(index_code);
CREATE INDEX IF NOT EXISTS idx_index_date ON index_kline(trade_date);

-- backend/sql/create_strategy_tables.sql
CREATE TABLE IF NOT EXISTS strategy_signals (
    id SERIAL PRIMARY KEY,
    signal_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    name VARCHAR(100) NOT NULL,
    buy_price NUMERIC(10,2),
    target_price NUMERIC(10,2),
    sell_price NUMERIC(10,2),
    sell_date DATE,
    is_success BOOLEAN,
    holding_days INTEGER,
    profit_rate NUMERIC(8,2)
);

CREATE TABLE IF NOT EXISTS strategy_summary (
    id SERIAL PRIMARY KEY,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    total_signals INTEGER,
    success_signals INTEGER,
    success_rate NUMERIC(6,2),
    total_profit_rate NUMERIC(10,2),
    avg_profit_per_signal NUMERIC(8,2)
);


