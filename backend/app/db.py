import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

# 数据库文件路径
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "wealth.db"

def get_conn():
    """
    获取数据库连接
    开启 WAL 模式以支持更好的并发读取（回测时很有用）
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # 允许通过列名访问数据
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def init_db():
    """
    初始化数据库表结构
    包含：基础信息、日 K 线、指数、核心池（当前）、核心池历史（回测用）
    """
    conn = get_conn()
    c = conn.cursor()

    print("🔧 Initializing database schema...")

    # 1. 股票基础信息表
    c.execute('''CREATE TABLE IF NOT EXISTS stock_basic (
        code TEXT PRIMARY KEY,
        code_name TEXT,
        ipo_date TEXT,
        is_st INTEGER DEFAULT 0,
        update_time TEXT
    )''')

    # 2. 指数基础信息表
    c.execute('''CREATE TABLE IF NOT EXISTS index_basic (
        code TEXT PRIMARY KEY,
        code_name TEXT,
        update_time TEXT
    )''')

    # 3. 股票日 K 线表 (含小时线衍生字段)
    c.execute('''CREATE TABLE IF NOT EXISTS stock_daily_k (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        code TEXT,
        code_name TEXT,
        open REAL, high REAL, low REAL, close REAL, preclose REAL,
        volume REAL, amount REAL, turn REAL, pctChg REAL,
        open_rate REAL, close_rate REAL, high_rate REAL, low_rate REAL,
        hour1_open_rate REAL, hour1_close_rate REAL, hour1_high_rate REAL, hour1_low_rate REAL,
        hour2_open_rate REAL, hour2_close_rate REAL, hour2_high_rate REAL, hour2_low_rate REAL,
        hour3_open_rate REAL, hour3_close_rate REAL, hour3_high_rate REAL, hour3_low_rate REAL,
        hour4_open_rate REAL, hour4_close_rate REAL, hour4_high_rate REAL, hour4_low_rate REAL,
        UNIQUE(date, code)
    )''')
    c.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_k_code_date_unique ON stock_daily_k(code, date)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_k_date ON stock_daily_k(date)')

    # 4. 指数日 K 线表
    c.execute('''CREATE TABLE IF NOT EXISTS index_daily_k (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        code TEXT,
        code_name TEXT,
        open REAL, high REAL, low REAL, close REAL, preclose REAL,
        volume REAL, amount REAL, turn REAL, pctChg REAL,
        open_rate REAL, close_rate REAL, high_rate REAL, low_rate REAL,
        hour1_open_rate REAL, hour1_close_rate REAL, hour1_high_rate REAL, hour1_low_rate REAL,
        hour2_open_rate REAL, hour2_close_rate REAL, hour2_high_rate REAL, hour2_low_rate REAL,
        hour3_open_rate REAL, hour3_close_rate REAL, hour3_high_rate REAL, hour3_low_rate REAL,
        hour4_open_rate REAL, hour4_close_rate REAL, hour4_high_rate REAL, hour4_low_rate REAL,
        UNIQUE(date, code)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_idx_code_date ON index_daily_k(code, date)')

    # 5. 概念板块映射表
    c.execute('''CREATE TABLE IF NOT EXISTS concept_mapping (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        concept_name TEXT,
        code TEXT,
        code_name TEXT,
        update_time TEXT,
        UNIQUE(concept_name, code)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_concept_name ON concept_mapping(concept_name)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_concept_code ON concept_mapping(code)')

    # 6. 概念板块 K 线表
    c.execute('''CREATE TABLE IF NOT EXISTS concept_kline (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        concept_name TEXT,
        date TEXT,
        open REAL, high REAL, low REAL, close REAL,
        volume REAL, amount REAL, pct_chg REAL,
        update_time TEXT,
        UNIQUE(concept_name, date)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_ck_concept ON concept_kline(concept_name)')

    # 7. 核心池 (当前最新状态 - 用于实盘)
    c.execute('''CREATE TABLE IF NOT EXISTS core_pool (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT,
        code_name TEXT,
        market_cap REAL,
        max_consecutive_limit INTEGER,
        total_limit_ups_1y INTEGER,
        total_limit_ups_1m INTEGER,
        total_limit_ups_2m INTEGER,
        reason TEXT,
        last_verified_date TEXT,
        is_active INTEGER DEFAULT 1,
        UNIQUE(code)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_core_code ON core_pool(code)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_core_active ON core_pool(is_active)')

    # 8. 【新增】核心池历史快照 (用于回测)
    # 记录每一天符合核心池标准的股票列表
    c.execute('''CREATE TABLE IF NOT EXISTS core_pool_history (
        trade_date TEXT,
        created_at TEXT,
        code TEXT,
        code_name TEXT,
        market_cap REAL,
        max_consecutive_limit INTEGER,
        total_limit_ups_1y INTEGER,
        total_limit_ups_1m INTEGER,
        total_limit_ups_2m INTEGER,
        total_limit_ups_5d INTEGER,
        reason TEXT,
        PRIMARY KEY (trade_date, code)
    )''')
    # 关键索引：加速按日期查询 (回测每天查一次) 和按代码查历史
    c.execute('CREATE INDEX IF NOT EXISTS idx_cph_date ON core_pool_history(trade_date)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_cph_code ON core_pool_history(code)')

    # 9. 回测交易记录表 (用于存储回测结果)
    c.execute('''CREATE TABLE IF NOT EXISTS backtest_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_name TEXT,
        trade_date TEXT,
        code TEXT,
        code_name TEXT,
        action TEXT, -- 'BUY' or 'SELL'
        price REAL,
        volume INTEGER,
        amount REAL,
        profit_loss REAL, -- 卖出时填写盈亏
        hold_days INTEGER, -- 持有天数
        reason TEXT,
        create_time TEXT DEFAULT CURRENT_TIMESTAMP
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_bt_strategy ON backtest_trades(strategy_name)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_bt_date ON backtest_trades(trade_date)')

    # 10. 回测每日账户快照
    c.execute('''CREATE TABLE IF NOT EXISTS backtest_daily_snapshot (
        trade_date TEXT PRIMARY KEY,
        strategy_name TEXT,
        cash REAL,
        total_value REAL,
        position_count INTEGER,
        daily_return REAL,
        cumulative_return REAL,
        max_drawdown REAL,
        update_time TEXT
    )''')

    # 11. 每日候选池表 (缺失修复)
    c.execute('''CREATE TABLE IF NOT EXISTS daily_candidates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_date TEXT,
        strategy_name TEXT DEFAULT 'default',
        code TEXT,
        score REAL,
        reason TEXT,
        sector TEXT,
        created_at TEXT,
        UNIQUE(trade_date, code)
    )''')
    c.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_dc_unique ON daily_candidates(trade_date, strategy_name, code)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_dc_strategy ON daily_candidates(strategy_name)')

    # 12. 分析报告表 (缺失修复)
    c.execute('''CREATE TABLE IF NOT EXISTS analysis_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_date TEXT,
        period TEXT, -- 'pre', 'intra', 'post'
        content TEXT,
        created_at TEXT
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_ar_date_period ON analysis_reports(report_date, period)')

    # 13. 用户持仓表 (缺失修复，用于模拟交易)
    c.execute('''CREATE TABLE IF NOT EXISTS positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        code TEXT,
        buy_date TEXT,
        hold_volume INTEGER,
        cost_price REAL,
        update_time TEXT,
        UNIQUE(user_id, code)
    )''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_pos_user ON positions(user_id)')

    # 14. 用户表 (基础权限)
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password_hash TEXT,
        role TEXT DEFAULT 'user', -- 'admin' or 'user'
        is_vip INTEGER DEFAULT 0,
        created_at TEXT
    )''')

    conn.commit()
    conn.close()
    print("✅ Database schema initialized successfully with all tables.")

def get_trade_date_range(conn, start_date, end_date):
    """
    获取指定范围内的所有有效交易日期
    从 stock_daily_k 中提取去重后的日期列表
    """
    c = conn.cursor()
    c.execute("""
        SELECT DISTINCT date FROM stock_daily_k
        WHERE date >= ? AND date <= ?
        ORDER BY date ASC
    """, (start_date, end_date))
    return [row[0] for row in c.fetchall()]

if __name__ == "__main__":
    # 手动运行此文件可初始化/更新数据库结构
    init_db()
    print(f"📍 Database location: {DB_PATH}")
