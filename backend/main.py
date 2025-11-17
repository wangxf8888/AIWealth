# /home/AIWealth/backend/main.py
import os
from datetime import datetime
import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="AIWealth API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'aiwealth'),
    'user': os.getenv('DB_USER', 'aiwealth'),
    'password': os.getenv('DB_PASSWORD')
}

@app.get("/api/health")
def health_check():
    return {"status": "ok", "message": "AIWealth 后端运行中！"}

@app.get("/api/strategy/performance")
def get_strategy_performance():
    """返回最新策略汇总数据"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    total_signals, success_signals, success_rate,
                    total_profit_rate, avg_profit_per_signal
                FROM strategy_summary
                ORDER BY created_at DESC
                LIMIT 1
            """)
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="暂无策略回测数据")
            return {
                "total_signals": row[0],
                "success_signals": row[1],
                "success_rate": float(row[2]),
                "total_profit_rate": float(row[3]),
                "avg_profit_per_signal": float(row[4])
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"数据库错误: {str(e)}")
    finally:
        if conn:
            conn.close()

@app.get("/api/strategy/performance-curve")
def get_performance_curve(year: int = None):
    """返回累计收益曲线，可选年份"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            if year:
                # 查询指定年份
                cur.execute("""
                    SELECT signal_date, profit_rate
                    FROM strategy_performance
                    WHERE sell_date IS NOT NULL
                      AND EXTRACT(YEAR FROM signal_date) = %s
                    ORDER BY signal_date
                """, (year,))
            else:
                # 查询全部
                cur.execute("""
                    SELECT signal_date, profit_rate
                    FROM strategy_performance
                    WHERE sell_date IS NOT NULL
                    ORDER BY signal_date
                """)
            signals = cur.fetchall()
            
            dates = []
            profits = []
            cum_profit = 0.0
            
            for date, profit in signals:
                cum_profit += float(profit)
                dates.append(date.strftime("%Y-%m-%d"))
                profits.append(round(cum_profit, 2))
                
            return {"dates": dates, "profits": profits}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取曲线失败: {str(e)}")
    finally:
        if conn:
            conn.close()

@app.get("/api/strategy/latest-signals")
def get_latest_signals():
    """获取最近一个交易日的龙抬头候选股"""
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(trade_date) FROM daily_kline")
            last_date = cur.fetchone()[0]
            if not last_date:
                raise HTTPException(status_code=404, detail="无交易数据")
            
            cur.execute("SELECT * FROM get_dragon_head_stocks(%s, 5)", (last_date,))
            candidates = cur.fetchall()
            
            return [
                {
                    "ts_code": row[0],
                    "name": row[1],
                    "current_price": float(row[2]),
                    "pullback_pct": float(row[4]),
                    "expected_profit": float(row[6])
                }
                for row in candidates
            ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取信号失败: {str(e)}")
    finally:
        if conn:
            conn.close()

