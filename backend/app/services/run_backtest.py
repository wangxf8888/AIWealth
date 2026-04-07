# backend/app/services/run_backtest.py
import sys
from pathlib import Path
from datetime import datetime

# 动态将 backend/app 加入 Python 路径
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from db import get_conn
from services.backtest_engine import BacktestEngine
from services.strategies.dragon_head_turnover import DragonHeadTurnoverStrategy
from services.strategies.chase_limit_up import ChaseLimitUpStrategy
from services.strategies.break_one_word import BreakOneWordStrategy
from services.strategies.turnover_shrink import TurnoverShrinkStrategy
from services.strategies.n_word_rebound import NWordReboundStrategy

def sync_positions_to_dashboard(strategy_key: str, volume: int = 1000):
    """回测结束后自动同步持仓到实盘看板（含 buy_date 修复）"""
    strategy_map = {"shrink": "Turnover_Shrink", "dragon": "DragonHead", "chase": "Chase_", "break": "BreakOneWord"}
    db_name = strategy_map.get(strategy_key, strategy_key)

    conn = get_conn()
    c = conn.cursor()
    try:
        # 1. 查找最新未平仓的买入记录
        c.execute("""
            SELECT code, price, trade_date FROM backtest_trades
            WHERE strategy_name LIKE ? AND action='BUY'
            AND code NOT IN (SELECT code FROM backtest_trades WHERE strategy_name LIKE ? AND action='SELL')
            ORDER BY trade_date DESC LIMIT 1
        """, (f"%{db_name}%", f"%{db_name}%"))

        row = c.fetchone()
        if row:
            code, price, date = row
            # 2. 清理旧记录
            c.execute("DELETE FROM positions WHERE user_id=1 AND code=? AND strategy_name=?", (code, strategy_key))
            # 3. 写入新持仓（明确包含 buy_date）
            c.execute("""
                INSERT INTO positions (user_id, code, hold_volume, cost_price, update_time, strategy_name, buy_date)
                VALUES (1, ?, ?, ?, ?, ?, ?)
            """, (code, volume, price, date, strategy_key, date))
            conn.commit()
            print(f"✅ 自动同步持仓: {code} @ {price} | 买入日期: {date} | 策略: {strategy_key}")
        else:
            c.execute("DELETE FROM positions WHERE user_id=1 AND strategy_name=?", (strategy_key,))
            conn.commit()
            print(f"ℹ️ 无未平仓头寸，已清理 {strategy_key} 的持仓记录")
    except Exception as e:
        print(f"❌ 同步失败: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    START_DATE = "2019-03-07"
    END_DATE = datetime.now().strftime("%Y-%m-%d")
    print(f"📅 回测时间范围：{START_DATE} 至 {END_DATE}")

    # 初始化策略
    # strategy = DragonHeadTurnoverStrategy()

    # 追板策略
    # strategy = ChaseLimitUpStrategy(entry_board=1, use_core_pool=False)

    # 一字板首破
    #strategy = BreakOneWordStrategy(use_core_pool=True)

    # 换手率选股
    strategy = TurnoverShrinkStrategy(use_core_pool=True)

    # N字反包
    #strategy = NWordReboundStrategy(use_core_pool=False)

    engine = BacktestEngine(strategy=strategy,
                            start_date=START_DATE,
                            end_date=END_DATE,
                            initial_capital=1000000.0)

    # 1. 执行回测（写入 backtest_trades / backtest_daily_snapshot）
    engine.run()

    # 2. 回测完成后，自动同步最新持仓到 positions 表
    print("\n🔄 正在同步最新持仓到实盘看板...")
    sync_positions_to_dashboard(strategy_key="shrink")
    print("✅ 同步完成，前端看板已更新。")

