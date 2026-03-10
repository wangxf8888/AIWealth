# backend/app/services/run_backtest.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.backtest_engine import BacktestEngine
from services.strategies.dragon_head_turnover import DragonHeadTurnoverStrategy

if __name__ == "__main__":
    # 配置回测区间 (根据你的数据库实际有效范围)
    START_DATE = "2025-03-07"
    END_DATE = "2026-03-06"

    # 初始化策略
    strategy = DragonHeadTurnoverStrategy()

    # 运行引擎
    engine = BacktestEngine(
        strategy=strategy,
        start_date=START_DATE,
        end_date=END_DATE,
        initial_capital=100000.0 # 10 万起步
    )

    engine.run()
