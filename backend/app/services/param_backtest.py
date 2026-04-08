#!/usr/bin/env python3
"""
参数化回测脚本
支持命令行选择策略和参数，自动搜索最优参数组合

用法:
  python3 param_backtest.py --strategy turnover --auto-optimize
  python3 param_backtest.py --strategy nword --params '{"limit_up_min_pct": 9.5, "pullback_max_pct": -6.0}'
  python3 param_backtest.py --strategy breakoneword --auto-optimize
"""

import argparse
import json
import sys
import os
from itertools import product

# 添加项目路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from db import init_db
from strategies.turnover_shrink import TurnoverShrinkStrategy
from strategies.n_word_rebound import NWordReboundStrategy
from strategies.break_one_word import BreakOneWordStrategy
from strategies.chase_limit_up import ChaseLimitUpStrategy
from strategies.dragon_head_turnover import DragonHeadTurnoverStrategy
from backtest_engine import BacktestEngine

def create_strategy(strategy_name, params=None):
    """根据名称和参数创建策略实例"""
    if params is None:
        params = {}

    strategies = {
        'turnover': lambda: TurnoverShrinkStrategy(use_core_pool=True),
        'nword': lambda: NWordReboundStrategy(use_core_pool=True),
        'breakoneword': lambda: BreakOneWordStrategy(use_core_pool=True),
        'chase': lambda: ChaseLimitUpStrategy(entry_board=1, use_core_pool=True),
        'dragon': lambda: DragonHeadTurnoverStrategy(use_core_pool=True),
    }

    if strategy_name not in strategies:
        print(f"❌ 未知策略: {strategy_name}")
        print(f"可用策略: {', '.join(strategies.keys())}")
        sys.exit(1)

    strategy = strategies[strategy_name]()

    # 应用自定义参数
    for key, value in params.items():
        if hasattr(strategy, key):
            setattr(strategy, key, value)
            print(f"   ⚙️ 设置 {key} = {value}")

    return strategy

def run_single_backtest(strategy_name, params):
    """运行单次回测，返回结果"""
    print(f"\n{'='*70}")
    print(f"🧪 测试策略: {strategy_name}")
    print(f"📋 参数: {json.dumps(params, ensure_ascii=False)}")
    print(f"{'='*70}")

    try:
        conn, c = init_db()
        strategy = create_strategy(strategy_name, params)
        engine = BacktestEngine(conn, c, strategy,
                               start_date='2024-03-07',
                               end_date='2026-04-08',
                               initial_capital=1000000)
        engine.run()

        # 提取关键指标
        final_value = engine.daily_snapshots[-1]['total_value'] if engine.daily_snapshots else engine.initial_capital
        total_return = (final_value - engine.initial_capital) / engine.initial_capital
        total_trades = sum(1 for t in engine.trades if t['action']=='SELL')
        win_trades = sum(1 for t in engine.trades if t['action']=='SELL' and t['profit']>0)
        win_rate = win_trades/total_trades if total_trades>0 else 0

        # 计算最大回撤
        max_drawdown = 0.0
        peak = engine.initial_capital
        for snap in engine.daily_snapshots:
            val = snap['total_value']
            if val > peak: peak = val
            dd = (peak - val) / peak
            if dd > max_drawdown: max_drawdown = dd

        conn.close()

        result = {
            'params': params,
            'total_return': total_return,
            'total_trades': total_trades,
            'win_rate': win_rate,
            'max_drawdown': max_drawdown,
            'final_value': final_value
        }

        print(f"\n✅ 结果:")
        print(f"   总收益率: {total_return*100:.2f}%")
        print(f"   交易次数: {total_trades}")
        print(f"   胜率: {win_rate*100:.2f}%")
        print(f"   最大回撤: {max_drawdown*100:.2f}%")
        print(f"   最终资金: {final_value:,.0f}")

        return result

    except Exception as e:
        print(f"\n❌ 回测失败: {e}")
        import traceback
        traceback.print_exc()
        return None

def optimize_nword():
    """优化N字反弹策略参数"""
    print("\n🔍 开始优化 N字反弹策略...")

    # 参数网格
    param_grid = {
        'limit_up_min_pct': [9.5, 9.8, 10.0],
        'pullback_max_pct': [-4.0, -5.0, -6.0, -8.0],
        'buy_min_open': [-5.0, -3.0, -2.0],
        'buy_max_open': [0.0, 1.0, 2.0],
        'take_profit_rate': [3.0, 5.0, 7.0],
        'stop_loss_rate': [-2.0, -3.0, -5.0],
        'max_hold_days': [2, 3, 5]
    }

    # 生成参数组合（限制数量）
    keys = list(param_grid.keys())
    values = list(param_grid.values())
    combinations = list(product(*values))

    # 随机采样50组（避免过多）
    import random
    random.seed(42)
    if len(combinations) > 50:
        combinations = random.sample(combinations, 50)

    print(f"📊 共 {len(combinations)} 组参数待测试\n")

    results = []
    for i, combo in enumerate(combinations, 1):
        params = dict(zip(keys, combo))
        print(f"\n[{i}/{len(combinations)}]", end='')
        result = run_single_backtest('nword', params)
        if result:
            results.append(result)

    # 排序并显示top 10
    if results:
        results.sort(key=lambda x: x['total_return'], reverse=True)
        print(f"\n\n{'='*70}")
        print(f"🏆 Top 10 最优参数组合 (按收益率排序)")
        print(f"{'='*70}")
        for i, r in enumerate(results[:10], 1):
            print(f"\n#{i}: 收益率 {r['total_return']*100:.2f}%")
            print(f"   参数: {json.dumps(r['params'], ensure_ascii=False)}")
            print(f"   交易: {r['total_trades']}次 | 胜率: {r['win_rate']*100:.1f}% | 回撤: {r['max_drawdown']*100:.1f}%")

        # 保存最优参数
        best = results[0]
        print(f"\n💾 最优参数已找到，建议配置:")
        print(json.dumps(best['params'], indent=2, ensure_ascii=False))

def optimize_breakoneword():
    """优化破一字策略参数"""
    print("\n🔍 开始优化 破一字策略...")

    # 测试不同买入时机
    param_grid = {
        'buy_on_day': [0, 1, 2],  # 0=当天, 1=隔天, 2=隔2天
        'buy_min_open': [-3.0, -2.0, -1.0, 0.0],
        'buy_max_open': [0.0, 1.0, 2.0],
        'take_profit_rate': [3.0, 5.0, 8.0],
        'stop_loss_rate': [-2.0, -3.0, -5.0],
        'max_hold_days': [1, 2, 3]
    }

    keys = list(param_grid.keys())
    values = list(param_grid.values())
    combinations = list(product(*values))

    import random
    random.seed(42)
    if len(combinations) > 50:
        combinations = random.sample(combinations, 50)

    print(f"📊 共 {len(combinations)} 组参数待测试\n")

    results = []
    for i, combo in enumerate(combinations, 1):
        params = dict(zip(keys, combo))
        print(f"\n[{i}/{len(combinations)}]", end='')
        result = run_single_backtest('breakoneword', params)
        if result:
            results.append(result)

    if results:
        results.sort(key=lambda x: x['total_return'], reverse=True)
        print(f"\n\n{'='*70}")
        print(f"🏆 Top 10 最优参数组合")
        print(f"{'='*70}")
        for i, r in enumerate(results[:10], 1):
            print(f"\n#{i}: 收益率 {r['total_return']*100:.2f}%")
            print(f"   参数: {json.dumps(r['params'], ensure_ascii=False)}")
            print(f"   交易: {r['total_trades']}次 | 胜率: {r['win_rate']*100:.1f}% | 回撤: {r['max_drawdown']*100:.1f}%")

def main():
    parser = argparse.ArgumentParser(description='参数化回测脚本')
    parser.add_argument('--strategy', type=str, required=True,
                       choices=['turnover', 'nword', 'breakoneword', 'chase', 'dragon'],
                       help='策略名称')
    parser.add_argument('--params', type=str, default=None,
                       help='JSON格式的参数配置')
    parser.add_argument('--auto-optimize', action='store_true',
                       help='自动优化参数')

    args = parser.parse_args()

    if args.auto_optimize:
        # 自动优化模式
        if args.strategy == 'nword':
            optimize_nword()
        elif args.strategy == 'breakoneword':
            optimize_breakoneword()
        else:
            print(f"❌ 策略 {args.strategy} 暂不支持自动优化")
    else:
        # 单次测试模式
        params = json.loads(args.params) if args.params else {}
        run_single_backtest(args.strategy, params)

if __name__ == '__main__':
    main()
