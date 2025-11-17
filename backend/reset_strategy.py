# /home/AIWealth/backend/reset_strategy.py
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_NAME', 'aiwealth'),
    'user': os.getenv('DB_USER', 'aiwealth'),
    'password': os.getenv('DB_PASSWORD', '')
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def reset_strategy_performance():
    """重置策略收益数据"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # 清空历史交易记录
        cur.execute("DELETE FROM strategy_performance")
        print("✅ 已清空 strategy_performance 表")
        
        # 重置摘要统计
        cur.execute("""
            INSERT INTO strategy_summary (id, total_signals, success_signals, success_rate, total_profit_rate, avg_profit_per_signal)
            VALUES (1, 0, 0, 0.0, 0.0, 0.0)
            ON CONFLICT (id) DO UPDATE SET
            total_signals = 0,
            success_signals = 0,
            success_rate = 0.0,
            total_profit_rate = 0.0,
            avg_profit_per_signal = 0.0
        """)
        conn.commit()
        print("✅ 已重置 strategy_summary 表")
        print("✅ 策略收益数据重置完成！")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ 重置失败: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    confirm = input("⚠️  此操作将清空所有策略历史收益数据，确定要继续吗？(y/N): ")
    if confirm.lower() == 'y':
        reset_strategy_performance()
    else:
        print("操作已取消")

