import sqlite3
from datetime import datetime
import sys
from pathlib import Path

# 设置路径
SCRIPT_DIR = Path(__file__).resolve().parent
APP_DIR = SCRIPT_DIR.parent
DB_PATH = APP_DIR / "wealth.db"

def get_conn():
    """获取数据库连接"""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"Database not found at {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    return conn

def export_core_pool_to_txt():
    print(f"🔍 Connecting to database at: {DB_PATH}...")

    try:
        conn = get_conn()
        c = conn.cursor()

        # 查询所有核心池数据
        sql = """
            SELECT code, code_name, market_cap, total_limit_ups_1y, total_limit_ups_1m, total_limit_ups_2m, reason, last_verified_date
            FROM core_pool
            WHERE is_active = 1
            ORDER BY total_limit_ups_2m DESC, total_limit_ups_1m DESC, market_cap ASC
        """

        c.execute(sql)
        rows = c.fetchall()
        conn.close()

        if not rows:
            print("⚠️ Core pool is empty!")
            return

        output_file = DB_PATH.parent / "core_pool_list.txt"

        with open(output_file, 'w', encoding='utf-8') as f:
            # 写入标题头
            f.write("=" * 105 + "\n")
            f.write(f"🔥 AI Wealth Core Pool (核心股票池) - Generated at {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
            f.write(f"📊 Total Stocks: {len(rows)} | Criteria: Cap 50-700B, LimitUp Rules\n")
            f.write("=" * 105 + "\n\n")

            # 表头
            header = f"{'No.':<4} | {'Code':<12} | {'Name':<15} | {'Cap(B)':<8} | {'1Y':<4} | {'1M':<4} | {'2M':<4} | {'Reason Summary'}\n"
            f.write(header)
            f.write("-" * 105 + "\n")

            for i, row in enumerate(rows, 1):
                code, name, cap, y1, m1, m2, reason, date = row

                # 截断过长的理由
                reason_display = (reason[:45] + "...") if len(reason) > 45 else reason

                line = f"{i:<4} | {code:<12} | {name:<15} | {cap:<8.2f} | {y1:<4} | {m1:<4} | {m2:<4} | {reason_display}\n"
                f.write(line)

            f.write("-" * 105 + "\n")
            f.write(f"\n💡 Tips:\n")
            f.write(f"   - Cap: Circulating Market Cap (Billion CNY)\n")
            f.write(f"   - 1Y/1M/2M: Limit-Ups in last 1 Year / 1 Month / 2 Months\n")
            f.write(f"   - Reason: Qualification criteria\n")
            f.write(f"\n✅ File saved to: {output_file}\n")

        print(f"🎉 Successfully exported {len(rows)} stocks!")
        print(f"📍 File location: {output_file}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    export_core_pool_to_txt()
