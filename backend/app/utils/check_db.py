import sqlite3
import os
from pathlib import Path

# 【关键修复】自动定位到 app 目录下的 wealth.db
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "wealth.db"

print(f"🔍 正在检查数据库：{DB_PATH}\n")

if not os.path.exists(DB_PATH):
    print(f"❌ 错误：文件不存在！路径：{DB_PATH}")
    print("💡 提示：请确认是否已运行 init_db() 或 sync_data 脚本")
    exit(1)

try:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row  # 让结果可以像字典一样访问
    cursor = conn.cursor()

    # 1. 获取所有表名 (排除系统表)
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;")
    tables = cursor.fetchall()

    if not tables:
        print("⚠️ 警告：数据库文件存在，但里面是空的（没有业务表）！")
    else:
        print(f"✅ 成功连接！发现 {len(tables)} 个业务表:\n")

        for table in tables:
            t_name = table[0]
            print(f"📋 表名：【{t_name}】")

            # 获取总行数
            cursor.execute(f"SELECT COUNT(*) FROM {t_name};")
            count = cursor.fetchone()[0]
            print(f"   📊 总记录数：{count}")

            if count == 0:
                print("   (空表，无数据预览)\n")
                continue

            # 获取前 10 条数据
            cursor.execute(f"SELECT * FROM {t_name} LIMIT 10;")
            rows = cursor.fetchall()

            if rows:
                # 获取列名
                cols = [description[0] for description in cursor.description]

                # 打印表头
                # 格式化表头，限制宽度以防太长
                header = " | ".join([f"{c[:15]:<15}" for c in cols])
                print(f"   前 10 条数据预览:")
                print(f"   {'-' * len(header)}")
                print(f"   {header}")
                print(f"   {'-' * len(header)}")

                # 打印数据行
                for row in rows:
                    # 将每一列的数据转为字符串，截断过长内容，保持对齐
                    row_data = []
                    for val in row:
                        s_val = str(val) if val is not None else "NULL"
                        if len(s_val) > 15:
                            s_val = s_val[:12] + "..."
                        row_data.append(f"{s_val:<15}")
                    print(f"   {' | '.join(row_data)}")

                print(f"   {'-' * len(header)}\n")
            else:
                print("   (查询不到数据)\n")

    conn.close()
    print("✅ 检查完成。")

except Exception as e:
    print(f"❌ 发生严重错误：{e}")
    import traceback
    traceback.print_exc()

