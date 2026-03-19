#!/bin/bash

# 设置环境变量
export PATH=/usr/local/bin:/usr/bin:/bin:$PATH

# 【关键修复】设置 PYTHONPATH，让 Python 能找到 app 包及其父级
# 这样 relative import (from ..db) 就能正常工作了
export PYTHONPATH=/home/AIWealth/backend:$PYTHONPATH

PROJECT_DIR=/home/AIWealth/backend/app
LOG_FILE=/home/AIWealth/logs/daily_update_$(date +%Y%m%d).log

# 确保日志目录存在
mkdir -p /home/AIWealth/logs

echo "========================================" >> $LOG_FILE
echo "🕒 [START] Daily Data Update at $(date)" >> $LOG_FILE
echo "========================================" >> $LOG_FILE
echo "💡 PYTHONPATH is set to: $PYTHONPATH" >> $LOG_FILE

cd $PROJECT_DIR

# 1. 更新基础信息和日 K 线
# 注意：这里依然用 -m services.sync_data，但有了 PYTHONPATH 就不会报错了
echo "🔄 Step 1: Syncing Basics and Daily K..." >> $LOG_FILE
python3 -m services.sync_data >> $LOG_FILE 2>&1

if [ $? -eq 0 ]; then
    echo "✅ Step 1 Completed Successfully." >> $LOG_FILE
else
    echo "❌ Step 1 Failed! Check logs above." >> $LOG_FILE
    exit 1
fi

echo "🏁 [END] Daily Data Update at $(date)" >> $LOG_FILE
echo "" >> $LOG_FILE
