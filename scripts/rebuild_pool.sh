#!/bin/bash

export PATH=/usr/local/bin:/usr/bin:/bin:$PATH

# 【关键修复】设置 PYTHONPATH
export PYTHONPATH=/home/AIWealth/backend:$PYTHONPATH

PROJECT_DIR=/home/AIWealth/backend/app
LOG_FILE=/home/AIWealth/logs/rebuild_pool_$(date +%Y%m%d).log

mkdir -p /home/AIWealth/logs

echo "========================================" >> $LOG_FILE
echo "🕒 [START] Core Pool Rebuild at $(date)" >> $LOG_FILE
echo "========================================" >> $LOG_FILE
echo "💡 PYTHONPATH is set to: $PYTHONPATH" >> $LOG_FILE

cd $PROJECT_DIR

# 1. 重建当前核心池
echo "🔄 Step 1: Building Current Core Pool..." >> $LOG_FILE
python3 -m services.build_core_pool >> $LOG_FILE 2>&1

# 2. 重建历史核心池
echo "🔄 Step 2: Building Historical Core Pool..." >> $LOG_FILE
python3 -m services.build_core_pool_history >> $LOG_FILE 2>&1

# 3. 生成明日候选
echo "🔄 Step 3: Generating Daily Candidates..." >> $LOG_FILE
python3 -m services.generate_strategy_candidates >> $LOG_FILE 2>&1

echo "🏁 [END] Core Pool Rebuild at $(date)" >> $LOG_FILE
echo "" >> $LOG_FILE
