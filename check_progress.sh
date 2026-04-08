#!/bin/bash
# 查看优化进度

echo "=================================="
echo "📊 策略优化进度监控"
echo "=================================="

echo -e "\n1️⃣  N字反弹策略优化:"
if [ -f /tmp/nword_optimize.log ]; then
    tail -5 /tmp/nword_optimize.log
    echo -e "\n   已完成测试数: $(grep -c "🧪 测试策略" /tmp/nword_optimize.log 2>/dev/null || echo 0)"
else
    echo "   ⏳ 尚未开始"
fi

echo -e "\n2️⃣  破一字策略优化:"
if [ -f /tmp/breakoneword_optimize.log ]; then
    tail -5 /tmp/breakoneword_optimize.log
    echo -e "\n   已完成测试数: $(grep -c "🧪 测试策略" /tmp/breakoneword_optimize.log 2>/dev/null || echo 0)"
else
    echo "   ⏳ 尚未开始"
fi

echo -e "\n3️⃣  后台任务状态:"
ps aux | grep param_backtest | grep -v grep | wc -l | xargs -I {} echo "   运行中任务数: {}"

echo -e "\n=================================="
echo "💡 查看详细结果:"
echo "   cat /tmp/nword_optimize.log | grep -A 5 'Top 10'"
echo "   cat /tmp/breakoneword_optimize.log | grep -A 5 'Top 10'"
echo "=================================="
