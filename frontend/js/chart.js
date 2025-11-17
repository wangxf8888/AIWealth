class PerformanceChart {
    constructor() {
        this.profitChart = null;
        this.holdingsChart = null;
    }
    
    async initialize() {
        const performanceData = await StrategyAPI.getStrategyPerformance();
        const signals = await StrategyAPI.getLatestSignals();
        
        this.updateSummary(performanceData);
        this.displaySignals(signals);
        
        // 初始化持仓饼图（模拟数据，实际应从API获取）
        this.renderHoldingsChart();
        
        // 默认加载全部历史曲线
        await this.loadYear(null);
    }
    
    async loadYear(year) {
        const curveData = await StrategyAPI.getPerformanceCurve(year);
        this.renderProfitChart(curveData);
    }
    
    updateSummary(data) {
        if (!data) return;
        
        const totalProfit = data.total_profit_rate || 0;
        const winRate = data.success_rate || 0;
        const avgProfit = data.avg_profit_per_signal || 0;
        
        document.getElementById('totalProfit').textContent = `${totalProfit}%`;
        document.getElementById('winRate').textContent = `${winRate}%`;
        document.getElementById('tradeCount').textContent = data.total_signals;
        document.getElementById('avgProfit').textContent = `${avgProfit}%`;
        
        // 更新颜色：盈利红色，亏损绿色
        this.setMetricStyle('totalProfit', totalProfit);
        this.setMetricStyle('avgProfit', avgProfit);
        
        // 更新账户概览
        this.updateAccountSummary(totalProfit);
    }
    
    updateAccountSummary(totalProfitRate) {
        // 模拟当前持仓数据
        const totalCost = 100000;
        const currentValue = totalCost * (1 + totalProfitRate / 100);
        const totalPnL = totalProfitRate;
        
        document.getElementById('totalCost').textContent = `¥${totalCost.toLocaleString()}`;
        document.getElementById('currentValue').textContent = `¥${Math.round(currentValue).toLocaleString()}`;
        document.getElementById('totalPnL').textContent = `${totalPnL.toFixed(2)}%`;
        
        // 中国股市习惯：盈利红色，亏损绿色
        const pnlElement = document.getElementById('totalPnL');
        if (totalPnL >= 0) {
            pnlElement.className = 'pnl profit';
        } else {
            pnlElement.className = 'pnl loss';
        }
    }
    
    setMetricStyle(elementId, value) {
        const element = document.getElementById(elementId);
        if (value >= 0) {
            element.className = 'metric positive'; // 红色
        } else {
            element.className = 'metric negative'; // 绿色
        }
    }
    
    renderProfitChart(data) {
        const ctx = document.getElementById('profitChart').getContext('2d');
        if (this.profitChart) {
            this.profitChart.destroy();
        }
        
        if (!data || !data.dates || !data.profits || data.dates.length === 0) {
            this.profitChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: ['无数据'],
                    datasets: [{
                        label: '累计收益率 (%)',
                        data: [0],
                        borderColor: '#e74c3c', // 红色
                        backgroundColor: 'rgba(231, 76, 60, 0.1)',
                        borderWidth: 3,
                        fill: true,
                        tension: 0.4,
                        pointRadius: 0
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: { enabled: false }
                    },
                    scales: {
                        x: { display: false },
                        y: { display: false }
                    }
                }
            });
            return;
        }
        
        this.profitChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.dates,
                datasets: [{
                    label: '累计收益率 (%)',
                    data: data.profits,
                    borderColor: '#e74c3c', // 红色
                    backgroundColor: 'rgba(231, 76, 60, 0.1)',
                    borderWidth: 3,
                    fill: true,
                    tension: 0.4,
                    pointRadius: 2,
                    pointHoverRadius: 6
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: true,
                        position: 'top'
                    },
                    tooltip: {
                        mode: 'index',
                        intersect: false
                    }
                },
                scales: {
                    x: {
                        grid: {
                            display: false
                        },
                        ticks: {
                            maxTicksLimit: 10
                        }
                    },
                    y: {
                        beginAtZero: false,
                        grid: {
                            color: 'rgba(0, 0, 0, 0.1)'
                        },
                        ticks: {
                            callback: function(value) {
                                return value + '%';
                            }
                        }
                    }
                }
            }
        });
    }
    
    renderHoldingsChart() {
        const ctx = document.getElementById('holdingsChart').getContext('2d');
        if (this.holdingsChart) {
            this.holdingsChart.destroy();
        }
        
        // 模拟当前持仓数据
        const holdingsLabels = ['贵州茅台', '宁德时代', '比亚迪', '现金'];
        const holdingsData = [30, 25, 20, 25];
        const backgroundColors = ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0'];
        
        // 计算总和用于百分比
        const total = holdingsData.reduce((sum, value) => sum + value, 0);
        
        this.holdingsChart = new Chart(ctx, {
            type: 'pie',
            data: {
                labels: holdingsLabels,
                datasets: [{
                    data: holdingsData,
                    backgroundColor: backgroundColors,
                    borderWidth: 2,
                    borderColor: '#fff'
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'right',
                        align: 'center',
                        labels: {
                            boxWidth: 12,
                            padding: 15,
                            usePointStyle: true,
                            font: {
                                size: 12
                            },
                            // 自定义图例标签：显示 "股票名 (百分比%)"
                            generateLabels: function(chart) {
                                const data = chart.data;
                                if (data.labels.length && data.datasets.length) {
                                    const dataset = data.datasets[0];
                                    const total = dataset.data.reduce((sum, value) => sum + value, 0);
                                    
                                    return data.labels.map((label, i) => {
                                        const value = dataset.data[i];
                                        const percentage = total > 0 ? ((value / total) * 100).toFixed(1) : 0;
                                        return {
                                            text: `${label} (${percentage}%)`,
                                            fillStyle: dataset.backgroundColor[i],
                                            strokeStyle: '#fff',
                                            lineWidth: 2,
                                            hidden: !chart.isDatasetVisible(0) || (typeof dataset.data[i] === 'number' && isNaN(dataset.data[i])),
                                            index: i
                                        };
                                    });
                                }
                                return [];
                            }
                        }
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const total = context.dataset.data.reduce((sum, val) => sum + val, 0);
                                const percentage = total > 0 ? ((context.parsed / total) * 100).toFixed(1) : 0;
                                return `${context.label}: ${percentage}%`;
                            }
                        }
                    }
                },
                layout: {
                    padding: {
                        right: 20
                    }
                }
            }
        });
    }
    
    displaySignals(signals) {
        const container = document.getElementById('recommendationsList');
        if (!signals || signals.length === 0) {
            container.innerHTML = '<p>暂无推荐股票</p>';
            return;
        }
        
        let html = '';
        signals.forEach(stock => {
            html += `
                <div class="stock-item">
                    <div class="stock-name">${stock.name} (${stock.ts_code})</div>
                    <div class="stock-details">
                        <div>预期收益: <span class="expected-profit">${stock.expected_profit}%</span></div>
                        <div>回调幅度: ${stock.pullback_pct}%</div>
                    </div>
                </div>
            `;
        });
        container.innerHTML = html;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    const chart = new PerformanceChart();
    chart.initialize();
    window.performanceChartInstance = chart;
});

