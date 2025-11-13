class PerformanceChart {
    constructor() {
        this.chart = null;
    }
    
    async initialize() {
        const performanceData = await StrategyAPI.getStrategyPerformance();
        const curveData = await StrategyAPI.getPerformanceCurve();
        const signals = await StrategyAPI.getLatestSignals();
        
        this.updateSummary(performanceData);
        this.renderChart(curveData);
        this.displaySignals(signals);
    }
    
    updateSummary(data) {
        if (!data) return;
        
        document.getElementById('totalProfit').textContent = `${data.total_profit_rate}%`;
        document.getElementById('winRate').textContent = `${data.success_rate}%`;
        document.getElementById('tradeCount').textContent = data.total_signals;
        document.getElementById('avgProfit').textContent = `${data.avg_profit_per_signal}%`;
        
        this.setMetricStyle('totalProfit', data.total_profit_rate);
        this.setMetricStyle('winRate', data.success_rate - 50);
        this.setMetricStyle('avgProfit', data.avg_profit_per_signal);
    }
    
    setMetricStyle(elementId, value) {
        const element = document.getElementById(elementId);
        if (value > 0) {
            element.className = 'metric positive';
        } else {
            element.className = 'metric';
        }
    }
    
    renderChart(data) {
        if (!data || !data.dates || !data.profits) return;
        
        const ctx = document.getElementById('profitChart').getContext('2d');
        if (this.chart) {
            this.chart.destroy();
        }
        
        this.chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.dates,
                datasets: [{
                    label: '累计收益率 (%)',
                    data: data.profits,
                    borderColor: '#e74c3c',
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
    
    // 每60秒自动刷新（可选）
    // setInterval(() => chart.initialize(), 60000);
});

