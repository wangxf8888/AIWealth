const API_BASE = '/api';

class StrategyAPI {
    static async getStrategyPerformance() {
        try {
            const response = await fetch(`${API_BASE}/strategy/performance`);
            if (!response.ok) throw new Error('获取数据失败');
            return await response.json();
        } catch (error) {
            console.error('获取策略表现失败:', error);
            return null;
        }
    }
    
    static async getPerformanceCurve() {
        try {
            const response = await fetch(`${API_BASE}/strategy/performance-curve`);
            if (!response.ok) throw new Error('获取数据失败');
            return await response.json();
        } catch (error) {
            console.error('获取表现曲线失败:', error);
            return null;
        }
    }
    
    static async getLatestSignals() {
        try {
            const response = await fetch(`${API_BASE}/strategy/latest-signals`);
            if (!response.ok) throw new Error('获取数据失败');
            return await response.json();
        } catch (error) {
            console.error('获取最新信号失败:', error);
            return null;
        }
    }
}

