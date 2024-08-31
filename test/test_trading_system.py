# test_trading_system.py

import pytest
from portfolio import TradingSystem  # 假設你的主程式文件名為 trading_system.py

# 假設的歷年數據字典
database_dict = {
    'AAPL': {
        '2024-01-01': {'open': 100, 'close': 100},
        '2024-01-02': {'open': 110, 'close': 110},
        '2024-01-03': {'open': 120, 'close': 120},
    },
    'GOOG': {
        '2024-01-01': {'open': 100, 'close': 100},
        '2024-01-02': {'open': 110, 'close': 110},
        '2024-01-03': {'open': 120, 'close': 120},
    }
}

# 假設的日期列表
date_list = ['2024-01-01', '2024-01-02', '2024-01-03']

@pytest.fixture
def trading_system():
    return TradingSystem(database_dict, date_list)

def test_long_stock_buy(trading_system):
    # 測試買入 AAPL 10 股
    trading_system.long_stock('2024-01-01', 'AAPL', 100, 10, 'buy')
    
    assert 'AAPL' in trading_system.portfolio
    assert trading_system.portfolio['AAPL']['long_quantity'] == 10
    assert trading_system.initial_money < 10000000  # 資金應該減少
    assert len(trading_system.trade_log) == 1
    assert trading_system.trade_log[0]['action'] == 'buy'

def test_long_stock_sell(trading_system):
    # 先買入 AAPL 10 股
    trading_system.long_stock('2024-01-01', 'AAPL', 100, 10, 'buy')
    # 然後賣出 AAPL 10 股
    trading_system.long_stock('2024-01-02', 'AAPL', 110, 10, 'sell')

    assert 'AAPL' not in trading_system.portfolio  # 應該沒有持倉
    assert len(trading_system.trade_log) == 2
    assert trading_system.trade_log[1]['action'] == 'sell'
    assert trading_system.initial_money > 10000000  # 資金應該增加

def test_short_stock(trading_system):
    # 測試做空 GOOG 10 股
    trading_system.short_stock('2024-01-02', 'GOOG', 110, 10, 'sell')
    
    assert 'GOOG' in trading_system.portfolio
    assert trading_system.portfolio['GOOG']['short_quantity'] == 10
    assert len(trading_system.trade_log) == 1
    assert trading_system.trade_log[0]['action'] == 'sell'

def test_cover_short(trading_system):
    # 先做空 GOOG 10 股
    trading_system.short_stock('2024-01-02', 'GOOG', 110, 10, 'sell')
    # 然後回補 GOOG 10 股
    trading_system.short_stock('2024-01-03', 'GOOG', 120, 10, 'buy')

    assert 'GOOG' not in trading_system.portfolio  # 應該沒有持倉
    assert len(trading_system.trade_log) == 2
    assert trading_system.trade_log[1]['action'] == 'buy'
    assert trading_system.initial_money < 10000000  # 應該少於初始資金，因為虧損

def test_show_portfolio(trading_system, capsys):
    # 測試顯示投資組合
    trading_system.long_stock('2024-01-01', 'AAPL', 100, 10, 'buy')
    trading_system.show_portfolio('2024-01-02', 'close')
    captured = capsys.readouterr()
    assert "AAPL 多頭數量: 10" in captured.out