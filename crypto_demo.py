#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
加密货币数据提供者演示脚本

展示如何使用新添加的 CryptoFetcher 获取 BTC 和 ETH 数据
"""

from data_provider import DataFetcherManager

def demo_crypto_data():
    """演示加密货币数据获取"""
    print("=== 加密货币数据提供者演示 ===\n")

    # 创建数据管理器
    manager = DataFetcherManager()
    print(f"可用数据源: {manager.available_fetchers}\n")

    # 获取 BTC 数据
    print("正在获取 BTC (比特币) 数据...")
    try:
        btc_df, source = manager.get_daily_data('BTC', days=7)
        print(f"✅ BTC 数据获取成功 (来源: {source})")
        print(f"数据行数: {len(btc_df)}")
        print("最新数据:")
        latest = btc_df.iloc[-1]
        print(".2f"        print(".2f"        print(f"成交量: {latest['volume']:,.0f}")
        print(".2f"    except Exception as e:
        print(f"❌ BTC 数据获取失败: {e}")

    print("\n" + "="*50 + "\n")

    # 获取 ETH 数据
    print("正在获取 ETH (以太坊) 数据...")
    try:
        eth_df, source = manager.get_daily_data('ETH', days=7)
        print(f"✅ ETH 数据获取成功 (来源: {source})")
        print(f"数据行数: {len(eth_df)}")
        print("最新数据:")
        latest = eth_df.iloc[-1]
        print(".2f"        print(".2f"        print(f"成交量: {latest['volume']:,.0f}")
        print(".2f"    except Exception as e:
        print(f"❌ ETH 数据获取失败: {e}")

    print("\n" + "="*50 + "\n")
    print("🎉 加密货币数据提供者集成完成！")
    print("现在您可以在分析系统中使用 BTC、ETH 等加密货币代码了。")

if __name__ == "__main__":
    demo_crypto_data()