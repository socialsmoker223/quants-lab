import asyncio
import logging
import os
import sys
from datetime import timedelta

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    from core.task_base import TaskOrchestrator

    from tasks.data_collection.coin_glass_data_downloader_task import CoinGlassDataDownloaderTask

    from tasks.data_collection.aggregated_orderbook_downloader_task import AggregatedOrderbookDownloaderTask, config as aggregated_orderbook_config
    from tasks.data_collection.aggregated_taker_downloader_task import AggregatedTakerDownloaderTask, config as aggregated_taker_config
    from tasks.data_collection.aggregated_taker_volume_downloader_task import AggregatedTakerVolumeDownloaderTask, config as aggregated_taker_volume_config
    from tasks.data_collection.ahr999_downloader_task import AHR999DownloaderTask, config as ahr999_config
    from tasks.data_collection.bitcoin_bubble_index_downloader_task import BitcoinBubbleIndexDownloaderTask, config as bitcoin_bubble_config
    from tasks.data_collection.bitcoin_profitable_days_downloader_task import BitcoinProfitableDaysDownloaderTask, config as bitcoin_profitable_days_config
    from tasks.data_collection.bitcoin_rainbow_chart_downloader_task import BitcoinRainbowChartDownloaderTask, config as bitcoin_rainbow_config
    from tasks.data_collection.bitfinex_margin_downloader_task import BitfinexMarginDownloaderTask, config as bitfinex_margin_config
    from tasks.data_collection.candles_downloader_task import CandlesDownloaderTask, config as candles_config
    from tasks.data_collection.coinbase_premium_index_downloader_task import CoinbasePremiumIndexDownloaderTask, config as coinbase_premium_config
    from tasks.data_collection.coins_markets_downloader_task import CoinsMarketsDownloaderTask, config as coins_markets_config
    from tasks.data_collection.crypto_fear_greed_downloader_task import CryptoFearGreedDownloaderTask, config as crypto_fear_greed_config
    from tasks.data_collection.etf_flow_downloader_task import ETFFlowDownloaderTask, config as etf_flow_config
    from tasks.data_collection.etf_net_assets_downloader_task import ETFNetAssetsDownloaderTask, config as etf_net_assets_config
    from tasks.data_collection.etf_premium_discount_downloader_task import ETFPremiumDiscountDownloaderTask, config as etf_premium_discount_config
    from tasks.data_collection.exchange_open_interest_downloader_task import ExchangeOpenInterestDownloaderTask, config as exchange_open_interest_config
    from tasks.data_collection.exchange_volume_downloader_task import ExchangeVolumeDownloaderTask, config as exchange_volume_config
    from tasks.data_collection.funding_rate_downloader_task import FundingRateDownloaderTask, config as funding_rate_config
    from tasks.data_collection.glassnode.accumulation_balance_task import AccumulationBalance, config as accumulation_balance_config
    from tasks.data_collection.glassnode.address_balance import AddressBalance, config as address_balance_config
    from tasks.data_collection.glassnode.market_cap import MarketCap, config as market_cap_config
    from tasks.data_collection.glassnode.market_price import MarketPrice
    from tasks.data_collection.glassnode.supply_held_by_address_balance import SupplyHeldByAddressBalance
    from tasks.data_collection.glassnode.futures_funding_rate_perpetual import FuturesFundingRatePerpetual


    orchestrator = TaskOrchestrator()

    timescale_config = {
        "host": os.getenv("TIMESCALE_HOST", "localhost"),
        "port": os.getenv("TIMESCALE_PORT", 5432),
        "user": os.getenv("TIMESCALE_USER", "admin"),
        "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
        "database": os.getenv("TIMESCALE_DB", "timescaledb"),
    }

    trades_downloader_task = CoinGlassDataDownloaderTask(
        name="CoinGlass liquidation aggregated history",
        config={
            "timescale_config": timescale_config,
            "end_point": "global_long_short_account_ratio",            
            "days_data_retention": 7,
            "api_key": os.getenv("CG_API_KEY"),
            "trading_pairs": ["SUI-USDT","BTC-USDT", "ETH-USDT", "1000PEPE-USDT", "SOL-USDT"],
            "interval": ["1h"],
            "limit": 1000,
        },
        frequency=timedelta(minutes=30),
    )

    orchestrator.add_task(trades_downloader_task)
    tasks = [
        AggregatedOrderbookDownloaderTask("Aggregated Orderbook Downloader", timedelta(hours=1), {
            "connector_name": "Bybit_perpetual",
            "days_data_retention": 30,
            "coinglass_api_key": os.getenv("CG_API_KEY"),
            "trading_pairs": ["BTC", "ETH","1000PEPE","SOL"],
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        AggregatedTakerDownloaderTask("Aggregated Taker Downloader", timedelta(hours=1), {
            "connector_name": "Bybit_perpetual",
            "days_data_retention": 30,        
            "trading_pairs": ["BTCUSDT", "ETHUSDT","1000PEPEUSDT","SOLUSDT"],
            "coinglass_api_key": os.getenv("CG_API_KEY"),
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        AggregatedTakerVolumeDownloaderTask("Aggregated Taker Volume Downloader", timedelta(hours=1), {
            "connector_name": "Bybit_perpetual",
            "days_data_retention": 30,        
            "trading_pairs": ["BTC", "ETH","1000PEPE","SOL"],
            "coinglass_api_key": os.getenv("CG_API_KEY"),
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        AHR999DownloaderTask("AHR999 Downloader", timedelta(hours=1), {
            "connector_name": "Bybit_perpetual",
            "days_data_retention": 30,
            "coinglass_api_key": os.getenv("CG_API_KEY"),
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        BitcoinBubbleIndexDownloaderTask("Bitcoin Bubble Index Downloader", timedelta(hours=1), {
            "connector_name": "Bybit_perpetual",
            "days_data_retention": 30,
            "coinglass_api_key": os.getenv("CG_API_KEY"),
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        BitcoinProfitableDaysDownloaderTask("Bitcoin Profitable Days Downloader", timedelta(hours=1), {
            "connector_name": "Bybit_perpetual",
            "days_data_retention": 30,
            "coinglass_api_key": os.getenv("CG_API_KEY"),
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        BitcoinRainbowChartDownloaderTask("Bitcoin Rainbow Chart Downloader", timedelta(hours=1), {
            "connector_name": "Bybit_perpetual",
            "days_data_retention": 30,
            "coinglass_api_key": os.getenv("CG_API_KEY"),
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        BitfinexMarginDownloaderTask("Bitfinex Margin Downloader", timedelta(hours=1), {
            "connector_name": "Bybit",
            "days_data_retention": 30,        
            "trading_pairs": ["BTC", "ETH","SOL"],
            "coinglass_api_key": os.getenv("CG_API_KEY"),
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        CoinbasePremiumIndexDownloaderTask("Coinbase Premium Index Downloader", timedelta(hours=1), {
            "connector_name": "Bybit",
            "days_data_retention": 30,
            "coinglass_api_key": os.getenv("CG_API_KEY"),
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        CoinsMarketsDownloaderTask("Coins Markets Downloader", timedelta(hours=1), {
            "connector_name": "Bybit_perpetual",
            "days_data_retention": 30,
            "coinglass_api_key": os.getenv("CG_API_KEY"),
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        CryptoFearGreedDownloaderTask("Crypto Fear & Greed Index Downloader", timedelta(hours=1), {
            "connector_name": "Bybit",
            "days_data_retention": 30,
            "coinglass_api_key": os.getenv("CG_API_KEY"),
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        ETFFlowDownloaderTask("ETF Flow Downloader", timedelta(hours=1), {
            "connector_name": "Bybit",
            "days_data_retention": 30,
            "coinglass_api_key": os.getenv("CG_API_KEY"),
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        ETFNetAssetsDownloaderTask("ETF Net Assets Downloader", timedelta(hours=1), {
            "connector_name": "Bybit_perpetual",
            "days_data_retention": 30,
            "coinglass_api_key": os.getenv("CG_API_KEY"),
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        ETFPremiumDiscountDownloaderTask("ETF Premium Discount Downloader", timedelta(hours=1), {
            "connector_name": "Bybit",
            "days_data_retention": 30,
            "coinglass_api_key": os.getenv("CG_API_KEY"),
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        ExchangeOpenInterestDownloaderTask("Exchange Open Interest Downloader", timedelta(hours=1), {
           "connector_name": "Bybit_perpetual",
            "days_data_retention": 30,
            "coinglass_api_key": os.getenv("CG_API_KEY"),
            "trading_pairs": ["BTC","ETH","SOL","1000PEPE"],
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        ExchangeVolumeDownloaderTask("Exchange Volume Downloader", timedelta(hours=1), {
            "connector_name": "Bybit",
            "days_data_retention": 30,
            "coinglass_api_key": os.getenv("CG_API_KEY"),
            "trading_pairs": ["BTC","ETH","SOL","1000PEPE"],
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        FundingRateDownloaderTask("Funding Rate Downloader", timedelta(hours=1), {
            "connector_name": "Bybit_perpetual",
            "quote_asset": "USDT",
            "days_data_retention": 30,
            "coinglass_api_key": os.getenv("CG_API_KEY"),
            "trading_pairs": ["BTCUSDT","ETHUSDT","SOLUSDT","1000PEPEUSDT"],
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        AccumulationBalance("Accumulation Balance", timedelta(hours=1), {
            "connector_name": "Bybit_perpetual",
            "days_data_retention": 30,
            "coinglass_api_key": os.getenv("GN_API_KEY"),
            "trading_pairs": ["BTC"],
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        AddressBalance("Address Balance", timedelta(hours=1), {
            "connector_name": "Bybit_perpetual",
            "days_data_retention": 30,                
            "glassnode_api_key": os.getenv("GN_API_KEY"),
            "amount_balance":["1_usd_count","10_usd_count","100_usd_count","100k_usd_count","10k_usd_count","1k_usd_count","1m_usd_count"],
            "trading_pairs": ["BTC","ETH"],
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        MarketCap("Market Cap", timedelta(hours=1), {
            "days_data_retention": 30,
            "glassnode_api_key": os.getenv("GN_API_KEY"),
            "trading_pairs": ["BTC","ETH","SOL","PEPE"],
            "eth_network":["eth","arb","aggregated"],
            "timescale_config": {
                    "host": os.getenv("TIMESCALE_HOST", "localhost"),
                    "port": os.getenv("TIMESCALE_PORT", 5432),
                    "user": "postgres",
                    "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                    "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        MarketPrice("Market Price",timedelta(hours=1), {
            "days_data_retention": 30,
            "glassnode_api_key": os.getenv("GN_API_KEY"),
            "trading_pairs": ["BTC","ETH","SOL","PEPE","KAS"],
            "timescale_config": {
                    "host": os.getenv("TIMESCALE_HOST", "localhost"),
                    "port": os.getenv("TIMESCALE_PORT", 5432),
                    "user": "postgres",
                    "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                    "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        SupplyHeldByAddressBalance("SupplyHeldByAddressBalance",timedelta(hours=1), {
            "days_data_retention": 30,
            "glassnode_api_key": os.getenv("GN_API_KEY"),
            "amount_balance":["less_0001","more_100k","0001_001","001_01","01_1","1_10","10_100","100_1k","10k_100k","1k_10k"],
            "trading_pairs": ["BTC","ETH","SOL"],
            "eth_network":["eth","arb","aggregated"],
            "timescale_config": {
                "host": os.getenv("TIMESCALE_HOST", "localhost"),
                "port": os.getenv("TIMESCALE_PORT", 5432),
                "user": "postgres",
                "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                "database": os.getenv("TIMESCALE_DB", "timescaledb"),
            }
        }),
        FuturesFundingRatePerpetual("FuturesFundingRatePerpetual",timedelta(hours=1),{
            "days_data_retention": 30,
            "coinglass_api_key": os.getenv("GN_API_KEY"),
            "trading_pairs": ["BTC","ETH","PEPE","SOL"],
            "timescale_config": {
                    "host": os.getenv("TIMESCALE_HOST", "localhost"),
                    "port": os.getenv("TIMESCALE_PORT", 5432),
                    "user": "postgres",
                    "password": os.getenv("TIMESCALE_PASSWORD", "admin"),
                    "database": os.getenv("TIMESCALE_DB", "timescaledb"),
                }
        })
    ]
    for task in tasks:
        orchestrator.add_task(task)
    await orchestrator.run()


if __name__ == "__main__":
    asyncio.run(main())
