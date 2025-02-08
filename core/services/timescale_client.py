import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union, Any

import asyncpg
import pandas as pd

from core.data_structures.candles import Candles

INTERVAL_MAPPING = {
    '1s': 's',  # seconds
    '1m': 'T',  # minutes
    '3m': '3T',
    '5m': '5T',
    '15m': '15T',
    '30m': '30T',
    '1h': 'H',  # hours
    '2h': '2H',
    '4h': '4H',
    '6h': '6H',
    '12h': '12H',
    '1d': 'D',  # days
    '3d': '3D',
    '1w': 'W'  # weeks
}


class TimescaleClient:
    def __init__(self, host: str = "localhost", port: int = 5432,
                 user: str = "admin", password: str = "admin", database: str = "timescaledb"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )

    @staticmethod
    def get_trades_table_name(connector_name: str, trading_pair: str) -> str:
        return f"{connector_name}_{trading_pair.replace('-', '_')}_trades"

    @staticmethod
    def get_ohlc_table_name(connector_name: str, trading_pair: str, interval: str) -> str:
        return f"{connector_name}_{trading_pair.lower().replace('-', '_')}_{interval}"

    @property
    def metrics_table_name(self):
        return "summary_metrics"

    @property
    def screener_table_name(self):
        return "screener_metrics"

    async def create_candles_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    timestamp TIMESTAMPTZ NOT NULL,
                    open NUMERIC NOT NULL,
                    high NUMERIC NOT NULL,
                    low NUMERIC NOT NULL,
                    close NUMERIC NOT NULL,
                    volume NUMERIC NOT NULL,
                    quote_asset_volume NUMERIC NOT NULL,
                    n_trades INTEGER NOT NULL,
                    taker_buy_base_volume NUMERIC NOT NULL,
                    taker_buy_quote_volume NUMERIC NOT NULL,
                    PRIMARY KEY (timestamp)
                )
            ''')

    async def create_screener_table(self):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {self.screener_table_name} (
                    connector_name TEXT NOT NULL,
                    trading_pair TEXT NOT NULL,
                    price JSONB NOT NULL,
                    volume_24h REAL NOT NULL,
                    price_cbo JSONB NOT NULL,
                    volume_cbo JSONB NOT NULL,
                    one_min JSONB NOT NULL,
                    three_min JSONB NOT NULL,
                    five_min JSONB NOT NULL,
                    fifteen_min JSONB NOT NULL,
                    one_hour JSONB NOT NULL,
                    start_time TIMESTAMPTZ NOT NULL,
                    end_time TIMESTAMPTZ NOT NULL
                );
            ''')

    async def create_metrics_table(self):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {self.metrics_table_name} (
                    connector_name TEXT NOT NULL,
                    trading_pair TEXT NOT NULL,
                    trade_amount REAL,
                    price_avg REAL,
                    price_max REAL,
                    price_min REAL,
                    price_median REAL,
                    from_timestamp TIMESTAMPTZ NOT NULL,
                    to_timestamp TIMESTAMPTZ NOT NULL,
                    volume_usd REAL
                );
            ''')

    async def create_trades_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    trade_id BIGINT NOT NULL,
                    connector_name TEXT NOT NULL,
                    trading_pair TEXT NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    price NUMERIC NOT NULL,
                    volume NUMERIC NOT NULL,
                    sell_taker BOOLEAN NOT NULL,
                    UNIQUE (connector_name, trading_pair, trade_id)
                );
            ''')

    async def drop_trades_table(self):
        async with self.pool.acquire() as conn:
            await conn.execute('DROP TABLE IF EXISTS Trades')

    async def delete_trades(self, connector_name: str, trading_pair: str, timestamp: Optional[float] = None):
        table_name = self.get_trades_table_name(connector_name, trading_pair)
        async with self.pool.acquire() as conn:
            query = f"DELETE FROM {table_name}"
            params = []

            if timestamp is not None:
                query += " WHERE timestamp < $1"
                params.append(datetime.fromtimestamp(timestamp))
            await conn.execute(query, *params)

    async def delete_candles(self, connector_name: str, trading_pair: str, interval: str,
                             timestamp: Optional[float] = None):
        table_name = self.get_ohlc_table_name(connector_name, trading_pair, interval)
        async with self.pool.acquire() as conn:
            query = f"DELETE FROM {table_name}"
            params = []

            if timestamp is not None:
                query += " WHERE timestamp < $1"
                params.append(datetime.fromtimestamp(timestamp))
            await conn.execute(query, *params)

    async def append_trades(self, table_name: str, trades: List[Tuple[int, str, str, float, float, float, bool]]):
        async with self.pool.acquire() as conn:
            await self.create_trades_table(table_name)
            await conn.executemany(f'''
                INSERT INTO {table_name} (trade_id, connector_name, trading_pair, timestamp, price, volume, sell_taker)
                VALUES ($1, $2, $3, to_timestamp($4), $5, $6, $7)
                ON CONFLICT (connector_name, trading_pair, trade_id) DO NOTHING
            ''', trades)

    async def append_candles(self, table_name: str, candles: List[Tuple[float, float, float, float, float]]):
        async with self.pool.acquire() as conn:
            await self.create_candles_table(table_name)
            await conn.executemany(f'''
                INSERT INTO {table_name} (timestamp, open, high, low, close, volume, quote_asset_volume, n_trades,
                taker_buy_base_volume, taker_buy_quote_volume)
                VALUES (to_timestamp($1), $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (timestamp) DO NOTHING
            ''', candles)

    async def append_screener_metrics(self, screener_metrics: Dict[str, Any]):
        async with self.pool.acquire() as conn:
            await self.create_screener_table()
            delete_query = f"""
                        DELETE FROM {self.screener_table_name}
                        WHERE connector_name = '{screener_metrics["connector_name"]}' AND trading_pair = '{screener_metrics["trading_pair"]}';
                        """
            query = f"""
                        INSERT INTO {self.screener_table_name} (
                            connector_name,
                            trading_pair,
                            price,
                            volume_24h,
                            price_cbo,
                            volume_cbo,
                            one_min,
                            three_min,
                            five_min,
                            fifteen_min,
                            one_hour,
                            start_time,
                            end_time
                        )
                        VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
                        );
                    """
            async with self.pool.acquire() as conn:
                await self.create_screener_table()
                await conn.execute(delete_query)
                await conn.execute(
                    query,
                    screener_metrics["connector_name"],
                    screener_metrics["trading_pair"],
                    screener_metrics["price"],
                    screener_metrics["volume_24h"],
                    screener_metrics["price_cbo"],
                    screener_metrics["volume_cbo"],
                    screener_metrics["one_min"],
                    screener_metrics["three_min"],
                    screener_metrics["five_min"],
                    screener_metrics["fifteen_min"],
                    screener_metrics["one_hour"],
                    screener_metrics["start_time"],
                    screener_metrics["end_time"],

                )

    async def get_last_trade_id(self, connector_name: str, trading_pair: str, table_name: str) -> int:
        async with self.pool.acquire() as conn:
            await self.create_trades_table(table_name)
            result = await conn.fetchval(f'''
                SELECT MAX(trade_id) FROM {table_name}
                WHERE connector_name = $1 AND trading_pair = $2
            ''', connector_name, trading_pair)
            return result

    async def get_last_candle_timestamp(self, connector_name: str, trading_pair: str, interval: str) -> Optional[float]:
        table_name = self.get_ohlc_table_name(connector_name, trading_pair, interval)
        async with self.pool.acquire() as conn:
            result = await conn.fetchval(f'''
                SELECT MAX(timestamp) FROM {table_name}
            ''')
            return result.timestamp() if result else None

    # Methods for Coinglass data types
    async def create_liquidation_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    long_liquidation_usd DOUBLE PRECISION,
                    short_liquidation_usd DOUBLE PRECISION,
                    timeframe TEXT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def create_liquidation_aggregated_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    long_liquidation_usd DOUBLE PRECISION,
                    short_liquidation_usd DOUBLE PRECISION,
                    timeframe TEXT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def create_global_account_ratio_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    timeframe TEXT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def create_top_account_ratio_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    timeframe TEXT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    long_account DOUBLE PRECISION,
                    short_account DOUBLE PRECISION,
                    long_short_ratio DOUBLE PRECISION
                )
            ''')

    async def create_top_position_ratio_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    timeframe TEXT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    long_account DOUBLE PRECISION,
                    short_account DOUBLE PRECISION,
                    long_short_ratio DOUBLE PRECISION
                )
            ''')

    async def create_aggregated_taker_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    timeframe TEXT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    long_short_ratio DOUBLE PRECISION
                )
            ''')

    async def create_aggregated_taker_volume_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    timeframe TEXT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    buy DOUBLE PRECISION,
                    sell DOUBLE PRECISION
                )
            ''')

    async def create_aggregated_orderbook_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    timeframe TEXT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    bids_usd DOUBLE PRECISION,
                    bids_amount DOUBLE PRECISION,
                    asks_usd DOUBLE PRECISION,
                    asks_amount DOUBLE PRECISION
                )
            ''')

    async def create_bitfinex_margin_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    timeframe TEXT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    long_qty DOUBLE PRECISION,
                    short_qty DOUBLE PRECISION
                )
            ''')

    async def create_etf_net_assets_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    net_assets DOUBLE PRECISION,
                    
                    price DOUBLE PRECISION,
                    date_time TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def create_etf_flow_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    change_usd DOUBLE PRECISION,
                    close_price DOUBLE PRECISION,
                    price DOUBLE PRECISION,
                    list_ticker TEXT NOT NULL,
                    list_change_usd DOUBLE PRECISION,
                    date_time TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def create_etf_premium_discount_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    nav DOUBLE PRECISION,
                    market_price DOUBLE PRECISION,
                    premium_discount_percent DOUBLE PRECISION,
                    ticker TEXT NOT NULL,
                    date_time TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def create_option_info_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    exchange_name TEXT NOT NULL,
                    open_interest DOUBLE PRECISION,
                    rate DOUBLE PRECISION,
                    h24_change DOUBLE PRECISION,
                    exchange_logo TEXT,
                    open_interest_usd DOUBLE PRECISION,
                    vol_usd DOUBLE PRECISION,
                    h24_vol_change_percent DOUBLE PRECISION,
                    date_time TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    # Insert methods for each table type
    async def insert_liquidation(self, table_name: str, **kwargs):
        await self.create_liquidation_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    symbol, exchange, provider, timestamp, long_liquidation_usd,
                    short_liquidation_usd, timeframe, date_time, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ''', kwargs["symbol"], kwargs["exchange"], kwargs["provider"],
                kwargs["timestamp"], kwargs["long_liquidation_usd"],
                kwargs["short_liquidation_usd"], kwargs["timeframe"],
                kwargs["date_time"], kwargs["created_at"])

    async def insert_liquidation_aggregated(self, table_name: str, **kwargs):
        await self.create_liquidation_aggregated_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    symbol, exchange, provider, timestamp, long_liquidation_usd,
                    short_liquidation_usd, timeframe, date_time, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ''', kwargs["symbol"], kwargs["exchange"], kwargs["provider"],
                kwargs["timestamp"], kwargs["long_liquidation_usd"],
                kwargs["short_liquidation_usd"], kwargs["timeframe"],
                kwargs["date_time"], kwargs["created_at"])

    async def insert_global_account_ratio(self, table_name: str, **kwargs):
        await self.create_global_account_ratio_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    symbol, exchange, provider, timestamp, timeframe,
                    date_time, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ''', kwargs["symbol"], kwargs["exchange"], kwargs["provider"],
                kwargs["timestamp"], kwargs["timeframe"],
                kwargs["date_time"], kwargs["created_at"])

    async def insert_top_account_ratio(self, table_name: str, **kwargs):
        await self.create_top_account_ratio_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    symbol, exchange, provider, timestamp, timeframe,
                    date_time, created_at, long_account, short_account,
                    long_short_ratio
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ''', kwargs["symbol"], kwargs["exchange"], kwargs["provider"],
                kwargs["timestamp"], kwargs["timeframe"], kwargs["date_time"],
                kwargs["created_at"], kwargs["long_account"], kwargs["short_account"],
                kwargs["long_short_ratio"])

    async def insert_top_position_ratio(self, table_name: str, **kwargs):
        await self.create_top_position_ratio_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    symbol, exchange, provider, timestamp, timeframe,
                    date_time, created_at, long_account, short_account,
                    long_short_ratio
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ''', kwargs["symbol"], kwargs["exchange"], kwargs["provider"],
                kwargs["timestamp"], kwargs["timeframe"], kwargs["date_time"],
                kwargs["created_at"], kwargs["long_account"], kwargs["short_account"],
                kwargs["long_short_ratio"])

    async def insert_aggregated_taker(self, table_name: str, **kwargs):
        await self.create_aggregated_taker_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    symbol, exchange, provider, timestamp, timeframe,
                    date_time, created_at, long_short_ratio
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ''', kwargs["symbol"], kwargs["exchange"], kwargs["provider"],
                kwargs["timestamp"], kwargs["timeframe"], kwargs["date_time"],
                kwargs["created_at"], kwargs["long_short_ratio"])

    async def insert_aggregated_taker_volume(self, table_name: str, **kwargs):
        await self.create_aggregated_taker_volume_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    symbol, exchange, provider, timestamp, timeframe,
                    date_time, created_at, buy, sell
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ''', kwargs["symbol"], kwargs["exchange"], kwargs["provider"],
                kwargs["timestamp"], kwargs["timeframe"], kwargs["date_time"],
                kwargs["created_at"], kwargs["buy"], kwargs["sell"])

    async def insert_aggregated_orderbook(self, table_name: str, **kwargs):
        await self.create_aggregated_orderbook_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    symbol, exchange, provider, timestamp, timeframe,
                    date_time, created_at, bids_usd, bids_amount,
                    asks_usd, asks_amount
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ''', kwargs["symbol"], kwargs["exchange"], kwargs["provider"],
                kwargs["timestamp"], kwargs["timeframe"], kwargs["date_time"],
                kwargs["created_at"], kwargs["bids_usd"], kwargs["bids_amount"],
                kwargs["asks_usd"], kwargs["asks_amount"])

    async def insert_bitfinex_margin(self, table_name: str, **kwargs):
        await self.create_bitfinex_margin_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    symbol, exchange, provider, timestamp, timeframe,
                    date_time, created_at, long_qty, short_qty
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ''', kwargs["symbol"], kwargs["exchange"], kwargs["provider"],
                kwargs["timestamp"], kwargs["timeframe"], kwargs["date_time"],
                kwargs["created_at"], kwargs["long_qty"], kwargs["short_qty"])

    async def insert_etf_net_assets(self, table_name: str, **kwargs):
        await self.create_etf_net_assets_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, net_assets, price, date_time,
                    timestamp, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6)
            ''', kwargs["provider"], kwargs["net_assets"],
                kwargs["price"], kwargs["date_time"], kwargs["timestamp"],
                kwargs["created_at"])

    async def insert_etf_flow(self, table_name: str, **kwargs):
        await self.create_etf_flow_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, change_usd, close_price, price, list_ticker,list_change_usd,
                    date_time, timestamp, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8,$9)
            ''', kwargs["provider"], kwargs["change_usd"], kwargs["close_price"],
                kwargs["price"], kwargs["list_ticker"], kwargs["list_change_usd"], kwargs["date_time"],
                kwargs["timestamp"], kwargs["created_at"])

    async def insert_etf_premium_discount(self, table_name: str, **kwargs):
        await self.create_etf_premium_discount_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, nav, market_price, premium_discount_percent,
                    ticker, date_time, timestamp, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ''', kwargs["provider"], kwargs["nav"], kwargs["market_price"],
                kwargs["premium_discount_percent"], kwargs["ticker"],
                kwargs["date_time"], kwargs["timestamp"], kwargs["created_at"])

    async def create_ahr999_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    avg DOUBLE PRECISION,
                    value TEXT NOT NULL,
                    ahr999 DOUBLE PRECISION,
                    date_time TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_ahr999(self, table_name: str, **kwargs):
        await self.create_ahr999_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, avg, value, ahr999, date_time, timestamp, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ''', kwargs["provider"], kwargs["avg"], kwargs["value"],
                kwargs["ahr999"], kwargs["date_time"], kwargs["timestamp"],
                kwargs["created_at"])

    async def create_bitcoin_bubble_index_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    price DOUBLE PRECISION,
                    index DOUBLE PRECISION,
                    google_trend DOUBLE PRECISION,
                    difficulty DOUBLE PRECISION,
                    transactions DOUBLE PRECISION,
                    sent_by_address DOUBLE PRECISION,
                    tweets DOUBLE PRECISION,
                    date_time TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_bitcoin_bubble_index(self, table_name: str, **kwargs):
        await self.create_bitcoin_bubble_index_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, price, index, google_trend, difficulty,
                    transactions, sent_by_address, tweets, date_time,
                    timestamp, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ''', kwargs["provider"], kwargs["price"], kwargs["index"],
                kwargs["google_trend"], kwargs["difficulty"], kwargs["transactions"],
                kwargs["sent_by_address"], kwargs["tweets"], kwargs["date_time"],
                kwargs["timestamp"], kwargs["created_at"])

    async def create_bitcoin_profitable_days_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    price DOUBLE PRECISION,
                    side DOUBLE PRECISION,
                    date_time TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_bitcoin_profitable_days(self, table_name: str, **kwargs):
        await self.create_bitcoin_profitable_days_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, price, side, date_time,
                    timestamp, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6)
            ''', kwargs["provider"], kwargs["price"], kwargs["side"],
                kwargs["date_time"], kwargs["timestamp"],
                kwargs["created_at"])

    async def create_bitcoin_rainbow_chart_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    btc_price DOUBLE PRECISION,
                    model_price DOUBLE PRECISION,
                    fire_sale DOUBLE PRECISION,
                    buy DOUBLE PRECISION,
                    accumulate DOUBLE PRECISION,
                    still_cheap DOUBLE PRECISION,
                    hold DOUBLE PRECISION,
                    is_this_a_bubble DOUBLE PRECISION,
                    fomo_intensifies DOUBLE PRECISION,
                    sell_seriously_sell DOUBLE PRECISION,
                    maximum_bubble_territory DOUBLE PRECISION,                    
                    date_time TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_bitcoin_rainbow_chart(self, table_name: str, **kwargs):
        await self.create_bitcoin_rainbow_chart_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, btc_price, model_price, fire_sale, buy,
                    accumulate, still_cheap, hold, is_this_a_bubble,
                    fomo_intensifies, sell_seriously_sell,
                    maximum_bubble_territory,
                    date_time, timestamp, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ''', kwargs["provider"], kwargs["btc_price"], kwargs["model_price"],
                kwargs["fire_sale"], kwargs["buy"], kwargs["accumulate"],
                kwargs["still_cheap"], kwargs["hold"], kwargs["is_this_a_bubble"],
                kwargs["fomo_intensifies"], kwargs["sell_seriously_sell"],
                kwargs["maximum_bubble_territory"], 
                kwargs["date_time"], kwargs["timestamp"],
                kwargs["created_at"])

    async def create_coinbase_premium_index_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    premium DOUBLE PRECISION,
                    premiumRate DOUBLE PRECISION,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_coinbase_premium_index(self, table_name: str, **kwargs):
        await self.create_coinbase_premium_index_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, timestamp, premium,premiumRate, date_time, created_at
                ) VALUES ($1, $2, $3, $4, $5,$6)
            ''', kwargs["provider"], kwargs["timestamp"], kwargs["premium"],kwargs["premiumRate"],
                kwargs["date_time"], kwargs["created_at"])

    async def create_coins_markets_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    price DOUBLE PRECISION,
                    marketCap DOUBLE PRECISION, 
                    oiMarketCapRatio DOUBLE PRECISION,
                    avgFundingRateByOi DOUBLE PRECISION,
                    avgFundingRateByVol DOUBLE PRECISION, 
                    open_interest DOUBLE PRECISION,
                    openInterestAmount DOUBLE PRECISION,
                    oiVolRatio DOUBLE PRECISION,
                    volUsd DOUBLE PRECISION,
                    volChangePercent5m DOUBLE PRECISION, 
                    volChangePercent15m DOUBLE PRECISION,
                    volChangePercent30m DOUBLE PRECISION,
                    volChangePercent1h DOUBLE PRECISION,
                    volChangePercent4h DOUBLE PRECISION,
                    volChangePercent24h DOUBLE PRECISION,
                    volChange1h DOUBLE PRECISION,
                    volChange4h DOUBLE PRECISION,
                    volChange24h DOUBLE PRECISION,
                    oiVolRatioChangePercent1h DOUBLE PRECISION,
                    oiVolRatioChangePercent4h DOUBLE PRECISION,
                    oiVolRatioChangePercent24h DOUBLE PRECISION,
                    oiChangePercent30m DOUBLE PRECISION,
                    oiChangePercent15m DOUBLE PRECISION,
                    oiChangePercent5m DOUBLE PRECISION,
                    oiChangePercent24h DOUBLE PRECISION,
                    oiChangePercent1h DOUBLE PRECISION,
                    oiChangePercent4h DOUBLE PRECISION,
                    oiChange24h DOUBLE PRECISION,
                    oiChange1h DOUBLE PRECISION,
                    oiChange4h DOUBLE PRECISION,
                    oiChange30m DOUBLE PRECISION,
                    oiChange15m DOUBLE PRECISION,
                    oiChange5m DOUBLE PRECISION,
                    priceChangePercent5m DOUBLE PRECISION,
                    priceChangePercent15m DOUBLE PRECISION,
                    priceChangePercent30m DOUBLE PRECISION,
                    priceChangePercent1h DOUBLE PRECISION,
                    priceChangePercent4h DOUBLE PRECISION,
                    priceChangePercent12h DOUBLE PRECISION,
                    priceChangePercent24h DOUBLE PRECISION,
                    ls5m DOUBLE PRECISION,
                    longVolUsd5m DOUBLE PRECISION,
                    shortVolUsd5m DOUBLE PRECISION,
                    ls15m DOUBLE PRECISION,
                    longVolUsd15m DOUBLE PRECISION,
                    shortVolUsd15m DOUBLE PRECISION,
                    ls30m DOUBLE PRECISION,
                    longVolUsd30m DOUBLE PRECISION,
                    shortVolUsd30m DOUBLE PRECISION,
                    ls1h DOUBLE PRECISION,
                    longVolUsd1h DOUBLE PRECISION,
                    shortVolUsd1h DOUBLE PRECISION,
                    ls4h DOUBLE PRECISION,
                    longVolUsd4h DOUBLE PRECISION,
                    shortVolUsd4h DOUBLE PRECISION,
                    ls12h DOUBLE PRECISION,
                    longVolUsd12h DOUBLE PRECISION,
                    shortVolUsd12h DOUBLE PRECISION,
                    ls24h DOUBLE PRECISION,
                    longVolUsd24h DOUBLE PRECISION,
                    shortVolUsd24h DOUBLE PRECISION,
                    liquidationUsd24h DOUBLE PRECISION,
                    longLiquidationUsd24h DOUBLE PRECISION,
                    shortLiquidationUsd24h DOUBLE PRECISION,
                    liquidationUsd12h DOUBLE PRECISION,
                    longLiquidationUsd12h DOUBLE PRECISION,
                    shortLiquidationUsd12h DOUBLE PRECISION,
                    liquidationUsd4h DOUBLE PRECISION,
                    longLiquidationUsd4h DOUBLE PRECISION,
                    shortLiquidationUsd4h DOUBLE PRECISION,
                    liquidationUsd1h DOUBLE PRECISION,
                    longLiquidationUsd1h DOUBLE PRECISION,
                    shortLiquidationUsd1h DOUBLE PRECISION,
                    created_date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_coins_markets(self, table_name: str, **kwargs):
        await self.create_coins_markets_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    exchange, provider, symbol, price, marketCap, oiMarketCapRatio,
                    avgFundingRateByOi, avgFundingRateByVol, open_interest, openInterestAmount,
                    oiVolRatio, volUsd, volChangePercent5m, volChangePercent15m,
                    volChangePercent30m, volChangePercent1h, volChangePercent4h,
                    volChangePercent24h, volChange1h, volChange4h, volChange24h,
                    oiVolRatioChangePercent1h, oiVolRatioChangePercent4h,
                    oiVolRatioChangePercent24h, oiChangePercent30m, oiChangePercent15m,
                    oiChangePercent5m, oiChangePercent24h, oiChangePercent1h,
                    oiChangePercent4h, oiChange24h, oiChange1h, oiChange4h,
                    oiChange30m, oiChange15m, oiChange5m, priceChangePercent5m,
                    priceChangePercent15m, priceChangePercent30m, priceChangePercent1h,
                    priceChangePercent4h, priceChangePercent12h, priceChangePercent24h,
                    ls5m, longVolUsd5m, shortVolUsd5m, ls15m, longVolUsd15m,
                    shortVolUsd15m, ls30m, longVolUsd30m, shortVolUsd30m,
                    ls1h, longVolUsd1h, shortVolUsd1h, ls4h, longVolUsd4h,
                    shortVolUsd4h, ls12h, longVolUsd12h, shortVolUsd12h,
                    ls24h, longVolUsd24h, shortVolUsd24h, liquidationUsd24h,
                    longLiquidationUsd24h, shortLiquidationUsd24h, liquidationUsd12h,
                    longLiquidationUsd12h, shortLiquidationUsd12h, liquidationUsd4h,
                    longLiquidationUsd4h, shortLiquidationUsd4h, liquidationUsd1h,
                    longLiquidationUsd1h, shortLiquidationUsd1h, created_date_time,
                    created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                          $11, $12, $13, $14, $15, $16, $17, $18,
                          $19, $20, $21, $22, $23, $24, $25, $26,
                          $27, $28, $29, $30, $31, $32, $33, $34,
                          $35, $36, $37, $38, $39, $40, $41, $42,
                          $43, $44, $45, $46, $47, $48, $49, $50,
                          $51, $52, $53, $54, $55, $56, $57, $58,
                          $59, $60, $61, $62, $63, $64, $65, $66,
                          $67, $68, $69, $70, $71, $72, $73, $74,
                          $75, $76, $77, $78)
            ''', kwargs["exchange"], kwargs["provider"], kwargs["symbol"],
                kwargs["price"], kwargs["marketCap"], kwargs["oiMarketCapRatio"],
                kwargs["avgFundingRateByOi"], kwargs["avgFundingRateByVol"],
                kwargs["open_interest"], kwargs["openInterestAmount"], kwargs["oiVolRatio"],
                kwargs["volUsd"], kwargs["volChangePercent5m"], kwargs["volChangePercent15m"],
                kwargs["volChangePercent30m"], kwargs["volChangePercent1h"],
                kwargs["volChangePercent4h"], kwargs["volChangePercent24h"],
                kwargs["volChange1h"], kwargs["volChange4h"], kwargs["volChange24h"],
                kwargs["oiVolRatioChangePercent1h"], kwargs["oiVolRatioChangePercent4h"],
                kwargs["oiVolRatioChangePercent24h"], kwargs["oiChangePercent30m"],
                kwargs["oiChangePercent15m"], kwargs["oiChangePercent5m"],
                kwargs["oiChangePercent24h"], kwargs["oiChangePercent1h"],
                kwargs["oiChangePercent4h"], kwargs["oiChange24h"], kwargs["oiChange1h"],
                kwargs["oiChange4h"], kwargs["oiChange30m"], kwargs["oiChange15m"],
                kwargs["oiChange5m"], kwargs["priceChangePercent5m"],
                kwargs["priceChangePercent15m"], kwargs["priceChangePercent30m"],
                kwargs["priceChangePercent1h"], kwargs["priceChangePercent4h"],
                kwargs["priceChangePercent12h"], kwargs["priceChangePercent24h"],
                kwargs["ls5m"], kwargs["longVolUsd5m"], kwargs["shortVolUsd5m"],
                kwargs["ls15m"], kwargs["longVolUsd15m"], kwargs["shortVolUsd15m"],
                kwargs["ls30m"], kwargs["longVolUsd30m"], kwargs["shortVolUsd30m"],
                kwargs["ls1h"], kwargs["longVolUsd1h"], kwargs["shortVolUsd1h"],
                kwargs["ls4h"], kwargs["longVolUsd4h"], kwargs["shortVolUsd4h"],
                kwargs["ls12h"], kwargs["longVolUsd12h"], kwargs["shortVolUsd12h"],
                kwargs["ls24h"], kwargs["longVolUsd24h"], kwargs["shortVolUsd24h"],
                kwargs["liquidationUsd24h"], kwargs["longLiquidationUsd24h"],
                kwargs["shortLiquidationUsd24h"], 
                kwargs["liquidationUsd12h"], kwargs["longLiquidationUsd12h"],
                kwargs["shortLiquidationUsd12h"], kwargs["liquidationUsd4h"],
                kwargs["longLiquidationUsd4h"], kwargs["shortLiquidationUsd4h"],
                kwargs["liquidationUsd1h"], kwargs["longLiquidationUsd1h"],
                kwargs["shortLiquidationUsd1h"], kwargs["created_date_time"],
                kwargs["created_at"])

    async def create_crypto_fear_greed_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    price DOUBLE PRECISION,
                    value DOUBLE PRECISION,
                    date_time TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_crypto_fear_greed(self, table_name: str, **kwargs):
        await self.create_crypto_fear_greed_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, price, value, date_time,
                    timestamp, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6)
            ''', kwargs["provider"], kwargs["price"], kwargs["value"],
                kwargs["date_time"], kwargs["timestamp"],
                kwargs["created_at"])

    async def create_golden_ratio_multiplier_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    price DOUBLE PRECISION,
                    ma350 TEXT NOT NULL,
                    two_low_bull_high TEXT NOT NULL,
                    three_low_bull_high TEXT NULL,
                    x8 TEXT NULL,
                    accumulation_high TEXT NOT NULL,
                    x21 TEXT NULL,
                    x13 TEXT NULL,
                    x3 TEXT NULL,
                    x5 TEXT NULL,
                    date_time TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_golden_ratio_multiplier(self, table_name: str, **kwargs):
        await self.create_golden_ratio_multiplier_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, price, ma350, two_low_bull_high,three_low_bull_high,
                    x8, accumulation_high, x21, x13, x3, x5,
                    date_time,timestamp, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ''', kwargs["provider"], kwargs["price"], kwargs["ma350"],
                kwargs["two_low_bull_high"],kwargs["three_low_bull_high"],   kwargs["x8"],
                kwargs["accumulation_high"], kwargs["x21"], kwargs["x13"],
                kwargs["x3"], kwargs["x5"], kwargs["date_time"],
                kwargs["timestamp"], kwargs["created_at"])

    async def create_pi_cycle_top_indicator_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    price DOUBLE PRECISION,
                    ma110 DOUBLE PRECISION,
                    ma350_mu2 DOUBLE PRECISION,
                    provider_date_time TEXT NOT NULL,
                    provider_timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_pi_cycle_top_indicator(self, table_name: str, **kwargs):
        await self.create_pi_cycle_top_indicator_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, price, ma110, ma350_mu2,
                    provider_date_time, provider_timestamp, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ''', kwargs["provider"], kwargs["price"], kwargs["ma110"],
                kwargs["ma350_mu2"], kwargs["provider_date_time"],
                kwargs["provider_timestamp"], kwargs["created_at"])

    async def create_puell_multiple_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    buy_qty DOUBLE PRECISION,
                    price DOUBLE PRECISION,
                    puell_multiple DOUBLE PRECISION,
                    sell_qty DOUBLE PRECISION,
                    provider_date_time TEXT NOT NULL,
                    provider_timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_puell_multiple(self, table_name: str, **kwargs):
        await self.create_puell_multiple_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, buy_qty, price, puell_multiple, sell_qty,
                    provider_date_time, provider_timestamp, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ''', kwargs["provider"], kwargs["buy_qty"], kwargs["price"],
                kwargs["puell_multiple"], kwargs["sell_qty"],
                kwargs["provider_date_time"], kwargs["provider_timestamp"],
                kwargs["created_at"])

    async def create_stablecoin_market_cap_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    market_cap DOUBLE PRECISION,
                    btc_price DOUBLE PRECISION,
                    provider_date_time TEXT NOT NULL,
                    provider_timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_stablecoin_market_cap(self, table_name: str, **kwargs):
        await self.create_stablecoin_market_cap_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, market_cap, btc_price, provider_date_time,
                    provider_timestamp, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6)
            ''', kwargs["provider"], kwargs["market_cap"], kwargs["btc_price"],
                kwargs["provider_date_time"], kwargs["provider_timestamp"],
                kwargs["created_at"])

    async def create_stock_to_flow_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    price DOUBLE PRECISION,
                    next_halving DOUBLE PRECISION,
                    provider_date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_stock_to_flow(self, table_name: str, **kwargs):
        await self.create_stock_to_flow_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, price, next_halving, provider_date_time, created_at
                ) VALUES ($1, $2, $3, $4, $5)
            ''', kwargs["provider"], kwargs["price"], kwargs["next_halving"],
                kwargs["provider_date_time"], kwargs["created_at"])

    async def create_two_hundred_week_ma_heatmap_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    buy_qty DOUBLE PRECISION,
                    price DOUBLE PRECISION,
                    ma1440 DOUBLE PRECISION,
                    ma1440ip DOUBLE PRECISION,
                    sell_qty DOUBLE PRECISION,
                    provider_date_time TEXT NOT NULL,
                    provider_timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_two_hundred_week_ma_heatmap(self, table_name: str, **kwargs):
        await self.create_two_hundred_week_ma_heatmap_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, buy_qty, price, ma1440, ma1440ip, sell_qty,
                    provider_date_time, provider_timestamp, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ''', kwargs["provider"], kwargs["buy_qty"], kwargs["price"],
                kwargs["ma1440"], kwargs["ma1440ip"], kwargs["sell_qty"],
                kwargs["provider_date_time"], kwargs["provider_timestamp"],
                kwargs["created_at"])

    async def create_two_year_ma_multiplier_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    buy_qty DOUBLE PRECISION,
                    price DOUBLE PRECISION,
                    ma730_mu5 DOUBLE PRECISION,
                    ma730 DOUBLE PRECISION,
                    sell_qty DOUBLE PRECISION,
                    provider_date_time TEXT NOT NULL,
                    provider_timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def create_liquidation_aggregated_heatmap_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    price DOUBLE PRECISION,
                    long_volume DOUBLE PRECISION,
                    short_volume DOUBLE PRECISION,
                    timestamp BIGINT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_liquidation_aggregated_heatmap(self, table_name: str, **kwargs):
        await self.create_liquidation_aggregated_heatmap_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    symbol, exchange, provider, price, long_volume,
                    short_volume, timestamp, date_time, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ''', kwargs["symbol"], kwargs["exchange"], kwargs["provider"],
                kwargs["price"], kwargs["long_volume"], kwargs["short_volume"],
                kwargs["timestamp"], kwargs["date_time"], kwargs["created_at"])

    async def create_liquidation_aggregated_heatmap_model2_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    price DOUBLE PRECISION,
                    long_volume DOUBLE PRECISION,
                    short_volume DOUBLE PRECISION,
                    timestamp BIGINT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_liquidation_aggregated_heatmap_model2(self, table_name: str, **kwargs):
        await self.create_liquidation_aggregated_heatmap_model2_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    symbol, provider, price, long_volume, short_volume,
                    timestamp, date_time, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ''', kwargs["symbol"], kwargs["provider"], kwargs["price"],
                kwargs["long_volume"], kwargs["short_volume"], kwargs["timestamp"],
                kwargs["date_time"], kwargs["created_at"])

    async def create_liquidation_heatmap_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    price DOUBLE PRECISION,
                    long_volume DOUBLE PRECISION,
                    short_volume DOUBLE PRECISION,
                    timestamp BIGINT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_liquidation_heatmap(self, table_name: str, **kwargs):
        await self.create_liquidation_heatmap_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    symbol, exchange, provider, price, long_volume,
                    short_volume, timestamp, date_time, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ''', kwargs["symbol"], kwargs["exchange"], kwargs["provider"],
                kwargs["price"], kwargs["long_volume"], kwargs["short_volume"],
                kwargs["timestamp"], kwargs["date_time"], kwargs["created_at"])

    async def create_liquidation_heatmap_model2_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    price DOUBLE PRECISION,
                    long_volume DOUBLE PRECISION,
                    short_volume DOUBLE PRECISION,
                    timestamp BIGINT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_liquidation_heatmap_model2(self, table_name: str, **kwargs):
        await self.create_liquidation_heatmap_model2_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    symbol, exchange, provider, price, long_volume,
                    short_volume, timestamp, date_time, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ''', kwargs["symbol"], kwargs["exchange"], kwargs["provider"],
                kwargs["price"], kwargs["long_volume"], kwargs["short_volume"],
                kwargs["timestamp"], kwargs["date_time"], kwargs["created_at"])

    async def create_liquidation_map_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    price DOUBLE PRECISION,
                    long_volume DOUBLE PRECISION,
                    short_volume DOUBLE PRECISION,
                    timestamp BIGINT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_liquidation_map(self, table_name: str, **kwargs):
        await self.create_liquidation_map_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    symbol, exchange, provider, price, long_volume,
                    short_volume, timestamp, date_time, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ''', kwargs["symbol"], kwargs["exchange"], kwargs["provider"],
                kwargs["price"], kwargs["long_volume"], kwargs["short_volume"],
                kwargs["timestamp"], kwargs["date_time"], kwargs["created_at"])

    async def create_liquidation_exchange_list_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    long_volume DOUBLE PRECISION,
                    short_volume DOUBLE PRECISION,
                    timestamp BIGINT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def create_price_ohlc_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    timeframe TEXT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_price_ohlc(self, table_name: str, **kwargs):
        await self.create_price_ohlc_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    symbol, exchange, provider, timestamp, open,
                    high, low, close, timeframe, date_time, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ''', kwargs["symbol"], kwargs["exchange"], kwargs["provider"],
                kwargs["timestamp"], kwargs["open"], kwargs["high"],
                kwargs["low"], kwargs["close"], kwargs["timeframe"],
                kwargs["date_time"], kwargs["created_at"])

    async def create_oi_weight_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    timeframe TEXT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_oi_weight(self, table_name: str, **kwargs):
        await self.create_oi_weight_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    symbol, exchange, provider, timestamp, open,
                    high, low, close, timeframe, date_time, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ''', kwargs["symbol"], kwargs["exchange"], kwargs["provider"],
                kwargs["timestamp"], kwargs["open"], kwargs["high"],
                kwargs["low"], kwargs["close"], kwargs["timeframe"],
                kwargs["date_time"], kwargs["created_at"])

    async def create_vol_weight_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    timeframe TEXT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_vol_weight(self, table_name: str, **kwargs):
        await self.create_vol_weight_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    symbol, exchange, provider, timestamp, open,
                    high, low, close, timeframe, date_time, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ''', kwargs["symbol"], kwargs["exchange"], kwargs["provider"],
                kwargs["timestamp"], kwargs["open"], kwargs["high"],
                kwargs["low"], kwargs["close"], kwargs["timeframe"],
                kwargs["date_time"], kwargs["created_at"])
            
    async def create_exchange_open_interest_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,                   
                    provider TEXT NOT NULL,                
                    priceList DOUBLE PRECISION,
                    Deribit DOUBLE PRECISION,
                    CME DOUBLE PRECISION,
                    OKX DOUBLE PRECISION,
                    Binance DOUBLE PRECISION,
                    Bybit DOUBLE PRECISION,
                    timestamp BIGINT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_exchange_open_interest(self, table_name: str, **kwargs):
        await self.create_exchange_open_interest_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                symbol, provider, priceList, Deribit,CME,OKX,Binance,Bybit, timestamp, date_time, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ''', kwargs["symbol"], 
                kwargs["provider"], kwargs["priceList"], 
                kwargs["Deribit"], kwargs["CME"], kwargs["OKX"]
                , kwargs["Binance"],kwargs["Bybit"],kwargs["timestamp"],
                kwargs["date_time"],kwargs["created_at"])   
                        
    async def create_exchange_volume_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                   
                    provider TEXT NOT NULL,                
                    priceList DOUBLE PRECISION,
                    Deribit DOUBLE PRECISION,
                    CME DOUBLE PRECISION,
                    OKX DOUBLE PRECISION,
                    Binance DOUBLE PRECISION,
                    Bybit DOUBLE PRECISION,
                    timestamp BIGINT NOT NULL,
                    date_time TEXT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_exchange_volume(self, table_name: str, **kwargs):
        await self.create_exchange_volume_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                symbol, provider, priceList, Deribit,CME,OKX,Binance,Bybit, timestamp, date_time, created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ''', kwargs["symbol"], 
                kwargs["provider"], kwargs["priceList"], 
                kwargs["Deribit"], kwargs["CME"], kwargs["OKX"]
                , kwargs["Binance"],kwargs["Bybit"],kwargs["timestamp"],
                kwargs["date_time"],kwargs["created_at"])            

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def get_min_timestamp(self, table_name):
        async with self.pool.acquire() as conn:
            start_time = await conn.fetchval(f'''
                SELECT MIN(timestamp) FROM {table_name}
                ''')
            return start_time.timestamp()

    async def get_max_timestamp(self, table_name):
        async with self.pool.acquire() as conn:
            end_timestamp = await conn.fetchval(f'''
                SELECT MAX(timestamp) FROM {table_name}
                ''')
            return end_timestamp.timestamp()

    async def get_trades(self, connector_name: str, trading_pair: str, start_time: Optional[float],
                         end_time: Optional[float] = None, chunk_size: timedelta = timedelta(hours=6)) -> pd.DataFrame:
        table_name = self.get_trades_table_name(connector_name, trading_pair)
        if end_time is None:
            end_time = datetime.now().timestamp()
        if start_time is None:
            start_time = await self.get_min_timestamp(table_name)

        start_dt = datetime.fromtimestamp(start_time)
        end_dt = datetime.fromtimestamp(end_time)

        async def fetch_chunk(chunk_start: datetime, chunk_end: datetime) -> pd.DataFrame:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(f'''
                    SELECT trade_id, timestamp, price, volume, sell_taker
                    FROM {table_name}
                    WHERE connector_name = $1 AND trading_pair = $2
                    AND timestamp BETWEEN $3 AND $4
                    ORDER BY timestamp
                ''', connector_name, trading_pair, chunk_start, chunk_end)

            df = pd.DataFrame(rows, columns=["trade_id", 'timestamp', 'price', 'volume', 'sell_taker'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit="s")
            df["price"] = df["price"].astype(float)
            df["volume"] = df["volume"].astype(float)
            return df

        chunks = []
        current_start = start_dt
        while current_start < end_dt:
            current_end = min(current_start + chunk_size, end_dt)
            chunks.append(fetch_chunk(current_start, current_end))
            current_start = current_end

        results = await asyncio.gather(*chunks)

        df = pd.concat(results, ignore_index=True)
        df.set_index('timestamp', inplace=True)
        return df

    async def compute_resampled_ohlc(self, connector_name: str, trading_pair: str, interval: str):
        candles = await self.get_candles(connector_name, trading_pair, interval, from_trades=True)
        ohlc_table_name = self.get_ohlc_table_name(connector_name, trading_pair, interval)
        async with self.pool.acquire() as conn:
            # Drop the existing OHLC table if it exists
            await conn.execute(f'DROP TABLE IF EXISTS {ohlc_table_name}')
            # Create a new OHLC table
            await conn.execute(f'''
                CREATE TABLE {ohlc_table_name} (
                    timestamp TIMESTAMPTZ NOT NULL,
                    open NUMERIC NOT NULL,
                    high NUMERIC NOT NULL,
                    low NUMERIC NOT NULL,
                    close NUMERIC NOT NULL,
                    volume NUMERIC NOT NULL,
                    PRIMARY KEY (timestamp)
                )
            ''')
            # Insert the resampled candles into the new table
            await conn.executemany(f'''
                INSERT INTO {ohlc_table_name} (timestamp, open, high, low, close, volume)
                VALUES ($1, $2, $3, $4, $5, $6)
            ''', [
                (
                    datetime.fromtimestamp(row["timestamp"]),
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume']
                )
                for i, row in candles.data.iterrows()
            ])

    async def execute_query(self, query: str):
        async with self.pool.acquire() as connection:
            return await connection.fetch(query)

    def metrics_query_str(self, connector_name, trading_pair):
        table_name = self.get_trades_table_name(connector_name, trading_pair)
        return f'''
            SELECT COUNT(*) AS trade_amount,
                   AVG(price) AS price_avg,
                   MAX(price) AS price_max,
                   MIN(price) AS price_min,
                   PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS price_median,
                   MIN(timestamp) AS from_timestamp,
                   MAX(timestamp) AS to_timestamp,
                   SUM(price * volume) AS volume_usd
            FROM {table_name}
        '''

    async def get_screener_df(self):
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(f"""
            SELECT *
            FROM {self.screener_table_name}""")
        df_cols = [
            "connector_name",
            "trading_pair",
            "price",
            "volume_24h",
            "price_cbo",
            "volume_cbo",
            "one_min",
            "three_min",
            "five_min",
            "fifteen_min",
            "one_hour",
            "start_time",
            "end_time"
        ]
        df = pd.DataFrame(rows, columns=df_cols)
        return df

    async def get_db_status_df(self):
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
            SELECT *
            FROM summary_metrics""")
        df_cols = [
            "connector_name",
            "trading_pair",
            "trade_amount",
            "price_avg",
            "price_max",
            "price_min",
            "price_median",
            "from_timestamp",
            "to_timestamp",
            "volume_usd"
        ]
        df = pd.DataFrame(rows, columns=df_cols)
        return df

    async def append_db_status_metrics(self, connector_name: str, trading_pair: str):
        async with self.pool.acquire() as conn:
            query = self.metrics_query_str(connector_name, trading_pair)
            metrics = await self.execute_query(query)
            metric_data = dict(metrics[0])
            metric_data["connector_name"] = connector_name
            metric_data["trading_pair"] = trading_pair
            delete_query = f"""
                DELETE FROM {self.metrics_table_name}
                WHERE connector_name = '{metric_data["connector_name"]}' AND trading_pair = '{metric_data["trading_pair"]}';
                """
            query = f"""
                INSERT INTO {self.metrics_table_name} (
                    connector_name,
                    trading_pair,
                    trade_amount,
                    price_avg,
                    price_max,
                    price_min,
                    price_median,
                    from_timestamp,
                    to_timestamp,
                    volume_usd
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                );
            """
            async with self.pool.acquire() as conn:
                await self.create_metrics_table()
                await conn.execute(delete_query)
                await conn.execute(
                    query,
                    metric_data['connector_name'],
                    metric_data['trading_pair'],
                    metric_data['trade_amount'],
                    metric_data['price_avg'],
                    metric_data['price_max'],
                    metric_data['price_min'],
                    metric_data['price_median'],
                    metric_data['from_timestamp'],
                    metric_data['to_timestamp'],
                    metric_data['volume_usd']
                )

    async def get_candles(self, connector_name: str, trading_pair: str, interval: str,
                          start_time: Optional[float] = None,
                          end_time: Optional[float] = None, from_trades: bool = False) -> Candles:
        if from_trades:
            trades = await self.get_trades(connector_name=connector_name,
                                           trading_pair=trading_pair,
                                           start_time=start_time,
                                           end_time=end_time)
            if trades.empty:
                candles_df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume', 'timestamp'])
            else:
                pandas_interval = self.convert_interval_to_pandas_freq(interval)
                candles_df = trades.resample(pandas_interval).agg({"price": "ohlc", "volume": "sum"}).ffill()
                candles_df.columns = candles_df.columns.droplevel(0)
                candles_df["timestamp"] = pd.to_numeric(candles_df.index) // 1e9
        else:
            table_name = self.get_ohlc_table_name(connector_name, trading_pair, interval)
            async with self.pool.acquire() as conn:
                query = f'''
                    SELECT timestamp, open, high, low, close, volume
                    FROM {table_name}
                    WHERE timestamp BETWEEN $1 AND $2
                    ORDER BY timestamp
                '''
                start_dt = datetime.fromtimestamp(start_time) if start_time else datetime.min
                end_dt = datetime.fromtimestamp(end_time) if end_time else datetime.max
                rows = await conn.fetch(query, start_dt, end_dt)
            candles_df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            # candles_df.set_index('timestamp', inplace=True)
            candles_df['timestamp'] = candles_df['timestamp'].apply(lambda x: x.timestamp())
            candles_df = candles_df.astype(
                {'open': float, 'high': float, 'low': float, 'close': float, 'volume': float})

        return Candles(candles_df=candles_df, connector_name=connector_name, trading_pair=trading_pair,
                       interval=interval)

    async def get_candles_last_days(self,
                                    connector_name: str,
                                    trading_pair: str,
                                    interval: str,
                                    days: int) -> Candles:
        end_time = int(time.time())
        start_time = end_time - days * 24 * 60 * 60
        return await self.get_candles(connector_name, trading_pair, interval, start_time, end_time)

    async def get_available_pairs(self) -> List[Tuple[str, str]]:
        async with self.pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name LIKE '%_trades'
                ORDER BY table_name
            ''')

        available_pairs = []
        for row in rows:
            table_name = row['table_name']
            parts = table_name.split('_')
            base = parts[-3].upper()
            quote = parts[-2].upper()
            trading_pair = f"{base}-{quote}"
            connector_name = parts[:-3]
            if len(connector_name) > 1:
                connector_name = '_'.join(connector_name)
            available_pairs.append((connector_name, trading_pair))

        return available_pairs

    async def get_available_candles(self) -> List[Tuple[str, str, str]]:
        async with self.pool.acquire() as conn:
            # TODO: fix regex to match intervals
            timeframe_regex = r'_(\d+[smhdw])'
            rows = await conn.fetch('''
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name ~ $1
                ORDER BY table_name
            ''', timeframe_regex)
        available_candles = []
        for row in rows:
            table_name = row['table_name']
            parts = table_name.split('_')
            connector_name = parts[:-3]
            base = parts[-3].upper()
            quote = parts[-2].upper()
            trading_pair = f"{base}-{quote}"
            interval = parts[-1]
            if interval == "trades":
                continue
            if len(connector_name) > 1:
                connector_name = '_'.join(connector_name)
            available_candles.append((connector_name, trading_pair, interval))
        return available_candles

    async def get_all_candles(self, connector_name: str, trading_pair: str, interval: str) -> Candles:
        table_name = self.get_ohlc_table_name(connector_name, trading_pair, interval)
        query = f'''
            SELECT * FROM {table_name}
        '''
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
        return Candles(
            candles_df=pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume", "quote_asset_volume",
                                        "n_trades", "taker_buy_base_volume", "taker_buy_quote_volume"]),
            connector_name=connector_name,
            trading_pair=trading_pair,
            interval=interval)

    async def get_data_range(self, connector_name: str, trading_pair: str) -> Dict[str, Union[datetime, str]]:
        if not connector_name or not trading_pair:
            return {"error": "Both connector_name and trading_pair must be provided"}

        table_name = self.get_trades_table_name(connector_name, trading_pair)

        query = f'''
        SELECT
        MIN(timestamp) as start_time,
        MAX(timestamp) as end_time
        FROM {table_name}
        '''

        async with self.pool.acquire() as conn:
            try:
                row = await conn.fetchrow(query)
            except asyncpg.UndefinedTableError:
                return {"error": f"Table for {connector_name} and {trading_pair} does not exist"}

        if row['start_time'] is None or row['end_time'] is None:
            return {"error": f"No data found for {connector_name} and {trading_pair}"}

        return {
            'start_time': row['start_time'],
            'end_time': row['end_time']
        }

    async def get_all_data_ranges(self) -> Dict[Tuple[str, str], Dict[str, datetime]]:
        available_pairs = await self.get_available_pairs()
        data_ranges = {}
        for connector_name, trading_pair in available_pairs:
            data_ranges[(connector_name, trading_pair)] = await self.get_data_range(connector_name, trading_pair)
        return data_ranges

    @staticmethod
    def convert_interval_to_pandas_freq(interval: str) -> str:
        """
        Converts a candle interval string to a pandas frequency string.
        """
        return INTERVAL_MAPPING.get(interval, 'T')

    async def create_funding_rates_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,                   
                    exchange TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    date_time TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                );
            ''')

    async def insert_funding_rates(self, table_name: str,  **kwargs):
        await self.create_funding_rates_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} 
                (symbol,exchange,provider,open, high, low, close,date_time, timestamp, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ''',kwargs["symbol"],kwargs["exchange"],kwargs["provider"], kwargs["open"],kwargs["high"],kwargs["low"],kwargs["close"],kwargs["date_time"],kwargs["timestamp"],kwargs["created_at"])

    async def delete_funding_rates(self, connector_name: str, trading_pair: str, timestamp: float):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                DELETE FROM funding_rates_{connector_name.lower()}
                WHERE timestamp < $1
            ''', datetime.fromtimestamp(timestamp))

    

    async def get_last_timestamp(self, table_name: str) -> Optional[datetime]:
        query = f"""
            SELECT MAX(timestamp)
            FROM {table_name}
        """
        result = await self.pool.fetchval(query)
        return result
    
    async def create_accumulation_balance_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    value  DOUBLE PRECISION,
                    date_time TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_accumulation_balance(self, table_name: str, **kwargs):
        await self.create_accumulation_balance_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, value,symbol,
                    date_time,timestamp, created_at
                ) VALUES ($1, $2, $3, $4 ,$5,$6)
            ''', kwargs["provider"],  kwargs["value"],kwargs["symbol"],  kwargs["date_time"],
                kwargs["timestamp"], kwargs["created_at"])

    async def create_address_supply_balance_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    value  DOUBLE PRECISION,
                    date_time TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_address_supply_balance(self, table_name: str, **kwargs):
        await self.create_address_supply_balance_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, value,symbol,
                    date_time,timestamp, created_at
                ) VALUES ($1, $2, $3, $4 ,$5,$6)
            ''', kwargs["provider"],  kwargs["value"],kwargs["symbol"],  kwargs["date_time"],
                kwargs["timestamp"], kwargs["created_at"])            
            
    async def create_address_balance_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    volume  DOUBLE PRECISION,
                    date_time TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_address_balance(self, table_name: str, **kwargs):
        await self.create_address_balance_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, volume,symbol,
                    date_time,timestamp, created_at
                ) VALUES ($1, $2, $3, $4 ,$5,$6)
            ''', kwargs["provider"],  kwargs["volume"],kwargs["symbol"],  kwargs["date_time"],
                kwargs["timestamp"], kwargs["created_at"])            

    async def create_market_price_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    value  DOUBLE PRECISION,
                    date_time TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_market_price(self, table_name: str, **kwargs):
        await self.create_market_price_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, value,symbol,
                    date_time,timestamp, created_at
                ) VALUES ($1, $2, $3, $4 ,$5,$6)
            ''', kwargs["provider"],  kwargs["value"],kwargs["symbol"],  kwargs["date_time"],
                kwargs["timestamp"], kwargs["created_at"])                              
            
    async def create_market_cap_table(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    value  DOUBLE PRECISION,
                    date_time TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_market_cap(self, table_name: str, **kwargs):
        await self.create_market_cap_table(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, value,symbol,
                    date_time,timestamp, created_at
                ) VALUES ($1, $2, $3, $4 ,$5,$6)
            ''', kwargs["provider"],  kwargs["value"],kwargs["symbol"],  kwargs["date_time"],
                kwargs["timestamp"], kwargs["created_at"])                                          
    async def create_futures_funding_rate_perpetual_1h(self, table_name: str):
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    provider TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    value  DOUBLE PRECISION,
                    date_time TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                )
            ''')

    async def insert_futures_funding_rate_perpetual_1h(self, table_name: str, **kwargs):
        await self.create_futures_funding_rate_perpetual_1h(table_name)
        async with self.pool.acquire() as conn:
            await conn.execute(f'''
                INSERT INTO {table_name} (
                    provider, value,symbol,
                    date_time,timestamp, created_at
                ) VALUES ($1, $2, $3, $4 ,$5,$6)
            ''', kwargs["provider"],  kwargs["value"],kwargs["symbol"],  kwargs["date_time"],
                kwargs["timestamp"], kwargs["created_at"])            