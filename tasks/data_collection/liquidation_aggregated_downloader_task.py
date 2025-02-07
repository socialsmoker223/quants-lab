import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import pandas as pd
import requests
from dotenv import load_dotenv

from core.services.timescale_client import TimescaleClient
from core.task_base import BaseTask

logging.basicConfig(level=logging.INFO)
load_dotenv()

class LiquidationAggregatedDownloaderTask(BaseTask):
    def __init__(self, name: str, frequency: timedelta, config: Dict[str, Any]):
        super().__init__(name, frequency, config)
        self.connector_name = config["connector_name"]
        self.days_data_retention = config.get("days_data_retention", 7)
        self.api_key = config.get("coinglass_api_key", "")

    async def execute(self):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        logging.info(f"{now} - Starting Liquidation Aggregated downloader")

        timescale_client = TimescaleClient(
            host=self.config["timescale_config"].get("host", "localhost"),
            port=self.config["timescale_config"].get("port", 5432),
            user=self.config["timescale_config"].get("user", "admin"),
            password=self.config["timescale_config"].get("password", "admin"),
            database=self.config["timescale_config"].get("database", "timescaledb")
        )
        await timescale_client.connect()

        try:
            table_name="coinglass_futures_liquidation_aggregated_history_1h_1h"

            trading_pairs = self.config.get("trading_pairs", ["BTC-USDT"])

            for trading_pair in trading_pairs:
                symbol = trading_pair.split("-")[0]  # Only need base currency for this endpoint
                url = "https://open-api-v3.coinglass.com/api/futures/liquidation/v2/aggregated-history"
                headers = {
                    "accept": "application/json",
                    "CG-API-KEY": self.api_key
                }

                last_timestamp = await timescale_client.get_last_timestamp(
                    table_name=table_name,
                    trading_pair=trading_pair
                )

                params = {
                    "symbol": symbol,
                    "interval": "1h",
                    "limit": 1000
                }
                if last_timestamp:
                    params["startTime"] = int(last_timestamp.timestamp() * 1000)

                response = requests.get(url, headers=headers, params=params)
                if response.status_code != 200:
                    logging.error(f"Error fetching Liquidation Aggregated data for {symbol}: {response.text}")
                    continue

                data = response.json()
                if not data.get("data"):
                    logging.info(f"No Liquidation Aggregated data available for {symbol}")
                    continue

                for element in data["data"]:
                    await timescale_client.insert_liquidation_aggregated(
                        table_name=table_name,
                        symbol=trading_pair,
                        exchange=self.connector_name.replace("_perpetual", ""),
                        provider="coinglass",
                        timestamp=element["timestamp"],
                        long_liquidation_usd=element["longLiquidationUsd"],
                        short_liquidation_usd=element["shortLiquidationUsd"],
                        timeframe="1h",
                        date_time=datetime.fromtimestamp(element["timestamp"]).strftime("%Y-%m-%d"),
                        created_at=int(datetime.now().timestamp())
                    )

                logging.info(f"{now} - Inserted Liquidation Aggregated data for {symbol}")
                await asyncio.sleep(1)

        except Exception as e:
            logging.exception(f"{now} - Error processing Liquidation Aggregated data: {e}")

        finally:
            await timescale_client.close()

if __name__ == "__main__":
    config = {
        "connector_name": "Bybit",
        "days_data_retention": 30,
        "coinglass_api_key": "579e0b5e25eb46339ca299d5d9ed891e",
        "trading_pairs": ["BTC-USDT", "ETH-USDT"],
        "timescale_config": {
            "host": "localhost",
            "port": 5434,
            "user": "postgres",
            "password": "P@ssw0rd12",
            "database": "timescaledb"
        }
    }
    
    task = LiquidationAggregatedDownloaderTask(
        "Liquidation Aggregated Downloader",
        timedelta(hours=1),
        config
    )
    asyncio.run(task.execute())
