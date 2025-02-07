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

class LiquidationExchangeListDownloaderTask(BaseTask):
    def __init__(self, name: str, frequency: timedelta, config: Dict[str, Any]):
        super().__init__(name, frequency, config)
        self.connector_name = config["connector_name"]
        self.days_data_retention = config.get("days_data_retention", 7)
        self.api_key = config.get("coinglass_api_key", "")

    async def execute(self):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        logging.info(f"{now} - Starting Liquidation Exchange List downloader")

        timescale_client = TimescaleClient(
            host=self.config["timescale_config"].get("host", "localhost"),
            port=self.config["timescale_config"].get("port", 5432),
            user=self.config["timescale_config"].get("user", "admin"),
            password=self.config["timescale_config"].get("password", "admin"),
            database=self.config["timescale_config"].get("database", "timescaledb")
        )
        await timescale_client.connect()

        try:
            table_name="coinglass_futures_liquidation_exchange_list_1h"

            trading_pairs = self.config.get("trading_pairs", ["BTC-USDT"])

            for trading_pair in trading_pairs:
                symbol = trading_pair.split("-")[0]  # Only need base currency for this endpoint
                url = "https://open-api-v3.coinglass.com/api/futures/liquidation/exchange-list"
                headers = {
                    "accept": "application/json",
                    "CG-API-KEY": self.api_key
                }

                params = {
                    "symbol": symbol,
                    "range": "1h"  # Default to 1h interval
                }

                response = requests.get(url, headers=headers, params=params)
                if response.status_code != 200:
                    logging.error(f"Error fetching Liquidation Exchange List data for {symbol}: {response.text}")
                    continue

                data = response.json()
                if not data.get("data"):
                    logging.info(f"No Liquidation Exchange List data available for {symbol}")
                    continue

                for element in data["data"]:
                    await timescale_client.insert_liquidation_exchange_list(
                        table_name=table_name,
                        symbol=trading_pair,
                        exchange=element["exchangeName"],
                        provider="coinglass",
                        long_volume=element["longVolUsd"],
                        short_volume=element["shortVolUsd"],
                        timestamp=int(datetime.now().timestamp()),
                        date_time=datetime.now().strftime("%Y-%m-%d"),
                        created_at=int(datetime.now().timestamp())
                    )

                logging.info(f"{now} - Inserted Liquidation Exchange List data for {symbol}")
                await asyncio.sleep(1)

        except Exception as e:
            logging.exception(f"{now} - Error processing Liquidation Exchange List data: {e}")

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
    
    task = LiquidationExchangeListDownloaderTask(
        "Liquidation Exchange List Downloader",
        timedelta(hours=1),
        config
    )
    asyncio.run(task.execute())
