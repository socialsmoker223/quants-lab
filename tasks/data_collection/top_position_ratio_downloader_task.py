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

class TopPositionRatioDownloaderTask(BaseTask):
    def __init__(self, name: str, frequency: timedelta, config: Dict[str, Any]):
        super().__init__(name, frequency, config)
        self.connector_name = config["connector_name"]
        self.days_data_retention = config.get("days_data_retention", 7)
        self.api_key = config.get("coinglass_api_key", "")

    async def execute(self):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        logging.info(f"{now} - Starting Top Position Ratio downloader")

        timescale_client = TimescaleClient(
            host=self.config["timescale_config"].get("host", "localhost"),
            port=self.config["timescale_config"].get("port", 5432),
            user=self.config["timescale_config"].get("user", "admin"),
            password=self.config["timescale_config"].get("password", "admin"),
            database=self.config["timescale_config"].get("database", "timescaledb")
        )
        await timescale_client.connect()

        try:
            table_name="coinglass_futures_top_long_short_position_ratio_history_1h"

            trading_pairs = self.config.get("trading_pairs", ["BTC-USDT"])

            for trading_pair in trading_pairs:
                symbol = trading_pair.replace("-", "")  # Convert BTC-USDT to BTCUSDT
                url = "https://open-api-v3.coinglass.com/api/futures/topLongShortPositionRatio/history"
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
                    "limit": 1000,
                    "exchange": self.connector_name.replace("_perpetual", "")
                }
                if last_timestamp:
                    params["startTime"] = int(last_timestamp.timestamp() * 1000)

                response = requests.get(url, headers=headers, params=params)
                if response.status_code != 200:
                    logging.error(f"Error fetching Top Position Ratio data for {symbol}: {response.text}")
                    continue

                data = response.json()
                if not data.get("data"):
                    logging.info(f"No Top Position Ratio data available for {symbol}")
                    continue

                for element in data["data"]:
                    await timescale_client.insert_top_position_ratio(
                        table_name=table_name,
                        symbol=trading_pair,
                        exchange=self.connector_name.replace("_perpetual", ""),
                        provider="coinglass",
                        timestamp=element["timestamp"],
                        timeframe="1h",
                        date_time=datetime.fromtimestamp(element["timestamp"]).strftime("%Y-%m-%d"),
                        created_at=int(datetime.now().timestamp()),
                        long_account=element["longPosition"],
                        short_account=element["shortPosition"],
                        long_short_ratio=element["longShortRatio"]
                    )

                logging.info(f"{now} - Inserted Top Position Ratio data for {trading_pair}")
                await asyncio.sleep(1)

        except Exception as e:
            logging.exception(f"{now} - Error processing Top Position Ratio data: {e}")

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
    
    task = TopPositionRatioDownloaderTask(
        "Top Position Ratio Downloader",
        timedelta(hours=1),
        config
    )
    asyncio.run(task.execute())
