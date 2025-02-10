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

class StablecoinMarketCapDownloaderTask(BaseTask):
    def __init__(self, name: str, frequency: timedelta, config: Dict[str, Any]):
        super().__init__(name, frequency, config)
        self.connector_name = config["connector_name"]
        self.days_data_retention = config.get("days_data_retention", 7)
        self.api_key = config.get("coinglass_api_key", "")

    async def execute(self):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        logging.info(f"{now} - Starting Stablecoin Market Cap downloader")

        timescale_client = TimescaleClient(
            host=self.config["timescale_config"].get("host", "localhost"),
            port=self.config["timescale_config"].get("port", 5432),
            user=self.config["timescale_config"].get("user", "admin"),
            password=self.config["timescale_config"].get("password", "admin"),
            database=self.config["timescale_config"].get("database", "timescaledb")
        )
        await timescale_client.connect()

        try:
            table_name="coinglass_stablecoin_marketcap_history_1h"

            url = "https://open-api-v3.coinglass.com/api/index/stableCoin-marketCap-history"
            headers = {
                "accept": "application/json",
                "CG-API-KEY": self.api_key
            }

            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                logging.error(f"Error fetching Stablecoin Market Cap data: {response.text}")
                return

            data = response.json()
            if not data.get("data"):
                logging.info("No Stablecoin Market Cap data available")
                return

            for element in data["data"]:
                await timescale_client.insert_stablecoin_market_cap(
                    table_name=table_name,
                    provider="coinglass",
                    market_cap=element["marketCap"],
                    btc_price=element["btcPrice"],
                    provider_date_time=datetime.fromtimestamp(element["date"]).strftime("%Y-%m-%d"),
                    provider_timestamp=element["date"],
                    created_at=int(datetime.now().timestamp())
                )

            logging.info(f"{now} - Inserted Stablecoin Market Cap data")

        except Exception as e:
            logging.exception(f"{now} - Error processing Stablecoin Market Cap data: {e}")

        finally:
            await timescale_client.close()

if __name__ == "__main__":
    config = {
        "connector_name": "Bybit",
        "days_data_retention": 30,
        "coinglass_api_key": "579e0b5e25eb46339ca299d5d9ed891e",
        "timescale_config": {
            "host": "localhost",
            "port": 5434,
            "user": "postgres",
            "password": "P@ssw0rd12",
            "database": "timescaledb"
        }
    }
    
    task = StablecoinMarketCapDownloaderTask(
        "Stablecoin Market Cap Downloader",
        timedelta(hours=1),
        config
    )
    asyncio.run(task.execute())
