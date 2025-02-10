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

class GoldenRatioMultiplierDownloaderTask(BaseTask):
    def __init__(self, name: str, frequency: timedelta, config: Dict[str, Any]):
        super().__init__(name, frequency, config)
        self.connector_name = config["connector_name"]
        self.days_data_retention = config.get("days_data_retention", 7)
        self.api_key = config.get("coinglass_api_key", "")

    async def execute(self):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        logging.info(f"{now} - Starting Golden Ratio Multiplier downloader")

        timescale_client = TimescaleClient(
            host=self.config["timescale_config"].get("host", "localhost"),
            port=self.config["timescale_config"].get("port", 5432),
            user=self.config["timescale_config"].get("user", "admin"),
            password=self.config["timescale_config"].get("password", "admin"),
            database=self.config["timescale_config"].get("database", "timescaledb")
        )
        await timescale_client.connect()

        try:
            table_name="coinglass_golden_ratio_multiplier_1d"

            url = "https://open-api-v3.coinglass.com/api/index/golden-ratio-multiplier"
            headers = {
                "accept": "application/json",
                "CG-API-KEY": self.api_key
            }

            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                logging.error(f"Error fetching Golden Ratio Multiplier data: {response.text}")
                return

            data = response.json()
            if not data.get("data"):
                logging.info("No Golden Ratio Multiplier data available")
                return
            try:
                last_timestamp = await timescale_client.get_last_timestamp(
                    table_name=table_name,                    
                )
            except:
                last_timestamp=0
            for element in data["data"]:
                if(element["createTime"]>last_timestamp):
                    await timescale_client.insert_golden_ratio_multiplier(
                        table_name=table_name,
                        provider="coinglass",
                        price=element["price"],
                        ma350=element["ma350"],
                        two_low_bull_high=element["2LowBullHigh"],
                        three_low_bull_high=element.get("3LowBullHigh", None),
                        x8=element.get("x8", None),
                        accumulation_high=element["1.6AccumulationHigh"],
                        x21=element.get("x21", None),
                        x13=element.get("x13", None),
                        x3=element.get("x3", None),
                        x5=element.get("x5", None),
                        date_time=datetime.fromtimestamp(element["createTime"]/1000).strftime("%Y-%m-%d"),
                        timestamp=element["createTime"],
                        created_at=int(datetime.now().timestamp())
                    )

            logging.info(f"{now} - Inserted Golden Ratio Multiplier data")

        except Exception as e:
            logging.exception(f"{now} - Error processing Golden Ratio Multiplier data: {e}")

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
    
    task = GoldenRatioMultiplierDownloaderTask(
        "Golden Ratio Multiplier Downloader",
        timedelta(hours=1),
        config
    )
    asyncio.run(task.execute())
