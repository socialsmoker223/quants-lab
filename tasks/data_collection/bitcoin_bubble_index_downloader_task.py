import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
import os 
import pandas as pd
import requests
from dotenv import load_dotenv

from core.services.timescale_client import TimescaleClient
from core.task_base import BaseTask

logging.basicConfig(level=logging.INFO)
load_dotenv()

class BitcoinBubbleIndexDownloaderTask(BaseTask):
    def __init__(self, name: str, frequency: timedelta, config: Dict[str, Any]):
        super().__init__(name, frequency, config)
        self.connector_name = config["connector_name"]
        self.days_data_retention = config.get("days_data_retention", 7)
        self.api_key = config.get("coinglass_api_key", "")

    async def execute(self):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        logging.info(f"{now} - Starting Bitcoin Bubble Index downloader")

        timescale_client = TimescaleClient(
            host=self.config["timescale_config"].get("host", "localhost"),
            port=self.config["timescale_config"].get("port", 5432),
            user=self.config["timescale_config"].get("user", "admin"),
            password=self.config["timescale_config"].get("password", "admin"),
            database=self.config["timescale_config"].get("database", "timescaledb")
        )
        await timescale_client.connect()

        try:
            
            table_name="coinglass_bitcoin_bubble_index_1d"

            url = "https://open-api-v3.coinglass.com/api/index/bitcoin-bubble-index"
            headers = {
                "accept": "application/json",
                "CG-API-KEY": self.api_key
            }

            try:
                last_timestamp = await timescale_client.get_last_timestamp(
                        table_name=table_name,                 
                    )
            except:
                last_timestamp=0
                
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                logging.error(f"Error fetching Bitcoin Bubble Index data: {response.text}")
                return

            data = response.json()
            if not data.get("data"):
                logging.info("No Bitcoin Bubble Index data available")
                return

            for element in data["data"]:
                if(element["date"]>last_timestamp):                     
                    await timescale_client.insert_bitcoin_bubble_index(
                        table_name=table_name,
                        provider="coinglass",
                        price=element["price"],
                        index=element["index"],
                        google_trend=element["googleTrend"],
                        difficulty=element["difficulty"],
                        transactions=element["transcations"],  # Note: API response has a typo
                        sent_by_address=element["sentByAddress"],
                        tweets=element["tweets"],
                        date_time=element["date"],
                        timestamp=element["date"],
                        created_at=int(datetime.now().timestamp())
                    )

            logging.info(f"{now} - Inserted Bitcoin Bubble Index data")

        except Exception as e:
            logging.exception(f"{now} - Error processing Bitcoin Bubble Index data: {e}")

        finally:
            await timescale_client.close()

if __name__ == "__main__":
    config = {
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
    }
    
    task = BitcoinBubbleIndexDownloaderTask(
        "Bitcoin Bubble Index Downloader",
        timedelta(hours=1),
        config
    )
    asyncio.run(task.execute())
