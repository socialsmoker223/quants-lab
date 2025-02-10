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

class BitcoinRainbowChartDownloaderTask(BaseTask):
    def __init__(self, name: str, frequency: timedelta, config: Dict[str, Any]):
        super().__init__(name, frequency, config)
        self.connector_name = config["connector_name"]
        self.days_data_retention = config.get("days_data_retention", 7)
        self.api_key = config.get("coinglass_api_key", "")

    async def execute(self):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        logging.info(f"{now} - Starting Bitcoin Rainbow Chart downloader")

        timescale_client = TimescaleClient(
            host=self.config["timescale_config"].get("host", "localhost"),
            port=self.config["timescale_config"].get("port", 5432),
            user=self.config["timescale_config"].get("user", "admin"),
            password=self.config["timescale_config"].get("password", "admin"),
            database=self.config["timescale_config"].get("database", "timescaledb")
        )
        await timescale_client.connect()

        try:
            table_name="coinglass_bitcoin_rainbow_chart_1d"

            url = "https://open-api-v3.coinglass.com/api/index/bitcoin-rainbow-chart"
            headers = {
                "accept": "application/json",
                "CG-API-KEY": self.api_key
            }

            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                logging.error(f"Error fetching Bitcoin Rainbow Chart data: {response.text}")
                return

            data = response.json()
            if not data.get("data"):
                logging.info("No Bitcoin Rainbow Chart data available")
                return
            try:
                last_timestamp = await timescale_client.get_last_timestamp(
                        table_name=table_name,                 
                    )            
            except:
                last_timestamp=0
                
            for element in data["data"]:
                if(element[11]>last_timestamp):
                    await timescale_client.insert_bitcoin_rainbow_chart(
                        table_name=table_name,
                        provider="coinglass",
                        btc_price=element[0],
                        model_price=element[1],
                        fire_sale=element[2],
                        buy=element[3],
                        accumulate=element[4],
                        still_cheap=element[5],
                        hold=element[6],
                        is_this_a_bubble=element[7],
                        fomo_intensifies=element[8],
                        sell_seriously_sell=element[9],
                        maximum_bubble_territory=element[10],
                        date_time=datetime.fromtimestamp(element[11]/1000).strftime("%Y-%m-%d"),
                        timestamp=element[11],
                        created_at=int(datetime.now().timestamp())
                    )

            logging.info(f"{now} - Inserted Bitcoin Rainbow Chart data")

        except Exception as e:
            logging.exception(f"{now} - Error processing Bitcoin Rainbow Chart data: {e}")

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
    
    task = BitcoinRainbowChartDownloaderTask(
        "Bitcoin Rainbow Chart Downloader",
        timedelta(hours=1),
        config
    )
    asyncio.run(task.execute())
