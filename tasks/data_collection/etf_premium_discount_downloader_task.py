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

class ETFPremiumDiscountDownloaderTask(BaseTask):
    def __init__(self, name: str, frequency: timedelta, config: Dict[str, Any]):
        super().__init__(name, frequency, config)
        self.connector_name = config["connector_name"]
        self.days_data_retention = config.get("days_data_retention", 7)
        self.api_key = config.get("coinglass_api_key", "")

    async def execute(self):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        logging.info(f"{now} - Starting ETF Premium Discount downloader")

        timescale_client = TimescaleClient(
            host=self.config["timescale_config"].get("host", "localhost"),
            port=self.config["timescale_config"].get("port", 5432),
            user=self.config["timescale_config"].get("user", "admin"),
            password=self.config["timescale_config"].get("password", "admin"),
            database=self.config["timescale_config"].get("database", "timescaledb")
        )
        await timescale_client.connect()

        try:
            table_name="coinglass_etf_premium_discount_history"

            url = "https://open-api-v3.coinglass.com/api/bitcoin/etf/premium-discount-history"
            headers = {
                "accept": "application/json",
                "CG-API-KEY": self.api_key
            }
        
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                logging.error(f"Error fetching ETF Premium Discount data for {response.text}")
                
            data = response.json()
            if not data.get("data"):
                logging.info(f"No ETF Premium Discount data available")

            try:                
                last_timestamp = await timescale_client.get_last_timestamp(
                        table_name=table_name
                    )                 
            except:
                last_timestamp=0
                       
            for element in data["data"]:
                if(element["date"]>last_timestamp):
                    for data_item in element["list"]:
                        await timescale_client.insert_etf_premium_discount(
                            table_name=table_name,
                            provider="coinglass",
                            nav=data_item["nav"],
                            market_price=data_item["marketPrice"],
                            premium_discount_percent=data_item["premiumDiscountPercent"],
                            ticker=data_item["ticker"],
                            date_time=datetime.fromtimestamp(element["date"]/1000).strftime("%Y-%m-%d"),
                            timestamp=element["date"],
                            created_at=int(datetime.now().timestamp())
                        )
            logging.info(f"{now} - Inserted ETF Premium Discount data")
            await asyncio.sleep(1)

        except Exception as e:
            logging.exception(f"{now} - Error processing ETF Premium Discount data: {e}")

        finally:
            await timescale_client.close()

if __name__ == "__main__":
    config = {
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
    }
    
    task = ETFPremiumDiscountDownloaderTask(
        "ETF Premium Discount Downloader",
        timedelta(hours=1),
        config
    )
    asyncio.run(task.execute())
