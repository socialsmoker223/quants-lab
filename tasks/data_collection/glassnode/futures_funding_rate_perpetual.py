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

class FuturesFundingRatePerpetual (BaseTask):
    def __init__(self, name: str, frequency: timedelta, config: Dict[str, Any]):
        super().__init__(name, frequency, config)
        
        self.days_data_retention = config.get("days_data_retention", 7)
        self.api_key = config.get("glassnode_api_key")

    async def execute(self):
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f UTC")
        logging.info(f"{now} - Starting Futures Funding Rate Perpetual downloader")

        timescale_client = TimescaleClient(
            host=self.config["timescale_config"].get("host"),
            port=self.config["timescale_config"].get("port"),
            user=self.config["timescale_config"].get("user"),
            password=self.config["timescale_config"].get("password"),
            database=self.config["timescale_config"].get("database")
        )
        await timescale_client.connect()

        try:
            

            trading_pairs = self.config.get("trading_pairs", ["BTC"])

            for trading_pair in trading_pairs:

                table_name = f"glassnode_{trading_pair}_futures_funding_rate_perpetual_1h"
                symbol = trading_pair.split("-")[0]
                url = "https://api.glassnode.com/v1/metrics/derivatives/futures_funding_rate_perpetual"
                headers = {
                    "accept": "application/json",
                    "X-API-KEY": self.api_key
                }
                
                params = {
                    "a": symbol,
                    "i": "1h",                    
                    "f": "JSON",                        
                    "e":"aggregated"            
                }
                try:
                    last_timestamp = await timescale_client.get_last_timestamp(
                        table_name=table_name                    
                    )
                    if last_timestamp:
                        params["s"] = int(last_timestamp)
                except:
                    last_timestamp=0        
                
                response = requests.get(url, headers=headers, params=params)
                if response.status_code != 200:
                    logging.error(f"Error fetching Futures Funding Rate Perpetual data for {symbol}: {response.text}")
                    continue

                data = response.json()
                
                if not data :
                    logging.info(f"No Futures Funding Rate Perpetual data available for {symbol} or all values are zero")
                    continue
                
                for element in data: 
                    if(element["t"]>last_timestamp):
                        await timescale_client.insert_futures_funding_rate_perpetual_1h(
                            table_name=table_name,
                            symbol=trading_pair,
                            provider="glassnode",
                            value=element["v"],
                            timestamp=element["t"],
                            date_time=datetime.fromtimestamp(element["t"]).strftime("%Y-%m-%d"),
                            created_at=int(datetime.now().timestamp()),
                        )
                
                logging.info(f"{now} - Inserted Futures Funding Rate Perpetual data for {trading_pair}")
                await asyncio.sleep(1)

        except Exception as e:
            logging.exception(f"{now} - Error processing Futures Funding Rate Perpetual data: {e}")

        finally:
            await timescale_client.close()

if __name__ == "__main__":
    config = {        
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
    }
    
    task = FuturesFundingRatePerpetual(
        "Futures Funding Rate Perpetual",
        timedelta(hours=1),
        config
    )
    asyncio.run(task.execute())
