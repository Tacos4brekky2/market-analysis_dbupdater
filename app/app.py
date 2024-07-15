import os
from tools import fetch_data, format_data, load_api_configs
import asyncio
import aioredis
import json
import async_timeout
import datetime

CONFIG_DIR = "api-config"
REDIS_URI = f'redis://{os.getenv("REDIS_HOST")}:{os.getenv("REDIS_PORT")}'
SUBSCRIPTION_CHANNEL = "api-request_channel"
PUBLISH_CHANNEL = "db-store_channel"
VER = "0.2.0"


class Updater:
    def __init__(
        self,
        redis_uri: str,
        api_configs: dict,
        subscription_channel: str,
        publish_channel: str,
        version: str,
    ):
        self.redis_uri = redis_uri
        self.api_configs = api_configs
        self.sub_channel = subscription_channel
        self.pub_channel = publish_channel
        self.version = version
        self.redis = None
        self.pubsub = None

    async def setup(self):
        print("---STARTING---")
        print(f"Updater Version: {VER}")
        self.redis = aioredis.from_url(
            self.redis_uri, password=os.getenv("REDIS_PASSWORD")
        )
        print("Connected to Redis")
        self.pubsub = self.redis.pubsub()
        await self.pubsub.subscribe(SUBSCRIPTION_CHANNEL)
        print(f"Subscribed to {SUBSCRIPTION_CHANNEL}")

    async def listen(self):
        while True and (self.pubsub is not None):
            message = await self.pubsub.get_message(ignore_subscribe_messages=True)
            if message:
                await self.handle_message(message)
            await asyncio.sleep(0.01)

    async def handle_message(self, message):
        try:
            data = json.loads(message["data"])
            meta = json.loads(message["metadata"])
            api = self.api_configs[data["api_name"]]
            print(f"Received message: {data}")
            match message["type"]:
                case "FETCH_DATA":
                    await self.process_update(
                        request_url=api["url"],
                        request_headers=api["headers"],
                        request_params=data["params"],
                        api_name=data["api_name"],
                        formatting_template=api["endpoints"][data["endpoint"]][
                            "format"
                        ],
                        correlation_id=meta["correlation_id"]
                    )
        except json.JSONDecodeError:
            print("Failed to decode JSON message")

    async def publish_message(
        self,
        message_type: str,
        priority: int,
        data: dict,
        correlation_id: str,
        api_name: str,
    ):
        print(f"Publishing to channel: {self.pub_channel}")
        message = {
            "type": message_type,
            "source": "market-analysis_dbupdater",
            "timestamp": datetime.datetime.now(),
            "priority": priority,
            "data": data,
            "metadata": {
                "version": self.version,
                "correlation_id": correlation_id,
                "api_name": api_name,
            },
        }
        if self.pubsub:
            await self.pubsub.publish(self.pub_channel, json.dumps(message))

    async def process_update(
        self,
        request_url: str,
        request_headers: dict,
        request_params: dict,
        api_name: str,
        formatting_template: dict,
        correlation_id: str
    ):
        try:
            print(f"Processing update with API: {api_name}")
            request_data = await fetch_data(
                url=request_url,
                headers=request_headers,
                params=request_params,
            )
            formatted_data = format_data(
                api_name=api_name,
                formatting_template=formatting_template,
                data=request_data if request_data else dict(),
            )
            if formatted_data:
                await self.publish_message(
                    message_type="STORE_DATA",
                    priority=0,
                    data=formatted_data,
                    correlation_id=correlation_id,
                    api_name=api_name,
                )
        except Exception as e:
            print(f"Error processing message: {e}")

    async def close(self):
        if self.pubsub and self.redis:
            await self.pubsub.unsubscribe(self.sub_channel)
            await self.redis.close()


async def main():
    api_configs = load_api_configs(config_dir=CONFIG_DIR)
    updater = Updater(
        redis_uri=REDIS_URI,
        api_configs=api_configs,
        publish_channel=PUBLISH_CHANNEL,
        subscription_channel=SUBSCRIPTION_CHANNEL,
        version=VER,
    )
    await updater.setup()
    try:
        await updater.listen()
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        await updater.close()


if __name__ == "__main__":
    asyncio.run(main())
