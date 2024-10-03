from tools import parse_response_csv, get_headers
import yaml
import os
import json
import asyncio
from redis import asyncio as aioredis
import aiohttp


# Database updater service for https://github.com/Tacos4brekky2/market-analysis_server

INPUT_STREAMS = ["updater-in"]
OUTPUT_STREAM = "updater-out"
CONSUMER_GROUP = "market-analysis_server"
CONSUMER_NAME = "updater-consumer"

REDIS_PARAMS = {
    "host": os.getenv("REDIS_HOST", "localhost"),
    "port": os.getenv("REDIS_PORT", "6379"),
    "password": os.getenv("REDIS_PASSWORD", ""),
}

CONFIG = dict()
with open("config/request_params.yaml") as file:
    CONFIG = yaml.safe_load(file)


def deserialize_message(message):
    deserialized_message = dict()
    for key, value in message.items():
        try:
            deserialized_message[key] = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            deserialized_message[key.decode("utf-8")] = value.decode("utf-8")
    return deserialized_message


async def create_redis_groups(redis, input_streams: list):
    for stream in input_streams:
        try:
            await redis.xgroup_create(stream, CONSUMER_GROUP, id="0", mkstream=True)
        except Exception as e:
            print(f"Error creating consumer group: {e}")


async def consume(redis, input_streams: list):
    await create_redis_groups(redis=redis, input_streams=input_streams)
    while True:
        messages = await redis.xreadgroup(
            groupname=CONSUMER_GROUP,
            consumername=CONSUMER_NAME,
            streams={x: ">" for x in input_streams},
            count=10,
        )
        for stream, message_list in messages:
            for message_id, message in message_list:
                deserialized_message = deserialize_message(message)
                await handle_message(
                    redis=redis,
                    stream=stream,
                    message=deserialized_message,
                    message_id=message_id,
                )
                print(f"Consumed message {message_id}")


async def handle_message(redis, stream: str, message, message_id):
    try:
        message_type = message["type"]
        request_id = message["request_id"]
        del message["request_id"]
        del message["type"]
        match message_type:
            case "FETCH_REQUEST":
                request_data = await fetch_data(
                    api_name=message["api_name"], params=message
                )
                await produce(
                    redis=redis,
                    message_type="DATA_FETCHED",
                    request_id=request_id,
                    params=message,
                    payload=request_data,
                )
                await redis.xack(stream, CONSUMER_GROUP, message_id)
    except json.JSONDecodeError:
        print("Failed to decode JSON message")
    except Exception as e:
        print(f"Updater error: {e}")


async def produce(
    redis, message_type: str, request_id: str, params, payload: dict = dict()
):
    message = {
        k: (json.dumps(v) if isinstance(v, dict) else str(v))
        for k, v in payload.items()
    }
    message["type"] = message_type
    message["request_id"] = request_id
    message["params"] = json.dumps(params)
    response = await redis.xadd(OUTPUT_STREAM, fields=message)
    print(f"Produce: {response}")


async def fetch_data(api_name: str, params: dict = dict()) -> dict:
    request_params = CONFIG[api_name]
    new_headers = await get_headers(request_params["headers"])
    request_params["headers"] = new_headers
    request_params["params"] = params
    async with aiohttp.ClientSession() as session:
        print(f"Fetching data from url: {request_params["url"]}")
        async with session.get(**request_params) as response:
            response.raise_for_status()
            try:
                data = await response.json()
                print("Found response json.")
                if data:
                    return data
            except Exception as e:
                print(f"Error getting response json: {e}")
            try:
                csv_text = await response.text()
                data = await parse_response_csv(csv_text)
                print("Found response csv.")
                if data:
                    return data
            except Exception as e:
                print(f"Error fetching data: {e}")
    return dict()


async def main():
    redis = aioredis.from_url(
        f'redis://{REDIS_PARAMS["host"]}:{REDIS_PARAMS["port"]}',
        password=os.getenv("REDIS_PASSWORD", ""),
    )
    try:
        while True:
            await consume(redis=redis, input_streams=INPUT_STREAMS)
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        print("Updater shut down.")


if __name__ == "__main__":
    asyncio.run(main())
