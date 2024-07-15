import pandas as pd
import csv
import aiohttp
from io import StringIO
import os
from collections import defaultdict


async def fetch_data(
    url: str,
    headers: dict = dict(),
    params: dict = dict(),
):
    async with aiohttp.ClientSession() as session:
        print(f"Fetching data from url: {url}")
        async with session.get(
            url=url,
            headers=get_headers(headers),
            params=params,
        ) as response:
            response.raise_for_status()
            try:
                data = await response.json()
                print("Found response json.")
                return data
            except Exception as e:
                print(e)
            try:
                csv_text = await response.text()
                return parse_response_csv(csv_text)
            except Exception as e:
                print(e)


def parse_response_csv(csv_text) -> dict:
    print("Parsing csv.")
    data = []
    csv_reader = csv.DictReader(StringIO(csv_text))
    for row in csv_reader:
        data.append(row)
    return pd.DataFrame(data).to_dict()


def get_headers(headers_template: dict):
    print("Getting headers.")
    headers = defaultdict()
    try:
        for key, values in headers_template.items():
            headers[key] = os.getenv(values.val) if values.env == "true" else values.val
        return headers
    except Exception as e:
        print(f"Error retrieving headers: {e}")
