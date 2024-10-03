import pandas as pd
import csv
from io import StringIO
import os


async def parse_response_csv(csv_text) -> dict:
    print("Parsing csv.")
    data = []
    csv_reader = csv.DictReader(StringIO(csv_text))
    for row in csv_reader:
        data.append(row)
    return pd.DataFrame(data).to_dict()


async def get_headers(headers_template: dict) -> dict:
    print("Getting headers.")
    headers = dict()
    try:
        for key, values in headers_template.items():
            if not isinstance(values, dict):
                continue
            headers[key] = os.getenv(values["val"]) if values["env"] == "true" else values["val"]
        return headers
    except Exception as e:
        print(f"Error retrieving headers: {e}")
        headers = dict()
    finally:
        return headers if headers else headers_template
