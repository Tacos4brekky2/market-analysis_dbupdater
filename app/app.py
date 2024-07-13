from tools import (
    message_maker,
    DBConnection,
    get_headers,
    load_api_configs,
)
from formatters import format_response
import json
import datetime
import requests
import toml
from flask import Flask, request, jsonify, Response


class UpdaterService:
    def __init__(self, flask_app: Flask, config_dir: str = "config"):
        self.app = flask_app
        self.secrets = toml.load("tools/secrets.toml")
        self.api_configs = load_api_configs(config_dir=config_dir)
        if not self.api_configs:
            return
        self.register_routes()

    def register_routes(self):
        self.app.add_url_rule("/", "home", self.home, methods=["GET"])
        self.app.add_url_rule("/update", "update", self.update, methods=["POST"])

    def home(self) -> Response:
        return message_maker("StonksDB Updater home.", 200)

    def update(self) -> Response:
        json_data = request.json
        debug_values = {
            "Request data": False,
            "Headers": False,
            "Table name": False,
            "Data formatted": False,
        }
        try:
            if not json_data:
                return message_maker("Request data not provided.", 560, debug_values)
            debug_values["Request data"] = True
            api_name = json_data["api_name"]
            category = json_data["category"]
            endpoint = json_data["endpoint"]
            config = self.api_configs[api_name][category][endpoint]
            kw = json_data["kw"]
            kw["function"] = config["function"]
            headers = get_headers(api_name)
            if not headers:
                return message_maker("Failed to load headers.", 560, debug_values)
            debug_values["Headers"] = True
            url = self.secrets[api_name]["url"]
            table_val = config["info"]["table"]
            table_name = self.get_table_name(table_val=table_val, kw=kw)
            collection_name = config["info"]["collection"]
            if not table_name:
                return message_maker(
                    "Failed to generate table name.", 560, debug_values
                )
            debug_values["Table name"] = True
            # Check cache before updating to avoid repeat API calls.
            cached_data = self.cache_read(collection=collection_name, table=table_name)
            if cached_data.status_code == 200:
                return jsonify(cached_data)
            # Update only if request is not cached, or an old request is cached.
            print("Updating")
            data_api_response = requests.get(url=url, headers=headers, params=kw)
            print(config)
            formatter_response = format_response(
                api_name=api_name,
                formatter_config=config["format"],
                response=data_api_response,
            )
            if formatter_response.status_code != 200:
                return jsonify(formatter_response)
            data = formatter_response.get_json()["data"]
            debug_values["Data formatted"] = True
            cache_response = self.cache_write(
                table=table_name, collection=collection_name, data=data
            )
            return cache_response
        except Exception as e:
            return message_maker(
                "Updater service exception.", 560, {**debug_values, **{"exception": e}}
            )

    def cache_write(
        self,
        collection: str,
        table: str,
        data: dict,
    ) -> Response:
        print("Caching request data.")
        debug_values = {"Cache connection": False, "Data cached": False}
        try:
            timestamp = datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S")
            cache = DBConnection().connect("redis")
            if not cache:
                return message_maker("Cache connection failed.", 560, debug_values)
            cache = cache.exception
            debug_values["Cache connection"] = True
            meta = {
                "timestamp": timestamp,
                "table": table,
                "collection": collection,
            }
            cache_info = {
                "sender": "updater",
                "destination": "stonksdb",
                "tags": [
                    "db:write",
                ],
            }
            cache_entry = json.dumps(
                {"meta": meta, "data": data, "cache_info": cache_info}
            )
            cache.set(f"{collection}:{table}", cache_entry)
            debug_values["Data cached"] = True
            return message_maker(
                "Cache write successful.", 200, debug_values, data=meta
            )
        except Exception as e:
            return message_maker(
                "Cache write exception",
                560,
                {**debug_values, **{"exception": e}},
            )

    def cache_read(self, collection: str, table: str) -> Response:
        print("Checking cache for request.")
        debug_values = {"Cache connection": False}
        try:
            cache = DBConnection().connect("redis")
            if not cache:
                return message_maker("Cache connection failed.", 560, debug_values)
            debug_values["Cache connection"] = True
            entry = json.loads(cache.get(f"{collection}:{table}"))
            if entry:
                return message_maker(
                    "Found cached data.",
                    200,
                    debug_values,
                    data=entry,
                )
            else:
                return message_maker(
                    "No data found in cache.",
                    560,
                    debug_values,
                )
        except Exception as e:
            return message_maker(
                "Cache read exception.", 560, {**debug_values, **{"exception": e}}
            )

    def get_table_name(self, table_val: str, kw: dict) -> str:
        table_name = [
            kw[x].lower() for x in kw.keys() if x.lower() == table_val.lower()
        ]
        if table_name:
            table_name = str(table_name[0])
            return table_name
        return str()


if __name__ == "__main__":
    app = Flask(__name__)
    port = 5000
    host = "localhost"
    updater = UpdaterService(flask_app=app)
    app.run(host=host, port=port, debug=False)
