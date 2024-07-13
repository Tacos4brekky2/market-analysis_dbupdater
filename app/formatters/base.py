import pandas as pd
import io
from tools import message_maker
from flask import Response


class FormatterBase:
    def __init__(self, response, formatter_config: dict):
        self.formatter_config = formatter_config
        self.data = dict()
        self.conversion = self._convert_response(response) 
        if self.conversion.status_code == 200:
            self.data = self.conversion.get_json()

    def format(self) -> dict:
        return dict()

    def _convert_response(self, response) -> Response:
        debug_values = {
                "File type": ""
                }
        try:
            if response.status_code != 200:
                return message_maker(
                        "Formatter response conversion error: Bad response code.",
                        560,
                        debug_values
                        )
            if self._is_csv_data(response):
                debug_values["File type"] = "csv"
                decoded_csv = response.content.decode("utf-8")
                return message_maker(
                        "Response converted successfully.",
                        200,
                        debug_values,
                        data=self._csv_to_dict(decoded_csv)
                        )
            else:
                debug_values["File type"] = "json"
                data = response.json()
                return message_maker(
                        "Response converted successfully.",
                        200,
                        debug_values,
                        data = data[self.formatter_config['data_key']]
                        )
        except Exception as e:
            return message_maker(
                    "Formatter response conversion exception.",
                    560,
                    {**debug_values, "exception": e}
                    )

    def _is_csv_data(self, obj):
        if isinstance(obj, list) and all(isinstance(item, dict) for item in obj):
            return True
        return False

    def _csv_to_dict(self, csv_content):
        file = io.StringIO(csv_content)
        data = pd.read_csv(file).to_dict()
        return data
