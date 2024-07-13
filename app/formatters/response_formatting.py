from flask import Response, jsonify
from .AlphaVantage import AlphaVantageFormatter
from tools import message_maker

formatter_map = {"alpha-vantage": AlphaVantageFormatter}


def format_response(api_name: str, formatter_config: dict, response) -> Response:
    debug_values = {"Formatter class": False, "Formatter": False}
    try:
        if api_name not in formatter_map.keys():
            return message_maker(
                f"No formatter class found for API: {api_name}", 560, debug_values
            )
        debug_values["Formatter class"] = True
        formatter_class = formatter_map[api_name]
        formatter = formatter_class(
            response=response, formatter_config=formatter_config
        )
        if not formatter.data:
            return jsonify(formatter.conversion)
        debug_values["Formatter"] = True
        return message_maker(
            "Data formatted successfully.", 200, debug_values, data=formatter.format()
        )
    except Exception as e:
        return message_maker(
            "Formatter exception.", 560, {**debug_values, **{"exception": e}}
        )
