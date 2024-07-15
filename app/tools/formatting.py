from formatters import AlphaVantageFormatter

formatter_map = {"alpha-vantage": AlphaVantageFormatter}


def format_data(api_name: str, formatting_template: dict, data: dict) -> dict:
    try:
        print("Formatting data.")
        formatter_class = formatter_map[api_name]
        formatter = formatter_class(
            data=data, formatter_config=formatting_template
        )
        return formatter.format()
    except Exception as e:
        print(f"Error formatting data: {e}")
        return dict()

