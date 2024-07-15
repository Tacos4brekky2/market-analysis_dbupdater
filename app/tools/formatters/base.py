class FormatterBase:
    def __init__(self, data: dict, formatter_config: dict):
        self.formatter_config = formatter_config
        self.data = data

    def format(self) -> dict:
        return dict()
