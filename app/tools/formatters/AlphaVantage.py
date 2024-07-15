from .base import FormatterBase
import pandas as pd

class AlphaVantageFormatter(FormatterBase):
    def __init__(self, data: dict, formatter_config: dict):
       super().__init__(data=data, formatter_config=formatter_config)

    def format(self) -> dict:
        df = pd.DataFrame(self.data)
        if self.formatter_config['transpose']:
            df = df.transpose()
        if self.formatter_config['reset_index']:
            df.reset_index(inplace=True, names=self.formatter_config['first_col'])
        if self.formatter_config['reverse_rows']:
            df = df[::-1]
        if self.formatter_config['drop_columns']:
            df.drop(columns=self.formatter_config['drop_columns'], inplace=True)
        for i, col_name in enumerate(df.columns):
            f_name = str(col_name).lstrip(f"{i}. ")
            df = df.rename(columns={col_name: f_name})
        data = df.to_dict()
        return data
