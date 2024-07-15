import os
import yaml

def load_api_configs(config_dir: str) -> dict:
    try:
        config_files = [x for x in os.listdir(config_dir) if x.endswith(".yaml")]
        data = dict()
        for file_name in config_files:
            file_path = os.path.join(config_dir, file_name)
            api_name = file_name.split(".")[0]
            with open(file_path, "r") as file:
                data[api_name] = yaml.safe_load(file)
        return data
    except Exception as e:
        print(f"Error loading API configs: {e}")
        return dict()
