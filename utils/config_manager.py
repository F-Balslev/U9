import json

from utils.custom_datastructures import Column, ForeignKey, TableData


def save_config(config, filepath: str = "config.json"):
    with open(filepath, "w") as file:
        json.dump(config, file)


def convert_config(config):
    tables = []
    for table in config["table_data"]:
        tmp_table = TableData(**table)
        tmp_table.columns = [Column(**col) for col in tmp_table.columns]
        tables.append(tmp_table)

    keys = []
    for key in config["foreign_keys"]:
        keys.append(ForeignKey(**key))

    res = {**config}
    res["table_data"] = tables
    res["foreign_keys"] = keys

    return res


def read_config(filepath: str = "config.json"):
    with open(filepath) as configfile:
        config = json.load(configfile)

    return convert_config(config)
