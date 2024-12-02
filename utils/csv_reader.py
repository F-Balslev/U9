import pandas as pd

from utils.database_connection import TableData


def read_csv(basepath: str, table_data: TableData):
    dtypes = {}
    for col in table_data.columns:
        dtypes[col.name] = col.dtype

    df = pd.read_csv(f"{basepath}{table_data.table_name}.csv", dtype=dtypes)

    return df


def read_all_csvs(config: dict[str, list[TableData]], basepath="data/"):
    for idx, table in enumerate(config["table_data"]):
        config["table_data"][idx].data = read_csv(basepath, table)

    return config
