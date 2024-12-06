from dataclasses import dataclass
from enum import Enum, auto

from pandas import DataFrame


@dataclass
class Column:
    name: str
    dtype: str
    sql_type: str


@dataclass
class TableData:
    table_name: str
    columns: list[Column]
    data: DataFrame


@dataclass
class ForeignKey:
    child_table: str
    child_column: str
    parent_table: str
    parent_column: str


class Procedure(Enum):
    CUSTOMER_NAME = auto()
    PRODUCTS_ON_MODEL_YEAR = auto()
    STORE_INVENTORY = auto()


@dataclass
class SQLData:
    n_rows: int = 0
    columns: list[str] = []
    data: DataFrame = DataFrame()
