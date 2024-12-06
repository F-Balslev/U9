import os
from pathlib import Path

import pandas as pd
import pyodbc
import tqdm
from dotenv import load_dotenv
from numpy import int64 as np_i64

from utils.custom_datastructures import ForeignKey, Procedure, SQLData, TableData


class DatabaseConnection:
    def __init__(
        self,
        database_name: str,
        recreate_database: bool,
        table_data: list[TableData],
        foreign_keys: list[ForeignKey],
    ):
        self.database_name = database_name
        self.recreate_database = recreate_database
        self.table_data = table_data
        self.foreign_keys = foreign_keys

        self.connection_string = self._get_connection_string()

    def __enter__(self):
        self.db_connection = pyodbc.connect(self.connection_string)

        self._init_database()
        for table in self.table_data:
            self._init_table(table)

        return self

    def __exit__(self, *_):
        self.db_connection.close()

    def _init_database(self):
        if self.recreate_database:
            self.execute(f"DROP DATABASE IF EXISTS {self.database_name};")

        self.execute(f"CREATE DATABASE IF NOT EXISTS {self.database_name};")
        self.execute(f"USE {self.database_name};")

    def _init_table(self, table: TableData):
        columns = [f"{col.name} {col.sql_type}" for col in table.columns]
        columns = ",".join(columns)
        query = f"CREATE TABLE IF NOT EXISTS {table.table_name}({columns})"

        self.execute(query)

    def _get_connection_string(self):
        load_dotenv(Path(__file__).parent.parent.joinpath(".env"))

        sql_driver = "DRIVER={MySQL ODBC 8.0 ANSI Driver}"
        sql_server = "SERVER=localhost"
        sql_username = f"UID={os.getenv("SQL_USERNAME")}"
        sql_password = f"PWD={os.getenv("SQL_PASSWORD")}"

        arguments = [sql_driver, sql_server, sql_username, sql_password]

        return ";".join(arguments)

    def execute(self, query):
        with self.db_connection.cursor() as cursor:
            cursor.execute(query)

    def write_table(self, table: TableData):
        column_names = [col.name for col in table.columns]

        column_str = ",".join(column_names)
        values_str = ",".join("?" * len(column_names))

        query = f"INSERT INTO {table.table_name} ({column_str}) values({values_str})"

        with self.db_connection.cursor() as cursor:
            for _, row in table.data.iterrows():
                data = [row[name] for name in column_names]
                data = [d if d is not pd.NA else None for d in data]

                if any([isinstance(d, np_i64) for d in data]):  # I hate pandas
                    print(f"Found a pesky np.int64 in table:\n{table}")
                    exit()

                cursor.execute(query, *data)

            cursor.commit()

    def write_tables(self):
        if not self.recreate_database:
            return

        print("Writing tables")
        for table in tqdm.tqdm(self.table_data):
            self.write_table(table)

    def add_foreign_keys(self):
        if not self.recreate_database:
            return

        for key in self.foreign_keys:
            self.execute(
                f"""ALTER TABLE {key.child_table}
                ADD CONSTRAINT {f"fk_{key.child_table}_{key.child_column}_{key.parent_column}"}
                FOREIGN KEY ({key.child_column})
                REFERENCES {key.parent_table}({key.parent_column})"""
            )

    def _process_sql_data(self, fetched_data, fetched_columns):
        data = [[*row] for row in fetched_data]
        column_names = [col[0] for col in fetched_columns]

        sql_data = SQLData(
            n_rows=len(data),
            columns=column_names,
            data=pd.DataFrame(data, columns=column_names),
        )

        return sql_data

    def _sp_customer_name(self, customer_id) -> SQLData:
        assert isinstance(customer_id, int) or (
            isinstance(customer_id, str) and customer_id.isnumeric()
        )

        with self.db_connection.cursor() as cursor:
            cursor.execute(f"CALL customer_name({customer_id}, @ans);")
            cursor.execute("SELECT @ans;")

            data = self._process_sql_data(cursor.fetchall(), [["customer_name"]])

            return data

    def _sp_product_information_per_model_year(self, model_year) -> SQLData:
        assert isinstance(model_year, int) or (
            isinstance(model_year, str) and model_year.isnumeric()
        )

        with self.db_connection.cursor() as cursor:
            cursor.execute(f"CALL yearly_product_information({model_year});")

            data = self._process_sql_data(cursor.fetchall(), cursor.description)

            # Don't really know why this is needed
            cursor.cancel()

            return data

    def _sp_store_inventory(self, store_id) -> SQLData:
        assert isinstance(store_id, int) or (
            isinstance(store_id, str) and store_id.isnumeric()
        )

        with self.db_connection.cursor() as cursor:
            cursor.execute(f"CALL store_inventory({store_id});")

            data = self._process_sql_data(cursor.fetchall(), cursor.description)

            # Don't really know why this is needed
            cursor.cancel()

            return data

    def call_stored_procedure(self, procedure: Procedure, args) -> SQLData:
        match procedure:
            case Procedure.CUSTOMER_NAME:
                return self._sp_customer_name(args)
            case Procedure.PRODUCTS_ON_MODEL_YEAR:
                return self._sp_product_information_per_model_year(args)
            case Procedure.STORE_INVENTORY:
                return self._sp_store_inventory(args)
            case _:
                return SQLData()
