from utils.config_manager import read_config
from utils.csv_reader import load_data
from utils.custom_datastructures import Procedure
from utils.database_connection import DatabaseConnection


def main():
    config = read_config("config.json")
    config = load_data(config)

    with DatabaseConnection(**config) as database:
        database.write_tables()
        database.add_foreign_keys()

        res1 = database.call_stored_procedure(Procedure.CUSTOMER_NAME, 1)
        print(f"Res1: {res1.data}")

        res2 = database.call_stored_procedure(Procedure.PRODUCTS_ON_MODEL_YEAR, 2016)
        print(f"Res2:\n{res2.data}")

        res3 = database.call_stored_procedure(Procedure.STORE_INVENTORY, 1)
        print(f"Res3:\n{res3.data}")

        breakpoint()


if __name__ == "__main__":
    main()
