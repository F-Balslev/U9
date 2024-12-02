from utils.config_manager import read_config
from utils.csv_reader import read_all_csvs
from utils.database_connection import DatabaseConnection


def main():
    config = read_config("config.json")
    config = read_all_csvs(config)

    with DatabaseConnection(**config) as database:
        database.write_tables()
        database.add_foreign_keys()


if __name__ == "__main__":
    main()
