from src.tables import get_tables
from src.migrator import MigratorManager


def main():
    for table_name in get_tables():
        with MigratorManager(table_name.lower()) as migrator:
            migrator.migrate()


if __name__ == "__main__":
    main()
