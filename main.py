import sys
from src.migrator import MigratorManager


def main():
    table_name = sys.argv[1]
    with MigratorManager(table_name.lower()) as migrator:
        migrator.migrate()


if __name__ == "__main__":
    main()
