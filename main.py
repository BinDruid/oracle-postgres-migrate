import sys
from src.migrator import Migrator


def main():
    table_name = sys.argv[1]
    migrator = Migrator(table_name)
    migrator.migrate()


if __name__ == "__main__":
    main()
