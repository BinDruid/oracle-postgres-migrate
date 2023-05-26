import json

TABLES = ["table_1", "table_2", "table_3", "table_4"]


def get_tables():
    with open("current.json", "r") as file:
        finished_table = json.load(file)["finished"]

    next_table = TABLES.index(finished_table) + 1
    return TABLES[next_table:]
