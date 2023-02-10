import os
import psycopg2
import psycopg2.extras
from src.utils.cursor import MigrationCursor, MigrationConnection

postgres_connection = MigrationConnection(os.environ["DB_URL"])
postgres_cursor: MigrationCursor = postgres_connection.cursor()

postgres_cursor.table_name = "dashboard_train"
postgres_cursor.set_validation_schema()
postgres_cursor.get_table()
for row in postgres_cursor:
    try:
        postgres_cursor.migrate_new_record(row, {"id": "int"})

    except psycopg2.Error as exp:
        print(exp.pgerror)
        break

postgres_connection.commit()
postgres_connection.close()
