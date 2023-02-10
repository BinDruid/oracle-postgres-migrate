import os
import psycopg2
import cx_Oracle
import psycopg2.extras
from src.utils.psycopg_extensions import PostgresConnection, PostgresCursor
from src.utils.cx_oracle_extensions import OracleConnection, OracleCursor


class Migrator:
    def __init__(self, table_name):
        self.table_name = table_name
        self.oracle_connection = OracleConnection()
        self.oracle_cursor: OracleCursor = self.oracle_connection.cursor()
        self.postgres_connection = PostgresConnection(os.environ["POSTGRES_URL"])
        self.postgres_cursor: PostgresCursor = self.postgres_connection.cursor()
        self.postgres_cursor.table_name = table_name

    def migrate(self):
        self.postgres_cursor.set_validation_schema()
        self.postgres_cursor.get_table()
        for row in self.postgres_cursor:
            try:
                self.postgres_cursor.migrate_new_record(row, {"id": "int"})

            except psycopg2.Error as exp:
                print(exp.pgerror)
                break

        self.postgres_connection.commit()
        self.postgres_connection.close()
