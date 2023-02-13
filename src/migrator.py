import os
import time
import psycopg2
import cx_Oracle
import psycopg2.extras
from src.utils.logging import Logger
from src.cursors.psycopg_extensions import PostgresConnection, PostgresCursor
from src.cursors.cx_oracle_extensions import OracleConnection, OracleCursor


class Migrator:
    def __init__(self, table_name):
        self.table_name = table_name
        self.oracle_connection = OracleConnection()
        self.oracle_cursor: OracleCursor = self.oracle_connection.cursor()
        self.oracle_cursor.table_name = table_name
        self.postgres_connection = PostgresConnection(os.environ["POSTGRES_URL"])
        self.postgres_cursor: PostgresCursor = self.postgres_connection.cursor()
        self.postgres_cursor.table_name = table_name
        self.logger = Logger()

    def migrate(self):
        total_records = self.oracle_cursor.get_total_records()
        columns, values, oracle_column_types = self.oracle_cursor.get_table_meta()
        self.oracle_cursor.set_row_factory()
        self.postgres_cursor.set_validation_schema()
        current_row = 1
        for row in self.oracle_cursor:
            try:
                insert_query = (
                    f"insert into {self.table_name} ({columns}) values({values});"
                )
                self.postgres_cursor.validate_new_record(row, oracle_column_types)
                values_list = tuple(
                    [
                        self.oracle_cursor.read_from_LOB(column)
                        for column in row.values()
                    ]
                )
                self.postgres_cursor.execute(insert_query, values_list)
                self.logger.progress(
                    current_row,
                    total_records,
                    suffix=f"Migrated {current_row} records out of {total_records}",
                )
                current_row += 1

            except psycopg2.Error as exp:
                self.logger.write_error(f"Error Code {exp.pgcode}:\n {exp.pgerror}")
                self.logger.write_error(row)
                break

        self.logger.write_warning(f"\nCommitting to database.")
        self.postgres_connection.commit()
        self.postgres_cursor.close()
        self.oracle_cursor.close()
        self.logger.write_success(f"\nSuccessfully populated {self.table_name}.")
        # TODO add time elapsed
