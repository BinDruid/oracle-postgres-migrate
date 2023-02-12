import os
import psycopg2
import cx_Oracle
import psycopg2.extras
from django.core.management.base import BaseCommand
from src.utils.psycopg_extensions import PostgresConnection, PostgresCursor
from src.utils.cx_oracle_extensions import OracleConnection, OracleCursor


class Migrator:
    def __init__(self, table_name):
        self.table_name = table_name
        self.oracle_connection = OracleConnection()
        self.oracle_cursor: OracleCursor = self.oracle_connection.cursor()
        self.oracle_cursor.table_name = table_name
        self.postgres_connection = PostgresConnection(os.environ["POSTGRES_URL"])
        self.postgres_cursor: PostgresCursor = self.postgres_connection.cursor()
        self.postgres_cursor.table_name = table_name
        self.logger = BaseCommand()

    def migrate(self):
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
                    [self.oracle_cursor.read_from_LOB(key) for key in row.values()]
                )
                self.postgres_cursor.execute(insert_query, values_list)
                self.logger.stdout.write(
                    self.logger.style.SUCCESS(
                        f"\nInserting row number < {current_row} >:\n {row} \n"
                    )
                )
                current_row += 1
            except psycopg2.Error as exp:
                self.logger.stdout.write(
                    self.logger.style.ERROR(f"Error Code {exp.pgcode}:\n {exp.pgerror}")
                )
                self.logger.stdout.write(self.logger.style.ERROR(row))
                break

        self.logger.stdout.write(self.logger.style.WARNING(f"\nCommiting to database."))
        self.postgres_connection.commit()
        self.postgres_cursor.close()
        self.oracle_cursor.close()
        self.logger.stdout.write(
            self.logger.style.SUCCESS(f"\nSuccessfully populated {self.table_name}.")
        )
