import time
import json
from psycopg2.extras import execute_values
from src.utils.logging import Logger
from src.cursors.psycopg_extensions import PostgresConnection, PostgresCursor
from src.cursors.oracle_extensions import OracleConnection, OracleCursor


class Migrator:
    def __init__(self, table_name):
        self.table_name = table_name
        self.oracle_connection = OracleConnection.new_connection()
        self.oracle_cursor: OracleCursor = self.oracle_connection.cursor()
        self.oracle_cursor.table_name = table_name
        self.postgres_connection = PostgresConnection.new_connection()
        self.postgres_cursor: PostgresCursor = self.postgres_connection.cursor()
        self.postgres_cursor.table_name = table_name
        self.total_records = self.oracle_cursor.get_total_records()
        (
            self.columns,
            self.values,
            self.oracle_column_types,
        ) = self.oracle_cursor.get_table_meta()
        self.insert_query = (
            f"insert into {self.table_name} ({self.columns}) values({self.values})"
        )
        self.insert_query = f"insert into {self.table_name} ({self.columns}) values %s"
        self.insert_bulk_size = self.oracle_cursor.arraysize
        self.logger = Logger()

    def _config_initial_setting(self):
        self.oracle_cursor.set_row_factory()
        self.postgres_cursor.set_validation_schema()

    def _extract_values(self, row):
        return tuple([column for column in row.values()])

    def migrate(self):
        self._config_initial_setting()
        self.postgres_cursor.disable_foreign_key()
        values_list = []
        current_row = 1
        for row in self.oracle_cursor:
            try:
                raw_data = self.oracle_cursor.read_values(row)
                self.postgres_cursor.validate_new_record(
                    raw_data, self.oracle_column_types
                )
                values = self._extract_values(raw_data)
                values_list.append(values)
                self.logger.progress(
                    current_row,
                    self.total_records,
                    suffix=f"Migrated {current_row} records out of {self.total_records}",
                )

                if (
                    current_row % self.insert_bulk_size == 0
                    or current_row == self.total_records
                ):
                    execute_values(
                        self.postgres_cursor,
                        self.insert_query,
                        values_list,
                    )

                    values_list.clear()

                current_row += 1

            except Exception as exp:
                return self.logger.write_error(exp, row)


class MigratorManager:
    def __init__(self, table_name):
        self.migrator = None
        self.table_name = table_name
        self.start_time = None
        self.end_time = None

    def __enter__(self) -> Migrator:
        self.migrator = Migrator(self.table_name)
        self.start_time = time.time()
        return self.migrator

    def __exit__(self):
        self.migrator.logger.write_warning(f"\nCommitting to database.")
        self.migrator.postgres_connection.commit()
        self.migrator.postgres_cursor.enable_foreign_key()
        self.migrator.postgres_connection.commit()
        self.migrator.postgres_cursor.close()
        self.migrator.oracle_cursor.close()
        self.end_time = time.time()
        elapsed_time = self.end_time - self.start_time
        self.migrator.logger.write_success(
            f"\nSuccessfully populated {self.table_name} in {elapsed_time/60:.2f} minutes"
        )
        with open("current.json", "w") as file:
            json_string = json.dumps({"current.json": self.table_name})
            file.write(json_string)
