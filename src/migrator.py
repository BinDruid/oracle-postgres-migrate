import psycopg2
import cx_Oracle
import psycopg2.extras
from psycopg2.extras import execute_values
from src.utils.logging import Logger
from src.cursors.psycopg_extensions import PostgresConnection, PostgresCursor
from src.cursors.cx_oracle_extensions import OracleConnection, OracleCursor


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

    def migrate(self):
        self._config_initial_setting()
        self.postgres_cursor.disable_foreign_key()
        values_list = []
        current_row = 1
        for row in self.oracle_cursor:
            try:
                self.postgres_cursor.validate_new_record(row, self.oracle_column_types)
                values_list.append(self.oracle_cursor.read_values(row))
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

            except psycopg2.Error as exp:
                self.logger.write_error(f"Error Code {exp.pgcode}:\n {exp.pgerror}")
                self.logger.write_error(row)
                break

            except Exception as exp:
                self.logger.write_error(f"\n{exp}")
                self.logger.write_error(row)
                break        
            
class MigratorManager:
    def __init__(self, table_name):
        self.migrator = None
        self.table_name = table_name
        
    def __enter__(self) -> Migrator :
        self.migrator = Migrator(self.table_name)
        return self.migrator
    
    def __exit__(self):
        self.migrator.logger.write_warning(f"\nCommitting to database.")
        self.migrator.postgres_connection.commit()
        self.migrator.postgres_cursor.enable_foreign_key()
        self.migrator.postgres_connection.commit()
        self.migrator.postgres_cursor.close()
        self.migrator.oracle_cursor.close()
        self.migrator.logger.write_success(f"\nSuccessfully populated {self.migrator.table_name}.")