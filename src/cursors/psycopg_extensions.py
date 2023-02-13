from psycopg2.extensions import cursor, connection
from src.cursors.mixins import CommonCursor
from src.utils.validator import Validator


class PostgresCursor(cursor, CommonCursor):
    def __init__(self, conn, name):
        super().__init__(conn, name)
        self._table_name = None
        self._validator = Validator()

    @property
    def _schema_query(self):
        return f"select column_name, data_type, is_nullable from information_schema.columns where table_name = '{self.table_name}'"

    def set_validation_schema(self):
        self.execute(self._schema_query)
        conditions = self.fetchall()
        self._validator.type_conditions = {
            column[0]: column[1] for column in conditions
        }
        self._validator.null_conditions = {
            column[0]: column[2] for column in conditions
        }

    def get_table(self):
        return self.execute(self._fetch_table_query)

    def validate_new_record(self, row, oracle_types):
        return self._validator.validate_new_record(row, oracle_types)


class PostgresConnection(connection):
    """A connection that uses `PostgresCursor` automatically."""

    def cursor(self, *args, **kwargs):
        kwargs.setdefault("cursor_factory", self.cursor_factory or PostgresCursor)
        return super().cursor(*args, **kwargs)
