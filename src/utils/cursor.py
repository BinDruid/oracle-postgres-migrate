from psycopg2.extensions import cursor, connection
from .validator import Validator


class MigrationCursor(cursor):
    _table_name = None
    _validator = Validator()

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, value):
        self._table_name = value

    @property
    def _schema_query(self):
        return f"select column_name, data_type, is_nullable from information_schema.columns where table_name = '{self.table_name}'"

    @property
    def _fetch_table_query(self):
        return f"select * from {self.table_name}"

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

    def migrate_new_record(self, row, context):
        return self._validator.validate_new_record(row, context)


class MigrationConnection(connection):
    """A connection that uses `MigrationCursor` automatically."""

    def cursor(self, *args, **kwargs):
        kwargs.setdefault("cursor_factory", self.cursor_factory or MigrationCursor)
        return super().cursor(*args, **kwargs)
