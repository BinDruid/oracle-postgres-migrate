import os
import cx_Oracle
from src.cursors.mixins import CommonCursor


class OracleCursor(cx_Oracle.Cursor, CommonCursor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prefetchrows = 5000
        self.arraysize = 5000
        self._table_name = None

    @property
    def _record_counts_query(self):
        return f"select count(*) from {self.table_name}"

    def _value_string(self, count):
        return ", ".join(["%s"] * count)

    def _get_table(self):
        return self.execute(self._fetch_table_query)

    def get_total_records(self):
        self.execute(self._record_counts_query)
        return self.fetchone()[0]

    def _fix_invalid_column_name(self, cols: list):
        pass
            
    def _set_table_columns(self):
        table_columns = [col[0].lower() for col in self.description]
        self._fix_invalid_column_name(table_columns)
        setattr(self, "_table_columns", table_columns)

    def get_table_meta(self):
        self._get_table()
        self._set_table_columns()
        columns = ", ".join(self._table_columns)
        value_string = self._value_string(len(self._table_columns))
        column_types = {
            col[0].lower(): col[1].name.replace("DB_TYPE_", "").lower()
            for col in self.description
        }
        return columns, value_string, column_types

    def set_row_factory(self):
        self.rowfactory = lambda *args: dict(zip(self._table_columns, args))

    def read_values(self, row):
        return tuple([self._read_from_LOB(column) for column in row.values()])

    def _read_from_LOB(self, column):
        if isinstance(column, cx_Oracle.LOB):
            return column.read()
        return column


class OracleConnection(cx_Oracle.Connection):
    @classmethod
    def new_connection(cls):
        dns = cx_Oracle.makedsn(
            os.environ["ORACLE_URL"],
            os.environ["ORACLE_PORT"],
            service_name=os.environ["ORACLE_SERVICE"],
        )
        return cls(
            user=os.environ["ORACLE_USER"],
            password=os.environ["ORACLE_PASS"],
            dsn=dns,
            encoding="UTF-8",
        )

    def cursor(self):
        return OracleCursor(self)
