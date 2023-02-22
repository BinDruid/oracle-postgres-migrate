import os
import oracledb
from src.cursors.mixins import CommonCursor
from src.utils.column_mapper import corrected_columns



class OracleCursor(oracledb.Cursor, CommonCursor):
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
        return [corrected_columns[col] if col in corrected_columns else col for col in cols ]
            
    def _set_table_columns(self):
        table_columns = [col[0].lower() for col in self.description]
        table_columns = self._fix_invalid_column_name(table_columns)
        setattr(self, "_table_columns", table_columns)

    def get_table_meta(self):
        self._get_table()
        self._set_table_columns()
        columns = ", ".join(self._table_columns)
        value_string = self._value_string(len(self._table_columns))
        column_types = [
            col[1].name.replace("DB_TYPE_", "").lower() for col in self.description
        ]
        column_types_dict = dict(zip(self._table_columns, column_types))
        return columns, value_string, column_types_dict

    def set_row_factory(self):
        self.rowfactory = lambda *args: dict(zip(self._table_columns, args))

    def read_values(self, row):
        return {field: self._read_from_LOB(value) for field, value in row.items() }

    def _read_from_LOB(self, column):
        if isinstance(column, oracledb.LOB):
            return column.read()
        return column


class OracleConnection(oracledb.Connection):
    @classmethod
    def new_connection(cls):
        dns = oracledb.makedsn(
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
