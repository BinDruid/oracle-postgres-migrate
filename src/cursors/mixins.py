class CommonCursor:
    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, value: str):
        self._table_name = value

    @property
    def _fetch_table_query(self):
        return f"select * from {self.table_name}"
