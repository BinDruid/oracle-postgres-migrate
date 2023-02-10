import os
import cx_Oracle


class OracleCursor(cx_Oracle.Cursor):
    def desc(self):
        pass


class OracleConnection(cx_Oracle.Connection):
    def __init__(self):
        dns = cx_Oracle.makedsn(
            os.environ["ORACLE_URL"],
            os.environ["ORACLE_PORT"],
            service_name=os.environ["ORACLE_SERVICE"],
        )
        super().__init__(
            user=os.environ["ORACLE_URSER"],
            password=os.environ["ORACLE_PASS"],
            dsn=dns,
            encoding="UTF-8",
        )

    def cursor(self):
        return OracleCursor(self)
