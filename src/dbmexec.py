import os
import sqlcipher3
from .pysql import create_conn


class DBConnectionError(Exception):
    pass


class DBMexec:
    def __init__(self, db_path, dbtarget, email, ui_logger=None):
        self.db_path = db_path
        self.key_file = dbtarget
        self.email = email
        self.ui_logger = ui_logger
        self.db = None
        self.cursor = None
        self.dbname = os.path.basename(db_path)

        self._conn_context = False

    def log(self, message):
        if self.ui_logger:
            self.ui_logger.appendPlainText(message)
        else:
            print(message)

    def __enter__(self):
        if not self.connect():
            raise DBConnectionError(f"Failed to connect to database: {self.db_path}")
        self._conn_context = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn_context:
            if exc_type is None:
                self.db.commit()
            else:
                self.db.rollback()
            self.close()
        self._conn_context = False

    def connect(self):
        try:

            self.db = create_conn(self.db_path, self.key_file, self.email, key=None)

            self.cursor = self.db.cursor()
        except sqlcipher3.Error as e:
            self.log(f"couldnt connect to {self.dbname}: {e}")
            return False
        return True

    def close(self):
        if self.db:
            self.db.close()
        self.db = None
        self.cursor = None

    def table_exists(self, table_name):
        if not self.db:
            return False
        self.cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        return self.cursor.fetchone() is not None

    def table_has_data(self, table_name):
        if not self.table_exists(table_name):
            return False

        sql = f"SELECT 1 FROM {table_name} LIMIT 1"
        try:
            self.cursor.execute(sql)
        except sqlcipher3.Error as e:
            self.log(f"SQL Error in table_has_data: {e}\n {sql}")
            return False
        return self.cursor.fetchone() is not None

    def execute(self, sql, params=None):
        if not self.db:
            raise DBConnectionError("No open connection for execute()")

        try:
            if params:
                self.cursor.execute(sql, params)
            else:
                self.cursor.execute(sql)
        except sqlcipher3.Error as e:
            self.log(f"SQL Error: {e}\n {sql}")
            return None
        return self.cursor

    def tables(self):
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [t[0] for t in self.cursor.fetchall()]

    def drop_table(self, table_name):
        if self.table_exists(table_name):
            return self.execute(f"DROP TABLE IF EXISTS {table_name}")
        return False

    def clear_table(self, table_name):
        if self.table_exists(table_name):
            if not self.execute(f"DELETE FROM {table_name}"):
                self.log(f"Failed to clear data from {table_name}")
                return False
            try:
                self.execute("DELETE FROM sqlite_sequence WHERE name = ?", (table_name,))
            except Exception as e:
                self.log(f"Warning: could not reset sequence for {table_name}: {e}")
        return True
