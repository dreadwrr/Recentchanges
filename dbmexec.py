import os
from PySide6.QtSql import QSqlDatabase, QSqlQuery


class DBConnectionError(Exception):
    pass


class DBMexec:
    def __init__(self, db_path, conn_name="sq_9", ui_logger=None):
        self.db_path = db_path
        self.conn_name = conn_name
        self.ui_logger = ui_logger
        self.db = None
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
        self.db.transaction()
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
        if QSqlDatabase.contains(self.conn_name):
            self.db = QSqlDatabase.database(self.conn_name)
        else:
            self.db = QSqlDatabase.addDatabase("QSQLITE", self.conn_name)
            self.db.setDatabaseName(self.db_path)

        if not self.db.isOpen() and not self.db.open():
            err = self.db.lastError().text()
            self.log(f"couldnt connect to {self.dbname}: {err}")
            return False
        return True

    def close(self):
        if self.db and self.db.isOpen():
            self.db.close()
        self.remove_conn()

    def remove_conn(self):
        if self.conn_name in QSqlDatabase.connectionNames():
            # del self.db
            self.db = None
            QSqlDatabase.removeDatabase(self.conn_name)

    def table_exists(self, table_name):
        return table_name in self.db.tables() if self.db and self.db.isOpen() else False

    def table_has_data(self, table_name):
        if not self.table_exists(table_name):
            return False

        query = QSqlQuery(self.db)
        sql = f"SELECT 1 FROM {table_name} LIMIT 1"

        if not query.exec(sql):
            self.log(f"SQL Error in table_has_data: {query.lastError().text()}\n {sql}")
            return False
        return query.next()

    def execute(self, sql, params=None):
        if not self.db or not self.db.isOpen():
            raise DBConnectionError("No open connection for execute()")

        query = QSqlQuery(self.db)
        if params:
            query.prepare(sql)
            for key, value in params.items():
                query.bindValue(f":{key}", value)
            ok = query.exec()
        else:
            ok = query.exec(sql)

        if not ok:
            self.log(f"SQL Error: {query.lastError().text()}\n {sql}")
            return None
        return query

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
                self.execute("DELETE FROM sqlite_sequence WHERE name = :name", {"name": table_name})
            except Exception as e:
                self.log(f"Warning: could not reset sequence for {table_name}: {e}")
        return True
