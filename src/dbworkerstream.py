import logging
# import sys
from PySide6.QtCore import Signal, QThread
from PySide6.QtSql import QSqlDatabase, QSqlQuery


class DbWorkerIncremental(QThread):
    headers_ready = Signal(list)
    batch_ready = Signal(list)
    finished_loading = Signal(int)

    log = Signal(str)
    exception = Signal(object, object, object)

    def __init__(self, db_path, table, sys_tables, superimpose=False, batch_size=500, log_label="dbstreamerRUNERR"):
        super().__init__()

        self.logger = logging.getLogger(log_label)

        self.db_path = db_path
        self.table = table
        self.sys_tables = sys_tables
        self.batch_size = batch_size
        self.superimpose = superimpose

    def set_superimpose_query(self):

        sys_a, sys_b = self.sys_tables

        return f"""
        WITH b_counts AS (
            SELECT filename, COUNT(*) AS num_changes
            FROM {sys_b}
            GROUP BY filename
        )
        SELECT a.*, 0 AS sort_order, bc.num_changes
        FROM {sys_a} a
        JOIN b_counts bc ON a.filename = bc.filename

        UNION ALL

        SELECT b.*, 1 AS sort_order, bc.num_changes
        FROM {sys_b} b
        JOIN b_counts bc ON b.filename = bc.filename

        ORDER BY num_changes DESC, filename, sort_order, count;
        """
        # sql_join = f"""
        # WITH change_counts AS (
        #     SELECT filename, COUNT(*) AS num_changes
        #     FROM {sys_b}
        #     GROUP BY filename
        # )
        # SELECT a.*, c.num_changes, 0 AS sort_order
        # FROM {sys_a} a
        # LEFT JOIN change_counts c ON a.filename = c.filename

        # UNION ALL

        # SELECT b.*, c.num_changes, 1 AS sort_order
        # FROM {sys_b} b
        # LEFT JOIN change_counts c ON b.filename = c.filename

        # ORDER BY num_changes DESC, filename, sort_order, count;
        # """
    def run(self):
        db = None
        query = None
        query_error = ""
        res = 1
        try:
            conn_name = f"worker_{self.table}_{id(self)}"  # unique per thread
            db = QSqlDatabase.addDatabase("QSQLITE", conn_name)
            db.setDatabaseName(self.db_path)

            if db.open():

                query = QSqlQuery(db)
                if not self.superimpose:
                    query.exec(f"SELECT * FROM {self.table}")
                else:

                    sql_join = self.set_superimpose_query()

                    query.exec(sql_join)

                if query.isActive():

                    headers = [query.record().fieldName(i) for i in range(query.record().count())]
                    self.headers_ready.emit(headers)

                    batch = []
                    while query.next():
                        row_data = [query.value(i) for i in range(query.record().count())]
                        batch.append(row_data)
                        if len(batch) >= self.batch_size:
                            self.batch_ready.emit(batch)
                            batch = []
                    if batch:
                        self.batch_ready.emit(batch)
                    res = 0
                else:
                    res = 2
                    err = query.lastError().text()
                    self.log.emit(f"Thread: Failed to query {self.table}: {err}")
            else:
                self.log.emit(f"Thread: Failed to open database {self.db_path} {db.lastError().text()}")

        except Exception as e:

            res = 1
            errorMsg = f"Sql exception: type: {type(e).__name__} err: {e}"  # {traceback.format_exc()}
            self.log.emit(errorMsg)

            if query is not None:
                query_error = f'\n last query error: {query.lastError().text().strip()}'
            self.logger.error(f"{errorMsg}{query_error}", exc_info=True)

            # exc_type, exc_value, exc_traceback = sys.exc_info()
            # self.exception.emit(exc_type, exc_value, exc_traceback)
        finally:
            if query is not None:
                query = None  # del query
            if db is not None:
                db.close()
                del db
            QSqlDatabase.removeDatabase(conn_name)
            self.finished_loading.emit(res)
