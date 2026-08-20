import logging
from PySide6.QtCore import Signal, QThread
from .pysql import create_conn


class DbWorkerIncremental(QThread):
    headers_ready = Signal(list)
    batch_ready = Signal(list)
    finished_loading = Signal(int)

    log = Signal(str)
    exception = Signal(object, object, object)

    def __init__(self, db_path, dbtarget, email, table, sys_tables=None, cache_tables=None, superimpose=False, batch_size=500, order_by=None, order_dir="ASC", log_label="dbstreamerRUNERR"):
        super().__init__()

        self.logger = logging.getLogger(log_label)

        self.db_path = db_path
        self.key_file = dbtarget
        self.email = email
        self.table = table
        self.sys_tables = sys_tables
        self.cache_tables = cache_tables
        self.batch_size = batch_size
        self.superimpose = superimpose
        self.order_by = order_by
        self.order_dir = order_dir if order_dir in ("ASC", "DESC") else "ASC"

    def set_superimpose_query(self):

        if self.sys_tables:
            # drops sys_a rows for filenames with no entry in sys_b
            # only files that changed or appear in sys_b are included
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
        elif self.cache_tables:
            # sort_order so cache_table comes before systimeche entries for each file. num_changes can come out
            cache_table, systimeche = self.cache_tables
            return f"""
            WITH changed AS (
                SELECT DISTINCT c.filename
                FROM {cache_table} c
                JOIN {systimeche} s ON s.filename = c.filename
                WHERE c.modified_time <> s.modified_time
            )
            SELECT c.*, 0 AS sort_order, 1 AS num_changes
            FROM {cache_table} c
            WHERE EXISTS (
                SELECT 1 FROM changed ch WHERE ch.filename = c.filename
            )

            UNION ALL

            SELECT s.*, 1 AS sort_order, 1 AS num_changes
            FROM {systimeche} s
            WHERE EXISTS (
                SELECT 1 FROM changed ch WHERE ch.filename = s.filename
            )

            ORDER BY filename, sort_order, idx_count;
            """

    def run(self):

        res = 1

        try:
            conn = create_conn(self.db_path, self.key_file, self.email)
            cur = conn.cursor()

            if not self.superimpose:

                if self.order_by:
                    cur.execute(f'SELECT * FROM {self.table} ORDER BY "{self.order_by}" {self.order_dir}')
                else:
                    cur.execute(f"SELECT * FROM {self.table}")

                # cur.execute(f"SELECT * FROM {self.table}")  # 08/14/2026 commented out. added sorting with sql

            else:

                sql_join = self.set_superimpose_query()
                if sql_join:
                    cur.execute(sql_join)

            headers = [col[0] for col in cur.description]
            self.headers_ready.emit(headers)

            rows = cur.fetchall()
            if rows:
                for row in rows:

                    batch = []

                    if self.isInterruptionRequested():
                        res = 7
                        break
                    row_data = list(row)
                    batch.append(row_data)
                    if len(batch) >= self.batch_size:
                        self.batch_ready.emit(batch)
                        batch = []
                    if batch:
                        self.batch_ready.emit(batch)
                    if res != 7:
                        res = 0
            else:
                self.log.emit(f"Thread: query returned None {self.db_path}")

        except Exception as e:

            res = 2
            errorMsg = f"Sql exception: type: {type(e).__name__} err: {e}"  # {traceback.format_exc()}
            self.log.emit(errorMsg)

            self.logger.error(f"{errorMsg}", exc_info=True)

            # exc_type, exc_value, exc_traceback = sys.exc_info()
            # self.exception.emit(exc_type, exc_value, exc_traceback)
        finally:
            self.finished_loading.emit(res)
