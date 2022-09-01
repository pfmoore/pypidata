import queue
import sqlite3
import threading


class DBWriter(threading.Thread):
    def __init__(self, dbname, SQL):
        self.dbname = dbname
        self.SQL = SQL
        self.queue = queue.Queue()
        self.stop_event = threading.Event()
        self.inserted = 0
        super().__init__()

    def submit(self, vals):
        self.queue.put(vals)

    def pending(self):
        # Drain the current items from the queue, yielding them all
        while not self.queue.empty():
            try:
                vals = self.queue.get_nowait()
            except queue.Empty:
                break
            yield vals

    def stop(self, abort=False):
        self.stop_event.set()
        if abort:
            # Drain the queue
            for _ in self.pending():
                pass

    def run(self):
        with sqlite3.connect(self.dbname) as conn:
            while not self.stop_event.is_set():
                records = list(self.pending())
                #print(f"Inserting {len(records)} rows")
                if records:
                    conn.executemany(self.SQL, records)
                    conn.commit()
                    self.inserted += len(records)
