import re
from pathlib import Path
from rich.progress import Progress
import xmlrpc.client
import sqlite3

def normalize(name):
    return re.sub(r"[-_.]+", "-", name).lower()

DB = "PyPI.db"
URL = "https://pypi.org/pypi"
pypi = xmlrpc.client.ServerProxy(URL)

# Open the database
conn = sqlite3.connect(DB)

since, = conn.execute("SELECT max(serial) FROM changelog").fetchone()

def params(batch):
    for n, v, t, a, s in batch:
        yield normalize(n), n, v, t, a, s

with Progress() as progress:
    latest = pypi.changelog_last_serial()
    print(f"Fetching {since}..{latest}")
    task = progress.add_task("[red]Getting changelog...", total=latest)
    while True:
        progress.update(task, completed=since)
        next_batch = pypi.changelog_since_serial(since)
        if not next_batch:
            break
        next_since = max(c[-1] for c in next_batch)
        with conn:
            conn.executemany("""\
                    INSERT INTO changelog (
                        name, display_name, version, timestamp, action, serial
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                params(next_batch)
            )
        since = next_since

conn.close()
