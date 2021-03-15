import re
from rich.progress import Progress
import xmlrpc.client
import sqlite3
import time

def normalize(name):
    return re.sub(r"[-_.]+", "-", name).lower()

def params(batch):
    for n, v, t, a, s in batch:
        yield normalize(n), n, v, t, a, s

class RateLimitedServerProxy(xmlrpc.client.ServerProxy):
    def __getattr__(self, name):
        time.sleep(1)
        return super(RateLimitedServerProxy, self).__getattr__(name)

def main(args):
    URL = "https://pypi.org/pypi"
    pypi = RateLimitedServerProxy(URL)

    # Open the database
    conn = sqlite3.connect(args.database)

    since, = conn.execute("SELECT max(serial) FROM changelog").fetchone()
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
