import sqlite3
import pickle
from pathlib import Path

DB = "PyPI.db"
PICKLE = Path("changelog.pickle")

# Open the database
conn = sqlite3.connect(DB)

# Get the saved data
with PICKLE.open("rb") as f:
    changelog = pickle.load(f)

with conn:
    conn.executemany(
        """\
            INSERT INTO changelog (name, version, timestamp, action, serial)
            VALUES (?, ?, ?, ?, ?)
        """,
        changelog
    )

conn.close()

