import sys
import json
from pathlib import Path
import sqlite3
from rich.progress import Progress, BarColumn, TimeRemainingColumn
import zlib
from .build_package import write_package

# conn = sqlite3.connect("PackageData.db")
# conn.execute("ATTACH DATABASE 'PyPI_raw.db' AS raw")
# 
# c = conn.cursor()
# c.execute("""\
#     SELECT
#         p.name
#     FROM
#         projects p,
#         raw.json_data j
#     WHERE
#         p.name = j.name and
#         p.last_serial != j.serial
# """)
# 
# names = [name for (name,) in c]
# 
# p = Progress(
#     "[progress.description]{task.description}",
#     BarColumn(),
#     "{task.completed}/{task.total}"
#     "[progress.percentage]{task.percentage:>3.0f}%",
#     TimeRemainingColumn(),
# )
# for name in p.track(names):
#     data, = conn.execute("SELECT data FROM raw.json_data WHERE name=?", (name,)).fetchone()
#     if not data:
#         continue
#     data = json.loads(zlib.decompress(data).decode("utf-8"))
#     write_package(conn, name, data)
#     conn.commit()


def get_package_names(db, args):
    names = []
    if args.file:
        if args.file == "-":
            text = sys.stdin.read()
        else:
            text = Path(args.file).read_text(encoding="utf-8")
        for name in text.splitlines():
            name = name.strip()
            if not name or name.startswith("#"):
                continue
            names.append(name)
    elif len(args.name) > 0:
        names = args.name[:]
    else:
        SQL = """\
            SELECT
                p.name
            FROM
                projects p,
                raw.json_data j
            WHERE
                p.name = j.name and
                p.last_serial != j.serial
            ORDER BY
                p.name
        """
        for row in db.execute(SQL):
            name, = row
            names.append(name)
    
    if args.limit:
        names = names[:args.limit]
    return names

def update(db, name):
    cursor = db.execute("SELECT data FROM raw.json_data WHERE name=?", (name,))
    data, = cursor.fetchone()
    if not data:
        return
    data = json.loads(zlib.decompress(data).decode("utf-8"))
    write_package(db, name, data)


def main(args):
    with sqlite3.connect(args.database) as db:
        print("Attaching the raw database...")
        db.execute("ATTACH DATABASE ? AS raw", (args.raw,))
        print("Getting packages to process...")
        names = get_package_names(db, args)
        print(f"Processing {len(names)} packages")

        if args.list:
            for name in names:
                print(name)
            return

        with Progress() as progress:
            t = progress.add_task("Updating...", total=len(names))
            for name in names:
                update(db, name)
                progress.update(t, advance=1)
        print("Committing changes")
        db.commit()
        print("Detaching the raw database")
        db.execute("DETACH DATABASE raw")
