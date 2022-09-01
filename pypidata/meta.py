import argparse
import concurrent.futures
import io
import json
import sqlite3
import zlib
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from signal import SIGINT, signal
from urllib.request import Request, urlopen
from zipfile import ZipFile

from lazyfile import LazyFile
from packaging.utils import canonicalize_name, canonicalize_version
from rich.progress import BarColumn, Progress, TimeRemainingColumn

from .db_writer import DBWriter

# Get the metadata from a wheel by lazily reading just enough
# data from the URL.

def url_len(url):
    req = Request(url, method="HEAD")
    with urlopen(req) as f:
        return int(f.headers["Content-Length"])

def getter(url, lo, hi):
    req = Request(url, headers={"Range": f"bytes={lo}-{hi-1}"})
    with urlopen(req) as f:
        data = f.read()
        assert len(data) == hi-lo, f"Data ({lo}, {hi}) = {data!r}"
    return data

#def get_meta_partial(filename, url, db, complete=None):
#    try:
#        g = partial(getter, url)
#        size = url_len(url)
#        f = LazyFile(size, g)
#        z = ZipFile(f)
#        candidates = []
#        for name in z.namelist():
#            if not name.endswith(".dist-info/METADATA"):
#                continue
#            if "/" in name[:-19]:
#                continue
#            candidates.append(name)
#        if len(candidates) != 1:
#            print(f"Too many potential metadata files for {filename}: {candidates}")
#            return
#        name = candidates[0]
#        db.submit({"filename": filename, "data": zlib.compress(z.read(name))})
#    except Exception as e:
#        print("Error:", e)
#    if complete:
#        complete()

def get_meta(filename, url, db):
    try:
        with urlopen(url) as f:
            data = io.BytesIO(f.read())
        z = ZipFile(data)
        name, version, *_ = filename.split("-", 2)
        name = canonicalize_name(name)
        version = canonicalize_version(version)
        content = [
            {"name": info.filename, "size": info.file_size, "timestamp": info.date_time}
            for info in z.infolist()
        ]
        for file in z.namelist():
            if file.endswith(".dist-info/METADATA"):
                n, hyph, v = file[:-19].partition("-")
                if hyph == "-":
                    n = canonicalize_name(n)
                    v = canonicalize_version(v)
                    if n == name and v == version:
                        break
        else:
            print(
                f"{url} does not contain metadata file {name}-{version}.dist-info/METADATA:",
                [n for n in z.namelist() if n.endswith("METADATA")]
            )
            db.submit({"filename": filename, "content": json.dumps(content), "data": None})
            return
        metadata_content = z.read(file)
        db.submit({"filename": filename, "content": json.dumps(content), "data": zlib.compress(metadata_content)})
    except Exception as e:
        print("Error:", e)

SELECT = """\
    SELECT filename, url
    FROM pkg.project_files
    WHERE filename like '%.whl'
    AND filename NOT IN (SELECT filename FROM project_metadata)
"""

SELECT = """\
    SELECT filename, url
    FROM (
        SELECT
            json_extract(f.value, '$.filename') filename,
            json_extract(f.value, '$.url') url
        FROM pkg.simple_data, json_each(files) f
    )
    WHERE filename like '%.whl'
    AND filename NOT IN (SELECT filename FROM project_metadata)
"""

def get_wheels(pkg: str, meta: str):
    with sqlite3.connect(meta) as conn:
        conn.execute("ATTACH DATABASE ? AS pkg", (pkg,))
        rows = conn.execute(SELECT).fetchall()
    # Return rows as a physical list so we can close the
    # database connection before returning
    return rows

PROGRESS_DISPLAY = [
    "[progress.description]{task.description}",
    BarColumn(),
    "[progress.percentage]{task.percentage:>3.0f}% ({task.completed}/{task.total})",
    TimeRemainingColumn(),
]

UPD = """\
INSERT INTO project_metadata (filename, content, metadata)
VALUES (:filename, :content, :data)
"""

def main(args: argparse.Namespace):
    print("Fetching list of wheels")
    rows = get_wheels(args.raw, args.database)
    if args.limit:
        print(f"Processing {args.limit} out of {len(rows)} wheels")
        rows = rows[:args.limit]
    else:
        print(f"Processing {len(rows)} wheels")

    db = DBWriter(args.database, UPD)
    db.start()

    with Progress(*PROGRESS_DISPLAY) as progress:
        submit = progress.add_task("Submit tasks", total=len(rows))
        task = progress.add_task("Fetch wheels", total=len(rows))
        ins = progress.add_task("Insert records", total=len(rows))
        def task():
            get_meta(filename, url, db)
            progress.update(task, advance=1)
            progress.update(ins, completed=db.inserted)
        with ThreadPoolExecutor() as executor:
            try:
                results = []
                for filename, url in rows:
                    results.append(executor.submit(task))
                    progress.update(submit, advance=1)
                # If we don't wait here for the futures, the executor waits in __exit__,
                # but that's too late to catch keyboard interrupts, so Ctrl-C hangs the
                # process.
                concurrent.futures.wait(results)
            except KeyboardInterrupt:
                executor.shutdown(False, cancel_futures=True)
    db.stop()
    print("Waiting for DB updates to complete")
    db.join()
    print("Done")

if __name__ == "__main__":
    metadata = Path(__file__).parent / "Metadata.db"
    raw = Path(__file__).parent / "PyPI_raw.db"
    def parse_cmdline(args=None):
        parser = argparse.ArgumentParser(description="Update the metadata database")
        parser.add_argument("--limit", "-l", type=int, help="Maximum number of files to update")
        parser.add_argument("--database", "--DB", default=str(metadata), help="The database to update")
        parser.add_argument("--raw", default=str(raw), help="The raw PyPI data")

        return parser.parse_args(args)

    args = parse_cmdline()

    main(args)