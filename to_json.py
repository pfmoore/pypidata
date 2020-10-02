import re
import json
import sqlite3
from pathlib import Path

def normalize(name):
    return re.sub(r"[-_.]+", "-", name).lower()

def read_jsons():
    conn = sqlite3.connect("PyPI.db")
    c = conn.execute("SELECT name, timestamp, json FROM package_data_json")
    for name, timestamp, json_val in c:
        data = json.loads(json_val)
        info = data["info"]
        last_serial = data["last_serial"]
        releases = data["releases"]
        assert data["urls"] == releases[info["version"]]

        assert normalize(name) == normalize(info["name"])

        assert set(info["downloads"].keys()) == {"last_day", "last_week", "last_month"}
        info["downloads_last_day"] = info["downloads"]["last_day"]
        info["downloads_last_week"] = info["downloads"]["last_week"]
        info["downloads_last_month"] = info["downloads"]["last_month"]
        del info["downloads"]

        # For now, let's not worry about splitting these two up
        if info["classifiers"]:
            assert not any("\n" in c for c in info["classifiers"])
            info["classifiers"] = "\n".join(info["classifiers"])
        else:
            info["classifiers"] = None

        if info["requires_dist"]:
            assert not any("\n" in r for r in info["requires_dist"])
            info["requires_dist"] = "\n".join(info["requires_dist"])
        else:
            info["requires_dist"] = None


        yield normalize(name), name, timestamp, last_serial, info, releases

SQL = """\
INSERT INTO projects (
    name,
    display_name,
    timestamp,
    last_serial,
    author,
    author_email,
    bugtrack_url,
    classifiers,
    description,
    description_content_type,
    docs_url,
    download_url,
    downloads_last_day,
    downloads_last_week,
    downloads_last_month,
    home_page,
    keywords,
    license,
    maintainer,
    maintainer_email,
    package_url,
    platform,
    project_url,
    release_url,
    requires_dist,
    requires_python,
    summary,
    version,
    yanked,
    yanked_reason
)
VALUES (
    :name,
    :display_name,
    :timestamp,
    :last_serial,
    :author,
    :author_email,
    :bugtrack_url,
    :classifiers,
    :description,
    :description_content_type,
    :docs_url,
    :download_url,
    :downloads_last_day,
    :downloads_last_week,
    :downloads_last_month,
    :home_page,
    :keywords,
    :license,
    :maintainer,
    :maintainer_email,
    :package_url,
    :platform,
    :project_url,
    :release_url,
    :requires_dist,
    :requires_python,
    :summary,
    :version,
    :yanked,
    :yanked_reason
)
"""

def insert_data(conn, name, display_name, timestamp, last_serial, info, releases):
    params = info.copy()
    params["name"] = name
    params["display_name"] = info["name"]
    params["timestamp"] = timestamp
    params["last_serial"] = last_serial
    try:
        conn.execute(SQL, params)
    except sqlite3.Error:
        from pprint import pprint
        pprint(params)
        raise

if __name__ == "__main__":
    from rich.progress import track
    # Just check the data parses for now
    conn = sqlite3.connect("PyPI.db")
    c = conn.cursor()
    n, = c.execute("SELECT count(*) FROM package_data_json").fetchone()
    print("Processing", n)

    conn = sqlite3.connect(":memory:")
    conn.executescript(Path("pkg_schema.sql").read_text())
    for name, display_name, timestamp, last_serial, info, releases in track(read_jsons(), total=n):
        insert_data(conn, name, display_name, timestamp, last_serial, info, releases)

    #save = sqlite3.connect("PyPI_packages.db")
    #conn.backup(save)
