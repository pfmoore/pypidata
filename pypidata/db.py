import json
import zlib

def add_json(db, name, serial, data):
    # TODO: What if data is None, or serial is 0?
    if data is not None:
        data = zlib.compress(data.encode("UTF-8"))

    db.execute(f"""\
        INSERT INTO json_data (
            name,
            serial,
            data
        )
        VALUES (:name, :serial, :data)
        ON CONFLICT(name) DO UPDATE SET
            serial = :serial,
            data = :data
        """,
        dict(name=name, serial=serial, data=data)
    )

def add_simple(db, name, serial, data):
    # TODO: What if data is None, or serial is 0?
    if data is not None:
        data = zlib.compress(data.encode("UTF-8"))

    db.execute(f"""\
        INSERT INTO simple_data (
            name,
            serial,
            data
        )
        VALUES (:name, :serial, :data)
        ON CONFLICT(name) DO UPDATE SET
            serial = :serial,
            data = :data
        """,
        dict(name=name, serial=serial, data=data)
    )

def add_packages(db, packages):
    # packages: (name, display_name, serial)
    SQL = """\
    INSERT INTO packages (name, display_name, last_serial)
    VALUES (:name, :display_name, :serial)
    ON CONFLICT(name) DO UPDATE SET
        display_name = :display_name,
        last_serial = :serial
        """
    db.executemany(
        SQL,
        [dict(name=n, display_name=d, serial=s) for (n, d, s) in packages]
    )

def add_changelogs(db, changelogs):
    # changelogs: (name, display_name, version, timestamp, action, serial)
    SQL = """\
    INSERT INTO changelog (
        name, display_name, version, timestamp, action, serial
    )
    VALUES (?, ?, ?, ?, ?, ?)
    """
    db.executemany(SQL, changelogs)

def get_json(db, name):
    # TODO: DB is attached as "raw"...
    c = db.execute("SELECT data FROM raw.json_data WHERE name = ?", (name,))
    row = c.fetchone()
    if row is None or not row[0]:
        return None
    return json.loads(zlib.decompress(row[0]).decode("utf-8"))

PROJECTS_SQL = """\
INSERT INTO projects (
    name,
    display_name,
    last_serial,
    author,
    author_email,
    bugtrack_url,
    classifiers,
    description,
    description_content_type,
    docs_url,
    download_url,
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
    :last_serial,
    :author,
    :author_email,
    :bugtrack_url,
    :classifiers,
    :description,
    :description_content_type,
    :docs_url,
    :download_url,
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
ON CONFLICT (name) DO UPDATE SET
    display_name = :display_name,
    last_serial = :last_serial,
    author = :author,
    author_email = :author_email,
    bugtrack_url = :bugtrack_url,
    classifiers = :classifiers,
    description = :description,
    description_content_type = :description_content_type,
    docs_url = :docs_url,
    download_url = :download_url,
    home_page = :home_page,
    keywords = :keywords,
    license = :license,
    maintainer = :maintainer,
    maintainer_email = :maintainer_email,
    package_url = :package_url,
    platform = :platform,
    project_url = :project_url,
    release_url = :release_url,
    requires_dist = :requires_dist,
    requires_python = :requires_python,
    summary = :summary,
    version = :version,
    yanked = :yanked,
    yanked_reason = :yanked_reason
"""

PROJECT_URLS_SQL = """\
INSERT INTO project_urls (
    project_name,
    url_type,
    url
)
VALUES (
    :project_name,
    :url_type,
    :url
)
ON CONFLICT (project_name, url_type) DO UPDATE SET
    url = :url
"""

PROJECT_FILES_SQL = """\
INSERT INTO project_files (
    project_name,
    version,
    comment_text,
    filename,
    has_sig,
    md5_digest,
    packagetype,
    python_version,
    requires_python,
    size,
    upload_time,
    upload_time_iso_8601,
    url,
    yanked,
    yanked_reason
)
VALUES (
    :project_name,
    :version,
    :comment_text,
    :filename,
    :has_sig,
    :md5_digest,
    :packagetype,
    :python_version,
    :requires_python,
    :size,
    :upload_time,
    :upload_time_iso_8601,
    :url,
    :yanked,
    :yanked_reason
)
ON CONFLICT (project_name, filename) DO UPDATE SET
    version = :version,
    comment_text = :comment_text,
    has_sig = :has_sig,
    md5_digest = :md5_digest,
    packagetype = :packagetype,
    python_version = :python_version,
    requires_python = :requires_python,
    size = :size,
    upload_time = :upload_time,
    upload_time_iso_8601 = :upload_time_iso_8601,
    url = :url,
    yanked = :yanked,
    yanked_reason = :yanked_reason
"""

FILE_DIGESTS_SQL = """\
INSERT INTO file_digests (
    file_id,
    digest_type,
    digest
)
VALUES (
    :file_id,
    :digest_type,
    :digest
)
ON CONFLICT (file_id, digest_type) DO UPDATE SET
    digest = :digest
"""

def write_package(db, name, package_data):
    info = package_data["info"]

    if info["classifiers"]:
        assert not any("\n" in c for c in info["classifiers"])
        classifiers = "\n".join(info["classifiers"])
    else:
        classifiers = None

    if info["requires_dist"]:
        assert not any("\n" in r for r in info["requires_dist"])
        requires_dist = "\n".join(info["requires_dist"])
    else:
        requires_dist = None

    projects_args = dict(
        name = name,
        display_name = info["name"],
        last_serial = package_data["last_serial"],
        author = info.get("author"),
        author_email = info.get("author_email"),
        bugtrack_url = info.get("bugtrack_url"),
        classifiers = classifiers,
        description = info.get("description"),
        description_content_type = info.get("description_content_type"),
        docs_url = info.get("docs_url"),
        download_url = info.get("download_url"),
        home_page = info.get("home_page"),
        keywords = info.get("keywords"),
        license = info.get("license"),
        maintainer = info.get("maintainer"),
        maintainer_email = info.get("maintainer_email"),
        package_url = info.get("package_url"),
        platform = info.get("platform"),
        project_url = info.get("project_url"),
        release_url = info.get("release_url"),
        requires_dist = requires_dist,
        requires_python = info.get("requires_python"),
        summary = info.get("summary"),
        version = info.get("version"),
        yanked = info.get("yanked"),
        yanked_reason = info.get("yanked_reason"),
    )

    db.execute(PROJECTS_SQL, projects_args)

    urls = info.get("project_urls")
    if urls:
        db.executemany(PROJECT_URLS_SQL, [dict(project_name=name, url_type=k, url=v) for k, v in urls.items()])

    releases = package_data["releases"]
    for rel in releases:
        for file in releases[rel]:
            project_files_args = dict(
                project_name = name,
                version = rel,
                comment_text = file.get("comment_text"),
                filename = file.get("filename"),
                has_sig = file.get("has_sig"),
                md5_digest = file.get("md5_digest"),
                packagetype = file.get("packagetype"),
                python_version = file.get("python_version"),
                requires_python = file.get("requires_python"),
                size = file.get("size"),
                upload_time = file.get("upload_time"),
                upload_time_iso_8601 = file.get("upload_time_iso_8601"),
                url = file.get("url"),
                yanked = file.get("yanked"),
                yanked_reason = file.get("yanked_reason"),
            )
            cursor = db.execute(PROJECT_FILES_SQL, project_files_args)
            file_id = cursor.lastrowid
            digests = file.get("digests")
            if digests:
                db.executemany(FILE_DIGESTS_SQL, [dict(file_id=file_id, digest_type=k, digest=v) for k, v in digests.items()])
