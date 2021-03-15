CREATE TABLE IF NOT EXISTS projects (
    name TEXT PRIMARY KEY,
    display_name TEXT,
    timestamp INTEGER,
    last_serial INTEGER,
    author TEXT,
    author_email TEXT,
    bugtrack_url TEXT,
    classifiers TEXT,
    description TEXT,
    description_content_type TEXT,
    docs_url TEXT,
    download_url TEXT,
    home_page TEXT,
    keywords TEXT,
    license TEXT,
    maintainer TEXT,
    maintainer_email TEXT,
    package_url TEXT,
    platform TEXT,
    project_url TEXT,
    release_url TEXT,
    requires_dist TEXT,
    requires_python TEXT,
    summary TEXT,
    version TEXT,
    yanked INTEGER,
    yanked_reason TEXT
);

CREATE TABLE IF NOT EXISTS project_urls (
    project_name TEXT,
    url_type TEXT,
    url TEXT,
    CONSTRAINT project_urls_pk PRIMARY KEY (project_name, url_type)
);

CREATE TABLE IF NOT EXISTS project_files (
    file_id INTEGER PRIMARY KEY,
    project_name TEXT,
    version TEXT,
    comment_text TEXT,
    filename TEXT,
    has_sig INTEGER,
    md5_digest TEXT,
    packagetype TEXT,
    python_version TEXT,
    requires_python TEXT,
    size INTEGER,
    upload_time TEXT,
    upload_time_iso_8601 TEXT,
    url TEXT,
    yanked INTEGER,
    yanked_reason TEXT,
    CONSTRAINT project_files_uk UNIQUE (project_name, filename)
);

CREATE TABLE IF NOT EXISTS file_digests (
    file_id INTEGER REFERENCES project_files(file_id),
    digest_type TEXT,
    digest TEXT,
    CONSTRAINT file_digests_pk PRIMARY KEY (file_id, digest_type)
);