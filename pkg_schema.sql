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
    downloads_last_day INTEGER,
    downloads_last_week INTEGER,
    downloads_last_month INTEGER,
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

