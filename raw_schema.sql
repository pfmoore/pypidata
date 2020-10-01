CREATE TABLE IF NOT EXISTS packages(
    name TEXT PRIMARY KEY,
    display_name TEXT,
    last_serial INT DEFAULT 0,
    url TEXT,
    gpg_sig TEXT,
    requires_python TEXT
);

CREATE TABLE IF NOT EXISTS changelog(
    name TEXT,
    display_name TEXT,
    version TEXT,
    timestamp INT,
    action TEXT,
    serial INT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS package_data_json(
    name TEXT PRIMARY KEY,
    timestamp INT,
    json TEXT
);

CREATE TABLE IF NOT EXISTS package_data_simple(
    name TEXT PRIMARY KEY,
    timestamp INT,
    page TEXT
);
