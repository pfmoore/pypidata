CREATE TABLE IF NOT EXISTS json_data (
    name TEXT PRIMARY KEY,
    serial INT NOT NULL,
    data TEXT
);
CREATE TABLE IF NOT EXISTS simple_data (
    name TEXT PRIMARY KEY,
    serial INT NOT NULL,
    data TEXT
);
CREATE TABLE IF NOT EXISTS changelog (
  name TEXT,
  display_name TEXT,
  serial INT PRIMARY KEY,
  version TEXT,
  timestamp INT,
  action TEXT
);
CREATE INDEX changelog_i1 ON changelog (name);
CREATE TABLE IF NOT EXISTS packages (
  name TEXT PRIMARY KEY,
  display_name TEXT,
  last_serial INT NOT NULL
);
CREATE INDEX packages_i1 on packages(last_serial);
