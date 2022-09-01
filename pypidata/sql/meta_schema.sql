CREATE TABLE IF NOT EXISTS project_metadata (
        filename VARCHAR NOT NULL,
        content TEXT,
        metadata BLOB,
        PRIMARY KEY (filename)
);
