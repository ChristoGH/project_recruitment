-- Add raw_content table
CREATE TABLE IF NOT EXISTS raw_content (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (url_id) REFERENCES urls(id)
);

-- Add index for faster lookups
CREATE INDEX IF NOT EXISTS idx_raw_content_url_id ON raw_content(url_id); 