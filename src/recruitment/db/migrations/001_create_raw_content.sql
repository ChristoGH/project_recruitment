-- Migration: Create raw_content table
CREATE TABLE IF NOT EXISTS raw_content (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL,
    content TEXT NOT NULL,
    search_id TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    processed BOOLEAN DEFAULT 0
); 