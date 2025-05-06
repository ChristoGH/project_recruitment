"""Database operations for recruitment data."""
import sqlite3
import os
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
import json
from datetime import datetime
import logging

# Set up logging
logger = logging.getLogger(__name__)

class DatabaseError(Exception):
    """Custom exception for database operations."""
    pass

class RecruitmentDatabase:
    """Handles database operations for recruitment data."""
    
    def __init__(self, db_path: Optional[str] = None):
        """Initialize database connection."""
        if db_path is None:
            # Get the project root directory (3 levels up from this file)
            project_root = Path(__file__).parent.parent.parent.parent
            db_dir = project_root / "databases"
            db_dir.mkdir(exist_ok=True, parents=True)
            self.db_path = str(db_dir / "recruitment.db")
        else:
            self.db_path = db_path
            
        logger.info(f"Initializing database at: {self.db_path}")
        self.init_db()
    
    def init_db(self) -> None:
        """Initialize the database and create necessary tables."""
        try:
            logger.info("Creating database tables...")
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create tables
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS urls (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        url TEXT UNIQUE NOT NULL,
                        domain TEXT NOT NULL,
                        source TEXT,
                        status TEXT DEFAULT 'pending',
                        error_count INTEGER DEFAULT 0,
                        error_message TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                logger.info("Created urls table")
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS jobs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        title TEXT NOT NULL,
                        description TEXT,
                        posted_date TEXT,
                        job_type TEXT,
                        url_id INTEGER,
                        company_id INTEGER,
                        location_id INTEGER,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (url_id) REFERENCES urls(id),
                        FOREIGN KEY (company_id) REFERENCES companies(id),
                        FOREIGN KEY (location_id) REFERENCES locations(id)
                    )
                """)
                logger.info("Created jobs table")
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS companies (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        website TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                logger.info("Created companies table")
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS locations (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        city TEXT,
                        state TEXT,
                        country TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                logger.info("Created locations table")
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS skills (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT UNIQUE NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                logger.info("Created skills table")
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS job_skills (
                        job_id INTEGER,
                        skill_id INTEGER,
                        PRIMARY KEY (job_id, skill_id),
                        FOREIGN KEY (job_id) REFERENCES jobs(id),
                        FOREIGN KEY (skill_id) REFERENCES skills(id)
                    )
                """)
                logger.info("Created job_skills table")
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS qualifications (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT UNIQUE NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                logger.info("Created qualifications table")
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS job_qualifications (
                        job_id INTEGER,
                        qualification_id INTEGER,
                        PRIMARY KEY (job_id, qualification_id),
                        FOREIGN KEY (job_id) REFERENCES jobs(id),
                        FOREIGN KEY (qualification_id) REFERENCES qualifications(id)
                    )
                """)
                logger.info("Created job_qualifications table")
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS attributes (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT UNIQUE NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                logger.info("Created attributes table")
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS job_attributes (
                        job_id INTEGER,
                        attribute_id INTEGER,
                        PRIMARY KEY (job_id, attribute_id),
                        FOREIGN KEY (job_id) REFERENCES jobs(id),
                        FOREIGN KEY (attribute_id) REFERENCES attributes(id)
                    )
                """)
                logger.info("Created job_attributes table")
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS duties (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        description TEXT UNIQUE NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                logger.info("Created duties table")
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS job_duties (
                        job_id INTEGER,
                        duty_id INTEGER,
                        PRIMARY KEY (job_id, duty_id),
                        FOREIGN KEY (job_id) REFERENCES jobs(id),
                        FOREIGN KEY (duty_id) REFERENCES duties(id)
                    )
                """)
                logger.info("Created job_duties table")
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS benefits (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        description TEXT UNIQUE NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                logger.info("Created benefits table")
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS job_benefits (
                        job_id INTEGER,
                        benefit_id INTEGER,
                        PRIMARY KEY (job_id, benefit_id),
                        FOREIGN KEY (job_id) REFERENCES jobs(id),
                        FOREIGN KEY (benefit_id) REFERENCES benefits(id)
                    )
                """)
                logger.info("Created job_benefits table")
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS agencies (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        website TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                logger.info("Created agencies table")
                
                conn.commit()
                logger.info("Database initialization completed successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            raise DatabaseError(f"Failed to initialize database: {str(e)}")
    
    def initialize(self) -> None:
        """Alias for init_db to maintain compatibility."""
        self.init_db()
    
    def insert_url(self, url: str, domain: str, source: str) -> int:
        """Insert a URL into the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO urls (url, domain, source) VALUES (?, ?, ?)",
                    (url, domain, source)
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert URL: {str(e)}")
    
    def update_url_processing_status(self, url_id: int, status: str, error_message: Optional[str] = None) -> None:
        """Update the processing status of a URL."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                if error_message:
                    cursor.execute(
                        "UPDATE urls SET status = ?, error_message = ?, error_count = error_count + 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (status, error_message, url_id)
                    )
                else:
                    cursor.execute(
                        "UPDATE urls SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                        (status, url_id)
                    )
                conn.commit()
        except Exception as e:
            raise DatabaseError(f"Failed to update URL status: {str(e)}")
    
    def insert_job(self, title: str, description: str, posted_date: str, job_type: str, url_id: int, company_id: int, location_id: int) -> int:
        """Insert a job into the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO jobs (title, description, posted_date, job_type, url_id, company_id, location_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (title, description, posted_date, job_type, url_id, company_id, location_id)
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert job: {str(e)}")
    
    def insert_company(self, name: str, website: Optional[str] = None) -> int:
        """Insert a company into the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO companies (name, website) VALUES (?, ?)",
                    (name, website)
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert company: {str(e)}")
    
    def insert_location(self, city: str, state: str, country: str) -> int:
        """Insert a location into the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO locations (city, state, country) VALUES (?, ?, ?)",
                    (city, state, country)
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert location: {str(e)}")
    
    def insert_skill(self, name: str) -> int:
        """Insert a skill into the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO skills (name) VALUES (?)",
                    (name,)
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert skill: {str(e)}")
    
    def link_job_skill(self, job_id: int, skill_id: int) -> None:
        """Link a job to a skill."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO job_skills (job_id, skill_id) VALUES (?, ?)",
                    (job_id, skill_id)
                )
                conn.commit()
        except Exception as e:
            raise DatabaseError(f"Failed to link job to skill: {str(e)}")
    
    def insert_qualification(self, name: str) -> int:
        """Insert a qualification into the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO qualifications (name) VALUES (?)",
                    (name,)
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert qualification: {str(e)}")
    
    def link_job_qualification(self, job_id: int, qualification_id: int) -> None:
        """Link a job to a qualification."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO job_qualifications (job_id, qualification_id) VALUES (?, ?)",
                    (job_id, qualification_id)
                )
                conn.commit()
        except Exception as e:
            raise DatabaseError(f"Failed to link job to qualification: {str(e)}")
    
    def insert_attribute(self, name: str) -> int:
        """Insert an attribute into the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO attributes (name) VALUES (?)",
                    (name,)
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert attribute: {str(e)}")
    
    def link_job_attribute(self, job_id: int, attribute_id: int) -> None:
        """Link a job to an attribute."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO job_attributes (job_id, attribute_id) VALUES (?, ?)",
                    (job_id, attribute_id)
                )
                conn.commit()
        except Exception as e:
            raise DatabaseError(f"Failed to link job to attribute: {str(e)}")
    
    def insert_duty(self, description: str) -> int:
        """Insert a duty into the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO duties (description) VALUES (?)",
                    (description,)
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert duty: {str(e)}")
    
    def link_job_duty(self, job_id: int, duty_id: int) -> None:
        """Link a job to a duty."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO job_duties (job_id, duty_id) VALUES (?, ?)",
                    (job_id, duty_id)
                )
                conn.commit()
        except Exception as e:
            raise DatabaseError(f"Failed to link job to duty: {str(e)}")
    
    def insert_benefit(self, description: str) -> int:
        """Insert a benefit into the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO benefits (description) VALUES (?)",
                    (description,)
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert benefit: {str(e)}")
    
    def link_job_benefit(self, job_id: int, benefit_id: int) -> None:
        """Link a job to a benefit."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO job_benefits (job_id, benefit_id) VALUES (?, ?)",
                    (job_id, benefit_id)
                )
                conn.commit()
        except Exception as e:
            raise DatabaseError(f"Failed to link job to benefit: {str(e)}")
    
    def insert_agency(self, name: str, website: Optional[str] = None) -> int:
        """Insert an agency into the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO agencies (name, website) VALUES (?, ?)",
                    (name, website)
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert agency: {str(e)}")
    
    def get_unprocessed_urls(self) -> List[Dict[str, Any]]:
        """Get all unprocessed URLs from the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id, url, domain, source FROM urls WHERE status = 'pending'"
                )
                rows = cursor.fetchall()
                return [
                    {
                        "id": row[0],
                        "url": row[1],
                        "domain": row[2],
                        "source": row[3]
                    }
                    for row in rows
                ]
        except Exception as e:
            raise DatabaseError(f"Failed to get unprocessed URLs: {str(e)}")
    
    def save_processed_url(self, url: str, content: str, transformed: Dict[str, Any], timestamp: str) -> None:
        """Save processed URL data to the database."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO processed_content (url, content, skills, confidence, source, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                    (url, content, json.dumps(transformed.get("skills", [])), transformed.get("confidence", 0.0), transformed.get("source", ""), timestamp)
                )
                conn.commit()
        except Exception as e:
            raise DatabaseError(f"Failed to save processed URL: {str(e)}")
    
    def check_connection(self) -> bool:
        """Check if the database connection is working."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                return True
        except Exception:
            return False
    
    def link_job_location(self, job_id: int, location_id: int) -> None:
        """Link a job to a location."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE jobs SET location_id = ? WHERE id = ?",
                    (location_id, job_id)
                )
                conn.commit()
        except Exception as e:
            raise DatabaseError(f"Failed to link job to location: {str(e)}") 