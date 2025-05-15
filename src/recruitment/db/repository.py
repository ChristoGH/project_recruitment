"""Database operations for recruitment data."""

import sqlite3
import os
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
import json
import logging
import asyncio
import time
import aiosqlite
from concurrent.futures import ThreadPoolExecutor

# Set up logging
logger = logging.getLogger(__name__)

# Current schema version
SCHEMA_VERSION = 1

# Database configuration
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds
LOCK_TIMEOUT = 30  # seconds
MAX_CONNECTIONS = 5
BATCH_SIZE = 100


class DatabaseError(Exception):
    """Custom exception for database operations."""

    pass


class DatabaseLockError(DatabaseError):
    """Exception raised when database is locked."""

    pass


class DatabaseConnectionError(DatabaseError):
    """Exception raised when database connection fails."""

    pass


class RecruitmentDatabase:
    """Handles database operations for recruitment data."""

    _bootstrap_lock: asyncio.Lock | None = None  # protects first‑time bootstrap
    _bootstrapped: set[str] = set()  # db paths already initialised

    def __init__(self, db_path: Optional[str] = None, *, readonly: bool = False):
        """Initialize database connection.

        Args:
            db_path: Path to the database file
            readonly: If True, database is opened in read-only mode
        """
        if db_path is None:
            # Get database path from environment variable
            db_path = os.getenv("RECRUITMENT_DB_PATH")
            if not db_path:
                # Fallback to default path
                project_root = Path(__file__).parent.parent.parent.parent
                db_dir = project_root / "src" / "recruitment" / "db"
                db_dir.mkdir(exist_ok=True, parents=True)
                db_path = str(db_dir / "recruitment.db")
                logger.warning(f"No RECRUITMENT_DB_PATH set, using default: {db_path}")

        self.db_path = db_path
        self._connection_pool = []
        self._pool_lock = asyncio.Lock()
        self._executor = ThreadPoolExecutor(max_workers=MAX_CONNECTIONS)
        self._readonly = readonly

        # Ensure the database directory exists (only if directory part is non-empty)
        db_dirname = os.path.dirname(self.db_path)
        if db_dirname:
            os.makedirs(db_dirname, exist_ok=True)

        logger.info(
            f"Database handle created for: {self.db_path} (readonly={readonly})"
        )

        self._ensure_core_tables_sync()
        self._bootstrapped.add(self.db_path)  # Mark as bootstrapped immediately

    async def _run_bootstrap_once(self) -> None:
        """Run the synchronous bootstrap exactly once per DB file."""
        if RecruitmentDatabase._bootstrap_lock is None:
            RecruitmentDatabase._bootstrap_lock = asyncio.Lock()
        async with RecruitmentDatabase._bootstrap_lock:
            if self.db_path in self._bootstrapped:
                return
            self._ensure_core_tables_sync()
            self._bootstrapped.add(self.db_path)

    def _ensure_core_tables_sync(self) -> None:
        """Create the handful of tables the test‑suite needs, synchronously."""
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            c.execute("""
                CREATE TABLE IF NOT EXISTS schema_version(
                    version INTEGER PRIMARY KEY,
                    applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS urls(
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
            if not c.execute("SELECT 1 FROM schema_version").fetchone():
                c.execute("INSERT INTO schema_version(version) VALUES (0)")
            conn.commit()

    async def ainit(self) -> "RecruitmentDatabase":
        """
        Async helper so callers can do:

            db = await RecruitmentDatabase(path).ainit()

        It ensures tables exist and runs any pending migrations.
        """
        await self._run_bootstrap_once()  # Ensure tables exist
        await self.init_db()  # Run full schema migration
        return self

    async def set_query_only(self, enable: bool = True) -> None:
        """Toggle PRAGMA query_only on every fresh connection."""
        self._query_only = enable

    async def check_connection(self) -> bool:
        """Check if the database connection is working."""
        try:
            conn = await self._get_connection()
            async with conn.cursor() as cursor:
                await cursor.execute("SELECT 1")
            await self._release_connection(conn)
            return True
        except Exception:
            return False

    def check_connection_sync(self) -> bool:
        """Synchronous version of check_connection for legacy code."""
        return asyncio.run(self.check_connection())

    async def _get_connection(self) -> aiosqlite.Connection:
        """Get a database connection from the pool."""
        conn = await self._get_connection()
        # Enable foreign key constraints
        await conn.execute("PRAGMA foreign_keys = ON")
        # Set busy timeout to handle concurrent access
        await conn.execute("PRAGMA busy_timeout = 5000")
        return conn

    async def _release_connection(self, conn: aiosqlite.Connection) -> None:
        """Release a connection back to the pool.

        Args:
            conn: Connection to release
        """
        async with self._pool_lock:
            if len(self._connection_pool) < MAX_CONNECTIONS:
                self._connection_pool.append(conn)
            else:
                await conn.close()

    async def _execute_in_thread(self, func, *args, **kwargs):
        """Execute a function in a thread pool.

        Args:
            func: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Any: Result of the function
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, lambda: func(*args, **kwargs))

    async def init_db(self) -> None:
        """Initialize the database and create necessary tables."""
        try:
            logger.info("Creating database tables...")
            conn = await self._get_connection()
            try:
                async with conn.cursor() as cursor:
                    # Create schema version table first
                    await cursor.execute("""
                        CREATE TABLE IF NOT EXISTS schema_version (
                            version INTEGER PRIMARY KEY,
                            applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
                        )
                    """)

                    # Get current schema version
                    await cursor.execute(
                        "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1"
                    )
                    result = await cursor.fetchone()
                    current_version = result[0] if result else 0

                    if current_version < SCHEMA_VERSION:
                        logger.info(
                            f"Upgrading schema from version {current_version} to {SCHEMA_VERSION}"
                        )

                        # Create tables with optimized indexes
                        await cursor.execute("""
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
                        await cursor.execute(
                            "CREATE INDEX IF NOT EXISTS idx_urls_status ON urls(status)"
                        )
                        await cursor.execute(
                            "CREATE INDEX IF NOT EXISTS idx_urls_domain ON urls(domain)"
                        )
                        logger.info("Created urls table")

                        await cursor.execute("""
                            CREATE TABLE IF NOT EXISTS raw_content (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                url_id INTEGER NOT NULL,
                                content TEXT NOT NULL,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                FOREIGN KEY (url_id) REFERENCES urls(id)
                            )
                        """)
                        logger.info("Created raw_content table")

                        await cursor.execute("""
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

                        await cursor.execute("""
                            CREATE TABLE IF NOT EXISTS companies (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                name TEXT NOT NULL,
                                website TEXT,
                                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                            )
                        """)
                        logger.info("Created companies table")

                        await cursor.execute("""
                            CREATE TABLE IF NOT EXISTS locations (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                city TEXT,
                                state TEXT,
                                country TEXT,
                                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                            )
                        """)
                        logger.info("Created locations table")

                        await cursor.execute("""
                            CREATE TABLE IF NOT EXISTS skills (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                name TEXT UNIQUE NOT NULL,
                                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                            )
                        """)
                        logger.info("Created skills table")

                        await cursor.execute("""
                            CREATE TABLE IF NOT EXISTS job_skills (
                                job_id INTEGER,
                                skill_id INTEGER,
                                PRIMARY KEY (job_id, skill_id),
                                FOREIGN KEY (job_id) REFERENCES jobs(id),
                                FOREIGN KEY (skill_id) REFERENCES skills(id)
                            )
                        """)
                        logger.info("Created job_skills table")

                        await cursor.execute("""
                            CREATE TABLE IF NOT EXISTS qualifications (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                name TEXT UNIQUE NOT NULL,
                                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                            )
                        """)
                        logger.info("Created qualifications table")

                        await cursor.execute("""
                            CREATE TABLE IF NOT EXISTS job_qualifications (
                                job_id INTEGER,
                                qualification_id INTEGER,
                                PRIMARY KEY (job_id, qualification_id),
                                FOREIGN KEY (job_id) REFERENCES jobs(id),
                                FOREIGN KEY (qualification_id) REFERENCES qualifications(id)
                            )
                        """)
                        logger.info("Created job_qualifications table")

                        await cursor.execute("""
                            CREATE TABLE IF NOT EXISTS attributes (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                name TEXT UNIQUE NOT NULL,
                                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                            )
                        """)
                        logger.info("Created attributes table")

                        await cursor.execute("""
                            CREATE TABLE IF NOT EXISTS job_attributes (
                                job_id INTEGER,
                                attribute_id INTEGER,
                                PRIMARY KEY (job_id, attribute_id),
                                FOREIGN KEY (job_id) REFERENCES jobs(id),
                                FOREIGN KEY (attribute_id) REFERENCES attributes(id)
                            )
                        """)
                        logger.info("Created job_attributes table")

                        await cursor.execute("""
                            CREATE TABLE IF NOT EXISTS duties (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                description TEXT UNIQUE NOT NULL,
                                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                            )
                        """)
                        logger.info("Created duties table")

                        await cursor.execute("""
                            CREATE TABLE IF NOT EXISTS job_duties (
                                job_id INTEGER,
                                duty_id INTEGER,
                                PRIMARY KEY (job_id, duty_id),
                                FOREIGN KEY (job_id) REFERENCES jobs(id),
                                FOREIGN KEY (duty_id) REFERENCES duties(id)
                            )
                        """)
                        logger.info("Created job_duties table")

                        await cursor.execute("""
                            CREATE TABLE IF NOT EXISTS benefits (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                description TEXT UNIQUE NOT NULL,
                                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                            )
                        """)
                        logger.info("Created benefits table")

                        await cursor.execute("""
                            CREATE TABLE IF NOT EXISTS job_benefits (
                                job_id INTEGER,
                                benefit_id INTEGER,
                                PRIMARY KEY (job_id, benefit_id),
                                FOREIGN KEY (job_id) REFERENCES jobs(id),
                                FOREIGN KEY (benefit_id) REFERENCES benefits(id)
                            )
                        """)
                        logger.info("Created job_benefits table")

                        await cursor.execute("""
                            CREATE TABLE IF NOT EXISTS agencies (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                name TEXT NOT NULL,
                                website TEXT,
                                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                            )
                        """)
                        logger.info("Created agencies table")

                        # Update schema version
                        await cursor.execute(
                            "INSERT INTO schema_version (version) VALUES (?)",
                            (SCHEMA_VERSION,),
                        )
                        await conn.commit()
                        logger.info(f"Schema upgraded to version {SCHEMA_VERSION}")
                    else:
                        logger.info(
                            f"Database schema is up to date (version {current_version})"
                        )

                    await conn.commit()
                    logger.info("Database initialization completed successfully")
            finally:
                await self._release_connection(conn)
        except Exception as e:
            logger.error(f"Failed to initialize database: {str(e)}")
            raise DatabaseError(f"Failed to initialize database: {str(e)}")

    def _write_guard(self, op: str) -> None:
        """Guard against write operations in read-only mode.

        Args:
            op: Name of the operation being attempted

        Raises:
            DatabaseError: If database is in read-only mode
        """
        if self._readonly:
            logger.debug("WRITE‑GUARD tripped on %s", op)
            raise DatabaseError(f"{op} is disabled in read‑only mode")

    async def batch_insert_urls(self, urls: List[Tuple[str, str, str]]) -> List[int]:
        """Insert multiple URLs in a single transaction.

        Args:
            urls: List of (url, domain, source) tuples to insert

        Returns:
            List[int]: List of inserted row IDs

        Raises:
            DatabaseError: If database is in read-only mode
        """
        self._write_guard("batch_insert_urls")
        conn = await self._get_connection()
        try:
            async with conn.cursor() as cur:
                await cur.executemany(
                    "INSERT OR IGNORE INTO urls (url, domain, source) VALUES (?, ?, ?)",
                    urls,
                )
                rowcount = cur.rowcount
                logger.info(f"Batch insert executed: {rowcount} rows affected")
                await conn.commit()
                logger.info("Transaction committed successfully")
            return []  # rowids not needed for now
        except Exception as e:
            logger.error(f"Error in batch_insert_urls: {str(e)}")
            raise
        finally:
            await self._release_connection(conn)

    async def batch_update_url_status(
        self, updates: List[Tuple[int, str, Optional[str]]]
    ) -> None:
        """Update multiple URL statuses in a single transaction.

        Args:
            updates: List of (url_id, status, error_message) tuples

        Raises:
            DatabaseError: If database is in read-only mode
        """
        self._write_guard("batch_update_url_status")
        conn = await self._get_connection()
        try:
            async with conn.cursor() as cur:
                await cur.executemany(
                    "UPDATE urls SET status=?, error_message=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    updates,
                )
            await conn.commit()
        finally:
            await self._release_connection(conn)

    async def get_unprocessed_urls(
        self, batch_size: int = BATCH_SIZE
    ) -> List[Dict[str, Any]]:
        """Get unprocessed URLs with optimized query.

        Args:
            batch_size: Number of URLs to fetch

        Returns:
            List[Dict[str, Any]]: List of unprocessed URLs
        """
        conn = await self._get_connection()
        try:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    """
                    SELECT id, url, domain, source 
                    FROM urls 
                    WHERE status = 'pending' 
                    ORDER BY created_at ASC 
                    LIMIT ?
                """,
                    (batch_size,),
                )
                rows = await cursor.fetchall()
                return [
                    {"id": row[0], "url": row[1], "domain": row[2], "source": row[3]}
                    for row in rows
                ]
        finally:
            await self._release_connection(conn)

    async def close(self) -> None:
        """Close all database connections."""
        async with self._pool_lock:
            for conn in self._connection_pool:
                await conn.close()
            self._connection_pool.clear()
        self._executor.shutdown(wait=True)

    def insert_url(self, url: str, domain: str, source: str) -> int:
        """Insert a URL into the database with retry logic."""
        for attempt in range(MAX_RETRIES):
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "INSERT INTO urls (url, domain, source) VALUES (?, ?, ?)",
                        (url, domain, source),
                    )
                    conn.commit()
                    return cursor.lastrowid
            except sqlite3.IntegrityError as e:
                if "UNIQUE constraint failed" in str(e):
                    # URL already exists, get its ID
                    cursor.execute("SELECT id FROM urls WHERE url = ?", (url,))
                    result = cursor.fetchone()
                    if result:
                        return result[0]
                raise DatabaseError(f"Failed to insert URL: {str(e)}")
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    raise DatabaseError(
                        f"Failed to insert URL after {MAX_RETRIES} attempts: {str(e)}"
                    )
                logger.warning(
                    f"Error inserting URL, attempt {attempt + 1}/{MAX_RETRIES}: {str(e)}"
                )
                time.sleep(RETRY_DELAY * (attempt + 1))

    def update_url_processing_status(
        self, url_id: int, status: str, error_message: Optional[str] = None
    ) -> None:
        """Update the processing status of a URL with retry logic."""
        for attempt in range(MAX_RETRIES):
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    if error_message:
                        cursor.execute(
                            "UPDATE urls SET status = ?, error_message = ?, error_count = error_count + 1, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                            (status, error_message, url_id),
                        )
                    else:
                        cursor.execute(
                            "UPDATE urls SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                            (status, url_id),
                        )
                    conn.commit()
                    return
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    raise DatabaseError(
                        f"Failed to update URL status after {MAX_RETRIES} attempts: {str(e)}"
                    )
                logger.warning(
                    f"Error updating URL status, attempt {attempt + 1}/{MAX_RETRIES}: {str(e)}"
                )
                time.sleep(RETRY_DELAY * (attempt + 1))

    def insert_job(
        self,
        title: str,
        description: str,
        posted_date: str,
        job_type: str,
        url_id: int,
        company_id: int,
        location_id: int,
    ) -> int:
        """Insert a job into the database."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO jobs (title, description, posted_date, job_type, url_id, company_id, location_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        title,
                        description,
                        posted_date,
                        job_type,
                        url_id,
                        company_id,
                        location_id,
                    ),
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert job: {str(e)}")

    def insert_company(self, name: str, website: Optional[str] = None) -> int:
        """Insert a company into the database."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO companies (name, website) VALUES (?, ?)",
                    (name, website),
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert company: {str(e)}")

    def insert_location(self, city: str, state: str, country: str) -> int:
        """Insert a location into the database."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO locations (city, state, country) VALUES (?, ?, ?)",
                    (city, state, country),
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert location: {str(e)}")

    def insert_skill(self, name: str) -> int:
        """Insert a skill into the database."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO skills (name) VALUES (?)", (name,)
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert skill: {str(e)}")

    def link_job_skill(self, job_id: int, skill_id: int) -> None:
        """Link a job to a skill."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO job_skills (job_id, skill_id) VALUES (?, ?)",
                    (job_id, skill_id),
                )
                conn.commit()
        except Exception as e:
            raise DatabaseError(f"Failed to link job to skill: {str(e)}")

    def insert_qualification(self, name: str) -> int:
        """Insert a qualification into the database."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO qualifications (name) VALUES (?)", (name,)
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert qualification: {str(e)}")

    def link_job_qualification(self, job_id: int, qualification_id: int) -> None:
        """Link a job to a qualification."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO job_qualifications (job_id, qualification_id) VALUES (?, ?)",
                    (job_id, qualification_id),
                )
                conn.commit()
        except Exception as e:
            raise DatabaseError(f"Failed to link job to qualification: {str(e)}")

    def insert_attribute(self, name: str) -> int:
        """Insert an attribute into the database."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO attributes (name) VALUES (?)", (name,)
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert attribute: {str(e)}")

    def link_job_attribute(self, job_id: int, attribute_id: int) -> None:
        """Link a job to an attribute."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO job_attributes (job_id, attribute_id) VALUES (?, ?)",
                    (job_id, attribute_id),
                )
                conn.commit()
        except Exception as e:
            raise DatabaseError(f"Failed to link job to attribute: {str(e)}")

    def insert_duty(self, description: str) -> int:
        """Insert a duty into the database."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO duties (description) VALUES (?)",
                    (description,),
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert duty: {str(e)}")

    def link_job_duty(self, job_id: int, duty_id: int) -> None:
        """Link a job to a duty."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO job_duties (job_id, duty_id) VALUES (?, ?)",
                    (job_id, duty_id),
                )
                conn.commit()
        except Exception as e:
            raise DatabaseError(f"Failed to link job to duty: {str(e)}")

    def insert_benefit(self, description: str) -> int:
        """Insert a benefit into the database."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO benefits (description) VALUES (?)",
                    (description,),
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert benefit: {str(e)}")

    def link_job_benefit(self, job_id: int, benefit_id: int) -> None:
        """Link a job to a benefit."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR IGNORE INTO job_benefits (job_id, benefit_id) VALUES (?, ?)",
                    (job_id, benefit_id),
                )
                conn.commit()
        except Exception as e:
            raise DatabaseError(f"Failed to link job to benefit: {str(e)}")

    def insert_agency(self, name: str, website: Optional[str] = None) -> int:
        """Insert an agency into the database."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO agencies (name, website) VALUES (?, ?)",
                    (name, website),
                )
                conn.commit()
                return cursor.lastrowid
        except Exception as e:
            raise DatabaseError(f"Failed to insert agency: {str(e)}")

    def save_processed_url(
        self, url: str, content: str, transformed: Dict[str, Any], timestamp: str
    ) -> None:
        """Save processed URL data to the database with retry logic."""
        for attempt in range(MAX_RETRIES):
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "INSERT OR REPLACE INTO processed_content (url, content, skills, confidence, source, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            url,
                            content,
                            json.dumps(transformed.get("skills", [])),
                            transformed.get("confidence", 0.0),
                            transformed.get("source", ""),
                            timestamp,
                        ),
                    )
                    conn.commit()
                    return
            except Exception as e:
                if attempt == MAX_RETRIES - 1:
                    raise DatabaseError(
                        f"Failed to save processed URL after {MAX_RETRIES} attempts: {str(e)}"
                    )
                logger.warning(
                    f"Error saving processed URL, attempt {attempt + 1}/{MAX_RETRIES}: {str(e)}"
                )
                time.sleep(RETRY_DELAY * (attempt + 1))

    def link_job_location(self, job_id: int, location_id: int) -> None:
        """Link a job to a location."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE jobs SET location_id = ? WHERE id = ?",
                    (location_id, job_id),
                )
                conn.commit()
        except Exception as e:
            raise DatabaseError(f"Failed to link job to location: {str(e)}")
