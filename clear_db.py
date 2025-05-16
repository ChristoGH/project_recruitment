"""Script to clear the recruitment database tables."""

import sqlite3
from pathlib import Path


def clear_tables():
    # Get the database path
    project_root = Path(__file__).parent
    db_path = project_root / "src" / "recruitment" / "db" / "recruitment.db"

    if not db_path.exists():
        print(f"Database not found at {db_path}")
        return

    try:
        # Connect to the database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Disable foreign key constraints temporarily
        cursor.execute("PRAGMA foreign_keys = OFF;")

        # Clear the tables
        cursor.execute("DELETE FROM raw_content;")
        cursor.execute("DELETE FROM urls;")

        # Reset autoincrement counters
        cursor.execute("DELETE FROM sqlite_sequence WHERE name IN ('raw_content', 'urls');")

        # Re-enable foreign key constraints
        cursor.execute("PRAGMA foreign_keys = ON;")

        # Commit the changes
        conn.commit()

        # Vacuum the database to reclaim space
        cursor.execute("VACUUM;")

        print("Successfully cleared raw_content and urls tables")

    except Exception as e:
        print(f"Error clearing tables: {e}")
    finally:
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    clear_tables()
