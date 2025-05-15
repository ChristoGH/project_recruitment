import pytest
import aiosqlite
from src.recruitment.db.repository import RecruitmentDatabase


@pytest.mark.asyncio
async def test_foreign_key_enforcement():
    """Test that foreign key constraints are enforced."""
    db = RecruitmentDatabase(":memory:")
    await db.ainit()

    # Create test tables
    async with db._get_connection() as conn:
        await conn.execute("""
            CREATE TABLE parent (
                id INTEGER PRIMARY KEY,
                name TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE child (
                id INTEGER PRIMARY KEY,
                parent_id INTEGER,
                name TEXT,
                FOREIGN KEY (parent_id) REFERENCES parent(id)
            )
        """)
        await conn.commit()

    # Try to insert child with non-existent parent
    async with db._get_connection() as conn:
        with pytest.raises(aiosqlite.IntegrityError):
            await conn.execute(
                "INSERT INTO child (parent_id, name) VALUES (?, ?)", (999, "test")
            )
            await conn.commit()

    # Insert parent and then child (should succeed)
    async with db._get_connection() as conn:
        await conn.execute(
            "INSERT INTO parent (id, name) VALUES (?, ?)", (1, "parent1")
        )
        await conn.execute(
            "INSERT INTO child (parent_id, name) VALUES (?, ?)", (1, "child1")
        )
        await conn.commit()

    # Verify data
    async with db._get_connection() as conn:
        async with conn.execute("SELECT COUNT(*) FROM child") as cursor:
            row = await cursor.fetchone()
            assert row[0] == 1
