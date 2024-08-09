import limbo
import sqlite3


def test_limbo_fetchall_select_all_users():
    conn = limbo.connect("tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")

    users = cursor.fetchall()
    assert users
    assert users == [(1, "alice"), (2, "bob")]


def test_sqlite3_fetchall_select_all_users():
    conn = sqlite3.connect("tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")

    users = cursor.fetchall()
    assert users
    assert users == [(1, "alice"), (2, "bob")]


def test_limbo_fetchall_select_user_ids():
    conn = limbo.connect("tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM users")

    user_ids = cursor.fetchall()
    assert user_ids
    assert user_ids == [(1,), (2,)]


def test_sqlite3_fetchall_select_user_ids():
    conn = sqlite3.connect("tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM users")

    user_ids = cursor.fetchall()
    assert user_ids
    assert user_ids == [(1,), (2,)]


def test_limbo_fetchone_select_all_users():
    conn = limbo.connect("tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")

    alice = cursor.fetchone()
    assert alice
    assert alice == (1, "alice")

    bob = cursor.fetchone()
    assert bob
    assert bob == (2, "bob")


def test_sqlite3_fetchone_select_all_users():
    conn = sqlite3.connect("tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")

    alice = cursor.fetchone()
    assert alice
    assert alice == (1, "alice")

    bob = cursor.fetchone()
    assert bob
    assert bob == (2, "bob")


def test_limbo_fetchone_select_max_user_id():
    conn = limbo.connect("tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(id) FROM users")

    max_id = cursor.fetchone()
    assert max_id
    assert max_id == (2,)


def test_sqlite3_fetchone_select_max_user_id():
    conn = sqlite3.connect("tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(id) FROM users")

    max_id = cursor.fetchone()
    assert max_id
    assert max_id == (2,)
