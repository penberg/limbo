import sqlite3

import pytest

import limbo


@pytest.mark.parametrize("provider", ["sqlite3", "limbo"])
def test_fetchall_select_all_users(provider):
    conn = connect(provider, "tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")

    users = cursor.fetchall()
    assert users
    assert users == [(1, "alice"), (2, "bob")]


@pytest.mark.parametrize("provider", ["sqlite3", "limbo"])
def test_fetchall_select_user_ids(provider):
    conn = connect(provider, "tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM users")

    user_ids = cursor.fetchall()
    assert user_ids
    assert user_ids == [(1,), (2,)]


@pytest.mark.parametrize("provider", ["sqlite3", "limbo"])
def test_fetchone_select_all_users(provider):
    conn = connect(provider, "tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users")

    alice = cursor.fetchone()
    assert alice
    assert alice == (1, "alice")

    bob = cursor.fetchone()
    assert bob
    assert bob == (2, "bob")


@pytest.mark.parametrize("provider", ["sqlite3", "limbo"])
def test_fetchone_select_max_user_id(provider):
    conn = connect(provider, "tests/database.db")
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(id) FROM users")

    max_id = cursor.fetchone()
    assert max_id
    assert max_id == (2,)


def connect(provider, database):
    if provider == "limbo":
        return limbo.connect(database)
    if provider == "sqlite3":
        return sqlite3.connect(database)
    raise Exception(f"Provider `{provider}` is not supported")
