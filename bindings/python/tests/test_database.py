import limbo

def test_select_all_database():
    d = limbo.Database('tests/database.db')
    users = d.exec("SELECT * from users");
    assert d
    assert users == [(1, 'alice'), (2, 'bob')]

def test_select_max_database():
    d = limbo.Database('tests/database.db')
    id = d.exec("SELECT max(id) from users");
    assert d
    assert id == [(2,)]

def test_select_limit_1_database():
    d = limbo.Database('tests/database.db')
    user = d.exec("SELECT * from users LIMIT 1");
    assert d
    assert user == [(1, 'alice')]
