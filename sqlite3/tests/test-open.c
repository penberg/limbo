#include "check.h"

#include <sqlite3.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>

void test_open_misuse(void)
{
	CHECK_EQUAL(SQLITE_MISUSE, sqlite3_open(NULL, NULL));

	CHECK_EQUAL(SQLITE_MISUSE, sqlite3_open("local.db", NULL));
}

void test_open_not_found(void)
{
	sqlite3 *db;

	CHECK_EQUAL(SQLITE_CANTOPEN, sqlite3_open("not-found/local.db", &db));
}

// TODO: test_open_create

void test_open_existing(void)
{
	sqlite3 *db;

	CHECK_EQUAL(SQLITE_OK, sqlite3_open("../../testing/testing.db", &db));
	CHECK_EQUAL(SQLITE_OK, sqlite3_close(db));
}
