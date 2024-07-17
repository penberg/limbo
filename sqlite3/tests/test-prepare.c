#include "check.h"

#include <sqlite3.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>

void test_prepare_misuse(void)
{
	sqlite3 *db;

	CHECK_EQUAL(SQLITE_OK, sqlite3_open("../../testing/testing.db", &db));

	// Database handle is NULL.
	// TODO: SIGSEGV with sqlite3
//	CHECK_EQUAL(SQLITE_MISUSE, sqlite3_prepare_v2(NULL, "SELECT 1", -1, NULL, NULL));

	// Output statement is NULL.
	// TODO: SIGSEGV with sqlite3
//	CHECK_EQUAL(SQLITE_MISUSE, sqlite3_prepare_v2(db, "SELECT 1", -1, NULL, NULL));

	// SQL string length is too short, truncating the statement.
	// TODO: SIGSEGV with sqlite3
//	CHECK_EQUAL(SQLITE_MISUSE, sqlite3_prepare_v2(db, "SELECT 1", 7, NULL, NULL));
	
	CHECK_EQUAL(SQLITE_OK, sqlite3_close(db));
}
