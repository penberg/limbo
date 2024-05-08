#include "sqlite3.h"

#include <stddef.h>
#include <assert.h>
#include <stdio.h>

int main(int argc, char *argv[])
{
	sqlite3 *db;
	int rc;

	rc = sqlite3_open("local.db", &db);
	assert(rc == SQLITE_OK);

	sqlite3_stmt *stmt;

	rc = sqlite3_prepare_v2(db, "SELECT 'hello, world' AS message", -1, &stmt, NULL);
	assert(rc == SQLITE_OK);

	rc = sqlite3_step(stmt);
	assert(rc == SQLITE_ROW);

	const unsigned char *result = sqlite3_column_text(stmt, 0);

	printf("result = %s\n", result);

	rc = sqlite3_step(stmt);
	assert(rc == SQLITE_DONE);

	sqlite3_finalize(stmt);

	sqlite3_close(db);

	return 0;
}
