#include <sqlite3.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>

#include "check.h"

void test_close(void)
{
	CHECK_EQUAL(SQLITE_OK, sqlite3_close(NULL));
}
