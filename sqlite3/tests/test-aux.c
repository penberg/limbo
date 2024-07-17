#include "check.h"

#include <sqlite3.h>

void test_libversion(void) {
    sqlite3_libversion();
}

void test_libversion_number(void) {
  sqlite3_libversion_number();
}
