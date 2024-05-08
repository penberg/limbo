#ifndef LIMBO_SQLITE3_H
#define LIMBO_SQLITE3_H

#include <stdint.h>

#define SQLITE_OK 0

#define SQLITE_ERROR 1

#define SQLITE_BUSY 5

#define SQLITE_NOTFOUND 14

#define SQLITE_MISUSE 21

#define SQLITE_ROW 100

#define SQLITE_DONE 101

typedef struct sqlite3 sqlite3;

typedef struct sqlite3_stmt sqlite3_stmt;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

int sqlite3_open(const char *filename, sqlite3 **db_out);

int sqlite3_close(sqlite3 *db);

int sqlite3_prepare_v2(sqlite3 *db, const char *sql, int len, sqlite3_stmt **out_stmt, const char **tail);

int sqlite3_finalize(sqlite3_stmt *stmt);

int sqlite3_step(sqlite3_stmt *stmt);

const unsigned char *sqlite3_column_text(sqlite3_stmt *stmt, int idx);

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus

#endif /* LIMBO_SQLITE3_H */
