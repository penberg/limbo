#ifndef MVCC_H
#define MVCC_H

#include <stdint.h>

typedef enum {
  MVCC_OK = 0,
  MVCC_IO_ERROR_READ = 266,
  MVCC_IO_ERROR_WRITE = 778,
} MVCCError;

typedef struct DbContext DbContext;

typedef struct ScanCursorContext ScanCursorContext;

typedef const DbContext *MVCCDatabaseRef;

typedef ScanCursorContext *MVCCScanCursorRef;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

MVCCDatabaseRef MVCCDatabaseOpen(const char *path);

void MVCCDatabaseClose(MVCCDatabaseRef db);

MVCCError MVCCDatabaseInsert(MVCCDatabaseRef db, uint64_t id, const void *value_ptr, uintptr_t value_len);

MVCCError MVCCDatabaseRead(MVCCDatabaseRef db, uint64_t id, uint8_t **value_ptr, int64_t *value_len);

void MVCCFreeStr(void *ptr);

MVCCScanCursorRef MVCCScanCursorOpen(MVCCDatabaseRef db);

void MVCCScanCursorClose(MVCCScanCursorRef cursor);

MVCCError MVCCScanCursorRead(MVCCScanCursorRef cursor, uint8_t **value_ptr, int64_t *value_len);

int MVCCScanCursorNext(MVCCScanCursorRef cursor);

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus

#endif /* MVCC_H */
