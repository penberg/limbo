#ifndef MVCC_H
#define MVCC_H

#include <stdint.h>

typedef enum {
  MVCC_OK = 0,
  MVCC_IO_ERROR_WRITE = 778,
} MVCCError;

typedef struct DbContext DbContext;

typedef const DbContext *MVCCDatabaseRef;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

MVCCDatabaseRef MVCCDatabaseOpen(const char *path);

void MVCCDatabaseClose(MVCCDatabaseRef db);

MVCCError MVCCDatabaseInsert(MVCCDatabaseRef db, uint64_t id, const uint8_t *value_ptr, uintptr_t value_len);

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus

#endif /* MVCC_H */
