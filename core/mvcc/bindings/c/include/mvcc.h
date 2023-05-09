#ifndef MVCC_H
#define MVCC_H

#define MVCC_OK 0

#define MVCC_IO_ERROR_WRITE 778

typedef struct DbContext DbContext;

typedef const DbContext *MVCCDatabaseRef;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

MVCCDatabaseRef MVCCDatabaseOpen(const char *path);

void MVCCDatabaseClose(MVCCDatabaseRef db);

int32_t MVCCDatabaseInsert(MVCCDatabaseRef db, uint64_t id, const uint8_t *value_ptr, uintptr_t value_len);

#ifdef __cplusplus
} // extern "C"
#endif // __cplusplus

#endif /* MVCC_H */
