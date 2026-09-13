/*
 * turso_tcl.c — Native Tcl extension for Turso/Limbo database.
 *
 * Provides the `sqlite3` Tcl command that creates in-process database
 * connections, replacing the subprocess-based shim in sqlite/conformance/upstream/tester.tcl.
 *
 * Supported db sub-commands:
 *   eval SQL ?array? ?script?   — execute SQL, return results as list
 *   one  SQL                    — return first column of first row
 *   exists SQL                  — return 1 if query returns any row
 *   changes                     — rows affected by last DML
 *   total_changes               — total rows changed since open
 *   last_insert_rowid           — rowid of last INSERT
 *   errorcode                   — most recent error code
 *   errmsg                      — most recent error message
 *   null ?value?                — get/set NULL representation string
 *   func name ?arg...? body     — register a Tcl-backed scalar SQL function
 *   status step|sort|autoindex|vmstep — statement counters from the last
 *                                 completed [db eval] statement
 *   transaction ?type? script   — run script inside a transaction
 *   close                       — close database and delete command
 *   limit ...                   — stub returning a default value
 */

#include <tcl.h>
#include <sqlite3.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>

/* Tcl_Size was introduced in Tcl 9.0; fall back to int for 8.x */
#ifndef TCL_SIZE_MAX
typedef int Tcl_Size;
#endif

#define TURSO_TCL_VERSION "1.0"
#define MAX_FUNC_ARGS 64
#define STMT_CACHE_SIZE 32

/* ------------------------------------------------------------------ */
/* TursoDb — state for a single open database connection               */
/* ------------------------------------------------------------------ */
typedef struct CachedStmt {
    char         *sql;     /* SQL text (cache key) */
    sqlite3_stmt *stmt;    /* prepared statement */
} CachedStmt;

#define TURSO_OPEN_READONLY  0x00000001
#define TURSO_OPEN_READWRITE 0x00000002
#define TURSO_OPEN_CREATE    0x00000004
#define TURSO_OPEN_URI       0x00000040

typedef struct TursoDb {
    sqlite3    *db;
    Tcl_Interp *interp;
    Tcl_Obj    *null_obj;   /* replacement string for NULL values */
    CachedStmt  stmt_cache[STMT_CACHE_SIZE];
    int         cache_count;
    int         txn_depth;  /* nesting depth of [db transaction] scripts */
    /* [db status] counters, captured from the last statement that ran to
     * completion in [db eval], as in upstream tclsqlite.c */
    int         n_step;     /* SQLITE_STMTSTATUS_FULLSCAN_STEP */
    int         n_sort;     /* SQLITE_STMTSTATUS_SORT */
    int         n_index;    /* SQLITE_STMTSTATUS_AUTOINDEX */
    int         n_vm_step;  /* SQLITE_STMTSTATUS_VM_STEP */
    /* Set once [sqlite3_close_v2] turned the connection into a zombie;
     * see zombie_dbs below. */
    char           *zombie_name;
    struct TursoDb *next_zombie;
} TursoDb;

/* Connections closed with [sqlite3_close_v2] while statements were still
 * outstanding. The library keeps such a zombie alive until its last
 * statement is finalized, and so must the handle command: upstream tests
 * expect [sqlite3_prepare $DB ...] on a zombie to fail with SQLITE_MISUSE,
 * not with "no such database". [sqlite3_finalize] deletes the command
 * once the library has freed the connection. */
static TursoDb *zombie_dbs = NULL;

static void zombie_add(TursoDb *tdb, const char *name)
{
    tdb->zombie_name = strdup(name);
    tdb->next_zombie = zombie_dbs;
    zombie_dbs = tdb;
}

static void zombie_unlink(TursoDb *tdb)
{
    TursoDb **pp;
    for (pp = &zombie_dbs; *pp; pp = &(*pp)->next_zombie) {
        if (*pp == tdb) {
            *pp = tdb->next_zombie;
            return;
        }
    }
}

/* A statement of DB was just finalized. If DB was a zombie and that was
 * its last statement, the library has freed it: drop the handle so no
 * command can touch the dangling pointer. */
static void zombie_stmt_finalized(sqlite3 *db, int was_last)
{
    TursoDb *tdb;
    if (!was_last) return;
    for (tdb = zombie_dbs; tdb; tdb = tdb->next_zombie) {
        if (tdb->db == db) {
            tdb->db = NULL;
            Tcl_DeleteCommand(tdb->interp, tdb->zombie_name);
            return;
        }
    }
}

/* ------------------------------------------------------------------ */
/* TclFuncData — state for a Tcl-backed scalar SQL function            */
/* ------------------------------------------------------------------ */
typedef struct TclFuncData {
    Tcl_Interp *interp;
    Tcl_Obj    *script;                    /* function body */
    int         n_args;
    Tcl_Obj    *arg_names[MAX_FUNC_ARGS];  /* argument variable names */
} TclFuncData;

/* A TCL script registered with [db collate NAME SCRIPT]. The engine calls
 * it with the two strings appended and expects an integer back. */
typedef struct TclCollateData {
    Tcl_Interp *interp;
    Tcl_Obj    *script;
} TclCollateData;

static int tcl_collate_bridge(void *pApp, int nA, const void *zA,
                              int nB, const void *zB)
{
    TclCollateData *data = (TclCollateData *)pApp;
    Tcl_Obj *cmd = Tcl_DuplicateObj(data->script);
    Tcl_IncrRefCount(cmd);
    Tcl_ListObjAppendElement(data->interp, cmd,
                             Tcl_NewStringObj((const char *)zA, nA));
    Tcl_ListObjAppendElement(data->interp, cmd,
                             Tcl_NewStringObj((const char *)zB, nB));
    int result = 0;
    if (Tcl_EvalObjEx(data->interp, cmd, TCL_EVAL_GLOBAL) == TCL_OK) {
        Tcl_GetIntFromObj(NULL, Tcl_GetObjResult(data->interp), &result);
    }
    Tcl_DecrRefCount(cmd);
    return result;
}

static void tcl_collate_destroy(void *pApp)
{
    TclCollateData *data = (TclCollateData *)pApp;
    Tcl_DecrRefCount(data->script);
    Tcl_Free((char *)data);
}

/* ------------------------------------------------------------------ */
/* Value helpers                                                        */
/* ------------------------------------------------------------------ */

/* Convert a column value to a Tcl_Obj. */
static Tcl_Obj *column_to_obj(sqlite3_stmt *stmt, int i, const char *null_str)
{
    int ctype = sqlite3_column_type(stmt, i);
    switch (ctype) {
    case SQLITE_INTEGER:
        return Tcl_NewWideIntObj((Tcl_WideInt)sqlite3_column_int64(stmt, i));
    case SQLITE_FLOAT:
        return Tcl_NewDoubleObj(sqlite3_column_double(stmt, i));
    case SQLITE_TEXT: {
        const char *text = (const char *)sqlite3_column_text(stmt, i);
        return Tcl_NewStringObj(text ? text : "", -1);
    }
    case SQLITE_BLOB: {
        const void *blob = sqlite3_column_blob(stmt, i);
        int nbytes = sqlite3_column_bytes(stmt, i);
        return Tcl_NewByteArrayObj((const unsigned char *)blob, nbytes);
    }
    default: /* NULL */
        return Tcl_NewStringObj(null_str ? null_str : "", -1);
    }
}

/* Convert a function argument (sqlite3_value*) to a Tcl_Obj. */
static Tcl_Obj *value_to_obj(void *argv_i)
{
    int vtype = sqlite3_value_type(argv_i);
    switch (vtype) {
    case SQLITE_INTEGER:
        return Tcl_NewWideIntObj((Tcl_WideInt)sqlite3_value_int64(argv_i));
    case SQLITE_FLOAT:
        return Tcl_NewDoubleObj(sqlite3_value_double(argv_i));
    case SQLITE_TEXT: {
        /* Pass the length explicitly: turso's sqlite3_value_text does not
         * NUL-terminate its buffer (unlike SQLite), so length -1 (strlen)
         * reads past the end and picks up garbage bytes. */
        const char *text = (const char *)sqlite3_value_text(argv_i);
        int nbytes = sqlite3_value_bytes(argv_i);
        return Tcl_NewStringObj(text ? text : "", text ? nbytes : 0);
    }
    case SQLITE_BLOB: {
        const void *blob = sqlite3_value_blob(argv_i);
        int nbytes = sqlite3_value_bytes(argv_i);
        return Tcl_NewByteArrayObj((const unsigned char *)blob, nbytes);
    }
    default: /* NULL */
        return Tcl_NewStringObj("", 0);
    }
}

/* ------------------------------------------------------------------ */
/* Prepared statement cache                                            */
/* ------------------------------------------------------------------ */

static sqlite3_stmt *cache_find(TursoDb *tdb, const char *sql)
{
    int i;
    for (i = 0; i < tdb->cache_count; i++) {
        if (strcmp(tdb->stmt_cache[i].sql, sql) == 0) {
            sqlite3_stmt *stmt = tdb->stmt_cache[i].stmt;
            sqlite3_reset(stmt);
            return stmt;
        }
    }
    return NULL;
}

static void cache_store(TursoDb *tdb, const char *sql, sqlite3_stmt *stmt)
{
    if (tdb->cache_count >= STMT_CACHE_SIZE) return;
    CachedStmt *cs = &tdb->stmt_cache[tdb->cache_count++];
    cs->sql = strdup(sql);
    cs->stmt = stmt;
}

static void cache_free(TursoDb *tdb)
{
    int i;
    for (i = 0; i < tdb->cache_count; i++) {
        sqlite3_finalize(tdb->stmt_cache[i].stmt);
        free(tdb->stmt_cache[i].sql);
    }
    tdb->cache_count = 0;
}

/* ------------------------------------------------------------------ */
/* TCL variable binding                                                */
/* ------------------------------------------------------------------ */

static void bind_tcl_variables(Tcl_Interp *interp, sqlite3_stmt *stmt)
{
    int nparams = sqlite3_bind_parameter_count(stmt);
    int i;
    for (i = 1; i <= nparams; i++) {
        const char *name = sqlite3_bind_parameter_name(stmt, i);
        if (!name) continue;

        /* Skip the leading $ : or @ */
        const char *varname = name;
        if (varname[0] == '$' || varname[0] == ':' || varname[0] == '@') {
            varname++;
        }

        Tcl_Obj *val = Tcl_GetVar2Ex(interp, varname, NULL, 0);
        if (!val) {
            sqlite3_bind_null(stmt, i);
            continue;
        }

        Tcl_WideInt ival;
        double dval;
        if (Tcl_GetWideIntFromObj(NULL, val, &ival) == TCL_OK) {
            sqlite3_bind_int64(stmt, i, (sqlite3_int64)ival);
        } else if (Tcl_GetDoubleFromObj(NULL, val, &dval) == TCL_OK) {
            sqlite3_bind_double(stmt, i, dval);
        } else {
            Tcl_Size len;
            const char *str = Tcl_GetStringFromObj(val, &len);
            sqlite3_bind_text(stmt, i, str, (int)len, SQLITE_TRANSIENT);
        }
    }
}

/* Save the [db status] counters from a statement that just ran to
 * completion. The reset flag clears the statement's counters so a cached
 * statement starts each run from zero, as in upstream tclsqlite.c. */
static void capture_stmt_status(TursoDb *tdb, sqlite3_stmt *stmt)
{
    tdb->n_step    = sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_FULLSCAN_STEP, 1);
    tdb->n_sort    = sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_SORT, 1);
    tdb->n_index   = sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_AUTOINDEX, 1);
    tdb->n_vm_step = sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_VM_STEP, 1);
}

/* ------------------------------------------------------------------ */
/* Tcl scalar function bridge                                           */
/* ------------------------------------------------------------------ */

static void tcl_scalar_bridge(void *ctx, int argc, void **argv)
{
    TclFuncData *func = (TclFuncData *)sqlite3_user_data(ctx);
    Tcl_Interp  *interp = func->interp;
    int          i, rc;

    if (func->n_args > 0) {
        /* Custom Turso behavior: Bind argument variables in the calling scope. */
        for (i = 0; i < argc && i < func->n_args; i++) {
            Tcl_Obj *val = value_to_obj(argv[i]);
            if (Tcl_ObjSetVar2(interp, func->arg_names[i], NULL, val,
                               TCL_LEAVE_ERR_MSG) == NULL) {
                sqlite3_result_error(ctx, Tcl_GetString(Tcl_GetObjResult(interp)), -1);
                return;
            }
        }

        /* Evaluate the script body. */
        rc = Tcl_EvalObjEx(interp, func->script, 0);
    } else {
        /* Standard SQLite behavior: Append SQL arguments to the script prefix. */
        Tcl_Obj *cmd = Tcl_DuplicateObj(func->script);
        Tcl_IncrRefCount(cmd);

        for (i = 0; i < argc; i++) {
            Tcl_ListObjAppendElement(interp, cmd, value_to_obj(argv[i]));
        }

        rc = Tcl_EvalObjEx(interp, cmd, TCL_EVAL_DIRECT);
        Tcl_DecrRefCount(cmd);
    }

    if (rc == TCL_ERROR) {
        const char *err = Tcl_GetString(Tcl_GetObjResult(interp));
        sqlite3_result_error(ctx, err, -1);
        return;
    }

    /* Convert the Tcl result to an SQL value. */
    Tcl_Obj    *result = Tcl_GetObjResult(interp);
    Tcl_WideInt ival;
    double      dval;

    if (Tcl_GetWideIntFromObj(NULL, result, &ival) == TCL_OK) {
        sqlite3_result_int64(ctx, (int64_t)ival);
    } else if (Tcl_GetDoubleFromObj(NULL, result, &dval) == TCL_OK) {
        sqlite3_result_double(ctx, dval);
    } else {
        Tcl_Size    slen;
        const char *str = Tcl_GetStringFromObj(result, &slen);
        sqlite3_result_text(ctx, str, slen, SQLITE_TRANSIENT);
    }
}

static void tcl_func_destroy(void *pApp)
{
    TclFuncData *func = (TclFuncData *)pApp;
    int i;
    if (!func) return;
    if (func->script) Tcl_DecrRefCount(func->script);
    for (i = 0; i < func->n_args; i++) {
        if (func->arg_names[i]) Tcl_DecrRefCount(func->arg_names[i]);
    }
    Tcl_Free((char *)func);
}

/* ------------------------------------------------------------------ */
/* Multi-statement SQL execution helpers                                */
/* ------------------------------------------------------------------ */

/*
 * Execute all statements in `sql`, collecting result rows from the last
 * statement that returns rows into `result_list`.
 * Returns TCL_OK or TCL_ERROR; sets the interpreter result on error.
 *
 * Uses prepared statement caching: single-statement SQL with bind parameters
 * (e.g. $varname) is cached and reused on subsequent calls.  TCL variables
 * referenced by parameter names are automatically bound.
 */
static int exec_sql_collect(TursoDb *tdb,
                             const char *sql, const char *null_str,
                             Tcl_Obj **result_list_out)
{
    Tcl_Interp *interp = tdb->interp;
    sqlite3    *db     = tdb->db;

    /* Fast path: check if this exact SQL string has a cached statement */
    sqlite3_stmt *cached_stmt = cache_find(tdb, sql);
    if (cached_stmt) {
        bind_tcl_variables(interp, cached_stmt);

        Tcl_Obj *result_list = Tcl_NewListObj(0, NULL);
        Tcl_IncrRefCount(result_list);
        int ncols = sqlite3_column_count(cached_stmt);
        int rc;

        while ((rc = sqlite3_step(cached_stmt)) == SQLITE_ROW) {
            int i;
            for (i = 0; i < ncols; i++) {
                Tcl_Obj *val = column_to_obj(cached_stmt, i, null_str);
                Tcl_ListObjAppendElement(interp, result_list, val);
            }
        }

        if (rc != SQLITE_DONE) {
            Tcl_DecrRefCount(result_list);
            Tcl_SetResult(interp, (char *)sqlite3_errmsg(db), TCL_VOLATILE);
            return TCL_ERROR;
        }

        capture_stmt_status(tdb, cached_stmt);

        *result_list_out = result_list;
        return TCL_OK;
    }

    /* Regular multi-statement path */
    Tcl_Obj    *result_list = Tcl_NewListObj(0, NULL);
    Tcl_IncrRefCount(result_list);
    const char *remaining   = sql;
    int         rc;

    while (remaining && *remaining) {
        /* skip leading whitespace and bare semicolons */
        while (*remaining == ' ' || *remaining == '\n' ||
               *remaining == '\t' || *remaining == '\r' ||
               *remaining == ';') {
            remaining++;
        }
        if (!*remaining) break;

        sqlite3_stmt *stmt = NULL;
        const char   *tail = NULL;

        rc = sqlite3_prepare_v2(db, remaining, -1, &stmt, &tail);
        if (rc != SQLITE_OK) {
            Tcl_DecrRefCount(result_list);
            Tcl_SetResult(interp, (char *)sqlite3_errmsg(db), TCL_VOLATILE);
            return TCL_ERROR;
        }
        if (!stmt) {
            /* empty / comment-only statement */
            remaining = tail;
            continue;
        }

        /* Bind TCL variables to any parameters */
        bind_tcl_variables(interp, stmt);

        int ncols = sqlite3_column_count(stmt);

        while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
            int i;
            for (i = 0; i < ncols; i++) {
                Tcl_Obj *val = column_to_obj(stmt, i, null_str);
                Tcl_ListObjAppendElement(interp, result_list, val);
            }
        }

        if (rc != SQLITE_DONE) {
            sqlite3_finalize(stmt);
            Tcl_DecrRefCount(result_list);
            Tcl_SetResult(interp, (char *)sqlite3_errmsg(db), TCL_VOLATILE);
            return TCL_ERROR;
        }

        capture_stmt_status(tdb, stmt);

        /* Cache single-statement SQL with bind parameters */
        if (sqlite3_bind_parameter_count(stmt) > 0) {
            /* Check if tail is empty (single statement) */
            const char *p = tail;
            if (p) {
                while (*p == ' ' || *p == '\n' || *p == '\t' ||
                       *p == '\r' || *p == ';') {
                    p++;
                }
            }
            if (!p || !*p) {
                cache_store(tdb, sql, stmt);
            } else {
                sqlite3_finalize(stmt);
            }
        } else {
            sqlite3_finalize(stmt);
        }

        remaining = tail;
    }

    *result_list_out = result_list;
    return TCL_OK;
}

/* ------------------------------------------------------------------ */
/* db command dispatcher                                                */
/* ------------------------------------------------------------------ */

static void TursoDbFree(ClientData cd)
{
    TursoDb *tdb = (TursoDb *)cd;
    if (!tdb) return;
    cache_free(tdb);
    if (tdb->zombie_name) {
        /* The library frees a zombie itself when its last statement is
         * finalized; closing it again would be misuse. */
        zombie_unlink(tdb);
        free(tdb->zombie_name);
    } else if (tdb->db) {
        sqlite3_close(tdb->db);
    }
    if (tdb->null_obj) Tcl_DecrRefCount(tdb->null_obj);
    Tcl_Free((char *)tdb);
}

static int TursoDbCmd(ClientData cd, Tcl_Interp *interp,
                      int objc, Tcl_Obj *const objv[])
{
    TursoDb    *tdb = (TursoDb *)cd;
    static const char *cmds[] = {
        "eval", "one", "exists", "changes", "total_changes",
        "last_insert_rowid", "errorcode", "errmsg", "null", "nullvalue",
        "func", "function", "close", "limit", "status", "transaction",
        "cache", "collate", "timeout", "interrupt", "version",
        "busy", "auth", "authorizer", "progress", "commit_hook",
        "update_hook", "rollback_hook", "wal_hook", "preupdate", "trace",
        "trace_v2", "profile", "unlock_notify", "enable_load_extension",
        "config", "erase", "bind_fallback", "collation_needed",
        "backup", "restore", "incrblob", "serialize", "deserialize",
        "copy",
        NULL
    };
    enum {
        CMD_EVAL, CMD_ONE, CMD_EXISTS, CMD_CHANGES, CMD_TOTAL_CHANGES,
        CMD_LAST_INSERT_ROWID, CMD_ERRORCODE, CMD_ERRMSG, CMD_NULL, CMD_NULLVALUE,
        CMD_FUNC, CMD_FUNCTION, CMD_CLOSE, CMD_LIMIT, CMD_STATUS, CMD_TRANSACTION,
        CMD_CACHE, CMD_COLLATE, CMD_TIMEOUT, CMD_INTERRUPT, CMD_VERSION,
        CMD_BUSY, CMD_AUTH, CMD_AUTHORIZER, CMD_PROGRESS, CMD_COMMIT_HOOK,
        CMD_UPDATE_HOOK, CMD_ROLLBACK_HOOK, CMD_WAL_HOOK, CMD_PREUPDATE, CMD_TRACE,
        CMD_TRACE_V2, CMD_PROFILE, CMD_UNLOCK_NOTIFY, CMD_ENABLE_LOAD_EXTENSION,
        CMD_CONFIG, CMD_ERASE, CMD_BIND_FALLBACK, CMD_COLLATION_NEEDED,
        CMD_BACKUP, CMD_RESTORE, CMD_INCRBLOB, CMD_SERIALIZE, CMD_DESERIALIZE,
        CMD_COPY
    };
    int cmdIdx;

    if (objc < 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "subcommand ?args?");
        return TCL_ERROR;
    }

    if (Tcl_GetIndexFromObj(interp, objv[1], cmds, "subcommand", 0,
                            &cmdIdx) != TCL_OK) {
        return TCL_ERROR;
    }

    switch (cmdIdx) {

    /* ---- simple counters / metadata ---- */

    case CMD_CHANGES:
        Tcl_SetObjResult(interp, Tcl_NewIntObj(sqlite3_changes(tdb->db)));
        return TCL_OK;

    case CMD_TOTAL_CHANGES:
        Tcl_SetObjResult(interp, Tcl_NewIntObj(sqlite3_total_changes(tdb->db)));
        return TCL_OK;

    case CMD_LAST_INSERT_ROWID:
        Tcl_SetObjResult(interp,
            Tcl_NewWideIntObj((Tcl_WideInt)sqlite3_last_insert_rowid(tdb->db)));
        return TCL_OK;

    case CMD_ERRORCODE:
        Tcl_SetObjResult(interp, Tcl_NewIntObj(sqlite3_errcode(tdb->db)));
        return TCL_OK;

    case CMD_ERRMSG:
        Tcl_SetResult(interp, (char *)sqlite3_errmsg(tdb->db), TCL_VOLATILE);
        return TCL_OK;

    /* ---- null value string ---- */

    case CMD_NULL:
    case CMD_NULLVALUE:
        if (objc == 3) {
            if (tdb->null_obj) Tcl_DecrRefCount(tdb->null_obj);
            tdb->null_obj = objv[2];
            Tcl_IncrRefCount(tdb->null_obj);
        }
        Tcl_SetObjResult(interp,
            tdb->null_obj ? tdb->null_obj : Tcl_NewStringObj("", 0));
        return TCL_OK;

    /* ---- close ---- */

    case CMD_CLOSE:
        Tcl_DeleteCommand(interp, Tcl_GetString(objv[0]));
        return TCL_OK;

    /* ---- limit (stub) ---- */

    case CMD_LIMIT:
        Tcl_SetObjResult(interp, Tcl_NewIntObj(1000000));
        return TCL_OK;

    /* ---- cache ---- */

    case CMD_CACHE: {
        /*
         * db cache flush — finalize the prepared statement cache.
         * db cache size N — upstream resizes the cache; ours is fixed,
         * so accept and ignore the value.
         */
        if (objc < 3) {
            Tcl_WrongNumArgs(interp, 2, objv, "flush|size ?N?");
            return TCL_ERROR;
        }
        const char *op = Tcl_GetString(objv[2]);
        if (strcmp(op, "flush") == 0) {
            cache_free(tdb);
        } else if (strcmp(op, "size") != 0) {
            Tcl_AppendResult(interp, "bad option \"", op,
                             "\": must be flush or size", NULL);
            return TCL_ERROR;
        }
        return TCL_OK;
    }

    /* ---- status ---- */

    case CMD_STATUS: {
        /*
         * db status step|sort|autoindex|vmstep
         *
         * Returns a counter from the last statement that ran to completion
         * in [db eval]; tests use this to check that the planner picked an
         * index scan (step) or avoided a sort pass (sort).
         */
        if (objc != 3) {
            Tcl_WrongNumArgs(interp, 2, objv, "(step|sort|autoindex|vmstep)");
            return TCL_ERROR;
        }
        const char *op = Tcl_GetString(objv[2]);
        int v;
        if (strcmp(op, "step") == 0) {
            v = tdb->n_step;
        } else if (strcmp(op, "sort") == 0) {
            v = tdb->n_sort;
        } else if (strcmp(op, "autoindex") == 0) {
            v = tdb->n_index;
        } else if (strcmp(op, "vmstep") == 0) {
            v = tdb->n_vm_step;
        } else {
            /* same error text as upstream tclsqlite.c */
            Tcl_AppendResult(interp,
                "bad argument: should be autoindex, step, sort or vmstep",
                NULL);
            return TCL_ERROR;
        }
        Tcl_SetObjResult(interp, Tcl_NewIntObj(v));
        return TCL_OK;
    }

    /* ---- transaction ---- */

    case CMD_TRANSACTION: {
        /*
         * db transaction ?deferred|immediate|exclusive? script
         *
         * Runs the script inside a transaction, committing on success and
         * rolling back if the script raises an error. Follows upstream
         * tclsqlite: a nested [db transaction] uses a savepoint so only the
         * outermost level owns the real BEGIN/COMMIT.
         */
        static const char *types[] = {
            "deferred", "exclusive", "immediate", NULL
        };
        static const char *begins[] = {
            "BEGIN", "BEGIN EXCLUSIVE", "BEGIN IMMEDIATE"
        };

        const char *begin_sql = "BEGIN";
        Tcl_Obj    *script;

        if (objc == 3) {
            script = objv[2];
        } else if (objc == 4) {
            int type_idx;
            if (Tcl_GetIndexFromObj(interp, objv[2], types, "transaction type",
                                    0, &type_idx) != TCL_OK) {
                return TCL_ERROR;
            }
            begin_sql = begins[type_idx];
            script = objv[3];
        } else {
            Tcl_WrongNumArgs(interp, 2, objv, "?TYPE? SCRIPT");
            return TCL_ERROR;
        }

        if (tdb->txn_depth > 0) {
            begin_sql = "SAVEPOINT _tcl_transaction";
        }

        if (sqlite3_exec(tdb->db, begin_sql, NULL, NULL, NULL) != SQLITE_OK) {
            Tcl_SetResult(interp, (char *)sqlite3_errmsg(tdb->db), TCL_VOLATILE);
            return TCL_ERROR;
        }

        tdb->txn_depth++;
        int rc = Tcl_EvalObjEx(interp, script, 0);
        tdb->txn_depth--;

        const char *end_sql;
        if (rc == TCL_ERROR) {
            end_sql = tdb->txn_depth > 0
                ? "ROLLBACK TO _tcl_transaction; RELEASE _tcl_transaction"
                : "ROLLBACK";
        } else {
            end_sql = tdb->txn_depth > 0
                ? "RELEASE _tcl_transaction"
                : "COMMIT";
        }

        if (sqlite3_exec(tdb->db, end_sql, NULL, NULL, NULL) != SQLITE_OK) {
            /* The commit (or release) itself failed: surface that error and
             * abandon the transaction so the connection is usable again. */
            if (rc != TCL_ERROR) {
                Tcl_SetResult(interp, (char *)sqlite3_errmsg(tdb->db),
                              TCL_VOLATILE);
                rc = TCL_ERROR;
            }
            sqlite3_exec(tdb->db, "ROLLBACK", NULL, NULL, NULL);
        }
        return rc;
    }

    /* ---- collate NAME SCRIPT ---- */

    case CMD_COLLATE: {
        if (objc != 4) {
            Tcl_WrongNumArgs(interp, 2, objv, "NAME SCRIPT");
            return TCL_ERROR;
        }
        TclCollateData *data = (TclCollateData *)Tcl_Alloc(sizeof(TclCollateData));
        data->interp = interp;
        data->script = objv[3];
        Tcl_IncrRefCount(data->script);
        int rc = sqlite3_create_collation_v2(
            tdb->db, Tcl_GetString(objv[2]), 0, (void *)data,
            (int (*)(void))tcl_collate_bridge,
            (void (*)(void))tcl_collate_destroy);
        if (rc != SQLITE_OK) {
            tcl_collate_destroy(data);
            Tcl_SetResult(interp, (char *)sqlite3_errmsg(tdb->db), TCL_VOLATILE);
            return TCL_ERROR;
        }
        return TCL_OK;
    }

    /* ---- timeout MS ---- */

    case CMD_TIMEOUT: {
        int ms;
        if (objc != 3) {
            Tcl_WrongNumArgs(interp, 2, objv, "MILLISECONDS");
            return TCL_ERROR;
        }
        if (Tcl_GetIntFromObj(interp, objv[2], &ms) != TCL_OK) return TCL_ERROR;
        sqlite3_busy_timeout(tdb->db, ms);
        return TCL_OK;
    }

    case CMD_INTERRUPT:
        sqlite3_interrupt(tdb->db);
        return TCL_OK;

    case CMD_VERSION:
        Tcl_SetResult(interp, (char *)sqlite3_libversion(), TCL_STATIC);
        return TCL_OK;

    /* Subcommands of the upstream binding that Turso has no engine support
     * for. They accept their arguments and do nothing, so a test file that
     * calls them at top level keeps running; the tests that depend on
     * their effect fail on their own assertions. A hook subcommand called
     * with no script returns the empty string, as upstream does when no
     * hook is set. */
    case CMD_BUSY:
    case CMD_AUTH:
    case CMD_AUTHORIZER:
    case CMD_PROGRESS:
    case CMD_COMMIT_HOOK:
    case CMD_UPDATE_HOOK:
    case CMD_ROLLBACK_HOOK:
    case CMD_WAL_HOOK:
    case CMD_PREUPDATE:
    case CMD_TRACE:
    case CMD_TRACE_V2:
    case CMD_PROFILE:
    case CMD_UNLOCK_NOTIFY:
    case CMD_ENABLE_LOAD_EXTENSION:
    case CMD_CONFIG:
    case CMD_ERASE:
    case CMD_BIND_FALLBACK:
    case CMD_COLLATION_NEEDED:
    case CMD_BACKUP:
    case CMD_RESTORE:
    case CMD_SERIALIZE:
    case CMD_DESERIALIZE:
    case CMD_COPY:
        Tcl_ResetResult(interp);
        return TCL_OK;

    case CMD_INCRBLOB:
        Tcl_SetResult(interp, (char *)"incremental blob I/O is not supported",
                      TCL_STATIC);
        return TCL_ERROR;

    /* ---- eval ---- */

    case CMD_EVAL: {
        if (objc < 3 || objc > 5) {
            Tcl_WrongNumArgs(interp, 2, objv, "sql ?array? ?script?");
            return TCL_ERROR;
        }

        const char *sql      = Tcl_GetString(objv[2]);
        const char *null_str = tdb->null_obj
                               ? Tcl_GetString(tdb->null_obj) : "";

        /* db eval sql — collect all result values into a flat list */
        if (objc == 3) {
            Tcl_Obj *result_list = NULL;
            int rc = exec_sql_collect(tdb, sql, null_str,
                                      &result_list);
            if (rc != TCL_OK) return rc;
            Tcl_SetObjResult(interp, result_list);
            Tcl_DecrRefCount(result_list);
            return TCL_OK;
        }

        /* db eval sql ?array? script — per-row callback. With an array
         * name, columns land in array(col); without one, each column is
         * set as a scalar variable named after it, as in the upstream
         * SQLite TCL binding. */
        if (objc == 4 || objc == 5) {
            Tcl_Obj *array_name = (objc == 5) ? objv[3] : NULL;
            Tcl_Obj *script     = objv[objc - 1];

            const char   *remaining = sql;
            int           loop_rc   = TCL_OK;

            while (remaining && *remaining) {
                while (*remaining == ' ' || *remaining == '\n' ||
                       *remaining == '\t' || *remaining == '\r' ||
                       *remaining == ';') {
                    remaining++;
                }
                if (!*remaining) break;

                sqlite3_stmt *stmt = NULL;
                const char   *tail = NULL;

                int rc = sqlite3_prepare_v2(tdb->db, remaining, -1, &stmt, &tail);
                if (rc != SQLITE_OK) {
                    Tcl_SetResult(interp, (char *)sqlite3_errmsg(tdb->db),
                                  TCL_VOLATILE);
                    return TCL_ERROR;
                }
                if (!stmt) { remaining = tail; continue; }

                /* Bind TCL variables to any parameters */
                bind_tcl_variables(interp, stmt);

                int ncols = sqlite3_column_count(stmt);

                /* Set array(*) to the list of column names. */
                int i;
                if (array_name) {
                    Tcl_Obj *col_list = Tcl_NewListObj(0, NULL);
                    for (i = 0; i < ncols; i++) {
                        const char *col = sqlite3_column_name(stmt, i);
                        Tcl_ListObjAppendElement(interp, col_list,
                            Tcl_NewStringObj(col ? col : "", -1));
                    }
                    Tcl_ObjSetVar2(interp, array_name,
                                   Tcl_NewStringObj("*", 1), col_list, 0);
                }

                while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
                    for (i = 0; i < ncols; i++) {
                        const char *col = sqlite3_column_name(stmt, i);
                        Tcl_Obj *val = column_to_obj(stmt, i, null_str);
                        Tcl_Obj *col_obj = Tcl_NewStringObj(col ? col : "", -1);
                        if (array_name) {
                            Tcl_ObjSetVar2(interp, array_name, col_obj, val, 0);
                        } else {
                            Tcl_ObjSetVar2(interp, col_obj, NULL, val, 0);
                        }
                    }

                    loop_rc = Tcl_EvalObjEx(interp, script, 0);
                    if (loop_rc == TCL_BREAK) {
                        loop_rc = TCL_OK;
                        break;
                    } else if (loop_rc == TCL_CONTINUE) {
                        loop_rc = TCL_OK;
                    } else if (loop_rc != TCL_OK) {
                        break;
                    }
                }

                if (rc == SQLITE_DONE) {
                    capture_stmt_status(tdb, stmt);
                }
                sqlite3_finalize(stmt);

                if (loop_rc != TCL_OK) return loop_rc;

                if (rc != SQLITE_DONE && rc != SQLITE_ROW) {
                    Tcl_SetResult(interp, (char *)sqlite3_errmsg(tdb->db),
                                  TCL_VOLATILE);
                    return TCL_ERROR;
                }

                remaining = tail;
            }

            Tcl_ResetResult(interp);
            return TCL_OK;
        }

        /* unreachable: objc validated to 3..5 above */
        Tcl_WrongNumArgs(interp, 2, objv, "sql ?array? ?script?");
        return TCL_ERROR;
    }

    /* ---- one ---- */

    case CMD_ONE: {
        if (objc != 3) {
            Tcl_WrongNumArgs(interp, 2, objv, "sql");
            return TCL_ERROR;
        }
        const char *sql      = Tcl_GetString(objv[2]);
        const char *null_str = tdb->null_obj
                               ? Tcl_GetString(tdb->null_obj) : "";

        sqlite3_stmt *stmt = NULL;
        int rc = sqlite3_prepare_v2(tdb->db, sql, -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            Tcl_SetResult(interp, (char *)sqlite3_errmsg(tdb->db), TCL_VOLATILE);
            return TCL_ERROR;
        }

        bind_tcl_variables(interp, stmt);

        Tcl_Obj *result = Tcl_NewStringObj(null_str, -1);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            result = column_to_obj(stmt, 0, null_str);
        }
        sqlite3_finalize(stmt);
        Tcl_SetObjResult(interp, result);
        return TCL_OK;
    }

    /* ---- exists ---- */

    case CMD_EXISTS: {
        if (objc != 3) {
            Tcl_WrongNumArgs(interp, 2, objv, "sql");
            return TCL_ERROR;
        }
        const char *sql = Tcl_GetString(objv[2]);

        sqlite3_stmt *stmt = NULL;
        int rc = sqlite3_prepare_v2(tdb->db, sql, -1, &stmt, NULL);
        if (rc != SQLITE_OK) {
            Tcl_SetResult(interp, (char *)sqlite3_errmsg(tdb->db), TCL_VOLATILE);
            return TCL_ERROR;
        }
        bind_tcl_variables(interp, stmt);
        int exists = (sqlite3_step(stmt) == SQLITE_ROW) ? 1 : 0;
        sqlite3_finalize(stmt);
        Tcl_SetObjResult(interp, Tcl_NewBooleanObj(exists));
        return TCL_OK;
    }

    /* ---- func / function ---- */

    case CMD_FUNC:
    case CMD_FUNCTION: {
        /*
         * db func name ?arglist? body
         * db function name ?arglist? body
         *
         * Registers a Tcl proc body as a scalar SQL function.  The arglist
         * mirrors proc syntax: it may be a single Tcl list object ({a b}) or
         * multiple individual words (a b) — both result in named variables
         * being bound before the body is evaluated.
         *
         *   objv[2]         = function name
         *   objv[3..objc-2] = argument variable names, OR a single Tcl list
         *   objv[objc-1]    = script body
         */
        if (objc < 4) {
            Tcl_WrongNumArgs(interp, 2, objv, "name ?arglist? body");
            return TCL_ERROR;
        }

        const char *func_name = Tcl_GetString(objv[2]);
        Tcl_Obj    *body      = objv[objc - 1];
        int         i;

        /* Resolve the argument variable names.
         *
         * objc == 4: db func name body          → no named args
         * objc == 5: db func name argspec body  → argspec is a Tcl list
         * objc >= 6: db func name a b … body    → each word is a name
         */
        Tcl_Size    n_args   = 0;
        Tcl_Obj   **arg_objs = NULL;

        if (objc == 5) {
            /* Single argspec object — split it as a Tcl list so that both
             * `db func f x body` and `db func f {x y} body` work. */
            if (Tcl_ListObjGetElements(interp, objv[3],
                                       &n_args, &arg_objs) != TCL_OK) {
                return TCL_ERROR;
            }
        } else if (objc > 5) {
            n_args   = objc - 4;
            arg_objs = (Tcl_Obj **)&objv[3];
        }

        TclFuncData *func_data =
            (TclFuncData *)Tcl_Alloc(sizeof(TclFuncData));
        memset(func_data, 0, sizeof(TclFuncData));
        func_data->interp  = interp;
        func_data->script  = body;
        Tcl_IncrRefCount(body);
        func_data->n_args  = (n_args < MAX_FUNC_ARGS) ? n_args : MAX_FUNC_ARGS;

        for (i = 0; i < func_data->n_args; i++) {
            func_data->arg_names[i] = arg_objs[i];
            Tcl_IncrRefCount(func_data->arg_names[i]);
        }

        int sql_n_args = (n_args == 0) ? -1 : n_args;
        int rc = sqlite3_create_function_v2(
            tdb->db,
            func_name,
            sql_n_args,
            0, /* SQLITE_UTF8 */
            (void *)func_data,
            (void (*)(void))tcl_scalar_bridge,
            NULL, NULL,
            (void (*)(void))tcl_func_destroy
        );

        if (rc != SQLITE_OK) {
            tcl_func_destroy(func_data);
            Tcl_SetResult(interp,
                (char *)sqlite3_errmsg(tdb->db), TCL_VOLATILE);
            return TCL_ERROR;
        }
        return TCL_OK;
    }

    } /* switch */

    return TCL_OK;
}

/* ------------------------------------------------------------------ */
/* sqlite3_exec command                                                 */
/* ------------------------------------------------------------------ */

/* Resolve a database handle name (e.g. "db") to its TursoDb state, or
 * NULL if the name is not a database command created by [sqlite3]. */
static TursoDb *find_turso_db(Tcl_Interp *interp, const char *name)
{
    Tcl_CmdInfo info;
    if (!Tcl_GetCommandInfo(interp, name, &info)) return NULL;
    if (info.objProc != TursoDbCmd) return NULL;
    return (TursoDb *)info.objClientData;
}

static int hex_to_int(int c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

/* Row callback for TursoExecCmd: mirrors upstream test1.c exec_printf_cb.
 * The first row appends the column names, then every row appends its
 * values, with NULL rendered as the string "NULL". */
typedef struct ExecCbState {
    Tcl_Interp *interp;
    Tcl_Obj    *list;
    int         seen_row;
} ExecCbState;

static int exec_collect_cb(void *ctx, int argc, char **argv, char **colv)
{
    ExecCbState *s = (ExecCbState *)ctx;
    int i;
    if (!s->seen_row) {
        for (i = 0; i < argc; i++) {
            Tcl_ListObjAppendElement(s->interp, s->list,
                Tcl_NewStringObj(colv[i] ? colv[i] : "", -1));
        }
        s->seen_row = 1;
    }
    for (i = 0; i < argc; i++) {
        Tcl_ListObjAppendElement(s->interp, s->list,
            Tcl_NewStringObj(argv[i] ? argv[i] : "NULL", -1));
    }
    return 0;
}

/*
 * sqlite3_exec DB SQL
 *
 * The upstream test-harness command from test1.c: runs SQL through the
 * sqlite3_exec C API and returns a two-element list {rc results}, where
 * results is column names followed by row values on success, or the error
 * message on failure. "%HH" sequences in the SQL are decoded to raw bytes
 * first, which is how upstream tests inject invalid UTF-8 into statements.
 */
static int TursoExecCmd(ClientData cd, Tcl_Interp *interp,
                        int objc, Tcl_Obj *const objv[])
{
    (void)cd;

    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "DB SQL");
        return TCL_ERROR;
    }

    const char *db_name = Tcl_GetString(objv[1]);
    TursoDb *tdb = find_turso_db(interp, db_name);
    if (!tdb) {
        Tcl_AppendResult(interp, "no such database: ", db_name, NULL);
        return TCL_ERROR;
    }

    /* Copy the SQL, decoding %HH escapes. Unlike upstream, only decode
     * when two hex digits follow, so a stray '%' (e.g. a LIKE pattern)
     * passes through unchanged. */
    Tcl_Size    in_len;
    const char *in  = Tcl_GetStringFromObj(objv[2], &in_len);
    char       *sql = Tcl_Alloc(in_len + 1);
    Tcl_Size    i, j;
    for (i = j = 0; i < in_len;) {
        if (in[i] == '%' && i + 2 < in_len &&
            hex_to_int(in[i + 1]) >= 0 && hex_to_int(in[i + 2]) >= 0) {
            sql[j++] = (char)((hex_to_int(in[i + 1]) << 4)
                              | hex_to_int(in[i + 2]));
            i += 3;
        } else {
            sql[j++] = in[i++];
        }
    }
    sql[j] = '\0';

    ExecCbState st;
    st.interp   = interp;
    st.list     = Tcl_NewListObj(0, NULL);
    st.seen_row = 0;
    Tcl_IncrRefCount(st.list);

    char *zerr = NULL;
    int rc = sqlite3_exec(tdb->db, sql, exec_collect_cb, &st, &zerr);
    Tcl_Free(sql);

    Tcl_Obj *result = Tcl_NewListObj(0, NULL);
    Tcl_ListObjAppendElement(interp, result, Tcl_NewIntObj(rc));
    if (rc == 0) {
        Tcl_ListObjAppendElement(interp, result, st.list);
    } else {
        Tcl_ListObjAppendElement(interp, result,
            Tcl_NewStringObj(zerr ? zerr : sqlite3_errmsg(tdb->db), -1));
    }
    Tcl_DecrRefCount(st.list);
    if (zerr) sqlite3_free(zerr);

    /* Like upstream, an SQL error is reported through the returned rc, not
     * as a TCL error, so tests can match on {1 {error message}}. */
    Tcl_SetObjResult(interp, result);
    return TCL_OK;
}

/*
 * sqlite3_connection_pointer DB
 *
 * Upstream test1.c returns the C-level sqlite3* for a TCL database
 * handle so tests can hand it to C-API-level commands ("set DB
 * [sqlite3_connection_pointer db]"). Our C-API commands resolve
 * handles by command name, so the name itself is the pointer:
 * validate that it names a database and return it unchanged.
 */
static int TursoConnectionPointerCmd(ClientData cd, Tcl_Interp *interp,
                                     int objc, Tcl_Obj *const objv[])
{
    (void)cd;

    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "DB");
        return TCL_ERROR;
    }

    const char *db_name = Tcl_GetString(objv[1]);
    if (!find_turso_db(interp, db_name)) {
        Tcl_AppendResult(interp, "no such database: ", db_name, NULL);
        return TCL_ERROR;
    }
    Tcl_SetObjResult(interp, objv[1]);
    return TCL_OK;
}

/* ------------------------------------------------------------------ */
/* sqlite3_blob_* commands                                              */
/* ------------------------------------------------------------------ */

/* Symbolic name of a primary result code, as upstream sqlite3ErrName;
 * the blob tests match on these (e.g. {1 SQLITE_ERROR}). */
static const char *turso_err_name(int rc)
{
    switch (rc & 0xff) {
    case 0:   return "SQLITE_OK";
    case 1:   return "SQLITE_ERROR";
    case 2:   return "SQLITE_INTERNAL";
    case 3:   return "SQLITE_PERM";
    case 4:   return "SQLITE_ABORT";
    case 5:   return "SQLITE_BUSY";
    case 6:   return "SQLITE_LOCKED";
    case 7:   return "SQLITE_NOMEM";
    case 8:   return "SQLITE_READONLY";
    case 9:   return "SQLITE_INTERRUPT";
    case 10:  return "SQLITE_IOERR";
    case 11:  return "SQLITE_CORRUPT";
    case 12:  return "SQLITE_NOTFOUND";
    case 13:  return "SQLITE_FULL";
    case 14:  return "SQLITE_CANTOPEN";
    case 17:  return "SQLITE_SCHEMA";
    case 18:  return "SQLITE_TOOBIG";
    case 19:  return "SQLITE_CONSTRAINT";
    case 20:  return "SQLITE_MISMATCH";
    case 21:  return "SQLITE_MISUSE";
    case 25:  return "SQLITE_RANGE";
    case 100: return "SQLITE_ROW";
    case 101: return "SQLITE_DONE";
    default:  return "SQLITE_ERROR";
    }
}

/* Open blob handles, keyed by a generated name so a stale or garbage
 * handle argument is a clean TCL error instead of a wild pointer. */
typedef struct BlobHandle {
    void              *blob;
    char               name[24];
    struct BlobHandle *next;
} BlobHandle;

static BlobHandle *blob_handles      = NULL;
static int         blob_handle_seq   = 0;

static BlobHandle *find_blob_handle(Tcl_Interp *interp, Tcl_Obj *name_obj)
{
    const char *name = Tcl_GetString(name_obj);
    BlobHandle *h;
    for (h = blob_handles; h; h = h->next) {
        if (strcmp(h->name, name) == 0) return h;
    }
    Tcl_AppendResult(interp, "no such blob handle: ", name, NULL);
    return NULL;
}

/*
 * sqlite3_blob_open DB DBNAME TABLE COLUMN ROWID FLAGS VARNAME
 *
 * The upstream test1.c command over the sqlite3_blob_open C API: on
 * success VARNAME is set to a handle for the other sqlite3_blob_*
 * commands; on failure the symbolic result code is raised as the TCL
 * error, which is what the tests match on.
 */
static int TursoBlobOpenCmd(ClientData cd, Tcl_Interp *interp,
                            int objc, Tcl_Obj *const objv[])
{
    (void)cd;

    if (objc != 8) {
        Tcl_WrongNumArgs(interp, 1, objv,
                         "DB DBNAME TABLE COLUMN ROWID FLAGS VARNAME");
        return TCL_ERROR;
    }

    TursoDb *tdb = find_turso_db(interp, Tcl_GetString(objv[1]));
    if (!tdb) {
        /* Upstream reports a non-database first argument as misuse. */
        Tcl_SetResult(interp, (char *)"SQLITE_MISUSE", TCL_STATIC);
        return TCL_ERROR;
    }

    Tcl_WideInt rowid;
    int         flags;
    if (Tcl_GetWideIntFromObj(interp, objv[5], &rowid) != TCL_OK) return TCL_ERROR;
    if (Tcl_GetIntFromObj(interp, objv[6], &flags) != TCL_OK) return TCL_ERROR;

    void *blob = NULL;
    int rc = sqlite3_blob_open(tdb->db, Tcl_GetString(objv[2]),
                               Tcl_GetString(objv[3]), Tcl_GetString(objv[4]),
                               (int64_t)rowid, flags, &blob);
    if (rc != 0) {
        Tcl_SetResult(interp, (char *)turso_err_name(rc), TCL_STATIC);
        return TCL_ERROR;
    }

    BlobHandle *h = (BlobHandle *)Tcl_Alloc(sizeof(BlobHandle));
    h->blob = blob;
    snprintf(h->name, sizeof(h->name), "incrblob_%d", ++blob_handle_seq);
    h->next = blob_handles;
    blob_handles = h;

    if (Tcl_SetVar2Ex(interp, Tcl_GetString(objv[7]), NULL,
                      Tcl_NewStringObj(h->name, -1),
                      TCL_LEAVE_ERR_MSG) == NULL) {
        return TCL_ERROR;
    }
    Tcl_ResetResult(interp);
    return TCL_OK;
}

/* sqlite3_blob_bytes HANDLE */
static int TursoBlobBytesCmd(ClientData cd, Tcl_Interp *interp,
                             int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "HANDLE");
        return TCL_ERROR;
    }
    BlobHandle *h = find_blob_handle(interp, objv[1]);
    if (!h) return TCL_ERROR;
    Tcl_SetObjResult(interp, Tcl_NewIntObj(sqlite3_blob_bytes(h->blob)));
    return TCL_OK;
}

/* sqlite3_blob_read HANDLE OFFSET N — returns N bytes as a byte array */
static int TursoBlobReadCmd(ClientData cd, Tcl_Interp *interp,
                            int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc != 4) {
        Tcl_WrongNumArgs(interp, 1, objv, "HANDLE OFFSET N");
        return TCL_ERROR;
    }
    BlobHandle *h = find_blob_handle(interp, objv[1]);
    if (!h) return TCL_ERROR;

    int offset, n;
    if (Tcl_GetIntFromObj(interp, objv[2], &offset) != TCL_OK) return TCL_ERROR;
    if (Tcl_GetIntFromObj(interp, objv[3], &n) != TCL_OK) return TCL_ERROR;
    if (n < 0) {
        Tcl_SetResult(interp, (char *)"SQLITE_ERROR", TCL_STATIC);
        return TCL_ERROR;
    }

    unsigned char *buf = (unsigned char *)Tcl_Alloc(n > 0 ? n : 1);
    int rc = sqlite3_blob_read(h->blob, buf, n, offset);
    if (rc != 0) {
        Tcl_Free((char *)buf);
        Tcl_SetResult(interp, (char *)turso_err_name(rc), TCL_STATIC);
        return TCL_ERROR;
    }
    Tcl_SetObjResult(interp, Tcl_NewByteArrayObj(buf, n));
    Tcl_Free((char *)buf);
    return TCL_OK;
}

/* sqlite3_blob_write HANDLE OFFSET DATA ?NDATA? */
static int TursoBlobWriteCmd(ClientData cd, Tcl_Interp *interp,
                             int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc != 4 && objc != 5) {
        Tcl_WrongNumArgs(interp, 1, objv, "HANDLE OFFSET DATA ?NDATA?");
        return TCL_ERROR;
    }
    BlobHandle *h = find_blob_handle(interp, objv[1]);
    if (!h) return TCL_ERROR;

    int offset;
    if (Tcl_GetIntFromObj(interp, objv[2], &offset) != TCL_OK) return TCL_ERROR;

    Tcl_Size        data_len;
    unsigned char  *data = Tcl_GetByteArrayFromObj(objv[3], &data_len);
    int             n    = (int)data_len;
    if (objc == 5) {
        if (Tcl_GetIntFromObj(interp, objv[4], &n) != TCL_OK) return TCL_ERROR;
        if (n > (int)data_len) n = (int)data_len;
    }

    int rc = sqlite3_blob_write(h->blob, data, n, offset);
    if (rc != 0) {
        Tcl_SetResult(interp, (char *)turso_err_name(rc), TCL_STATIC);
        return TCL_ERROR;
    }
    return TCL_OK;
}

/* sqlite3_blob_close HANDLE */
static int TursoBlobCloseCmd(ClientData cd, Tcl_Interp *interp,
                             int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "HANDLE");
        return TCL_ERROR;
    }
    BlobHandle *h = find_blob_handle(interp, objv[1]);
    if (!h) return TCL_ERROR;

    int rc = sqlite3_blob_close(h->blob);

    /* The handle is spent even when close reports an error. */
    BlobHandle **pp;
    for (pp = &blob_handles; *pp; pp = &(*pp)->next) {
        if (*pp == h) {
            *pp = h->next;
            break;
        }
    }
    Tcl_Free((char *)h);

    if (rc != 0) {
        Tcl_SetResult(interp, (char *)turso_err_name(rc), TCL_STATIC);
        return TCL_ERROR;
    }
    return TCL_OK;
}

/* ------------------------------------------------------------------ */
/* C-API statement commands (upstream test1.c)                          */
/* ------------------------------------------------------------------ */

/* Prepared statements made by [sqlite3_prepare], keyed by a generated
 * name so a stale or garbage handle argument is a clean TCL error
 * instead of a wild pointer, as with blob handles above. */
typedef struct StmtHandle {
    sqlite3_stmt      *stmt;
    sqlite3           *db;      /* owning connection, for error reports */
    char               name[24];
    struct StmtHandle *next;
} StmtHandle;

static StmtHandle *stmt_handles    = NULL;
static int         stmt_handle_seq = 0;

static StmtHandle *find_stmt_handle(Tcl_Interp *interp, Tcl_Obj *name_obj)
{
    const char *name = Tcl_GetString(name_obj);
    StmtHandle *h;
    for (h = stmt_handles; h; h = h->next) {
        if (strcmp(h->name, name) == 0) return h;
    }
    Tcl_AppendResult(interp, "no such statement handle: ", name, NULL);
    return NULL;
}

/* Turso prefixes some error messages (e.g. "Parse error: no such table")
 * where SQLite reports just the message; the C-API tests match on the
 * SQLite form, so strip the prefix like tester.tcl's normalize_errmsg. */
static const char *strip_err_prefix(const char *msg)
{
    static const char *prefixes[] = { "Parse error: ", "Runtime error: ", NULL };
    int i;
    if (!msg) return "";
    for (i = 0; prefixes[i]; i++) {
        size_t n = strlen(prefixes[i]);
        if (strncmp(msg, prefixes[i], n) == 0) return msg + n;
    }
    return msg;
}

/*
 * sqlite3_prepare DB SQL BYTES ?TAILVAR?
 * sqlite3_prepare_v2 DB SQL BYTES ?TAILVAR?
 *
 * The upstream test1.c commands: prepare the first statement of SQL,
 * store the unparsed remainder in TAILVAR, and return a statement
 * handle for the other C-API commands. On error the result is
 * "(rc) errmsg", which is what the tests match on. Turso has one
 * prepare implementation, so both spellings share it.
 */
static int TursoPrepareCmd(ClientData cd, Tcl_Interp *interp,
                           int objc, Tcl_Obj *const objv[])
{
    (void)cd;

    if (objc != 4 && objc != 5) {
        Tcl_WrongNumArgs(interp, 1, objv, "DB SQL BYTES ?TAILVAR?");
        return TCL_ERROR;
    }

    TursoDb *tdb = find_turso_db(interp, Tcl_GetString(objv[1]));
    if (!tdb) {
        Tcl_AppendResult(interp, "no such database: ",
                         Tcl_GetString(objv[1]), NULL);
        return TCL_ERROR;
    }

    Tcl_Size    sql_len;
    const char *sql = Tcl_GetStringFromObj(objv[2], &sql_len);
    int         bytes;
    if (Tcl_GetIntFromObj(interp, objv[3], &bytes) != TCL_OK) return TCL_ERROR;

    Tcl_Size limit = sql_len;
    if (bytes >= 0 && (Tcl_Size)bytes < sql_len) limit = bytes;

    sqlite3_stmt *stmt = NULL;
    const char   *tail = NULL;
    int rc = sqlite3_prepare_v2(tdb->db, sql, bytes, &stmt, &tail);

    if (objc == 5) {
        Tcl_Obj *tail_obj;
        if (tail && tail >= sql && tail <= sql + limit) {
            tail_obj = Tcl_NewStringObj(tail, (Tcl_Size)(sql + limit - tail));
        } else {
            tail_obj = Tcl_NewStringObj("", 0);
        }
        if (Tcl_ObjSetVar2(interp, objv[4], NULL, tail_obj,
                           TCL_LEAVE_ERR_MSG) == NULL) {
            if (stmt) sqlite3_finalize(stmt);
            return TCL_ERROR;
        }
    }

    if (rc != SQLITE_OK) {
        Tcl_SetObjResult(interp, Tcl_ObjPrintf("(%d) %s", rc,
            strip_err_prefix(sqlite3_errmsg(tdb->db))));
        return TCL_ERROR;
    }
    if (!stmt) {
        Tcl_ResetResult(interp);
        return TCL_OK;
    }

    StmtHandle *h = (StmtHandle *)Tcl_Alloc(sizeof(StmtHandle));
    h->stmt = stmt;
    h->db   = tdb->db;
    snprintf(h->name, sizeof(h->name), "stmt_%d", ++stmt_handle_seq);
    h->next = stmt_handles;
    stmt_handles = h;

    Tcl_SetObjResult(interp, Tcl_NewStringObj(h->name, -1));
    return TCL_OK;
}

/* sqlite3_step STMT — returns the symbolic result code (SQLITE_ROW,
 * SQLITE_DONE, or an error name) as a normal result. */
static int TursoStepCmd(ClientData cd, Tcl_Interp *interp,
                        int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "STMT");
        return TCL_ERROR;
    }
    StmtHandle *h = find_stmt_handle(interp, objv[1]);
    if (!h) return TCL_ERROR;
    Tcl_SetResult(interp, (char *)turso_err_name(sqlite3_step(h->stmt)),
                  TCL_STATIC);
    return TCL_OK;
}

/* An empty or "0" handle stands for a NULL statement pointer, which
 * upstream accepts in finalize and reset as a harmless no-op. */
static int is_null_stmt(Tcl_Obj *obj)
{
    const char *s = Tcl_GetString(obj);
    return s[0] == '\0' || strcmp(s, "0") == 0;
}

/* sqlite3_finalize STMT — finalizes and forgets the handle; returns the
 * symbolic result code. A null handle is a no-op, as upstream. */
static int TursoFinalizeCmd(ClientData cd, Tcl_Interp *interp,
                            int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "STMT");
        return TCL_ERROR;
    }
    if (is_null_stmt(objv[1])) {
        Tcl_SetResult(interp, (char *)"SQLITE_OK", TCL_STATIC);
        return TCL_OK;
    }
    StmtHandle *h = find_stmt_handle(interp, objv[1]);
    if (!h) return TCL_ERROR;

    sqlite3 *db = sqlite3_db_handle(h->stmt);
    int was_last = sqlite3_next_stmt(db, NULL) == h->stmt &&
                   sqlite3_next_stmt(db, h->stmt) == NULL;
    int rc = sqlite3_finalize(h->stmt);
    zombie_stmt_finalized(db, was_last);

    /* The handle is spent even when finalize reports an error. */
    StmtHandle **pp;
    for (pp = &stmt_handles; *pp; pp = &(*pp)->next) {
        if (*pp == h) {
            *pp = h->next;
            break;
        }
    }
    Tcl_Free((char *)h);

    Tcl_SetResult(interp, (char *)turso_err_name(rc), TCL_STATIC);
    return TCL_OK;
}

/* sqlite3_reset STMT — returns the symbolic result code. */
static int TursoResetCmd(ClientData cd, Tcl_Interp *interp,
                         int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "STMT");
        return TCL_ERROR;
    }
    if (is_null_stmt(objv[1])) {
        Tcl_SetResult(interp, (char *)"SQLITE_OK", TCL_STATIC);
        return TCL_OK;
    }
    StmtHandle *h = find_stmt_handle(interp, objv[1]);
    if (!h) return TCL_ERROR;
    Tcl_SetResult(interp, (char *)turso_err_name(sqlite3_reset(h->stmt)),
                  TCL_STATIC);
    return TCL_OK;
}

/* Shared argument parsing for STMT-only counter commands. */
static int stmt_only_args(Tcl_Interp *interp, int objc, Tcl_Obj *const objv[],
                          StmtHandle **out)
{
    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "STMT");
        return TCL_ERROR;
    }
    *out = find_stmt_handle(interp, objv[1]);
    return *out ? TCL_OK : TCL_ERROR;
}

/* sqlite3_column_count STMT */
static int TursoColumnCountCmd(ClientData cd, Tcl_Interp *interp,
                               int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    if (stmt_only_args(interp, objc, objv, &h) != TCL_OK) return TCL_ERROR;
    Tcl_SetObjResult(interp, Tcl_NewIntObj(sqlite3_column_count(h->stmt)));
    return TCL_OK;
}

/* sqlite3_data_count STMT */
static int TursoDataCountCmd(ClientData cd, Tcl_Interp *interp,
                             int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    if (stmt_only_args(interp, objc, objv, &h) != TCL_OK) return TCL_ERROR;
    Tcl_SetObjResult(interp, Tcl_NewIntObj(sqlite3_data_count(h->stmt)));
    return TCL_OK;
}

/* Shared argument parsing for STMT COLUMN accessor commands. */
static int stmt_col_args(Tcl_Interp *interp, int objc, Tcl_Obj *const objv[],
                         StmtHandle **out, int *col)
{
    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "STMT column");
        return TCL_ERROR;
    }
    *out = find_stmt_handle(interp, objv[1]);
    if (!*out) return TCL_ERROR;
    return Tcl_GetIntFromObj(interp, objv[2], col);
}

/* sqlite3_column_type STMT column — returns the type name. */
static int TursoColumnTypeCmd(ClientData cd, Tcl_Interp *interp,
                              int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    int col;
    if (stmt_col_args(interp, objc, objv, &h, &col) != TCL_OK) return TCL_ERROR;
    const char *name;
    switch (sqlite3_column_type(h->stmt, col)) {
    case SQLITE_INTEGER: name = "INTEGER"; break;
    case SQLITE_FLOAT:   name = "FLOAT";   break;
    case SQLITE_TEXT:    name = "TEXT";    break;
    case SQLITE_BLOB:    name = "BLOB";    break;
    default:             name = "NULL";    break;
    }
    Tcl_SetResult(interp, (char *)name, TCL_STATIC);
    return TCL_OK;
}

/* sqlite3_column_text STMT column */
static int TursoColumnTextCmd(ClientData cd, Tcl_Interp *interp,
                              int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    int col;
    if (stmt_col_args(interp, objc, objv, &h, &col) != TCL_OK) return TCL_ERROR;
    const char *text = (const char *)sqlite3_column_text(h->stmt, col);
    int nbytes = sqlite3_column_bytes(h->stmt, col);
    Tcl_SetObjResult(interp, Tcl_NewStringObj(text ? text : "",
                                              text ? nbytes : 0));
    return TCL_OK;
}

/* sqlite3_column_blob STMT column */
static int TursoColumnBlobCmd(ClientData cd, Tcl_Interp *interp,
                              int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    int col;
    if (stmt_col_args(interp, objc, objv, &h, &col) != TCL_OK) return TCL_ERROR;
    const void *blob = sqlite3_column_blob(h->stmt, col);
    int nbytes = sqlite3_column_bytes(h->stmt, col);
    Tcl_SetObjResult(interp, Tcl_NewByteArrayObj(
        (const unsigned char *)(blob ? blob : ""), blob ? nbytes : 0));
    return TCL_OK;
}

/* sqlite3_column_int STMT column */
static int TursoColumnIntCmd(ClientData cd, Tcl_Interp *interp,
                             int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    int col;
    if (stmt_col_args(interp, objc, objv, &h, &col) != TCL_OK) return TCL_ERROR;
    /* sqlite3_column_int is not in the compat header; the C API defines
     * it as the int64 value truncated to int, so do that directly. */
    Tcl_SetObjResult(interp,
        Tcl_NewIntObj((int)sqlite3_column_int64(h->stmt, col)));
    return TCL_OK;
}

/* sqlite3_column_int64 STMT column */
static int TursoColumnInt64Cmd(ClientData cd, Tcl_Interp *interp,
                               int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    int col;
    if (stmt_col_args(interp, objc, objv, &h, &col) != TCL_OK) return TCL_ERROR;
    Tcl_SetObjResult(interp, Tcl_NewWideIntObj(
        (Tcl_WideInt)sqlite3_column_int64(h->stmt, col)));
    return TCL_OK;
}

/* sqlite3_column_double STMT column */
static int TursoColumnDoubleCmd(ClientData cd, Tcl_Interp *interp,
                                int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    int col;
    if (stmt_col_args(interp, objc, objv, &h, &col) != TCL_OK) return TCL_ERROR;
    Tcl_SetObjResult(interp,
        Tcl_NewDoubleObj(sqlite3_column_double(h->stmt, col)));
    return TCL_OK;
}

/* sqlite3_column_bytes STMT column */
static int TursoColumnBytesCmd(ClientData cd, Tcl_Interp *interp,
                               int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    int col;
    if (stmt_col_args(interp, objc, objv, &h, &col) != TCL_OK) return TCL_ERROR;
    Tcl_SetObjResult(interp, Tcl_NewIntObj(sqlite3_column_bytes(h->stmt, col)));
    return TCL_OK;
}

/* sqlite3_column_name STMT column */
static int TursoColumnNameCmd(ClientData cd, Tcl_Interp *interp,
                              int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    int col;
    if (stmt_col_args(interp, objc, objv, &h, &col) != TCL_OK) return TCL_ERROR;
    const char *name = sqlite3_column_name(h->stmt, col);
    Tcl_SetObjResult(interp, Tcl_NewStringObj(name ? name : "", -1));
    return TCL_OK;
}

/* sqlite3_column_decltype STMT column */
static int TursoColumnDecltypeCmd(ClientData cd, Tcl_Interp *interp,
                                  int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    int col;
    if (stmt_col_args(interp, objc, objv, &h, &col) != TCL_OK) return TCL_ERROR;
    const char *type = sqlite3_column_decltype(h->stmt, col);
    Tcl_SetObjResult(interp, Tcl_NewStringObj(type ? type : "", -1));
    return TCL_OK;
}

/* sqlite3_column_table_name STMT column */
static int TursoColumnTableNameCmd(ClientData cd, Tcl_Interp *interp,
                                   int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    int col;
    if (stmt_col_args(interp, objc, objv, &h, &col) != TCL_OK) return TCL_ERROR;
    const char *name = sqlite3_column_table_name(h->stmt, col);
    Tcl_SetObjResult(interp, Tcl_NewStringObj(name ? name : "", -1));
    return TCL_OK;
}

/* sqlite3_stmt_readonly STMT — a NULL statement reports read-only,
 * matching the C API. */
static int TursoStmtReadonlyCmd(ClientData cd, Tcl_Interp *interp,
                                int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc == 2 && is_null_stmt(objv[1])) {
        Tcl_SetObjResult(interp, Tcl_NewBooleanObj(1));
        return TCL_OK;
    }
    StmtHandle *h;
    if (stmt_only_args(interp, objc, objv, &h) != TCL_OK) return TCL_ERROR;
    Tcl_SetObjResult(interp,
        Tcl_NewBooleanObj(sqlite3_stmt_readonly(h->stmt)));
    return TCL_OK;
}

/* sqlite3_stmt_busy STMT — a NULL statement is never busy. */
static int TursoStmtBusyCmd(ClientData cd, Tcl_Interp *interp,
                            int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc == 2 && is_null_stmt(objv[1])) {
        Tcl_SetObjResult(interp, Tcl_NewBooleanObj(0));
        return TCL_OK;
    }
    StmtHandle *h;
    if (stmt_only_args(interp, objc, objv, &h) != TCL_OK) return TCL_ERROR;
    Tcl_SetObjResult(interp, Tcl_NewBooleanObj(sqlite3_stmt_busy(h->stmt)));
    return TCL_OK;
}

/* sqlite3_stmt_isexplain STMT — 1 for EXPLAIN, 2 for EXPLAIN QUERY
 * PLAN, 0 otherwise. The compat layer has no such entry point, so
 * decide from the statement's SQL text, which is what determines the
 * property. */
static int TursoStmtIsexplainCmd(ClientData cd, Tcl_Interp *interp,
                                 int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc == 2 && is_null_stmt(objv[1])) {
        Tcl_SetObjResult(interp, Tcl_NewIntObj(0));
        return TCL_OK;
    }
    StmtHandle *h;
    if (stmt_only_args(interp, objc, objv, &h) != TCL_OK) return TCL_ERROR;

    const char *sql = sqlite3_sql(h->stmt);
    int result = 0;
    if (sql) {
        while (*sql == ' ' || *sql == '\n' || *sql == '\t' || *sql == '\r') {
            sql++;
        }
        if (strncasecmp(sql, "EXPLAIN", 7) == 0) {
            const char *rest = sql + 7;
            while (*rest == ' ' || *rest == '\n' || *rest == '\t' ||
                   *rest == '\r') {
                rest++;
            }
            result = (strncasecmp(rest, "QUERY", 5) == 0) ? 2 : 1;
        }
    }
    Tcl_SetObjResult(interp, Tcl_NewIntObj(result));
    return TCL_OK;
}

/*
 * sqlite3_next_stmt DB STMT
 *
 * Iterates over the prepared statements of a connection: STMT of 0 (or
 * empty) returns the first one, otherwise the one after STMT; an empty
 * result ends the iteration. Upstream walks the connection's own
 * statement list; we walk the harness handle registry filtered by
 * connection, which covers every statement the tests can name.
 */
static int TursoNextStmtCmd(ClientData cd, Tcl_Interp *interp,
                            int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "DB STMT");
        return TCL_ERROR;
    }
    TursoDb *tdb = find_turso_db(interp, Tcl_GetString(objv[1]));
    if (!tdb) {
        Tcl_AppendResult(interp, "no such database: ",
                         Tcl_GetString(objv[1]), NULL);
        return TCL_ERROR;
    }

    StmtHandle *from;
    if (is_null_stmt(objv[2])) {
        from = stmt_handles;
    } else {
        StmtHandle *h = find_stmt_handle(interp, objv[2]);
        if (!h) return TCL_ERROR;
        from = h->next;
    }
    for (; from; from = from->next) {
        if (from->db == tdb->db) {
            Tcl_SetObjResult(interp, Tcl_NewStringObj(from->name, -1));
            return TCL_OK;
        }
    }
    Tcl_ResetResult(interp);
    return TCL_OK;
}

/* ------------------------------------------------------------------ */
/* C-API bind commands (upstream test1.c)                               */
/* ------------------------------------------------------------------ */

/* Shared argument parsing for STMT IDX VALUE bind commands; the value
 * object stays in objv[3] for the caller to convert. */
static int stmt_bind_args(Tcl_Interp *interp, int objc, Tcl_Obj *const objv[],
                          int expected, const char *usage,
                          StmtHandle **out, int *idx)
{
    if (objc != expected) {
        Tcl_WrongNumArgs(interp, 1, objv, usage);
        return TCL_ERROR;
    }
    *out = find_stmt_handle(interp, objv[1]);
    if (!*out) return TCL_ERROR;
    return Tcl_GetIntFromObj(interp, objv[2], idx);
}

/* Report a bind result as upstream: empty result on success, the
 * symbolic result code as a TCL error otherwise. */
static int bind_result(Tcl_Interp *interp, int rc)
{
    if (rc != SQLITE_OK) {
        Tcl_SetResult(interp, (char *)turso_err_name(rc), TCL_STATIC);
        return TCL_ERROR;
    }
    Tcl_ResetResult(interp);
    return TCL_OK;
}

/* sqlite3_bind_int STMT IDX VALUE (also registered as sqlite3_bind_int64) */
static int TursoBindInt64Cmd(ClientData cd, Tcl_Interp *interp,
                             int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    int idx;
    Tcl_WideInt val;
    if (stmt_bind_args(interp, objc, objv, 4, "STMT N VALUE",
                       &h, &idx) != TCL_OK) return TCL_ERROR;
    if (Tcl_GetWideIntFromObj(interp, objv[3], &val) != TCL_OK) return TCL_ERROR;
    return bind_result(interp,
        sqlite3_bind_int64(h->stmt, idx, (int64_t)val));
}

/* sqlite3_bind_double STMT IDX VALUE */
static int TursoBindDoubleCmd(ClientData cd, Tcl_Interp *interp,
                              int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    int idx;
    double val;
    if (stmt_bind_args(interp, objc, objv, 4, "STMT N VALUE",
                       &h, &idx) != TCL_OK) return TCL_ERROR;
    if (Tcl_GetDoubleFromObj(interp, objv[3], &val) != TCL_OK) return TCL_ERROR;
    return bind_result(interp, sqlite3_bind_double(h->stmt, idx, val));
}

/* sqlite3_bind_null STMT IDX */
static int TursoBindNullCmd(ClientData cd, Tcl_Interp *interp,
                            int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    int idx;
    if (stmt_bind_args(interp, objc, objv, 3, "STMT N",
                       &h, &idx) != TCL_OK) return TCL_ERROR;
    return bind_result(interp, sqlite3_bind_null(h->stmt, idx));
}

/* sqlite3_bind_text STMT IDX STRING BYTES */
static int TursoBindTextCmd(ClientData cd, Tcl_Interp *interp,
                            int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    int idx, bytes;
    if (stmt_bind_args(interp, objc, objv, 5, "STMT N VALUE BYTES",
                       &h, &idx) != TCL_OK) return TCL_ERROR;
    if (Tcl_GetIntFromObj(interp, objv[4], &bytes) != TCL_OK) return TCL_ERROR;
    Tcl_Size len;
    const char *value = Tcl_GetStringFromObj(objv[3], &len);
    if (bytes < 0) bytes = (int)len;
    return bind_result(interp,
        sqlite3_bind_text(h->stmt, idx, value, bytes, SQLITE_TRANSIENT));
}

/* sqlite3_bind_blob STMT IDX DATA BYTES */
static int TursoBindBlobCmd(ClientData cd, Tcl_Interp *interp,
                            int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    int idx, bytes;
    if (stmt_bind_args(interp, objc, objv, 5, "STMT N DATA BYTES",
                       &h, &idx) != TCL_OK) return TCL_ERROR;
    if (Tcl_GetIntFromObj(interp, objv[4], &bytes) != TCL_OK) return TCL_ERROR;
    Tcl_Size len;
    unsigned char *data = Tcl_GetByteArrayFromObj(objv[3], &len);
    if (bytes < 0 || bytes > (int)len) bytes = (int)len;
    return bind_result(interp,
        sqlite3_bind_blob(h->stmt, idx, data, bytes, SQLITE_TRANSIENT));
}

/* sqlite3_bind_zeroblob STMT IDX N — turso's compat layer has no
 * zeroblob binder, and a zeroblob is defined as N zero bytes, so bind
 * exactly that. */
static int TursoBindZeroblobCmd(ClientData cd, Tcl_Interp *interp,
                                int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    int idx, n;
    if (stmt_bind_args(interp, objc, objv, 4, "STMT IDX N",
                       &h, &idx) != TCL_OK) return TCL_ERROR;
    if (Tcl_GetIntFromObj(interp, objv[3], &n) != TCL_OK) return TCL_ERROR;
    if (n < 0) n = 0;
    unsigned char *zeros = (unsigned char *)Tcl_Alloc(n > 0 ? n : 1);
    memset(zeros, 0, n > 0 ? n : 1);
    int rc = sqlite3_bind_blob(h->stmt, idx, zeros, n, SQLITE_TRANSIENT);
    Tcl_Free((char *)zeros);
    return bind_result(interp, rc);
}

/* sqlite3_bind_parameter_count STMT */
static int TursoBindParameterCountCmd(ClientData cd, Tcl_Interp *interp,
                                      int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    if (stmt_only_args(interp, objc, objv, &h) != TCL_OK) return TCL_ERROR;
    Tcl_SetObjResult(interp,
        Tcl_NewIntObj(sqlite3_bind_parameter_count(h->stmt)));
    return TCL_OK;
}

/* sqlite3_bind_parameter_name STMT N */
static int TursoBindParameterNameCmd(ClientData cd, Tcl_Interp *interp,
                                     int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    int idx;
    if (stmt_col_args(interp, objc, objv, &h, &idx) != TCL_OK) return TCL_ERROR;
    const char *name = sqlite3_bind_parameter_name(h->stmt, idx);
    Tcl_SetObjResult(interp, Tcl_NewStringObj(name ? name : "", -1));
    return TCL_OK;
}

/* sqlite3_bind_parameter_index STMT NAME */
static int TursoBindParameterIndexCmd(ClientData cd, Tcl_Interp *interp,
                                      int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "STMT NAME");
        return TCL_ERROR;
    }
    StmtHandle *h = find_stmt_handle(interp, objv[1]);
    if (!h) return TCL_ERROR;
    Tcl_SetObjResult(interp, Tcl_NewIntObj(
        sqlite3_bind_parameter_index(h->stmt, Tcl_GetString(objv[2]))));
    return TCL_OK;
}

/* sqlite3_clear_bindings STMT */
static int TursoClearBindingsCmd(ClientData cd, Tcl_Interp *interp,
                                 int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    StmtHandle *h;
    if (stmt_only_args(interp, objc, objv, &h) != TCL_OK) return TCL_ERROR;
    Tcl_SetResult(interp,
        (char *)turso_err_name(sqlite3_clear_bindings(h->stmt)), TCL_STATIC);
    return TCL_OK;
}

/* ------------------------------------------------------------------ */
/* C-API connection commands (upstream test1.c)                         */
/* ------------------------------------------------------------------ */

/* Shared argument parsing for DB-only commands. */
static int db_only_args(Tcl_Interp *interp, int objc, Tcl_Obj *const objv[],
                        TursoDb **out)
{
    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "DB");
        return TCL_ERROR;
    }
    *out = find_turso_db(interp, Tcl_GetString(objv[1]));
    if (!*out) {
        Tcl_AppendResult(interp, "no such database: ",
                         Tcl_GetString(objv[1]), NULL);
        return TCL_ERROR;
    }
    return TCL_OK;
}

/* sqlite3_errcode DB — symbolic name of the most recent primary result
 * code on the connection. */
static int TursoErrcodeCmd(ClientData cd, Tcl_Interp *interp,
                           int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    TursoDb *tdb;
    if (db_only_args(interp, objc, objv, &tdb) != TCL_OK) return TCL_ERROR;
    Tcl_SetResult(interp, (char *)turso_err_name(sqlite3_errcode(tdb->db)),
                  TCL_STATIC);
    return TCL_OK;
}

/* sqlite3_extended_errcode DB */
static int TursoExtendedErrcodeCmd(ClientData cd, Tcl_Interp *interp,
                                   int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    TursoDb *tdb;
    if (db_only_args(interp, objc, objv, &tdb) != TCL_OK) return TCL_ERROR;
    Tcl_SetResult(interp,
        (char *)turso_err_name(sqlite3_extended_errcode(tdb->db)), TCL_STATIC);
    return TCL_OK;
}

/* sqlite3_errmsg DB — most recent error message. */
static int TursoErrmsgCmd(ClientData cd, Tcl_Interp *interp,
                          int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    TursoDb *tdb;
    if (db_only_args(interp, objc, objv, &tdb) != TCL_OK) return TCL_ERROR;
    Tcl_SetResult(interp,
        (char *)strip_err_prefix(sqlite3_errmsg(tdb->db)), TCL_VOLATILE);
    return TCL_OK;
}

/* sqlite3_changes DB */
static int TursoChangesCmd(ClientData cd, Tcl_Interp *interp,
                           int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    TursoDb *tdb;
    if (db_only_args(interp, objc, objv, &tdb) != TCL_OK) return TCL_ERROR;
    Tcl_SetObjResult(interp, Tcl_NewIntObj(sqlite3_changes(tdb->db)));
    return TCL_OK;
}

/* sqlite3_total_changes DB */
static int TursoTotalChangesCmd(ClientData cd, Tcl_Interp *interp,
                                int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    TursoDb *tdb;
    if (db_only_args(interp, objc, objv, &tdb) != TCL_OK) return TCL_ERROR;
    Tcl_SetObjResult(interp, Tcl_NewIntObj(sqlite3_total_changes(tdb->db)));
    return TCL_OK;
}

/* sqlite3_get_autocommit DB */
static int TursoGetAutocommitCmd(ClientData cd, Tcl_Interp *interp,
                                 int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    TursoDb *tdb;
    if (db_only_args(interp, objc, objv, &tdb) != TCL_OK) return TCL_ERROR;
    Tcl_SetObjResult(interp, Tcl_NewIntObj(sqlite3_get_autocommit(tdb->db)));
    return TCL_OK;
}

/* sqlite3_interrupt DB */
static int TursoInterruptCmd(ClientData cd, Tcl_Interp *interp,
                             int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    TursoDb *tdb;
    if (db_only_args(interp, objc, objv, &tdb) != TCL_OK) return TCL_ERROR;
    sqlite3_interrupt(tdb->db);
    return TCL_OK;
}

/* sqlite3_extended_result_codes DB BOOLEAN */
static int TursoExtendedResultCodesCmd(ClientData cd, Tcl_Interp *interp,
                                       int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    int onoff;
    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "DB BOOLEAN");
        return TCL_ERROR;
    }
    TursoDb *tdb = find_turso_db(interp, Tcl_GetString(objv[1]));
    if (!tdb) {
        Tcl_AppendResult(interp, "no such database: ", Tcl_GetString(objv[1]), NULL);
        return TCL_ERROR;
    }
    if (Tcl_GetBooleanFromObj(interp, objv[2], &onoff) != TCL_OK) return TCL_ERROR;
    sqlite3_extended_result_codes(tdb->db, onoff);
    return TCL_OK;
}

/* sqlite3_db_readonly DB NAME */
static int TursoDbReadonlyCmd(ClientData cd, Tcl_Interp *interp,
                              int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "DB NAME");
        return TCL_ERROR;
    }
    TursoDb *tdb = find_turso_db(interp, Tcl_GetString(objv[1]));
    if (!tdb) {
        Tcl_AppendResult(interp, "no such database: ", Tcl_GetString(objv[1]), NULL);
        return TCL_ERROR;
    }
    Tcl_SetObjResult(interp, Tcl_NewIntObj(
        sqlite3_db_readonly(tdb->db, Tcl_GetString(objv[2]))));
    return TCL_OK;
}

/* sqlite3_wal_checkpoint DB ?NAME? */
static int TursoWalCheckpointCmd(ClientData cd, Tcl_Interp *interp,
                                 int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc != 2 && objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "DB ?NAME?");
        return TCL_ERROR;
    }
    TursoDb *tdb = find_turso_db(interp, Tcl_GetString(objv[1]));
    if (!tdb) {
        Tcl_AppendResult(interp, "no such database: ", Tcl_GetString(objv[1]), NULL);
        return TCL_ERROR;
    }
    const char *name = objc == 3 ? Tcl_GetString(objv[2]) : NULL;
    int rc = sqlite3_wal_checkpoint(tdb->db, name);
    Tcl_SetResult(interp, (char *)sqlite3_errstr(rc), TCL_STATIC);
    return TCL_OK;
}

/* sqlite3_wal_checkpoint_v2 DB MODE ?NAME? -> {busy nLog nCkpt} */
static int TursoWalCheckpointV2Cmd(ClientData cd, Tcl_Interp *interp,
                                   int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    static const char *modes[] = {"passive", "full", "restart", "truncate", NULL};
    int mode;
    if (objc != 3 && objc != 4) {
        Tcl_WrongNumArgs(interp, 1, objv, "DB MODE ?NAME?");
        return TCL_ERROR;
    }
    TursoDb *tdb = find_turso_db(interp, Tcl_GetString(objv[1]));
    if (!tdb) {
        Tcl_AppendResult(interp, "no such database: ", Tcl_GetString(objv[1]), NULL);
        return TCL_ERROR;
    }
    if (Tcl_GetIndexFromObj(interp, objv[2], modes, "mode", 0, &mode) != TCL_OK) {
        return TCL_ERROR;
    }
    const char *name = objc == 4 ? Tcl_GetString(objv[3]) : NULL;
    int n_log = 0, n_ckpt = 0;
    int rc = sqlite3_wal_checkpoint_v2(tdb->db, name, mode, &n_log, &n_ckpt);
    if (rc != SQLITE_OK && rc != SQLITE_BUSY) {
        Tcl_SetResult(interp, (char *)sqlite3_errstr(rc), TCL_STATIC);
        return TCL_ERROR;
    }
    Tcl_Obj *res = Tcl_NewListObj(0, NULL);
    Tcl_ListObjAppendElement(interp, res, Tcl_NewIntObj(rc == SQLITE_BUSY));
    Tcl_ListObjAppendElement(interp, res, Tcl_NewIntObj(n_log));
    Tcl_ListObjAppendElement(interp, res, Tcl_NewIntObj(n_ckpt));
    Tcl_SetObjResult(interp, res);
    return TCL_OK;
}

/* sqlite3_last_insert_rowid DB */
static int TursoLastInsertRowidCmd(ClientData cd, Tcl_Interp *interp,
                                   int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    TursoDb *tdb;
    if (db_only_args(interp, objc, objv, &tdb) != TCL_OK) return TCL_ERROR;
    Tcl_SetObjResult(interp,
        Tcl_NewWideIntObj((Tcl_WideInt)sqlite3_last_insert_rowid(tdb->db)));
    return TCL_OK;
}

/* sqlite3_complete SQL — 1 if the SQL ends in a complete statement. */
static int TursoCompleteCmd(ClientData cd, Tcl_Interp *interp,
                            int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "SQL");
        return TCL_ERROR;
    }
    Tcl_SetObjResult(interp,
        Tcl_NewIntObj(sqlite3_complete(Tcl_GetString(objv[1]))));
    return TCL_OK;
}

/*
 * sqlite3_open FILENAME ?OPTIONS?
 *
 * The upstream test1.c command over the sqlite3_open C API: returns a
 * handle for the other C-API commands (in upstream a pointer string;
 * here a generated command name so [sqlite3_prepare $H ...] resolves
 * it). OPTIONS is accepted and ignored, as upstream.
 */
static int TursoCApiOpenCmd(ClientData cd, Tcl_Interp *interp,
                            int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    static int open_seq = 0;

    if (objc != 2 && objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "FILENAME ?OPTIONS?");
        return TCL_ERROR;
    }

    sqlite3 *db = NULL;
    sqlite3_open(Tcl_GetString(objv[1]), &db);
    if (!db) {
        /* Upstream returns a pointer string even for a failed open (the
         * handle carries the error state); turso returns no object, so
         * an empty handle is the closest equivalent. */
        Tcl_ResetResult(interp);
        return TCL_OK;
    }

    TursoDb *tdb = (TursoDb *)Tcl_Alloc(sizeof(TursoDb));
    memset(tdb, 0, sizeof(TursoDb));
    tdb->db     = db;
    tdb->interp = interp;

    char handle_name[24];
    snprintf(handle_name, sizeof(handle_name), "dbptr_%d", ++open_seq);
    Tcl_CreateObjCommand(interp, handle_name, TursoDbCmd,
                         (ClientData)tdb, TursoDbFree);
    Tcl_SetObjResult(interp, Tcl_NewStringObj(handle_name, -1));
    return TCL_OK;
}

/* sqlite3_close DB — closes the connection and returns the symbolic
 * result code; the handle command is deleted on success. */
static int TursoCApiCloseCmd(ClientData cd, Tcl_Interp *interp,
                             int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "DB");
        return TCL_ERROR;
    }
    TursoDb *tdb = find_turso_db(interp, Tcl_GetString(objv[1]));
    if (!tdb) {
        Tcl_AppendResult(interp, "no such database: ",
                         Tcl_GetString(objv[1]), NULL);
        return TCL_ERROR;
    }

    /* Cached statements must die before the connection they belong to. */
    cache_free(tdb);
    int rc = sqlite3_close(tdb->db);
    if (rc == SQLITE_OK) {
        tdb->db = NULL; /* TursoDbFree must not close it again */
        Tcl_DeleteCommand(interp, Tcl_GetString(objv[1]));
    }
    Tcl_SetResult(interp, (char *)turso_err_name(rc), TCL_STATIC);
    return TCL_OK;
}

/* sqlite3_close_v2 DB — like sqlite3_close, except that a connection
 * with outstanding statements becomes a zombie instead of failing: the
 * handle command then stays until the last statement is finalized. */
static int TursoCApiCloseV2Cmd(ClientData cd, Tcl_Interp *interp,
                               int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "DB");
        return TCL_ERROR;
    }
    TursoDb *tdb = find_turso_db(interp, Tcl_GetString(objv[1]));
    if (!tdb) {
        Tcl_AppendResult(interp, "no such database: ",
                         Tcl_GetString(objv[1]), NULL);
        return TCL_ERROR;
    }
    cache_free(tdb);
    int outstanding = sqlite3_next_stmt(tdb->db, NULL) != NULL;
    int rc = sqlite3_close_v2(tdb->db);
    if (rc == SQLITE_OK) {
        if (outstanding) {
            zombie_add(tdb, Tcl_GetString(objv[1]));
        } else {
            tdb->db = NULL; /* TursoDbFree must not close it again */
            Tcl_DeleteCommand(interp, Tcl_GetString(objv[1]));
        }
    }
    Tcl_SetResult(interp, (char *)turso_err_name(rc), TCL_STATIC);
    return TCL_OK;
}

/* sqlite3_db_filename DB DBNAME */
static int TursoDbFilenameCmd(ClientData cd, Tcl_Interp *interp,
                              int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "DB DBNAME");
        return TCL_ERROR;
    }
    TursoDb *tdb = find_turso_db(interp, Tcl_GetString(objv[1]));
    if (!tdb) {
        Tcl_AppendResult(interp, "no such database: ",
                         Tcl_GetString(objv[1]), NULL);
        return TCL_ERROR;
    }
    const char *name = sqlite3_db_filename(tdb->db, Tcl_GetString(objv[2]));
    Tcl_SetObjResult(interp, Tcl_NewStringObj(name ? name : "", -1));
    return TCL_OK;
}

/*
 * sqlite3_table_column_metadata DB DBNAME TABLE ?COLUMN?
 *
 * Returns {decltype collseq notnull primarykey autoincrement} on
 * success, or raises the error message as a TCL error, as upstream.
 */
static int TursoTableColumnMetadataCmd(ClientData cd, Tcl_Interp *interp,
                                       int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc != 4 && objc != 5) {
        Tcl_WrongNumArgs(interp, 1, objv, "DB dbname tblname ?colname?");
        return TCL_ERROR;
    }
    TursoDb *tdb = find_turso_db(interp, Tcl_GetString(objv[1]));
    if (!tdb) {
        Tcl_AppendResult(interp, "no such database: ",
                         Tcl_GetString(objv[1]), NULL);
        return TCL_ERROR;
    }

    const char *db_name  = Tcl_GetString(objv[2]);
    const char *tbl_name = Tcl_GetString(objv[3]);
    const char *col_name = (objc == 5) ? Tcl_GetString(objv[4]) : NULL;
    if (db_name[0] == '\0') db_name = NULL;

    const char *decltype = NULL;
    const char *collseq  = NULL;
    int notnull = 0, primarykey = 0, autoincrement = 0;

    int rc = sqlite3_table_column_metadata(tdb->db, db_name, tbl_name,
        col_name, &decltype, &collseq, &notnull, &primarykey, &autoincrement);
    if (rc != SQLITE_OK) {
        Tcl_SetResult(interp,
            (char *)strip_err_prefix(sqlite3_errmsg(tdb->db)), TCL_VOLATILE);
        return TCL_ERROR;
    }

    Tcl_Obj *result = Tcl_NewListObj(0, NULL);
    Tcl_ListObjAppendElement(interp, result,
        Tcl_NewStringObj(decltype ? decltype : "", -1));
    Tcl_ListObjAppendElement(interp, result,
        Tcl_NewStringObj(collseq ? collseq : "", -1));
    Tcl_ListObjAppendElement(interp, result, Tcl_NewIntObj(notnull));
    Tcl_ListObjAppendElement(interp, result, Tcl_NewIntObj(primarykey));
    Tcl_ListObjAppendElement(interp, result, Tcl_NewIntObj(autoincrement));
    Tcl_SetObjResult(interp, result);
    return TCL_OK;
}

/* ------------------------------------------------------------------ */
/* sqlite3BitvecBuiltinTest command                                     */
/* ------------------------------------------------------------------ */

#define BV_SETBIT(p, n)  ((p)[(n) >> 3] |= (unsigned char)(1 << ((n) & 7)))
#define BV_CLEARBIT(p, n) ((p)[(n) >> 3] &= (unsigned char)~(1 << ((n) & 7)))
#define BV_TESTBIT(p, n) (((p)[(n) >> 3] >> ((n) & 7)) & 1)

/* Deterministic stand-in for sqlite3_randomness in the bitvec test
 * program; the expected results do not depend on the values drawn. */
static unsigned int bitvec_rand(void)
{
    static unsigned int state = 0x9e3779b9u;
    state ^= state << 13;
    state ^= state >> 17;
    state ^= state << 5;
    return state;
}

/*
 * sqlite3BitvecBuiltinTest SIZE PROGRAM
 *
 * Faithful port of upstream sqlite3BitvecBuiltinTest (bitvec.c): runs
 * the PROGRAM opcodes against a bitmap under test and a reference
 * bitmap, then returns 0 if they agree or the index of the first
 * mismatched bit. Turso has no C Bitvec object, so unlike upstream
 * this validates the harness's program interpreter (opcode 5 writes
 * only the reference, which is how bitvec.test checks that deliberate
 * mismatches are detected), not an engine data structure. It exists
 * so bitvec.test runs instead of aborting.
 *
 * Opcodes: 1=set linear, 2=clear linear, 3=set random, 4=clear random,
 * 5=set reference only; each instruction is {op count start incr} for
 * linear ops and {op count} for random ops, 0 terminates.
 */
static int TursoBitvecBuiltinTestCmd(ClientData cd, Tcl_Interp *interp,
                                     int objc, Tcl_Obj *const objv[])
{
    (void)cd;

    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "SIZE PROGRAM");
        return TCL_ERROR;
    }

    int sz;
    if (Tcl_GetIntFromObj(interp, objv[1], &sz) != TCL_OK) return TCL_ERROR;
    if (sz < 1) {
        Tcl_AppendResult(interp, "SIZE must be at least 1", NULL);
        return TCL_ERROR;
    }

    Tcl_Size  prog_len;
    Tcl_Obj **prog_objs;
    if (Tcl_ListObjGetElements(interp, objv[2],
                               &prog_len, &prog_objs) != TCL_OK) {
        return TCL_ERROR;
    }

    /* Mutable copy: the interpreter advances the start operand of linear
     * instructions in place, as upstream does. Zero-padded so a program
     * truncated mid-instruction reads harmless zeros instead of running
     * off the end. */
    int *ops = (int *)Tcl_Alloc((prog_len + 4) * sizeof(int));
    memset(ops, 0, (prog_len + 4) * sizeof(int));
    Tcl_Size k;
    for (k = 0; k < prog_len; k++) {
        if (Tcl_GetIntFromObj(interp, prog_objs[k], &ops[k]) != TCL_OK) {
            Tcl_Free((char *)ops);
            return TCL_ERROR;
        }
    }

    size_t         nbytes = (size_t)(sz + 7) / 8 + 1;
    unsigned char *bv     = (unsigned char *)Tcl_Alloc(nbytes);
    unsigned char *ref    = (unsigned char *)Tcl_Alloc(nbytes);
    memset(bv, 0, nbytes);
    memset(ref, 0, nbytes);

    int pc = 0;
    unsigned int i = 0;
    int op;
    while (pc <= (int)prog_len && (op = ops[pc]) != 0) {
        int nx;
        switch (op) {
        case 1:
        case 2:
        case 5:
            nx = 4;
            i = (unsigned int)(ops[pc + 2] - 1);
            ops[pc + 2] += ops[pc + 3];
            break;
        default:
            nx = 2;
            i = bitvec_rand();
            break;
        }
        if (--ops[pc + 1] > 0) nx = 0;
        pc += nx;
        i = (i & 0x7fffffff) % (unsigned int)sz;
        if (op & 1) {
            BV_SETBIT(ref, i + 1);
            if (op != 5) BV_SETBIT(bv, i + 1);
        } else {
            BV_CLEARBIT(ref, i + 1);
            BV_CLEARBIT(bv, i + 1);
        }
    }

    int rc = 0;
    int bit;
    for (bit = 1; bit <= sz; bit++) {
        if (BV_TESTBIT(ref, bit) != BV_TESTBIT(bv, bit)) {
            rc = bit;
            break;
        }
    }

    Tcl_Free((char *)ops);
    Tcl_Free((char *)bv);
    Tcl_Free((char *)ref);
    Tcl_SetObjResult(interp, Tcl_NewIntObj(rc));
    return TCL_OK;
}

/* ------------------------------------------------------------------ */
/* btree_varint_test command                                            */
/* ------------------------------------------------------------------ */

/* Test-only varint codec exports from the sqlite3 Rust crate
 * (bindings/c/src/lib.rs); both operate on 9-byte buffers. */
extern int turso_test_put_varint(unsigned char *buf,
                                 unsigned long long value);
extern int turso_test_get_varint(const unsigned char *buf,
                                 unsigned long long *out);

/*
 * btree_varint_test START MULTIPLIER COUNT INCREMENT
 *
 * The upstream test3.c command: starting from START*MULTIPLIER and
 * stepping by INCREMENT, write each value with the engine's varint
 * encoder, read it back with the decoder, and verify byte count and
 * value survive the round trip. Returns nothing on success and an
 * error describing the first mismatch otherwise. Here the codec under
 * test is core's write_varint/read_varint — the one the storage layer
 * uses for every cell — not a harness reimplementation.
 */
static int TursoBtreeVarintTestCmd(ClientData cd, Tcl_Interp *interp,
                                   int objc, Tcl_Obj *const objv[])
{
    (void)cd;

    Tcl_WideInt args[4];
    int i;
    if (objc != 5) {
        Tcl_WrongNumArgs(interp, 1, objv, "START MULTIPLIER COUNT INCREMENT");
        return TCL_ERROR;
    }
    for (i = 0; i < 4; i++) {
        if (Tcl_GetWideIntFromObj(interp, objv[i + 1], &args[i]) != TCL_OK) {
            return TCL_ERROR;
        }
    }

    unsigned long long in   = (unsigned int)args[0];
    unsigned long long incr = (unsigned int)args[3];
    Tcl_WideInt        count = args[2];
    in *= (unsigned int)args[1];

    Tcl_WideInt iter;
    for (iter = 0; iter < count; iter++) {
        unsigned char      buf[16];
        unsigned long long out = 0;

        int n1 = turso_test_put_varint(buf, in);
        if (n1 < 1 || n1 > 9) {
            Tcl_SetObjResult(interp, Tcl_ObjPrintf(
                "putVarint returned %d - should be between 1 and 9", n1));
            return TCL_ERROR;
        }
        int n2 = turso_test_get_varint(buf, &out);
        if (n1 != n2) {
            Tcl_SetObjResult(interp, Tcl_ObjPrintf(
                "putVarint returned %d and getVarint returned %d", n1, n2));
            return TCL_ERROR;
        }
        if (in != out) {
            Tcl_SetObjResult(interp, Tcl_ObjPrintf(
                "Wrote 0x%016llx and got back 0x%016llx", in, out));
            return TCL_ERROR;
        }
        in += incr;
    }
    return TCL_OK;
}

/* ------------------------------------------------------------------ */
/* sqlite3_mprintf / sqlite3_snprintf test-harness commands             */
/* ------------------------------------------------------------------ */

/* The upstream test1.c printf commands: each formats through the real
 * sqlite3_mprintf/sqlite3_snprintf C API (backed by core's printf
 * engine), so the printf.test expectations exercise turso's formatter
 * rather than a reimplementation. */

static void mprintf_result(Tcl_Interp *interp, char *z)
{
    Tcl_SetObjResult(interp, Tcl_NewStringObj(z ? z : "", -1));
    if (z) sqlite3_free(z);
}

/* Parse a TCL integer that may exceed 32 bits (e.g. 0xffffffff),
 * wrapping to the C type at the call site like upstream's Tcl_GetInt. */
static int get_wide(Tcl_Interp *interp, Tcl_Obj *obj, Tcl_WideInt *out)
{
    return Tcl_GetWideIntFromObj(interp, obj, out);
}

/* sqlite3_mprintf_int FORMAT INT INT INT */
static int TursoMprintfIntCmd(ClientData cd, Tcl_Interp *interp,
                              int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    Tcl_WideInt a[3];
    int i;
    if (objc != 5) {
        Tcl_WrongNumArgs(interp, 1, objv, "FORMAT INT INT INT");
        return TCL_ERROR;
    }
    for (i = 0; i < 3; i++) {
        if (get_wide(interp, objv[i + 2], &a[i]) != TCL_OK) return TCL_ERROR;
    }
    mprintf_result(interp, sqlite3_mprintf(Tcl_GetString(objv[1]),
        (int)a[0], (int)a[1], (int)a[2]));
    return TCL_OK;
}

/* sqlite3_mprintf_int64 FORMAT INT INT INT */
static int TursoMprintfInt64Cmd(ClientData cd, Tcl_Interp *interp,
                                int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    Tcl_WideInt a[3];
    int i;
    if (objc != 5) {
        Tcl_WrongNumArgs(interp, 1, objv, "FORMAT INT INT INT");
        return TCL_ERROR;
    }
    for (i = 0; i < 3; i++) {
        if (get_wide(interp, objv[i + 2], &a[i]) != TCL_OK) return TCL_ERROR;
    }
    mprintf_result(interp, sqlite3_mprintf(Tcl_GetString(objv[1]),
        (long long)a[0], (long long)a[1], (long long)a[2]));
    return TCL_OK;
}

/* sqlite3_mprintf_long FORMAT INT INT INT
 * As in upstream, each argument is truncated to 32 bits before being
 * passed as a C long, so 0xffffffff formats as 4294967295 via %lu. */
static int TursoMprintfLongCmd(ClientData cd, Tcl_Interp *interp,
                               int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    Tcl_WideInt a[3];
    long v[3];
    int i;
    if (objc != 5) {
        Tcl_WrongNumArgs(interp, 1, objv, "FORMAT INT INT INT");
        return TCL_ERROR;
    }
    for (i = 0; i < 3; i++) {
        if (get_wide(interp, objv[i + 2], &a[i]) != TCL_OK) return TCL_ERROR;
        v[i] = (long)(a[i] & 0xffffffffLL);
    }
    mprintf_result(interp, sqlite3_mprintf(Tcl_GetString(objv[1]),
        v[0], v[1], v[2]));
    return TCL_OK;
}

/* sqlite3_mprintf_str FORMAT INT INT ?STRING? */
static int TursoMprintfStrCmd(ClientData cd, Tcl_Interp *interp,
                              int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    Tcl_WideInt a[2];
    int i;
    if (objc != 4 && objc != 5) {
        Tcl_WrongNumArgs(interp, 1, objv, "FORMAT INT INT ?STRING?");
        return TCL_ERROR;
    }
    for (i = 0; i < 2; i++) {
        if (get_wide(interp, objv[i + 2], &a[i]) != TCL_OK) return TCL_ERROR;
    }
    mprintf_result(interp, sqlite3_mprintf(Tcl_GetString(objv[1]),
        (int)a[0], (int)a[1], objc == 5 ? Tcl_GetString(objv[4]) : NULL));
    return TCL_OK;
}

/* sqlite3_mprintf_stronly FORMAT STRING */
static int TursoMprintfStronlyCmd(ClientData cd, Tcl_Interp *interp,
                                  int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "FORMAT STRING");
        return TCL_ERROR;
    }
    mprintf_result(interp,
        sqlite3_mprintf(Tcl_GetString(objv[1]), Tcl_GetString(objv[2])));
    return TCL_OK;
}

/* sqlite3_mprintf_double FORMAT INT INT DOUBLE */
static int TursoMprintfDoubleCmd(ClientData cd, Tcl_Interp *interp,
                                 int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    Tcl_WideInt a[2];
    double r;
    int i;
    if (objc != 5) {
        Tcl_WrongNumArgs(interp, 1, objv, "FORMAT INT INT DOUBLE");
        return TCL_ERROR;
    }
    for (i = 0; i < 2; i++) {
        if (get_wide(interp, objv[i + 2], &a[i]) != TCL_OK) return TCL_ERROR;
    }
    if (Tcl_GetDoubleFromObj(interp, objv[4], &r) != TCL_OK) return TCL_ERROR;
    mprintf_result(interp, sqlite3_mprintf(Tcl_GetString(objv[1]),
        (int)a[0], (int)a[1], r));
    return TCL_OK;
}

/* sqlite3_mprintf_scaled FORMAT DOUBLE DOUBLE
 * Formats the product of the two doubles; the tests use this to reach
 * magnitudes (e.g. 1e308) that TCL literals cannot spell exactly. */
static int TursoMprintfScaledCmd(ClientData cd, Tcl_Interp *interp,
                                 int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    double r[2];
    int i;
    if (objc != 4) {
        Tcl_WrongNumArgs(interp, 1, objv, "FORMAT DOUBLE DOUBLE");
        return TCL_ERROR;
    }
    for (i = 0; i < 2; i++) {
        if (Tcl_GetDoubleFromObj(interp, objv[i + 2], &r[i]) != TCL_OK) {
            return TCL_ERROR;
        }
    }
    mprintf_result(interp,
        sqlite3_mprintf(Tcl_GetString(objv[1]), r[0] * r[1]));
    return TCL_OK;
}

/* sqlite3_mprintf_hexdouble FORMAT HEX
 * HEX is the 16-hex-digit IEEE-754 bit pattern of the double to format,
 * so tests can name exact values like Inf and denormals. */
static int TursoMprintfHexdoubleCmd(ClientData cd, Tcl_Interp *interp,
                                    int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    if (objc != 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "FORMAT HEX");
        return TCL_ERROR;
    }
    const char *hex = Tcl_GetString(objv[2]);
    unsigned long long bits = 0;
    int i;
    for (i = 0; hex[i]; i++) {
        int d = hex_to_int(hex[i]);
        if (d < 0 || i >= 16) {
            Tcl_AppendResult(interp, "invalid hex double: ", hex, NULL);
            return TCL_ERROR;
        }
        bits = (bits << 4) | (unsigned)d;
    }
    double r;
    memcpy(&r, &bits, sizeof(r));
    mprintf_result(interp, sqlite3_mprintf(Tcl_GetString(objv[1]), r));
    return TCL_OK;
}

/* sqlite3_mprintf_z_test SEPARATOR ARG0 ARG1 ...
 * Joins the arguments with the separator by repeatedly growing the
 * result through the %z (format-and-free) specifier. */
static int TursoMprintfZCmd(ClientData cd, Tcl_Interp *interp,
                            int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    char *result = NULL;
    int i;
    if (objc < 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "SEPARATOR ?ARG...?");
        return TCL_ERROR;
    }
    for (i = 2; i < objc && (i == 2 || result); i++) {
        result = sqlite3_mprintf("%z%s%s", result,
            Tcl_GetString(objv[1]), Tcl_GetString(objv[i]));
    }
    mprintf_result(interp, result);
    return TCL_OK;
}

/* sqlite3_mprintf_n_test STRING
 * Returns the character count that a trailing %n reports. */
static int TursoMprintfNCmd(ClientData cd, Tcl_Interp *interp,
                            int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    int n = 0;
    if (objc != 2) {
        Tcl_WrongNumArgs(interp, 1, objv, "STRING");
        return TCL_ERROR;
    }
    char *z = sqlite3_mprintf("%s%n", Tcl_GetString(objv[1]), &n);
    if (z) sqlite3_free(z);
    Tcl_SetObjResult(interp, Tcl_NewIntObj(n));
    return TCL_OK;
}

/* sqlite3_snprintf_int SIZE FORMAT INT
 * The buffer is pre-filled with the alphabet so tests can verify that
 * SIZE 0 leaves the buffer untouched. */
static int TursoSnprintfIntCmd(ClientData cd, Tcl_Interp *interp,
                               int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    char buf[100];
    int n, x;
    if (objc != 4) {
        Tcl_WrongNumArgs(interp, 1, objv, "SIZE FORMAT INT");
        return TCL_ERROR;
    }
    if (Tcl_GetIntFromObj(interp, objv[1], &n) != TCL_OK) return TCL_ERROR;
    if (Tcl_GetIntFromObj(interp, objv[3], &x) != TCL_OK) return TCL_ERROR;
    if (n > (int)sizeof(buf)) n = (int)sizeof(buf);
    strcpy(buf, "abcdefghijklmnopqrstuvwxyz");
    sqlite3_snprintf(n, buf, Tcl_GetString(objv[2]), x);
    Tcl_SetObjResult(interp, Tcl_NewStringObj(buf, -1));
    return TCL_OK;
}

/* sqlite3_snprintf_str SIZE FORMAT INT INT STRING */
static int TursoSnprintfStrCmd(ClientData cd, Tcl_Interp *interp,
                               int objc, Tcl_Obj *const objv[])
{
    (void)cd;
    char buf[100];
    int n, a0, a1;
    if (objc != 6) {
        Tcl_WrongNumArgs(interp, 1, objv, "SIZE FORMAT INT INT STRING");
        return TCL_ERROR;
    }
    if (Tcl_GetIntFromObj(interp, objv[1], &n) != TCL_OK) return TCL_ERROR;
    if (n < 0) {
        Tcl_AppendResult(interp, "SIZE must be non-negative", NULL);
        return TCL_ERROR;
    }
    if (Tcl_GetIntFromObj(interp, objv[3], &a0) != TCL_OK) return TCL_ERROR;
    if (Tcl_GetIntFromObj(interp, objv[4], &a1) != TCL_OK) return TCL_ERROR;
    if (n > (int)sizeof(buf)) n = (int)sizeof(buf);
    buf[0] = '\0';
    sqlite3_snprintf(n, buf, Tcl_GetString(objv[2]), a0, a1,
                     Tcl_GetString(objv[5]));
    Tcl_SetObjResult(interp, Tcl_NewStringObj(buf, -1));
    return TCL_OK;
}

/* ------------------------------------------------------------------ */
/* sqlite3 open command                                                 */
/* ------------------------------------------------------------------ */

static int TursoOpenCmd(ClientData cd, Tcl_Interp *interp,
                        int objc, Tcl_Obj *const objv[])
{
    (void)cd;

    if (objc < 3) {
        Tcl_WrongNumArgs(interp, 1, objv, "name filename ?options?");
        return TCL_ERROR;
    }

    const char *handle_name = Tcl_GetString(objv[1]);
    const char *filename    = Tcl_GetString(objv[2]);

    /* Options of the upstream binding. -readonly and -uri change how the
     * file is opened; -vfs, -key, -nomutex, -fullmutex, -nofollow and
     * -create have no Turso equivalent and are accepted and ignored. */
    int flags = TURSO_OPEN_READWRITE | TURSO_OPEN_CREATE;
    int i;
    for (i = 3; i + 1 < objc; i += 2) {
        const char *opt = Tcl_GetString(objv[i]);
        int         val = 0;
        if (strcmp(opt, "-readonly") == 0) {
            if (Tcl_GetBooleanFromObj(interp, objv[i + 1], &val) != TCL_OK) {
                return TCL_ERROR;
            }
            if (val) flags = (flags & ~(TURSO_OPEN_READWRITE | TURSO_OPEN_CREATE))
                             | TURSO_OPEN_READONLY;
        } else if (strcmp(opt, "-uri") == 0) {
            if (Tcl_GetBooleanFromObj(interp, objv[i + 1], &val) != TCL_OK) {
                return TCL_ERROR;
            }
            if (val) flags |= TURSO_OPEN_URI;
        } else if (strcmp(opt, "-create") == 0) {
            if (Tcl_GetBooleanFromObj(interp, objv[i + 1], &val) != TCL_OK) {
                return TCL_ERROR;
            }
            if (!val) flags &= ~TURSO_OPEN_CREATE;
        }
    }

    sqlite3 *db  = NULL;
    int      rc  = sqlite3_open_v2(filename, &db, flags, NULL);

    if (rc != SQLITE_OK) {
        const char *errmsg = db ? sqlite3_errmsg(db) : sqlite3_errstr(rc);
        Tcl_SetResult(interp, (char *)errmsg, TCL_VOLATILE);
        if (db) sqlite3_close(db);
        return TCL_ERROR;
    }

    TursoDb *tdb = (TursoDb *)Tcl_Alloc(sizeof(TursoDb));
    memset(tdb, 0, sizeof(TursoDb));
    tdb->db          = db;
    tdb->interp      = interp;
    tdb->null_obj    = NULL;
    tdb->cache_count = 0;
    tdb->txn_depth   = 0;
    tdb->n_step      = 0;
    tdb->n_sort      = 0;
    tdb->n_index     = 0;
    tdb->n_vm_step   = 0;

    Tcl_CreateObjCommand(interp, handle_name, TursoDbCmd,
                         (ClientData)tdb, TursoDbFree);
    /* The upstream TCL binding returns an empty result from [sqlite3],
     * and tests compare against exactly that. */
    Tcl_ResetResult(interp);
    return TCL_OK;
}

/* ------------------------------------------------------------------ */
/* Extension initialisation                                             */
/* ------------------------------------------------------------------ */

/* Defined in the sqlite3 Rust crate (bindings/c/src/lib.rs) */
extern int sqlite3_search_count;

int Tursotcl_Init(Tcl_Interp *interp)
{
    if (Tcl_InitStubs(interp, TCL_VERSION, 0) == NULL) {
        return TCL_ERROR;
    }

    turso_enable_experimental();

    Tcl_CreateObjCommand(interp, "sqlite3", TursoOpenCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_exec", TursoExecCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_connection_pointer",
                         TursoConnectionPointerCmd, NULL, NULL);

    Tcl_CreateObjCommand(interp, "sqlite3_prepare",
                         TursoPrepareCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_prepare_v2",
                         TursoPrepareCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_step",
                         TursoStepCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_finalize",
                         TursoFinalizeCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_reset",
                         TursoResetCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_column_count",
                         TursoColumnCountCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_data_count",
                         TursoDataCountCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_column_type",
                         TursoColumnTypeCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_column_text",
                         TursoColumnTextCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_column_blob",
                         TursoColumnBlobCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_column_int",
                         TursoColumnIntCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_column_int64",
                         TursoColumnInt64Cmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_column_double",
                         TursoColumnDoubleCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_column_bytes",
                         TursoColumnBytesCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_column_name",
                         TursoColumnNameCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_column_decltype",
                         TursoColumnDecltypeCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_column_table_name",
                         TursoColumnTableNameCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_stmt_readonly",
                         TursoStmtReadonlyCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_stmt_busy",
                         TursoStmtBusyCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_next_stmt",
                         TursoNextStmtCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_stmt_isexplain",
                         TursoStmtIsexplainCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_bind_int",
                         TursoBindInt64Cmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_bind_int64",
                         TursoBindInt64Cmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_bind_double",
                         TursoBindDoubleCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_bind_null",
                         TursoBindNullCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_bind_text",
                         TursoBindTextCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_bind_blob",
                         TursoBindBlobCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_bind_zeroblob",
                         TursoBindZeroblobCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_bind_parameter_count",
                         TursoBindParameterCountCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_bind_parameter_name",
                         TursoBindParameterNameCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_bind_parameter_index",
                         TursoBindParameterIndexCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_clear_bindings",
                         TursoClearBindingsCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_errcode",
                         TursoErrcodeCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_extended_errcode",
                         TursoExtendedErrcodeCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_errmsg",
                         TursoErrmsgCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_changes",
                         TursoChangesCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_total_changes",
                         TursoTotalChangesCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_get_autocommit",
                         TursoGetAutocommitCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_interrupt",
                         TursoInterruptCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_extended_result_codes",
                         TursoExtendedResultCodesCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_db_readonly",
                         TursoDbReadonlyCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_wal_checkpoint",
                         TursoWalCheckpointCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_wal_checkpoint_v2",
                         TursoWalCheckpointV2Cmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_last_insert_rowid",
                         TursoLastInsertRowidCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_complete",
                         TursoCompleteCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_open",
                         TursoCApiOpenCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_close",
                         TursoCApiCloseCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_close_v2",
                         TursoCApiCloseV2Cmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_db_filename",
                         TursoDbFilenameCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_table_column_metadata",
                         TursoTableColumnMetadataCmd, NULL, NULL);

    Tcl_CreateObjCommand(interp, "btree_varint_test",
                         TursoBtreeVarintTestCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3BitvecBuiltinTest",
                         TursoBitvecBuiltinTestCmd, NULL, NULL);

    Tcl_CreateObjCommand(interp, "sqlite3_blob_open",
                         TursoBlobOpenCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_blob_bytes",
                         TursoBlobBytesCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_blob_read",
                         TursoBlobReadCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_blob_write",
                         TursoBlobWriteCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_blob_close",
                         TursoBlobCloseCmd, NULL, NULL);

    Tcl_CreateObjCommand(interp, "sqlite3_mprintf_int",
                         TursoMprintfIntCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_mprintf_int64",
                         TursoMprintfInt64Cmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_mprintf_long",
                         TursoMprintfLongCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_mprintf_str",
                         TursoMprintfStrCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_mprintf_stronly",
                         TursoMprintfStronlyCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_mprintf_double",
                         TursoMprintfDoubleCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_mprintf_scaled",
                         TursoMprintfScaledCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_mprintf_hexdouble",
                         TursoMprintfHexdoubleCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_mprintf_z_test",
                         TursoMprintfZCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_mprintf_n_test",
                         TursoMprintfNCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_snprintf_int",
                         TursoSnprintfIntCmd, NULL, NULL);
    Tcl_CreateObjCommand(interp, "sqlite3_snprintf_str",
                         TursoSnprintfStrCmd, NULL, NULL);

    /* Link the global B-tree search counter so TCL tests can read/reset it. */
    Tcl_LinkVar(interp, "sqlite_search_count",
                (char *)&sqlite3_search_count, TCL_LINK_INT);

    Tcl_PkgProvide(interp, "tursotcl", TURSO_TCL_VERSION);
    return TCL_OK;
}
