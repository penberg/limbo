package limbo

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

// only construct limboStmt with initStmt function to ensure proper initialization
// inUse tracks whether or not `query` has been called. if inUse > 0, stmt no longer
// owns the underlying data and `rows` is responsible for cleaning it up on close.
type limboStmt struct {
	mu    sync.Mutex
	ctx   uintptr
	sql   string
	inUse int
}

// Initialize/register the FFI function pointers for the statement methods
func initStmt(ctx uintptr, sql string) *limboStmt {
	return &limboStmt{
		ctx:   uintptr(ctx),
		sql:   sql,
		inUse: 0,
	}
}

func (ls *limboStmt) NumInput() int {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return int(stmtParamCount(ls.ctx))
}

func (ls *limboStmt) Close() error {
	if ls.inUse == 0 {
		ls.mu.Lock()
		res := closeStmt(ls.ctx)
		ls.mu.Unlock()
		if ResultCode(res) != Ok {
			return fmt.Errorf("error closing statement: %s", ResultCode(res).String())
		}
	}
	ls.ctx = 0
	return nil
}

func (ls *limboStmt) Exec(args []driver.Value) (driver.Result, error) {
	ls.mu.Lock()
	argArray, cleanup, err := buildArgs(args)
	defer func() {
		cleanup()
		ls.mu.Unlock()
	}()
	if err != nil {
		return nil, err
	}
	argPtr := uintptr(0)
	argCount := uint64(len(argArray))
	if argCount > 0 {
		argPtr = uintptr(unsafe.Pointer(&argArray[0]))
	}
	var changes uint64
	rc := stmtExec(ls.ctx, argPtr, argCount, uintptr(unsafe.Pointer(&changes)))
	switch ResultCode(rc) {
	case Ok, Done:
		return driver.RowsAffected(changes), nil
	case Error:
		return nil, errors.New("error executing statement")
	case Busy:
		return nil, errors.New("busy")
	case Interrupt:
		return nil, errors.New("interrupted")
	case Invalid:
		return nil, errors.New("invalid statement")
	default:
		return nil, fmt.Errorf("unexpected status: %d", rc)
	}
}

func (ls *limboStmt) Query(args []driver.Value) (driver.Rows, error) {
	ls.mu.Lock()
	queryArgs, cleanup, err := buildArgs(args)
	defer func() { cleanup(); ls.mu.Unlock() }()
	if err != nil {
		return nil, err
	}
	argPtr := uintptr(0)
	if len(args) > 0 {
		argPtr = uintptr(unsafe.Pointer(&queryArgs[0]))
	}
	rowsPtr := stmtQuery(ls.ctx, argPtr, uint64(len(queryArgs)))
	if rowsPtr == 0 {
		return nil, fmt.Errorf("query failed for: %q", ls.sql)
	}
	ls.inUse++
	return initRows(rowsPtr), nil
}

// func (ls *limboStmt) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
// 	ls.mu.Lock()
// 	stripped := namedValueToValue(args)
// 	argArray, cleanup, err := getArgsPtr(stripped)
// 	defer func() { cleanup(); ls.mu.Unlock() }()
// 	if err != nil {
// 		return nil, err
// 	}
// 	select {
// 	case <-ctx.Done():
// 		return nil, ctx.Err()
// 	default:
// 	}
// 	var changes uint64
// 	res := stmtExec(ls.ctx, argArray, uint64(len(args)), uintptr(unsafe.Pointer(&changes)))
// 	switch ResultCode(res) {
// 	case Ok, Done:
// 		changes := uint64(changes)
// 		return driver.RowsAffected(changes), nil
// 	case Error:
// 		return nil, errors.New("error executing statement")
// 	case Busy:
// 		return nil, errors.New("busy")
// 	case Interrupt:
// 		return nil, errors.New("interrupted")
// 	default:
// 		return nil, fmt.Errorf("unexpected status: %d", res)
// 	}
// }
//
// func (ls *limboStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
// 	ls.mu.Lock()
// 	queryArgs, allocs, err := buildNamedArgs(args)
// 	defer func() { allocs(); ls.mu.Unlock() }()
// 	if err != nil {
// 		return nil, err
// 	}
// 	argsPtr := uintptr(0)
// 	if len(queryArgs) > 0 {
// 		argsPtr = uintptr(unsafe.Pointer(&queryArgs[0]))
// 	}
// 	select {
// 	case <-ctx.Done():
// 		return nil, ctx.Err()
// 	default:
// 	}
// 	rowsPtr := stmtQuery(ls.ctx, argsPtr, uint64(len(queryArgs)))
// 	if rowsPtr == 0 {
// 		return nil, fmt.Errorf("query failed for: %q", ls.sql)
// 	}
// 	ls.inUse++
// 	return initRows(rowsPtr), nil
// }
