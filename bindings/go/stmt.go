package limbo

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

type limboStmt struct {
	mu  sync.Mutex
	ctx uintptr
	sql string
	err error
}

func newStmt(ctx uintptr, sql string) *limboStmt {
	return &limboStmt{
		ctx: uintptr(ctx),
		sql: sql,
		err: nil,
	}
}

func (ls *limboStmt) NumInput() int {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	res := int(stmtParamCount(ls.ctx))
	if res < 0 {
		// set the error from rust
		_ = ls.getError()
	}
	return res
}

func (ls *limboStmt) Close() error {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	if ls.ctx == 0 {
		return nil
	}
	res := stmtClose(ls.ctx)
	ls.ctx = 0
	if ResultCode(res) != Ok {
		return fmt.Errorf("error closing statement: %s", ResultCode(res).String())
	}
	return nil
}

func (ls *limboStmt) Exec(args []driver.Value) (driver.Result, error) {
	argArray, cleanup, err := buildArgs(args)
	defer cleanup()
	if err != nil {
		return nil, err
	}
	argPtr := uintptr(0)
	argCount := uint64(len(argArray))
	if argCount > 0 {
		argPtr = uintptr(unsafe.Pointer(&argArray[0]))
	}
	var changes uint64
	ls.mu.Lock()
	defer ls.mu.Unlock()
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
		return nil, ls.getError()
	}
}

func (ls *limboStmt) Query(args []driver.Value) (driver.Rows, error) {
	queryArgs, cleanup, err := buildArgs(args)
	defer cleanup()
	if err != nil {
		return nil, err
	}
	argPtr := uintptr(0)
	if len(args) > 0 {
		argPtr = uintptr(unsafe.Pointer(&queryArgs[0]))
	}
	ls.mu.Lock()
	defer ls.mu.Unlock()
	rowsPtr := stmtQuery(ls.ctx, argPtr, uint64(len(queryArgs)))
	if rowsPtr == 0 {
		return nil, ls.getError()
	}
	return newRows(rowsPtr), nil
}

func (ls *limboStmt) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	stripped := namedValueToValue(args)
	argArray, cleanup, err := getArgsPtr(stripped)
	defer cleanup()
	if err != nil {
		return nil, err
	}
	ls.mu.Lock()
	select {
	case <-ctx.Done():
		ls.mu.Unlock()
		return nil, ctx.Err()
	default:
		var changes uint64
		defer ls.mu.Unlock()
		res := stmtExec(ls.ctx, argArray, uint64(len(args)), uintptr(unsafe.Pointer(&changes)))
		switch ResultCode(res) {
		case Ok, Done:
			changes := uint64(changes)
			return driver.RowsAffected(changes), nil
		case Busy:
			return nil, errors.New("Database is Busy")
		case Interrupt:
			return nil, errors.New("Interrupted")
		default:
			return nil, ls.getError()
		}
	}
}

func (ls *limboStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	queryArgs, allocs, err := buildNamedArgs(args)
	defer allocs()
	if err != nil {
		return nil, err
	}
	argsPtr := uintptr(0)
	if len(queryArgs) > 0 {
		argsPtr = uintptr(unsafe.Pointer(&queryArgs[0]))
	}
	ls.mu.Lock()
	select {
	case <-ctx.Done():
		ls.mu.Unlock()
		return nil, ctx.Err()
	default:
		defer ls.mu.Unlock()
		rowsPtr := stmtQuery(ls.ctx, argsPtr, uint64(len(queryArgs)))
		if rowsPtr == 0 {
			return nil, ls.getError()
		}
		return newRows(rowsPtr), nil
	}
}

func (ls *limboStmt) Err() error {
	if ls.err == nil {
		ls.mu.Lock()
		defer ls.mu.Unlock()
		ls.getError()
	}
	return ls.err
}

// mutex should always be locked when calling - always called after FFI
func (ls *limboStmt) getError() error {
	err := stmtGetError(ls.ctx)
	if err == 0 {
		return nil
	}
	defer freeCString(err)
	cpy := fmt.Sprintf("%s", GoString(err))
	ls.err = errors.New(cpy)
	return ls.err
}
