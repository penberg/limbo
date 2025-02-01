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
}

func newStmt(ctx uintptr, sql string) *limboStmt {
	return &limboStmt{
		ctx: uintptr(ctx),
		sql: sql,
	}
}

func (ls *limboStmt) NumInput() int {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return int(stmtParamCount(ls.ctx))
}

func (ls *limboStmt) Close() error {
	ls.mu.Lock()
	res := closeStmt(ls.ctx)
	ls.mu.Unlock()
	if ResultCode(res) != Ok {
		return fmt.Errorf("error closing statement: %s", ResultCode(res).String())
	}
	ls.ctx = 0
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
	rc := stmtExec(ls.ctx, argPtr, argCount, uintptr(unsafe.Pointer(&changes)))
	ls.mu.Unlock()
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
	rowsPtr := stmtQuery(ls.ctx, argPtr, uint64(len(queryArgs)))
	ls.mu.Unlock()
	if rowsPtr == 0 {
		return nil, fmt.Errorf("query failed for: %q", ls.sql)
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
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	var changes uint64
	ls.mu.Lock()
	res := stmtExec(ls.ctx, argArray, uint64(len(args)), uintptr(unsafe.Pointer(&changes)))
	ls.mu.Unlock()
	switch ResultCode(res) {
	case Ok, Done:
		changes := uint64(changes)
		return driver.RowsAffected(changes), nil
	case Error:
		return nil, errors.New("error executing statement")
	case Busy:
		return nil, errors.New("busy")
	case Interrupt:
		return nil, errors.New("interrupted")
	default:
		return nil, fmt.Errorf("unexpected status: %d", res)
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
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	ls.mu.Lock()
	rowsPtr := stmtQuery(ls.ctx, argsPtr, uint64(len(queryArgs)))
	ls.mu.Unlock()
	if rowsPtr == 0 {
		return nil, fmt.Errorf("query failed for: %q", ls.sql)
	}
	return newRows(rowsPtr), nil
}
