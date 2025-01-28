package limbo

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"unsafe"
)

// only construct limboStmt with initStmt function to ensure proper initialization
// inUse tracks whether or not `query` has been called. if inUse > 0, stmt no longer
// owns the underlying data and `rows` is responsible for cleaning it up on close.
type limboStmt struct {
	ctx           uintptr
	sql           string
	inUse         int
	query         func(stmtPtr uintptr, argsPtr uintptr, argCount uint64) uintptr
	execute       func(stmtPtr uintptr, argsPtr uintptr, argCount uint64, changes uintptr) int32
	getParamCount func(uintptr) int32
	closeStmt     func(uintptr) int32
}

// Initialize/register the FFI function pointers for the statement methods
func initStmt(ctx uintptr, sql string) *limboStmt {
	var query func(stmtPtr uintptr, argsPtr uintptr, argCount uint64) uintptr
	getFfiFunc(&query, FfiStmtQuery)
	var execute func(stmtPtr uintptr, argsPtr uintptr, argCount uint64, changes uintptr) int32
	getFfiFunc(&execute, FfiStmtExec)
	var getParamCount func(uintptr) int32
	getFfiFunc(&getParamCount, FfiStmtParameterCount)
	var closeStmt func(uintptr) int32
	getFfiFunc(&closeStmt, FfiStmtClose)
	return &limboStmt{
		ctx:           uintptr(ctx),
		sql:           sql,
		inUse:         0,
		execute:       execute,
		query:         query,
		getParamCount: getParamCount,
		closeStmt:     closeStmt,
	}
}

func (ls *limboStmt) NumInput() int {
	return int(ls.getParamCount(ls.ctx))
}

func (ls *limboStmt) Close() error {
	if ls.inUse == 0 {
		res := ls.closeStmt(ls.ctx)
		if ResultCode(res) != Ok {
			return fmt.Errorf("error closing statement: %s", ResultCode(res).String())
		}
	}
	ls.ctx = 0
	return nil
}

func (ls *limboStmt) Exec(args []driver.Value) (driver.Result, error) {
	argArray, err := buildArgs(args)
	if err != nil {
		return nil, err
	}
	argPtr := uintptr(0)
	argCount := uint64(len(argArray))
	if argCount > 0 {
		argPtr = uintptr(unsafe.Pointer(&argArray[0]))
	}
	var changes uint64
	rc := ls.execute(ls.ctx, argPtr, argCount, uintptr(unsafe.Pointer(&changes)))
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

func (st *limboStmt) Query(args []driver.Value) (driver.Rows, error) {
	queryArgs, err := buildArgs(args)
	if err != nil {
		return nil, err
	}
	argPtr := uintptr(0)
	if len(args) > 0 {
		argPtr = uintptr(unsafe.Pointer(&queryArgs[0]))
	}
	rowsPtr := st.query(st.ctx, argPtr, uint64(len(queryArgs)))
	if rowsPtr == 0 {
		return nil, fmt.Errorf("query failed for: %q", st.sql)
	}
	st.inUse++
	return initRows(rowsPtr), nil
}

func (ls *limboStmt) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	stripped := namedValueToValue(args)
	argArray, err := getArgsPtr(stripped)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	var changes uint64
	res := ls.execute(ls.ctx, argArray, uint64(len(args)), uintptr(unsafe.Pointer(&changes)))
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
	queryArgs, err := buildNamedArgs(args)
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
	rowsPtr := ls.query(ls.ctx, argsPtr, uint64(len(queryArgs)))
	if rowsPtr == 0 {
		return nil, fmt.Errorf("query failed for: %q", ls.sql)
	}
	ls.inUse++
	return initRows(rowsPtr), nil
}

// only construct limboRows with initRows function to ensure proper initialization
type limboRows struct {
	ctx       uintptr
	columns   []string
	closed    bool
	getCols   func(uintptr, *uint) uintptr
	next      func(uintptr) uintptr
	getValue  func(uintptr, int32) uintptr
	closeRows func(uintptr) uintptr
	freeCols  func(uintptr) uintptr
}

// Initialize/register the FFI function pointers for the rows methods
// DO NOT construct 'limboRows' without this function
func initRows(ctx uintptr) *limboRows {
	var getCols func(uintptr, *uint) uintptr
	getFfiFunc(&getCols, FfiRowsGetColumns)
	var getValue func(uintptr, int32) uintptr
	getFfiFunc(&getValue, FfiRowsGetValue)
	var closeRows func(uintptr) uintptr
	getFfiFunc(&closeRows, FfiRowsClose)
	var freeCols func(uintptr) uintptr
	getFfiFunc(&freeCols, FfiFreeColumns)
	var next func(uintptr) uintptr
	getFfiFunc(&next, FfiRowsNext)

	return &limboRows{
		ctx:       ctx,
		getCols:   getCols,
		getValue:  getValue,
		closeRows: closeRows,
		freeCols:  freeCols,
		next:      next,
	}
}

func (r *limboRows) Columns() []string {
	if r.columns == nil {
		var columnCount uint
		colArrayPtr := r.getCols(r.ctx, &columnCount)
		if colArrayPtr != 0 && columnCount > 0 {
			r.columns = cArrayToGoStrings(colArrayPtr, columnCount)
			defer r.freeCols(colArrayPtr)
		}
	}
	return r.columns
}

func (r *limboRows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.closeRows(r.ctx)
	r.ctx = 0
	return nil
}

func (r *limboRows) Next(dest []driver.Value) error {
	for {
		status := r.next(r.ctx)
		switch ResultCode(status) {
		case Row:
			for i := range dest {
				valPtr := r.getValue(r.ctx, int32(i))
				val := toGoValue(valPtr)
				dest[i] = val
			}
			return nil
		case Io:
			continue
		case Done:
			return io.EOF
		default:
			return fmt.Errorf("unexpected status: %d", status)
		}
	}
}
