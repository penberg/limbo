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
type limboStmt struct {
	ctx           uintptr
	sql           string
	query         stmtQueryFn
	execute       stmtExecuteFn
	getParamCount func(uintptr) int32
}

// Initialize/register the FFI function pointers for the statement methods
func initStmt(ctx uintptr, sql string) *limboStmt {
	var query stmtQueryFn
	var execute stmtExecuteFn
	var getParamCount func(uintptr) int32
	methods := []ExtFunc{{query, FfiStmtQuery}, {execute, FfiStmtExec}, {getParamCount, FfiStmtParameterCount}}
	for i := range methods {
		methods[i].initFunc()
	}
	return &limboStmt{
		ctx: uintptr(ctx),
		sql: sql,
	}
}

func (st *limboStmt) NumInput() int {
	return int(st.getParamCount(st.ctx))
}

func (st *limboStmt) Exec(args []driver.Value) (driver.Result, error) {
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
	rc := st.execute(st.ctx, argPtr, argCount, uintptr(unsafe.Pointer(&changes)))
	switch ResultCode(rc) {
	case Ok:
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
	rowsPtr := st.query(st.ctx, uintptr(unsafe.Pointer(&queryArgs[0])), uint64(len(queryArgs)))
	if rowsPtr == 0 {
		return nil, fmt.Errorf("query failed for: %q", st.sql)
	}
	return initRows(rowsPtr), nil
}

func (ts *limboStmt) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	stripped := namedValueToValue(args)
	argArray, err := getArgsPtr(stripped)
	if err != nil {
		return nil, err
	}
	var changes uintptr
	res := ts.execute(ts.ctx, argArray, uint64(len(args)), changes)
	switch ResultCode(res) {
	case Ok:
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

func (st *limboStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	queryArgs, err := buildNamedArgs(args)
	if err != nil {
		return nil, err
	}
	rowsPtr := st.query(st.ctx, uintptr(unsafe.Pointer(&queryArgs[0])), uint64(len(queryArgs)))
	if rowsPtr == 0 {
		return nil, fmt.Errorf("query failed for: %q", st.sql)
	}
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
	var getValue func(uintptr, int32) uintptr
	var closeRows func(uintptr) uintptr
	var freeCols func(uintptr) uintptr
	var next func(uintptr) uintptr
	methods := []ExtFunc{
		{getCols, FfiRowsGetColumns},
		{getValue, FfiRowsGetValue},
		{closeRows, FfiRowsClose},
		{freeCols, FfiFreeColumns},
		{next, FfiRowsNext}}
	for i := range methods {
		methods[i].initFunc()
	}

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
			if r.freeCols == nil {
				getFfiFunc(&r.freeCols, FfiFreeColumns)
			}
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
	status := r.next(r.ctx)
	switch ResultCode(status) {
	case Row:
		for i := range dest {
			valPtr := r.getValue(r.ctx, int32(i))
			val := toGoValue(valPtr)
			dest[i] = val
		}
		return nil
	case Done:
		return io.EOF
	default:
		return fmt.Errorf("unexpected status: %d", status)
	}
}
