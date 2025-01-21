package turso

import (
	"database/sql/driver"
	"fmt"
	"io"
)

type stmt struct {
	ctx uintptr
	sql string
}

type rows struct {
	ctx     uintptr
	rowsPtr uintptr
	columns []string
	err     error
}

func (ls *stmt) Query(args []driver.Value) (driver.Rows, error) {
	var dbPrepare func(uintptr, uintptr) uintptr
	getExtFunc(&dbPrepare, "db_prepare")

	queryPtr := toCString(ls.sql)
	defer freeCString(queryPtr)

	rowsPtr := dbPrepare(ls.ctx, queryPtr)
	if rowsPtr == 0 {
		return nil, fmt.Errorf("failed to prepare query")
	}
	var colFunc func(uintptr, uintptr) uintptr

	getExtFunc(&colFunc, "columns")

	rows := &rows{
		ctx:     ls.ctx,
		rowsPtr: rowsPtr,
	}
	return rows, nil
}

func (lr *rows) Columns() []string {
	return lr.columns
}

func (lr *rows) Close() error {
	var rowsClose func(uintptr)
	getExtFunc(&rowsClose, "rows_close")
	rowsClose(lr.rowsPtr)
	return nil
}

func (lr *rows) Next(dest []driver.Value) error {
	var rowsNext func(uintptr, uintptr) int32
	getExtFunc(&rowsNext, "rows_next")

	status := rowsNext(lr.ctx, lr.rowsPtr)
	switch ResultCode(status) {
	case Row:
		for i := range dest {
			getExtFunc(&rowsGetValue, "rows_get_value")

			valPtr := rowsGetValue(lr.ctx, int32(i))
			if valPtr != 0 {
				val := cStringToGoString(valPtr)
				dest[i] = val
				freeCString(valPtr)
			} else {
				dest[i] = nil
			}
		}
		return nil
	case 0: // No more rows
		return io.EOF
	default:
		return fmt.Errorf("unexpected status: %d", status)
	}
}
