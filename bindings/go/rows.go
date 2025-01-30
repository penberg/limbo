package limbo

import (
	"database/sql/driver"
	"fmt"
	"io"
	"sync"
)

// only construct limboRows with initRows function to ensure proper initialization
type limboRows struct {
	mu      sync.Mutex
	ctx     uintptr
	columns []string
	closed  bool
}

// Initialize/register the FFI function pointers for the rows methods
// DO NOT construct 'limboRows' without this function
func initRows(ctx uintptr) *limboRows {
	return &limboRows{
		mu:  sync.Mutex{},
		ctx: ctx,
	}
}

func (r *limboRows) Columns() []string {
	if r.ctx == 0 || r.closed {
		return nil
	}
	if r.columns == nil {
		var columnCount uint
		r.mu.Lock()
		defer r.mu.Unlock()
		colArrayPtr := rowsGetColumns(r.ctx, &columnCount)
		if colArrayPtr != 0 && columnCount > 0 {
			r.columns = cArrayToGoStrings(colArrayPtr, columnCount)
			if freeCols != nil {
				defer freeCols(colArrayPtr)
			}
		}
	}
	return r.columns
}

func (r *limboRows) Close() error {
	if r.closed {
		return nil
	}
	r.mu.Lock()
	r.closed = true
	closeRows(r.ctx)
	r.ctx = 0
	r.mu.Unlock()
	return nil
}

func (r *limboRows) Next(dest []driver.Value) error {
	if r.ctx == 0 || r.closed {
		return io.EOF
	}
	for {
		status := rowsNext(r.ctx)
		switch ResultCode(status) {
		case Row:
			for i := range dest {
				r.mu.Lock()
				valPtr := rowsGetValue(r.ctx, int32(i))
				r.mu.Unlock()
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
