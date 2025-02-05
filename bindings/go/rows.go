package limbo

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync"
)

type limboRows struct {
	mu      sync.Mutex
	ctx     uintptr
	columns []string
	err     error
	closed  bool
}

func newRows(ctx uintptr) *limboRows {
	return &limboRows{
		mu:      sync.Mutex{},
		ctx:     ctx,
		columns: nil,
		err:     nil,
		closed:  false,
	}
}

func (r *limboRows) isClosed() bool {
	if r.ctx == 0 || r.closed {
		return true
	}
	return false
}

func (r *limboRows) Columns() []string {
	if r.isClosed() {
		return nil
	}
	if r.columns == nil {
		r.mu.Lock()
		count := rowsGetColumns(r.ctx)
		if count > 0 {
			columns := make([]string, 0, count)
			for i := 0; i < int(count); i++ {
				cstr := rowsGetColumnName(r.ctx, int32(i))
				columns = append(columns, fmt.Sprintf("%s", GoString(cstr)))
				freeCString(cstr)
			}
			r.mu.Unlock()
			r.columns = columns
		}
	}
	return r.columns
}

func (r *limboRows) Close() error {
	r.err = errors.New(RowsClosedErr)
	if r.isClosed() {
		return r.err
	}
	r.mu.Lock()
	r.closed = true
	closeRows(r.ctx)
	r.ctx = 0
	r.mu.Unlock()
	return nil
}

func (r *limboRows) Err() error {
	if r.err == nil {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.getError()
	}
	return r.err
}

func (r *limboRows) Next(dest []driver.Value) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.isClosed() {
		return r.err
	}
	for {
		status := rowsNext(r.ctx)
		switch ResultCode(status) {
		case Row:
			for i := range dest {
				valPtr := rowsGetValue(r.ctx, int32(i))
				val := toGoValue(valPtr)
				if val == nil {
					r.getError()
				}
				dest[i] = val
			}
			return nil
		case Io:
			continue
		case Done:
			return io.EOF
		default:
			return r.getError()
		}
	}
}

// mutex will already be locked. this is always called after FFI
func (r *limboRows) getError() error {
	if r.isClosed() {
		return r.err
	}
	err := rowsGetError(r.ctx)
	if err == 0 {
		return nil
	}
	defer freeCString(err)
	cpy := fmt.Sprintf("%s", GoString(err))
	r.err = errors.New(cpy)
	return r.err
}
