package limbo

import (
	"database/sql/driver"
	"fmt"
	"io"
	"sync"
)

type limboRows struct {
	mu      sync.Mutex
	ctx     uintptr
	columns []string
	closed  bool
}

func newRows(ctx uintptr) *limboRows {
	return &limboRows{
		mu:      sync.Mutex{},
		ctx:     ctx,
		closed:  false,
		columns: nil,
	}
}

func (r *limboRows) Columns() []string {
	if r.ctx == 0 || r.closed {
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
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		status := rowsNext(r.ctx)
		switch ResultCode(status) {
		case Row:
			for i := range dest {
				valPtr := rowsGetValue(r.ctx, int32(i))
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
