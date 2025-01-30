package limbo

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"

	"github.com/ebitengine/purego"
)

const (
	driverName = "sqlite3"
	libName    = "lib_limbo_go"
)

type limboDriver struct {
	sync.Mutex
}

var library = sync.OnceValue(func() uintptr {
	lib, err := loadLibrary()
	if err != nil {
		panic(err)
	}
	return lib
})

var (
	libOnce        sync.Once
	loadErr        error
	dbOpen         func(string) uintptr
	dbClose        func(uintptr) uintptr
	connPrepare    func(uintptr, string) uintptr
	freeBlobFunc   func(uintptr)
	freeStringFunc func(uintptr)
	rowsGetColumns func(uintptr, *uint) uintptr
	rowsGetValue   func(uintptr, int32) uintptr
	closeRows      func(uintptr) uintptr
	freeCols       func(uintptr) uintptr
	rowsNext       func(uintptr) uintptr
	stmtQuery      func(stmtPtr uintptr, argsPtr uintptr, argCount uint64) uintptr
	stmtExec       func(stmtPtr uintptr, argsPtr uintptr, argCount uint64, changes uintptr) int32
	stmtParamCount func(uintptr) int32
	closeStmt      func(uintptr) int32
)

func ensureLibLoaded() error {
	libOnce.Do(func() {
		purego.RegisterLibFunc(&dbOpen, library(), FfiDbOpen)
		purego.RegisterLibFunc(&dbClose, library(), FfiDbClose)
		purego.RegisterLibFunc(&connPrepare, library(), FfiDbPrepare)
		purego.RegisterLibFunc(&freeBlobFunc, library(), FfiFreeBlob)
		purego.RegisterLibFunc(&freeStringFunc, library(), FfiFreeCString)
		purego.RegisterLibFunc(&rowsGetColumns, library(), FfiRowsGetColumns)
		purego.RegisterLibFunc(&rowsGetValue, library(), FfiRowsGetValue)
		purego.RegisterLibFunc(&closeRows, library(), FfiRowsClose)
		purego.RegisterLibFunc(&freeCols, library(), FfiFreeColumns)
		purego.RegisterLibFunc(&rowsNext, library(), FfiRowsNext)
		purego.RegisterLibFunc(&stmtQuery, library(), FfiStmtQuery)
		purego.RegisterLibFunc(&stmtExec, library(), FfiStmtExec)
		purego.RegisterLibFunc(&stmtParamCount, library(), FfiStmtParameterCount)
		purego.RegisterLibFunc(&closeStmt, library(), FfiStmtClose)
	})
	return loadErr
}

func (d *limboDriver) Open(name string) (driver.Conn, error) {
	d.Lock()
	defer d.Unlock()
	conn, err := openConn(name)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

type limboConn struct {
	sync.Mutex
	ctx uintptr
}

func newConn(ctx uintptr) *limboConn {
	return &limboConn{
		sync.Mutex{},
		ctx,
	}
}

func openConn(dsn string) (*limboConn, error) {
	ctx := dbOpen(dsn)
	if ctx == 0 {
		return nil, fmt.Errorf("failed to open database for dsn=%q", dsn)
	}
	return newConn(ctx), loadErr
}

func (c *limboConn) Close() error {
	if c.ctx == 0 {
		return nil
	}
	dbClose(c.ctx)
	c.ctx = 0
	return nil
}

func (c *limboConn) Prepare(query string) (driver.Stmt, error) {
	if c.ctx == 0 {
		return nil, errors.New("connection closed")
	}
	c.Lock()
	defer c.Unlock()
	stmtPtr := connPrepare(c.ctx, query)
	if stmtPtr == 0 {
		return nil, fmt.Errorf("failed to prepare query=%q", query)
	}
	return initStmt(stmtPtr, query), nil
}

// begin is needed to implement driver.Conn.. for now not implemented
func (c *limboConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions not implemented")
}
