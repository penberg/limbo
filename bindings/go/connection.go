package limbo

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"unsafe"

	"github.com/ebitengine/purego"
)

const (
	driverName = "sqlite3"
	libName    = "lib_limbo_go"
)

var limboLib uintptr

type limboDriver struct{}

func (d limboDriver) Open(name string) (driver.Conn, error) {
	return openConn(name)
}

func toCString(s string) uintptr {
	b := append([]byte(s), 0)
	return uintptr(unsafe.Pointer(&b[0]))
}

// helper to register an FFI function in the lib_limbo_go library
func getFfiFunc(ptr interface{}, name string) {
	purego.RegisterLibFunc(ptr, limboLib, name)
}

// TODO: sync primitives
type limboConn struct {
	ctx     uintptr
	prepare func(uintptr, string) uintptr
}

func newConn(ctx uintptr) *limboConn {
	var prepare func(uintptr, string) uintptr
	getFfiFunc(&prepare, FfiDbPrepare)
	return &limboConn{
		ctx,
		prepare,
	}
}

func openConn(dsn string) (*limboConn, error) {
	var dbOpen func(string) uintptr
	getFfiFunc(&dbOpen, FfiDbOpen)

	ctx := dbOpen(dsn)
	if ctx == 0 {
		return nil, fmt.Errorf("failed to open database for dsn=%q", dsn)
	}
	return newConn(ctx), nil
}

func (c *limboConn) Close() error {
	if c.ctx == 0 {
		return nil
	}
	var dbClose func(uintptr) uintptr
	getFfiFunc(&dbClose, FfiDbClose)

	dbClose(c.ctx)
	c.ctx = 0
	return nil
}

func (c *limboConn) Prepare(query string) (driver.Stmt, error) {
	if c.ctx == 0 {
		return nil, errors.New("connection closed")
	}
	if c.prepare == nil {
		panic("prepare function not set")
	}
	stmtPtr := c.prepare(c.ctx, query)
	if stmtPtr == 0 {
		return nil, fmt.Errorf("failed to prepare query=%q", query)
	}
	return initStmt(stmtPtr, query), nil
}

// begin is needed to implement driver.Conn.. for now not implemented
func (c *limboConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions not implemented")
}
