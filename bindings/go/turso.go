package turso

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
	"golang.org/x/sys/windows"
)

const turso = "../../target/debug/lib_turso_go"
const driverName = "turso"

var tursoLib uintptr

func getSystemLibrary() error {
	switch runtime.GOOS {
	case "darwin":
		slib, err := purego.Dlopen(fmt.Sprintf("%s.dylib", turso), purego.RTLD_LAZY)
		if err != nil {
			return err
		}
		tursoLib = slib
	case "linux":
		slib, err := purego.Dlopen(fmt.Sprintf("%s.so", turso), purego.RTLD_LAZY)
		if err != nil {
			return err
		}
		tursoLib = slib
	case "windows":
		slib, err := windows.LoadLibrary(fmt.Sprintf("%s.dll", turso))
		if err != nil {
			return err
		}
		tursoLib = slib
	default:
		panic(fmt.Errorf("GOOS=%s is not supported", runtime.GOOS))
	}
	return nil
}

func init() {
	err := getSystemLibrary()
	if err != nil {
		slog.Error("Error opening turso library: ", err)
		os.Exit(1)
	}
	sql.Register(driverName, &tursoDriver{})
}

type tursoDriver struct{}

func (d tursoDriver) Open(name string) (driver.Conn, error) {
	return openConn(name)
}

func toCString(s string) uintptr {
	b := append([]byte(s), 0)
	return uintptr(unsafe.Pointer(&b[0]))
}

// helper to register an FFI function in the lib_turso_go library
func getFfiFunc(ptr interface{}, name string) {
	purego.RegisterLibFunc(&ptr, tursoLib, name)
}

type tursoConn struct {
	ctx uintptr
	sync.Mutex
	prepare func(uintptr, uintptr) uintptr
}

func newConn(ctx uintptr) *tursoConn {
	var prepare func(uintptr, uintptr) uintptr
	getFfiFunc(&prepare, FfiDbPrepare)
	return &tursoConn{
		ctx,
		sync.Mutex{},
		prepare,
	}
}

func openConn(dsn string) (*tursoConn, error) {
	var dbOpen func(uintptr) uintptr
	getFfiFunc(&dbOpen, FfiDbOpen)

	cStr := toCString(dsn)
	defer freeCString(cStr)

	ctx := dbOpen(cStr)
	if ctx == 0 {
		return nil, fmt.Errorf("failed to open database for dsn=%q", dsn)
	}
	return &tursoConn{ctx: ctx}, nil
}

func (c *tursoConn) Close() error {
	if c.ctx == 0 {
		return nil
	}
	var dbClose func(uintptr) uintptr
	getFfiFunc(&dbClose, FfiDbClose)

	dbClose(c.ctx)
	c.ctx = 0
	return nil
}

func (c *tursoConn) Prepare(query string) (driver.Stmt, error) {
	if c.ctx == 0 {
		return nil, errors.New("connection closed")
	}
	if c.prepare == nil {
		var dbPrepare func(uintptr, uintptr) uintptr
		getFfiFunc(&dbPrepare, FfiDbPrepare)
		c.prepare = dbPrepare
	}
	qPtr := toCString(query)
	stmtPtr := c.prepare(c.ctx, qPtr)
	freeCString(qPtr)

	if stmtPtr == 0 {
		return nil, fmt.Errorf("prepare failed: %q", query)
	}
	return &tursoStmt{
		ctx: stmtPtr,
		sql: query,
	}, nil
}

// begin is needed to implement driver.Conn.. for now not implemented
func (c *tursoConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions not implemented")
}
