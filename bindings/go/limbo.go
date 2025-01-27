package limbo

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

const limbo = "../../target/debug/lib_limbo_go"
const driverName = "limbo"

var limboLib uintptr

func getSystemLibrary() error {
	switch runtime.GOOS {
	case "darwin":
		slib, err := purego.Dlopen(fmt.Sprintf("%s.dylib", limbo), purego.RTLD_LAZY)
		if err != nil {
			return err
		}
		limboLib = slib
	case "linux":
		slib, err := purego.Dlopen(fmt.Sprintf("%s.so", limbo), purego.RTLD_LAZY)
		if err != nil {
			return err
		}
		limboLib = slib
	case "windows":
		slib, err := windows.LoadLibrary(fmt.Sprintf("%s.dll", limbo))
		if err != nil {
			return err
		}
		limboLib = slib
	default:
		panic(fmt.Errorf("GOOS=%s is not supported", runtime.GOOS))
	}
	return nil
}

func init() {
	err := getSystemLibrary()
	if err != nil {
		slog.Error("Error opening limbo library: ", err)
		os.Exit(1)
	}
	sql.Register(driverName, &limboDriver{})
}

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
	purego.RegisterLibFunc(&ptr, limboLib, name)
}

type limboConn struct {
	ctx uintptr
	sync.Mutex
	prepare func(uintptr, uintptr) uintptr
}

func newConn(ctx uintptr) *limboConn {
	var prepare func(uintptr, uintptr) uintptr
	getFfiFunc(&prepare, FfiDbPrepare)
	return &limboConn{
		ctx,
		sync.Mutex{},
		prepare,
	}
}

func openConn(dsn string) (*limboConn, error) {
	var dbOpen func(uintptr) uintptr
	getFfiFunc(&dbOpen, FfiDbOpen)

	cStr := toCString(dsn)
	defer freeCString(cStr)

	ctx := dbOpen(cStr)
	if ctx == 0 {
		return nil, fmt.Errorf("failed to open database for dsn=%q", dsn)
	}
	return &limboConn{ctx: ctx}, nil
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
	return &limboStmt{
		ctx: stmtPtr,
		sql: query,
	}, nil
}

// begin is needed to implement driver.Conn.. for now not implemented
func (c *limboConn) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions not implemented")
}
