package turso

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"log/slog"
	"os"
	"sync"
	"unsafe"

	"github.com/ebitengine/purego"
)

const (
	turso = "../../target/debug/lib_turso_go.so"
)

func toGoStr(ptr uintptr, length int) string {
	if ptr == 0 {
		return ""
	}
	uptr := unsafe.Pointer(ptr)
	s := (*string)(uptr)
	if s == nil {
		// redundant
		return ""
	}
	return *s
}

func init() {
	slib, err := purego.Dlopen(turso, purego.RTLD_LAZY)
	if err != nil {
		slog.Error("Error opening turso library: ", err)
		os.Exit(1)
	}
	lib = slib
	sql.Register("turso", &tursoDriver{})
}

type tursoDriver struct {
	tursoCtx
}

func toCString(s string) uintptr {
	b := append([]byte(s), 0)
	return uintptr(unsafe.Pointer(&b[0]))
}

func getExtFunc(ptr interface{}, name string) {
	purego.RegisterLibFunc(ptr, lib, name)
}

type conn struct {
	ctx uintptr
	sync.Mutex
	writeTimeFmt string
	lastInsertID int64
	lastAffected int64
}

func newConn() *conn {
	return &conn{
		0,
		sync.Mutex{},
		"2006-01-02 15:04:05",
		0,
		0,
	}
}

func open(dsn string) (*conn, error) {
	var open func(uintptr) uintptr
	getExtFunc(&open, ExtDBOpen)
	c := newConn()
	path := toCString(dsn)
	ctx := open(path)
	c.ctx = ctx
	return c, nil
}

type tursoCtx struct {
	conn *conn
	tx   *sql.Tx
	err  error
	rows *sql.Rows
	stmt *sql.Stmt
}

func (lc tursoCtx) Open(dsn string) (driver.Conn, error) {
	conn, err := open(dsn)
	if err != nil {
		return nil, err
	}
	nc := tursoCtx{conn: conn}
	return nc, nil
}

func (lc tursoCtx) Close() error {
	var closedb func(uintptr) uintptr
	getExtFunc(&closedb, ExtDBClose)
	closedb(lc.conn.ctx)
	return nil
}

// TODO: Begin not implemented
func (lc tursoCtx) Begin() (driver.Tx, error) {
	return nil, nil
}

func (ls tursoCtx) Prepare(sql string) (driver.Stmt, error) {
	var prepare func(uintptr, uintptr) uintptr
	getExtFunc(&prepare, ExtDBPrepare)
	s := toCString(sql)
	statement := prepare(ls.conn.ctx, s)
	if statement == 0 {
		return nil, errors.New("no rows")
	}
	ls.stmt = stmt{
		ctx: statement,

	}

	}
	return nil, nil
}
