package turso

type ResultCode int

const (
	Error        ResultCode = -1
	Ok           ResultCode = 0
	Row          ResultCode = 1
	Busy         ResultCode = 2
	Done         ResultCode = 3
	Io           ResultCode = 4
	Interrupt    ResultCode = 5
	Invalid      ResultCode = 6
	Null         ResultCode = 7
	NoMem        ResultCode = 8
	ReadOnly     ResultCode = 9
	ExtDBOpen    string     = "db_open"
	ExtDBClose   string     = "db_close"
	ExtDBPrepare string     = "db_prepare"
)

var (
	lib          uintptr
	dbPrepare    func(uintptr, uintptr) uintptr
	rowsNext     func(rowsPtr uintptr) int32
	rowsGetValue func(rowsPtr uintptr, colIdx uint) uintptr
	freeCString  func(strPtr uintptr)
)
