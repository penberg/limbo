package limbo

import (
	"database/sql/driver"
	"fmt"
	"unsafe"
)

type ResultCode int32

const (
	Error               ResultCode = -1
	Ok                  ResultCode = 0
	Row                 ResultCode = 1
	Busy                ResultCode = 2
	Io                  ResultCode = 3
	Interrupt           ResultCode = 4
	Invalid             ResultCode = 5
	Null                ResultCode = 6
	NoMem               ResultCode = 7
	ReadOnly            ResultCode = 8
	NoData              ResultCode = 9
	Done                ResultCode = 10
	SyntaxErr           ResultCode = 11
	ConstraintViolation ResultCode = 12
	NoSuchEntity        ResultCode = 13
)

func (rc ResultCode) String() string {
	switch rc {
	case Error:
		return "Error"
	case Ok:
		return "Ok"
	case Row:
		return "Row"
	case Busy:
		return "Busy"
	case Io:
		return "Io"
	case Interrupt:
		return "Query was interrupted"
	case Invalid:
		return "Invalid"
	case Null:
		return "Null"
	case NoMem:
		return "Out of memory"
	case ReadOnly:
		return "Read Only"
	case NoData:
		return "No Data"
	case Done:
		return "Done"
	case SyntaxErr:
		return "Syntax Error"
	case ConstraintViolation:
		return "Constraint Violation"
	case NoSuchEntity:
		return "No such entity"
	default:
		return "Unknown response code"
	}
}

const (
	FfiDbOpen             string = "db_open"
	FfiDbClose            string = "db_close"
	FfiDbPrepare          string = "db_prepare"
	FfiStmtExec           string = "stmt_execute"
	FfiStmtQuery          string = "stmt_query"
	FfiStmtParameterCount string = "stmt_parameter_count"
	FfiStmtClose          string = "stmt_close"
	FfiRowsClose          string = "rows_close"
	FfiRowsGetColumns     string = "rows_get_columns"
	FfiRowsNext           string = "rows_next"
	FfiRowsGetValue       string = "rows_get_value"
	FfiFreeColumns        string = "free_columns"
	FfiFreeCString        string = "free_string"
)

// convert a namedValue slice into normal values until named parameters are supported
func namedValueToValue(named []driver.NamedValue) []driver.Value {
	out := make([]driver.Value, len(named))
	for i, nv := range named {
		out[i] = nv.Value
	}
	return out
}

func buildNamedArgs(named []driver.NamedValue) ([]limboValue, error) {
	args := namedValueToValue(named)
	return buildArgs(args)
}

type valueType int32

const (
	intVal  valueType = 0
	textVal valueType = 1
	blobVal valueType = 2
	realVal valueType = 3
	nullVal valueType = 4
)

func (vt valueType) String() string {
	switch vt {
	case intVal:
		return "int"
	case textVal:
		return "text"
	case blobVal:
		return "blob"
	case realVal:
		return "real"
	case nullVal:
		return "null"
	default:
		return "unknown"
	}
}

// struct to pass Go values over FFI
type limboValue struct {
	Type  valueType
	_     [4]byte // padding to align Value to 8 bytes
	Value [8]byte
}

// struct to pass byte slices over FFI
type Blob struct {
	Data uintptr
	Len  uint
}

// convert a limboValue to a native Go value
func toGoValue(valPtr uintptr) interface{} {
	if valPtr == 0 {
		return nil
	}
	val := (*limboValue)(unsafe.Pointer(valPtr))
	switch val.Type {
	case intVal:
		return *(*int64)(unsafe.Pointer(&val.Value))
	case realVal:
		return *(*float64)(unsafe.Pointer(&val.Value))
	case textVal:
		textPtr := *(*uintptr)(unsafe.Pointer(&val.Value))
		return GoString(textPtr)
	case blobVal:
		blobPtr := *(*uintptr)(unsafe.Pointer(&val.Value))
		return toGoBlob(blobPtr)
	case nullVal:
		return nil
	default:
		return nil
	}
}

func getArgsPtr(args []driver.Value) (uintptr, error) {
	if len(args) == 0 {
		return 0, nil
	}
	argSlice, err := buildArgs(args)
	if err != nil {
		return 0, err
	}
	return uintptr(unsafe.Pointer(&argSlice[0])), nil
}

// convert a byte slice to a Blob type that can be sent over FFI
func makeBlob(b []byte) *Blob {
	if len(b) == 0 {
		return nil
	}
	blob := &Blob{
		Data: uintptr(unsafe.Pointer(&b[0])),
		Len:  uint(len(b)),
	}
	return blob
}

// converts a blob received via FFI to a native Go byte slice
func toGoBlob(blobPtr uintptr) []byte {
	if blobPtr == 0 {
		return nil
	}
	blob := (*Blob)(unsafe.Pointer(blobPtr))
	return unsafe.Slice((*byte)(unsafe.Pointer(blob.Data)), blob.Len)
}

func cArrayToGoStrings(arrayPtr uintptr, length uint) []string {
	if arrayPtr == 0 || length == 0 {
		return nil
	}

	ptrSlice := unsafe.Slice(
		(**byte)(unsafe.Pointer(arrayPtr)),
		length,
	)

	out := make([]string, 0, length)
	for _, cstr := range ptrSlice {
		out = append(out, GoString(uintptr(unsafe.Pointer(cstr))))
	}
	return out
}

// convert a Go slice of driver.Value to a slice of limboValue that can be sent over FFI
func buildArgs(args []driver.Value) ([]limboValue, error) {
	argSlice := make([]limboValue, len(args))
	for i, v := range args {
		limboVal := limboValue{}
		switch val := v.(type) {
		case nil:
			limboVal.Type = nullVal
		case int64:
			limboVal.Type = intVal
			limboVal.Value = *(*[8]byte)(unsafe.Pointer(&val))
		case float64:
			limboVal.Type = realVal
			limboVal.Value = *(*[8]byte)(unsafe.Pointer(&val))
		case string:
			limboVal.Type = textVal
			cstr := CString(val)
			*(*uintptr)(unsafe.Pointer(&limboVal.Value)) = uintptr(unsafe.Pointer(cstr))
		case []byte:
			argSlice[i].Type = blobVal
			blob := makeBlob(val)
			*(*uintptr)(unsafe.Pointer(&limboVal.Value)) = uintptr(unsafe.Pointer(blob))
		default:
			return nil, fmt.Errorf("unsupported type: %T", v)
		}
		argSlice[i] = limboVal
	}
	return argSlice, nil
}

func storeInt64(data *[8]byte, val int64) {
	*(*int64)(unsafe.Pointer(data)) = val
}

func storeFloat64(data *[8]byte, val float64) {
	*(*float64)(unsafe.Pointer(data)) = val
}

func storePointer(data *[8]byte, ptr *byte) {
	*(*uintptr)(unsafe.Pointer(data)) = uintptr(unsafe.Pointer(ptr))
}

/* Credit below (Apache2 License) to:
https://github.com/ebitengine/purego/blob/main/internal/strings/strings.go
*/

func hasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

func CString(name string) *byte {
	if hasSuffix(name, "\x00") {
		return &(*(*[]byte)(unsafe.Pointer(&name)))[0]
	}
	b := make([]byte, len(name)+1)
	copy(b, name)
	return &b[0]
}

func GoString(c uintptr) string {
	ptr := *(*unsafe.Pointer)(unsafe.Pointer(&c))
	if ptr == nil {
		return ""
	}
	var length int
	for {
		if *(*byte)(unsafe.Add(ptr, uintptr(length))) == '\x00' {
			break
		}
		length++
	}
	return string(unsafe.Slice((*byte)(ptr), length))
}
