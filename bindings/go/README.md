# Limbo driver for Go's `database/sql` library


**NOTE:** this is currently __heavily__ W.I.P and is not yet in a usable state.

This driver uses the awesome [purego](https://github.com/ebitengine/purego) library to call C (in this case Rust with C ABI) functions from Go without the use of `CGO`.


## To use: (_UNSTABLE_ testing or development purposes only)

### Linux | MacOS

_All commands listed are relative to the bindings/go directory in the limbo repository_

```
cargo build --package limbo-go

# Your LD_LIBRARY_PATH environment variable must include limbo's `target/debug` directory

export LD_LIBRARY_PATH="/path/to/limbo/target/debug:$LD_LIBRARY_PATH"

```

## Windows

```
cargo build --package limbo-go

# You must add limbo's `target/debug` directory to your PATH
# or you could built + copy the .dll to a location in your PATH
# or just the CWD of your go module

cp path\to\limbo\target\debug\lib_limbo_go.dll .

go test

```
**Temporarily** you may have to clone the limbo repository and run:

`go mod edit -replace github.com/tursodatabase/limbo=/path/to/limbo/bindings/go`

```go
import (
    "fmt"
    "database/sql"
    _"github.com/tursodatabase/limbo"
)

func main() {
	conn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
        fmt.Printf("Error: %v\n", err)
        os.Exit(1)
	}
    sql := "CREATE table go_limbo (foo INTEGER, bar TEXT)"
    _ = conn.Exec(sql)

    sql = "INSERT INTO go_limbo (foo, bar) values (?, ?)"
    stmt, _ := conn.Prepare(sql)
    defer stmt.Close()
    _  = stmt.Exec(42, "limbo")
    rows, _ := conn.Query("SELECT * from go_limbo")
    defer rows.Close()
    for rows.Next() {
        var a int
        var b string
		_ = rows.Scan(&a, &b)
        fmt.Printf("%d, %s", a, b)
    }
}
```
