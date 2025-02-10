package limbo_test

import (
	"database/sql"
	"fmt"
	"log"
	"testing"

	_ "github.com/tursodatabase/limbo"
)

var conn *sql.DB
var connErr error

func TestMain(m *testing.M) {
	conn, connErr = sql.Open("sqlite3", ":memory:")
	if connErr != nil {
		panic(connErr)
	}
	defer conn.Close()
	err := createTable(conn)
	if err != nil {
		log.Fatalf("Error creating table: %v", err)
	}
	m.Run()
}

func TestInsertData(t *testing.T) {
	err := insertData(conn)
	if err != nil {
		t.Fatalf("Error inserting data: %v", err)
	}
}

func TestQuery(t *testing.T) {
	query := "SELECT * FROM test;"
	stmt, err := conn.Prepare(query)
	if err != nil {
		t.Fatalf("Error preparing query: %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		t.Fatalf("Error executing query: %v", err)
	}
	defer rows.Close()

	expectedCols := []string{"foo", "bar", "baz"}
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("Error getting columns: %v", err)
	}
	if len(cols) != len(expectedCols) {
		t.Fatalf("Expected %d columns, got %d", len(expectedCols), len(cols))
	}
	for i, col := range cols {
		if col != expectedCols[i] {
			t.Errorf("Expected column %d to be %s, got %s", i, expectedCols[i], col)
		}
	}
	var i = 1
	for rows.Next() {
		var a int
		var b string
		var c []byte
		err = rows.Scan(&a, &b, &c)
		if err != nil {
			t.Fatalf("Error scanning row: %v", err)
		}
		if a != i || b != rowsMap[i] || !slicesAreEq(c, []byte(rowsMap[i])) {
			t.Fatalf("Expected %d, %s, %s, got %d, %s, %s", i, rowsMap[i], rowsMap[i], a, b, string(c))
		}
		fmt.Println("RESULTS: ", a, b, string(c))
		i++
	}

	if err = rows.Err(); err != nil {
		t.Fatalf("Row iteration error: %v", err)
	}

}

func TestFunctions(t *testing.T) {
	insert := "INSERT INTO test (foo, bar, baz) VALUES (?, ?, zeroblob(?));"
	stmt, err := conn.Prepare(insert)
	if err != nil {
		t.Fatalf("Error preparing statement: %v", err)
	}
	_, err = stmt.Exec(60, "TestFunction", 400)
	if err != nil {
		t.Fatalf("Error executing statement with arguments: %v", err)
	}
	stmt.Close()
	stmt, err = conn.Prepare("SELECT baz FROM test where foo = ?")
	if err != nil {
		t.Fatalf("Error preparing select stmt: %v", err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(60)
	if err != nil {
		t.Fatalf("Error executing select stmt: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var b []byte
		err = rows.Scan(&b)
		if err != nil {
			t.Fatalf("Error scanning row: %v", err)
		}
		if len(b) != 400 {
			t.Fatalf("Expected 100 bytes, got %d", len(b))
		}
	}
	sql := "SELECT uuid4_str();"
	stmt, err = conn.Prepare(sql)
	if err != nil {
		t.Fatalf("Error preparing statement: %v", err)
	}
	defer stmt.Close()
	rows, err = stmt.Query()
	if err != nil {
		t.Fatalf("Error executing query: %v", err)
	}
	defer rows.Close()
	var i int
	for rows.Next() {
		var b string
		err = rows.Scan(&b)
		if err != nil {
			t.Fatalf("Error scanning row: %v", err)
		}
		if len(b) != 36 {
			t.Fatalf("Expected 36 bytes, got %d", len(b))
		}
		i++
		fmt.Printf("uuid: %s\n", b)
	}
	if i != 1 {
		t.Fatalf("Expected 1 row, got %d", i)
	}
	fmt.Println("zeroblob + uuid functions passed")
}

func TestDuplicateConnection(t *testing.T) {
	newConn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Error opening new connection: %v", err)
	}
	err = createTable(newConn)
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}
	err = insertData(newConn)
	if err != nil {
		t.Fatalf("Error inserting data: %v", err)
	}
	query := "SELECT * FROM test;"
	rows, err := newConn.Query(query)
	if err != nil {
		t.Fatalf("Error executing query: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var a int
		var b string
		var c []byte
		err = rows.Scan(&a, &b, &c)
		if err != nil {
			t.Fatalf("Error scanning row: %v", err)
		}
		fmt.Println("RESULTS: ", a, b, string(c))
	}
}

func TestDuplicateConnection2(t *testing.T) {
	newConn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Error opening new connection: %v", err)
	}
	sql := "CREATE TABLE test (foo INTEGER, bar INTEGER, baz BLOB);"
	newConn.Exec(sql)
	sql = "INSERT INTO test (foo, bar, baz) VALUES (?, ?, uuid4());"
	stmt, err := newConn.Prepare(sql)
	stmt.Exec(242345, 2342434)
	defer stmt.Close()
	query := "SELECT * FROM test;"
	rows, err := newConn.Query(query)
	if err != nil {
		t.Fatalf("Error executing query: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var a int
		var b int
		var c []byte
		err = rows.Scan(&a, &b, &c)
		if err != nil {
			t.Fatalf("Error scanning row: %v", err)
		}
		fmt.Println("RESULTS: ", a, b, string(c))
		if len(c) != 16 {
			t.Fatalf("Expected 16 bytes, got %d", len(c))
		}
	}
}

func TestConnectionError(t *testing.T) {
	newConn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Error opening new connection: %v", err)
	}
	sql := "CREATE TABLE test (foo INTEGER, bar INTEGER, baz BLOB);"
	newConn.Exec(sql)
	sql = "INSERT INTO test (foo, bar, baz) VALUES (?, ?, notafunction(?));"
	_, err = newConn.Prepare(sql)
	if err == nil {
		t.Fatalf("Expected error, got nil")
	}
	expectedErr := "Parse error: unknown function notafunction"
	if err.Error() != expectedErr {
		t.Fatalf("Error test failed, expected: %s, found: %v", expectedErr, err)
	}
	fmt.Println("Connection error test passed")
}

func TestStatementError(t *testing.T) {
	newConn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Error opening new connection: %v", err)
	}
	sql := "CREATE TABLE test (foo INTEGER, bar INTEGER, baz BLOB);"
	newConn.Exec(sql)
	sql = "INSERT INTO test (foo, bar, baz) VALUES (?, ?, ?);"
	stmt, err := newConn.Prepare(sql)
	if err != nil {
		t.Fatalf("Error preparing statement: %v", err)
	}
	_, err = stmt.Exec(1, 2)
	if err == nil {
		t.Fatalf("Expected error, got nil")
	}
	if err.Error() != "sql: expected 3 arguments, got 2" {
		t.Fatalf("Unexpected : %v\n", err)
	}
	fmt.Println("Statement error test passed")
}

func TestDriverRowsErrorMessages(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = db.Exec("INSERT INTO test (id, name) VALUES (?, ?)", 1, "Alice")
	if err != nil {
		t.Fatalf("failed to insert row: %v", err)
	}

	rows, err := db.Query("SELECT id, name FROM test")
	if err != nil {
		t.Fatalf("failed to query table: %v", err)
	}

	if !rows.Next() {
		t.Fatalf("expected at least one row")
	}
	var id int
	var name string
	err = rows.Scan(&name, &id)
	if err == nil {
		t.Fatalf("expected error scanning wrong type: %v", err)
	}
	t.Log("Rows error behavior test passed")
}

func slicesAreEq(a, b []byte) bool {
	if len(a) != len(b) {
		fmt.Printf("LENGTHS NOT EQUAL: %d != %d\n", len(a), len(b))
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			fmt.Printf("SLICES NOT EQUAL: %v != %v\n", a, b)
			return false
		}
	}
	return true
}

var rowsMap = map[int]string{1: "hello", 2: "world", 3: "foo", 4: "bar", 5: "baz"}

func createTable(conn *sql.DB) error {
	insert := "CREATE TABLE test (foo INT, bar TEXT, baz BLOB);"
	stmt, err := conn.Prepare(insert)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	return err
}

func insertData(conn *sql.DB) error {
	for i := 1; i <= 5; i++ {
		insert := "INSERT INTO test (foo, bar, baz) VALUES (?, ?, ?);"
		stmt, err := conn.Prepare(insert)
		if err != nil {
			return err
		}
		defer stmt.Close()
		if _, err = stmt.Exec(i, rowsMap[i], []byte(rowsMap[i])); err != nil {
			return err
		}
	}
	return nil
}
