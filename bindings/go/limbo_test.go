package limbo_test

import (
	"database/sql"
	"fmt"
	"log"
	"testing"

	_ "limbo"
)

var conn *sql.DB
var connErr error

func TestMain(m *testing.M) {
	conn, connErr = sql.Open("sqlite3", ":memory:")
	if connErr != nil {
		panic(connErr)
	}
	defer conn.Close()
	err := createTable()
	if err != nil {
		log.Fatalf("Error creating table: %v", err)
	}
	m.Run()
}

func TestInsertData(t *testing.T) {
	err := insertData()
	if err != nil {
		t.Fatalf("Error inserting data: %v", err)
	}
}

func TestFunction(t *testing.T) {
	insert := "INSERT INTO test (foo, bar, baz) VALUES (?, ?, zeroblob(?));"
	stmt, err := conn.Prepare(insert)
	if err != nil {
		t.Fatalf("Error preparing statement: %v", err)
	}
	_, err = stmt.Exec(1, "hello", 100)
	if err != nil {
		t.Fatalf("Error executing statment with arguments: %v", err)
	}
	stmt.Close()
	stmt, err = conn.Prepare("SELECT baz FROM test where foo = ?")
	if err != nil {
		t.Fatalf("Error preparing select stmt: %v", err)
	}
	defer stmt.Close()
	rows, err := stmt.Query(1)
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
		fmt.Println("RESULTS: ", string(b))
	}
}

func TestQuery(t *testing.T) {
	err := insertData()
	if err != nil {
		t.Fatalf("Error inserting data: %v", err)
	}

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

func createTable() error {
	insert := "CREATE TABLE test (foo INT, bar TEXT, baz BLOB);"
	stmt, err := conn.Prepare(insert)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	return err
}

func insertData() error {
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
