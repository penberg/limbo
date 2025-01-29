package limbo_test

import (
	"database/sql"
	"fmt"
	"testing"

	_ "limbo"
)

func TestConnection(t *testing.T) {
	conn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Error opening database: %v", err)
	}
	defer conn.Close()
}

func TestCreateTable(t *testing.T) {
	conn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Error opening database: %v", err)
	}
	defer conn.Close()

	err = createTable(conn)
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}
}

func TestInsertData(t *testing.T) {
	conn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Error opening database: %v", err)
	}
	defer conn.Close()

	err = createTable(conn)
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}

	err = insertData(conn)
	if err != nil {
		t.Fatalf("Error inserting data: %v", err)
	}
}

func TestQuery(t *testing.T) {
	conn, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Error opening database: %v", err)
	}
	defer conn.Close()

	err = createTable(conn)
	if err != nil {
		t.Fatalf("Error creating table: %v", err)
	}

	err = insertData(conn)
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
		if a != i || b != rowsMap[i] || string(c) != rowsMap[i] {
			t.Fatalf("Expected %d, %s, got %d, %s, %b", i, rowsMap[i], a, b, c)
		}
		fmt.Println("RESULTS: ", a, b, string(c))
		i++
	}

	if err = rows.Err(); err != nil {
		t.Fatalf("Row iteration error: %v", err)
	}
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
