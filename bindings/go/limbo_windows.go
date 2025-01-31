//go:build windows

package limbo

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

func loadLibrary() error {
	libName := fmt.Sprintf("%s.dll", libName)
	pathEnv := os.Getenv("PATH")
	paths := strings.Split(pathEnv, ";")

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	paths = append(paths, cwd)
	for _, path := range paths {
		dllPath := filepath.Join(path, libName)
		if _, err := os.Stat(dllPath); err == nil {
			slib, loadErr := windows.LoadLibrary(dllPath)
			if loadErr != nil {
				return fmt.Errorf("failed to load library at %s: %w", dllPath, loadErr)
			}
			limboLib = uintptr(slib)
			return nil
		}
	}

	return fmt.Errorf("library %s not found in PATH or CWD", libName)
}

func init() {
	err := loadLibrary()
	if err != nil {
		fmt.Println("Error opening limbo library: ", err)
		os.Exit(1)
	}
	sql.Register("sqlite3", &limboDriver{})
}
