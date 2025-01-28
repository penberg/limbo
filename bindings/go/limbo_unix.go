//go:build linux || darwin

package limbo

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/ebitengine/purego"
)

func loadLibrary() error {
	var libraryName string
	switch runtime.GOOS {
	case "darwin":
		libraryName = fmt.Sprintf("%s.dylib", libName)
	case "linux":
		libraryName = fmt.Sprintf("%s.so", libName)
	default:
		return fmt.Errorf("GOOS=%s is not supported", runtime.GOOS)
	}

	libPath := os.Getenv("LD_LIBRARY_PATH")
	paths := strings.Split(libPath, ":")
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	paths = append(paths, cwd)

	for _, path := range paths {
		libPath := filepath.Join(path, libraryName)
		if _, err := os.Stat(libPath); err == nil {
			slib, dlerr := purego.Dlopen(libPath, purego.RTLD_LAZY)
			if dlerr != nil {
				return fmt.Errorf("failed to load library at %s: %w", libPath, dlerr)
			}
			limboLib = slib
			return nil
		}
	}
	return fmt.Errorf("%s library not found in LD_LIBRARY_PATH or CWD", libName)
}

func init() {
	err := loadLibrary()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	sql.Register("sqlite3", &limboDriver{})
}
