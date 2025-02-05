//go:build windows

package limbo

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

func loadLibrary() (uintptr, error) {
	libName := fmt.Sprintf("%s.dll", libName)
	pathEnv := os.Getenv("PATH")
	paths := strings.Split(pathEnv, ";")

	cwd, err := os.Getwd()
	if err != nil {
		return 0, err
	}
	paths = append(paths, cwd)
	for _, path := range paths {
		dllPath := filepath.Join(path, libName)
		if _, err := os.Stat(dllPath); err == nil {
			slib, loadErr := windows.LoadLibrary(dllPath)
			if loadErr != nil {
				return 0, fmt.Errorf("failed to load library at %s: %w", dllPath, loadErr)
			}
			return uintptr(slib), nil
		}
	}

	return 0, fmt.Errorf("library %s not found in PATH or CWD", libName)
}
