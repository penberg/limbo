//go:build linux || darwin

package limbo

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/ebitengine/purego"
)

func loadLibrary() (uintptr, error) {
	var libraryName string
	switch runtime.GOOS {
	case "darwin":
		libraryName = fmt.Sprintf("%s.dylib", libName)
	case "linux":
		libraryName = fmt.Sprintf("%s.so", libName)
	default:
		return 0, fmt.Errorf("GOOS=%s is not supported", runtime.GOOS)
	}

	libPath := os.Getenv("LD_LIBRARY_PATH")
	paths := strings.Split(libPath, ":")
	cwd, err := os.Getwd()
	if err != nil {
		return 0, err
	}
	paths = append(paths, cwd)

	for _, path := range paths {
		libPath := filepath.Join(path, libraryName)
		if _, err := os.Stat(libPath); err == nil {
			slib, dlerr := purego.Dlopen(libPath, purego.RTLD_NOW|purego.RTLD_GLOBAL)
			if dlerr != nil {
				return 0, fmt.Errorf("failed to load library at %s: %w", libPath, dlerr)
			}
			return slib, nil
		}
	}
	return 0, fmt.Errorf("%s library not found in LD_LIBRARY_PATH or CWD", libName)
}
