## Limbo driver for Go's `database/sql` library


**NOTE:** this is currently __heavily__ W.I.P and is not yet in a usable state. This is merged in only for the purposes of incremental progress and not because the existing code here proper. Expect many and frequent changes.

This uses the [purego](https://github.com/ebitengine/purego) library to call C (in this case Rust with C ABI) functions from Go without the use of `CGO`.




### To test


## Linux | MacOS

_All commands listed are relative to the bindings/go directory in the limbo repository_

```
cargo build --package limbo-go


# Your LD_LIBRARY_PATH environment variable must include limbo's `target/debug` directory

LD_LIBRARY_PATH="../../target/debug:$LD_LIBRARY_PATH" go test

```


## Windows

```
cargo build --package limbo-go

# Copy the lib_limbo_go.dll into the current working directory (bindings/go)
# Alternatively, you could add the .dll to a location in your PATH

cp ../../target/debug/lib_limbo_go.dll .

go test

```
