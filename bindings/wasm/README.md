# Limbo Wasm bindings

This source tree contains Limbo Wasm bindings.

## Building

For nodejs
```
./scripts/build
```
For web

```
./scripts/build web
```

# Browser Support

Adding experimental support for limbo in the browser. This is done by adding support for OPFS as a VFS.

To see a basic example of this `npm run dev` and navigate to `http://localhost:5173/limbo-opfs-test.html` and open the console.

## Design

This design mirrors sqlite's approach for OPFS support. It has a sync api in `opfs.js` which communicates with `opfs-sync-proxy.js` via `SharedArrayBuffer` and `Atomics.wait`. This allows us to live the VFS api in `lib.rs` unchanged.

You can see `limbo-opfs-test.html` for basic usage.

## UTs

There are OPFS specific unit tests and then some basic limbo unit tests. These are run via `npm test` or `npx vitest`.

For more info and log output you can run `npx vitest:ui` but you can get some parallel execution of test cases which cause issues.


## TODO

-[] Add a wrapper js that provides a clean interface to the `limbo-worker.js`
-[] Add more tests for opfs.js operations
-[] Add error return handling
-[] Make sure posix flags for open are handled instead of just being ignored (this requires creating a mapping of behaviors from posix to opfs as far as makes sense)

