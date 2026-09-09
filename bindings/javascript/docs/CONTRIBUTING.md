# Contributing

So you want to contribute to Limbo's binding for the ~second~ best language in the world? Awesome.

First things first you'll need to install [napi-rs](https://napi.rs/), follow the instructions [here](https://napi.rs/docs/introduction/getting-started) although is highly recommended to use `yarn` with:

```sh
yarn global add @napi-rs/cli
```

Run `yarn build` to build our napi project and run `yarn test` to run our test suite, if nothing breaks you're ready to start!

## API

You can check the API docs [here](./API.md), it aims to be fully compatible with [better-sqlite](https://github.com/WiseLibs/better-sqlite3/) and borrows some things from [libsql](https://github.com/tursodatabase/libsql-js). So if you find some incompability in behaviour and/or lack of functions/attributes, that's an issue and you should work on it for a great good :)

## Code Structure

The Rust code for the bind is on [lib.rs](../src/lib.rs). It is exposed to JS users through the TypeScript wrappers in [packages/native](../packages/native/), where you can
use some JS' ~weirdness~ facilities, for instance, since Rust doesn't have variadic functions the wrapper enables us to "normalize" `bindParameters` into an array.

All tests should be within the relevant package's test directory, such as [packages/common](../packages/common/) and [packages/native](../packages/native/).

# Before open a PR

Please be assured that:

- Your fix/feature has a test checking the new behaviour;
- Your Rust code is formatted with `cargo fmt`;
- Your JavaScript code is formatted with `tsserver` (VSCode's default);
- If applicable, update the [API docs](./API.md) to match the current implementation;
