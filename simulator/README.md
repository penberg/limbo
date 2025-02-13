# Limbo Simulator

Limbo simulator uses randomized deterministic simulations to test the Limbo database behaviors.

Each simulation begins with a random configurations:

- the database workload distribution(percentages of reads, writes, deletes...),
- database parameters(page size),
- number of reader or writers, etc.

Based on these parameters, we randomly generate **interaction plans**. Interaction plans consist of statements/queries, and assertions that will be executed in order. The building blocks of interaction plans are:

- Randomly generated SQL queries satisfying the workload distribution,
- Properties, which contain multiple matching queries with assertions indicating the expected result.

An example of a property is the following:

```json
{
  "name": "Read your own writes",
  "queries": [
    "INSERT INTO t1 (id) VALUES (1)",
    "SELECT * FROM t1 WHERE id = 1"
  ],
  "assertions": [
    "result.rows.length == 1",
    "result.rows[0].id == 1"
  ]
}
```

The simulator executes the interaction plans in a loop, and checks the assertions. It can add random queries unrelated to the properties without
breaking the property invariants to reach more diverse states and respect the configured workload distribution.

The simulator code is broken into 4 main parts:

- **Simulator(main.rs)**: The main entry point of the simulator. It generates random configurations and interaction plans, and executes them.
- **Model(model.rs, model/table.rs, model/query.rs)**: A simpler model of the database, it contains atomic actions for insertion and selection, we use this model while deciding the next actions.
- **Generation(generation.rs, generation/table.rs, generation/query.rs, generation/plan.rs)**: Random generation functions for the database model and interaction plans.
- **Properties(properties.rs)**: Contains the properties that we want to test.

## Running the simulator

To run the simulator, you can use the following command:

```bash
cargo run
```

This prompt (in the future) will invoke a clap command line interface to configure the simulator. For now, the simulator runs with the default configurations changing the `main.rs` file. If you want to see the logs, you can change the `RUST_LOG` environment variable.

```bash
RUST_LOG=info cargo run --bin limbo_sim
```

## Adding new properties

Todo

## Adding new generation functions

Todo

## Adding new models

Todo

## Coverage with Limbo

Todo

## Automatic Compatibility Testing with SQLite

Todo

## Resources
- [(reading) TigerBeetle Deterministic Simulation Testing](https://docs.tigerbeetle.com/about/vopr/)
- [(reading) sled simulation guide (jepsen-proof engineering)](https://sled.rs/simulation.html)
- [(video) "Testing Distributed Systems w/ Deterministic Simulation" by Will Wilson](https://www.youtube.com/watch?v=4fFDFbi3toc)
- [(video) FF meetup #4 - Deterministic simulation testing](https://www.youtube.com/live/29Vz5wkoUR8)
- [(code) Hiisi: a proof of concept libSQL written in Rust following TigerBeetle-style with deterministic simulation testing](https://github.com/penberg/hiisi)
