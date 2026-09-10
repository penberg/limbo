# Limbo Simulator Docker Runner

This directory contains the script that runs inside the `limbo-sim` Docker image. The script continuously runs the `limbo-sim` program in a loop until it encounters a panic, at which point it automatically creates a GitHub issue in the `limbo` repository.

## What it does

1. The limbo-sim image is built and pushed to ECR by [.github/workflows/build-sim.yml](../../../.github/workflows/build-sim.yml) on every main commit
2. When the container starts, this script:
   - Runs the [limbo-sim](../Cargo.toml) program with a random seed
   - If a panic occurs:
     - Captures the seed value and commit hash
     - Creates a GitHub issue with reproduction steps
     - Includes panic output and relevant metadata
   - Continues running with a new seed until a panic occurs or TIME_LIMIT_MINUTES is reached

The script acts as the entrypoint for the Docker container, automatically starting the simulation loop when the container launches.

## How do I see the open issues created by the simulator?

[GitHub issues](https://github.com/tursodatabase/turso/issues?q=is%3Aissue+is%3Aopen+label%3A%22automated%22)
