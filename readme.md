# Running queue system

## Requirements
You'll need a postgres instance running. The binary assumes one is running that it can connect to using port `7777`.
The environment variables for the postgres instance should use
```
POSTGRES_USER = miniswap
POSTGRES_DB = miniswap
POSTGRES_PASSWORD = miniswap
```

## Running binary
Run the queue with this command
```
cargo run --bin miniswap-task
```

## Generating data to simulate the system
Run the `generate_tasks` binary first before starting the `miniswap-task` binary to get the full usage
You can run `generate_tasks` like this:
```
cargo run --bin generate_tasks
```
This will continuously generate random tasks with payloads into the database. With each insert the `miniswap-task` binary
will listen for notifications that something was inserted and will launch a task to handle it if it should.
