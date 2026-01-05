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
Run the `miniswap-task` queue binary first before starting the `generate_tasks` binary to get the full usage
You can run `generate_tasks` like this:
```
cargo run --bin generate_tasks
```
This will continuously generate and insert random tasks with payloads into the database. With each insert the `miniswap-task` binary will listen for notifications with a payload with the task's id that was inserted. It will launch a task to handle it if it should.
