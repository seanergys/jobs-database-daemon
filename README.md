# Flux Lifecycle V2 — Build, Run and Test on LXP-TestBed

This guide explains how to run and test the Flux Lifecycle V2 integration with jobs-database daemon on Lux-Provide LXP Testbed.

## Architecture

Flux V2 publisher → SeanerBUS → V2 database consumer → PostgreSQL

**Dataplane UUID:** `834d23e1-fd27-4537-b467-9702ef9ca8a2`

**Message type:** `FluxLifecycleEventV2` (ordinal 23)

## Prerequisites

- Flux with the V2 lifecycle publisher plugin.
- A running SeanerBUS broker.
- PostgreSQL with the V2 database schema initialized.
- The V2 consumer and its dependencies built successfully.

Login into the SEANERGYS system on the LXP Testbed.
ssh seanergys-lxp-cpu01

Check that all the source code and installations (for eg. SeanerBUS, flux etc.) are in the /nfs/share01/ directory.

confirm that the following docker containers are up and running! If not, then start the docker containers!

hpc-flux-lifecycle-daemon-v2-test
hpc-postgres-v2


## Terminal 1 — SeanerBUS
Verify that the broker is listening on the expected endpoint before continuing.

**Important:** When using the shared LXP broker, use the existing service.
Do not restart it or change its configuration.

To check or to verify that the seanerbus/demobus is running on lxp-testbed:

[u1020@seanergys-lxp-cpu01 ~]$ pgrep -af 'demobus|seanerbus'
1641 /nfs/share01/seanerbus/install/bin/demobus
1647 /nfs/share01/seanerbus-hta/build/db
1649 /nfs/share01/seanerbus/install/bin/naming_server
4000 python clients/seanerbus_bridge.py
13946 python3.12 /localhome/u1003/llview/da/rms/seanergys/seanerbus_subscription.py --config /nfs/share01/llview-seanergys-configs//plugins/seanerbus.yaml
2376831 docker run --rm -it --name hpc-flux-lifecycle-daemon-v2-test --network host --mount type=bind,source=/home/u1020/lxp-v2-work/seanerbus-jobs-database-v2-build,target=/workspace,readonly --workdir /workspace --env HPC_JOB_DB=host=127.0.0.1 port=5434 dbname=hpcjobs_v2 user=hpc password=hpc --env SEANERBUS_PUBSUB_ENDPOINT=demobus://localhost:5398 --env SEANERBUS_REQRES_ENDPOINT=demobus://localhost:5398 hpc-v2-consumer-build:debian13 ./build-v2-consumer/flux_lifecycle_daemon_v2


Is the broker running?	Yes, PID 1641.
Is it listening on port 5398?	Yes, on [::1]:5398.
Does its source contain useful debug logs?	Yes, including subscription and message-forwarding logs.
Can your account currently read its system journal?	No; permission is insufficient.

[u1020@seanergys-lxp-cpu01 ~]$ ss -ltn | grep ':5398'
LISTEN 0      128            [::1]:5398          [::]:*   


## Terminal 2 — Start the V2 consumer

If V2 C++ consumer is already running, then do this:

Stop the V2 consumer

Run this on the LXP testbed:

docker stop hpc-flux-lifecycle-daemon-v2-test

Verify that it stopped:

docker ps -a --filter 'name=^/hpc-flux-lifecycle-daemon-v2-test$'
Inspect the docker container for the V2 consumer!

docker inspect hpc-flux-lifecycle-daemon-v2-test \
  --format 'Status={{.State.Status}} Running={{.State.Running}}'


Start the V2 consumer again!

cd ~/lxp-v2-work/seanerbus-jobs-database-v2-build

docker run -d \
  --name hpc-flux-lifecycle-daemon-v2-test \
  --restart unless-stopped \
  --network host \
  --mount type=bind,source="$PWD",target=/workspace,readonly \
  --workdir /workspace \
  --env 'HPC_JOB_DB=host=127.0.0.1 port=5434 dbname=hpcjobs_v2 user=hpc password=hpc' \
  --env 'SEANERBUS_PUBSUB_ENDPOINT=demobus://localhost:5398' \
  --env 'SEANERBUS_REQRES_ENDPOINT=demobus://localhost:5398' \
  hpc-v2-consumer-build:debian13 \
  ./build-v2-consumer/jobs-database-daemon



To see new consumer logs, open another terminal and run:

docker logs --follow hpc-flux-lifecycle-daemon-v2-test

## Terminal 3 — Start a Python observer

[u1020@seanergys-lxp-cpu01 ~]$ ```
/nfs/share01/venv/bin/python - <<'PY'
import asyncio
import json
import capnp
import seanerbus.client

UUID = "834d23e1-fd27-4537-b467-9702ef9ca8a2"
SCHEMA_PATH = "/tmp/flux_v2_private_v6t4699q.capnp"

schema = capnp.load(SCHEMA_PATH)

async def main():
    print("Connecting to the existing LXP SeanerBUS...", flush=True)

    connection = seanerbus.client.Connection(
        "demobus://localhost:5398",
        "demobus://localhost:5398",
    )

    next_message = await connection.subscribe(UUID)

    print(f"Subscribed to {UUID}", flush=True)
    print("Waiting for V2 Flux lifecycle messages...", flush=True)

    count = 0
    while True:
        message = await next_message()
        count += 1
        print(f"\nMessage #{count} received", flush=True)

        with schema.FluxLifecycleEventV2.from_bytes(
            message.payload()
        ) as payload:
            print(json.dumps(payload.to_dict(), indent=2), flush=True)

asyncio.run(capnp.run(main()))
PY
```


Then message logs like below shall be seen on the terminal!
Connecting to the existing LXP SeanerBUS...
Subscribed to 834d23e1-fd27-4537-b467-9702ef9ca8a2
Waiting for V2 Flux lifecycle messages...



## Terminal 4 — Submit a real Flux job


/nfs/share01/flux-core/install/bin/flux submit -n1 sleep 5

This submits a real, short-lived job through the existing Flux installation.
A flux job-Id like this will be seen on the terminal! 
ƒCYZUcRCCtF

## Terminal 5 — PostgreSQL

Start the local V2 PostgreSQL database, if it is not already running:

Then check the database connection and tables
docker exec hpc-postgres-v2 psql -U hpc -d hpcjobs_v2 -c "
SELECT current_database() AS database,
       current_user AS db_user,
       version();
"

Then check the tables:
docker exec hpc-postgres-v2 psql -U hpc -d hpcjobs_v2 -c "\dt"


1. Inspect the schema of all three tables

Run this on the LXP testbed:

docker exec -it hpc-postgres-v2 psql -U hpc -d hpcjobs_v2

You should now be at the PostgreSQL prompt:

hpcjobs_v2=#
Show the schema of messages
\d+ messages
Show the schema of jobs
\d+ jobs
Show the schema of job_events
\d+ job_events

The \d+ command shows columns, data types, nullability, defaults, indexes, constraints, and additional table information.


2. Display the complete contents of all three tables

Warning: These queries display every row. If the tables have accumulated many records, the output may be large.

Table 1: messages
SELECT *
FROM messages
ORDER BY timestamp_ns, message_id;

This shows all stored messages, including their message IDs, job IDs, message types, payload type IDs, timestamps, and serialized Cap'n Proto payloads (if stored in capnp_payload).

If the payload column contains binary data, its output may be difficult to read. We'll inspect the column type and decide how best to examine it.

Table 2: jobs
SELECT *
FROM jobs
ORDER BY job_id;

This shows every job currently recorded in the database, including its stored metadata and current state.

Table 3: job_events
SELECT *
FROM job_events
ORDER BY job_id, event_seq;

This shows all lifecycle events, grouped by job and ordered by event sequence.

Exit PostgreSQL

When finished, type:

\q

This exits the PostgreSQL client; it does not stop the database.

3. Inspect all indexes

Indexes affect query performance and are worth checking as we develop the V2 database.

From the PostgreSQL prompt, run:

SELECT
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename IN ('messages', 'jobs', 'job_events')
ORDER BY tablename, indexname;

This shows the index definitions for the three tables.

4. Check row counts for all three tables

Run:

SELECT 'messages' AS table_name, COUNT(*) AS row_count
FROM messages

UNION ALL

SELECT 'jobs', COUNT(*)
FROM jobs

UNION ALL

SELECT 'job_events', COUNT(*)
FROM job_events;

These are exact counts, not estimates.

5. Get a concise overview of the complete database

If you want to understand the database before examining individual rows, this query summarizes the number of jobs and lifecycle events:

SELECT
    current_state,
    COUNT(*) AS job_count
FROM jobs
GROUP BY current_state
ORDER BY current_state;

And for lifecycle events:

SELECT
    flux_state,
    normalized_state,
    COUNT(*) AS event_count
FROM job_events
GROUP BY flux_state, normalized_state
ORDER BY flux_state, normalized_state;

You can execute all PostgreSQL commands directly from your normal LXP terminal, without entering the PostgreSQL interactive shell.

Run these commands directly in your normal terminal:

messages

docker exec hpc-postgres-v2 psql -U hpc -d hpcjobs_v2 -c '\d+ messages'

jobs

docker exec hpc-postgres-v2 psql -U hpc -d hpcjobs_v2 -c '\d+ jobs'

job_events

docker exec hpc-postgres-v2 psql -U hpc -d hpcjobs_v2 -c '\d+ job_events'
2. Display the complete contents of each table

All messages

docker exec hpc-postgres-v2 psql -U hpc -d hpcjobs_v2 -c \
'SELECT * FROM messages ORDER BY timestamp_ns, message_id;'

All jobs

docker exec hpc-postgres-v2 psql -U hpc -d hpcjobs_v2 -c \
'SELECT * FROM jobs ORDER BY job_id;'

All job events

docker exec hpc-postgres-v2 psql -U hpc -d hpcjobs_v2 -c \
'SELECT * FROM job_events ORDER BY job_id, event_seq;'

These commands display every row. If the tables are large, the output could be extensive.

3. Count rows in all three tables
docker exec hpc-postgres-v2 psql -U hpc -d hpcjobs_v2 -c "
SELECT 'messages' AS table_name, COUNT(*) AS row_count FROM messages
UNION ALL
SELECT 'jobs', COUNT(*) FROM jobs
UNION ALL
SELECT 'job_events', COUNT(*) FROM job_events;
"
4. Display all indexes
docker exec hpc-postgres-v2 psql -U hpc -d hpcjobs_v2 -c "
SELECT tablename, indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'public'
  AND tablename IN ('messages', 'jobs', 'job_events')
ORDER BY tablename, indexname;
"
