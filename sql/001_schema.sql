/*
 * SPDX-FileCopyrightText: 2026 SEANERGYS Partners <https://seanergys.eu>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

BEGIN;

CREATE TABLE IF NOT EXISTS messages (
    message_id          BIGSERIAL PRIMARY KEY,
    address_upper       NUMERIC(20,0) NOT NULL,
    address_lower       NUMERIC(20,0) NOT NULL,
    timestamp_ns        BIGINT NOT NULL,
    message_type        TEXT NOT NULL,
    payload_type_id     NUMERIC(20,0) NOT NULL,
    job_id              BIGINT,
    received_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    capnp_payload       BYTEA NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_address_time
    ON messages(address_upper, address_lower, timestamp_ns, message_id);
CREATE INDEX IF NOT EXISTS idx_messages_job_time
    ON messages(job_id, timestamp_ns, message_id) WHERE job_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS jobs (
    job_id                 BIGINT PRIMARY KEY,
    jobid_f58              TEXT NOT NULL DEFAULT '',
    state                  TEXT NOT NULL DEFAULT '',
    prev_state             TEXT NOT NULL DEFAULT '',
    owner_id               BIGINT NOT NULL DEFAULT 0,
    owner_name             TEXT NOT NULL DEFAULT '',
    job_name               TEXT NOT NULL DEFAULT '',
    job_display_name       TEXT NOT NULL DEFAULT '',
    budget                 TEXT NOT NULL DEFAULT '',
    queue                  TEXT NOT NULL DEFAULT '',
    submit_time_us         BIGINT NOT NULL DEFAULT 0,
    requested_walltime_us  BIGINT NOT NULL DEFAULT 0,
    nslots                 BIGINT NOT NULL DEFAULT 0,
    nnodes                 BIGINT NOT NULL DEFAULT 0,
    ncpus                  BIGINT NOT NULL DEFAULT 0,
    ngpus                  BIGINT NOT NULL DEFAULT 0,
    priority               BIGINT,
    node_list              TEXT[],
    r_lite                 JSONB,
    start_time_us          BIGINT,
    finish_time_us         BIGINT,
    rc_state               TEXT,
    error_type             TEXT,
    error_severity         BIGINT,
    error_note             TEXT,
    error_user_id          BIGINT,
    error_wait_status      BIGINT,
    current_state          TEXT NOT NULL DEFAULT 'UNKNOWN',
    last_event_ts_ns       BIGINT NOT NULL DEFAULT 0,
    last_message_id        BIGINT,
    last_capnp_payload     BYTEA,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_jobs_owner ON jobs(owner_id);
CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state);
CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs(queue);
CREATE INDEX IF NOT EXISTS idx_jobs_submit ON jobs(submit_time_us);
CREATE INDEX IF NOT EXISTS idx_jobs_f58 ON jobs(jobid_f58);

CREATE TABLE IF NOT EXISTS job_events (
    event_seq              BIGSERIAL PRIMARY KEY,
    message_id             BIGINT NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
    job_id                 BIGINT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    event_id               BIGINT NOT NULL,
    event_ts_ns            BIGINT NOT NULL,
    flux_state             TEXT NOT NULL,
    flux_prev_state        TEXT NOT NULL,
    owner_id               BIGINT NOT NULL,
    owner_name             TEXT NOT NULL,
    job_name               TEXT NOT NULL,
    job_display_name       TEXT NOT NULL,
    budget                 TEXT NOT NULL,
    queue                  TEXT NOT NULL,
    submit_time_us         BIGINT NOT NULL,
    requested_walltime_us  BIGINT NOT NULL,
    nslots                 BIGINT NOT NULL,
    nnodes                 BIGINT NOT NULL,
    ncpus                  BIGINT NOT NULL,
    ngpus                  BIGINT NOT NULL,
    priority               BIGINT,
    node_list              TEXT[],
    r_lite                 JSONB,
    start_time_us          BIGINT,
    finish_time_us         BIGINT,
    rc_state               TEXT,
    error_type             TEXT,
    error_severity         BIGINT,
    error_note             TEXT,
    error_user_id          BIGINT,
    error_wait_status      BIGINT,
    normalized_state       TEXT NOT NULL,
    reason                 TEXT NOT NULL DEFAULT '',
    UNIQUE(job_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_job_events_job_time
    ON job_events(job_id, event_ts_ns, event_seq);
CREATE INDEX IF NOT EXISTS idx_job_events_flux_state_time
    ON job_events(flux_state, event_ts_ns);
CREATE INDEX IF NOT EXISTS idx_job_events_rc_state
    ON job_events(rc_state) WHERE rc_state IS NOT NULL;

CREATE TABLE IF NOT EXISTS subscriptions (
    address_upper NUMERIC(20,0) NOT NULL,
    address_lower NUMERIC(20,0) NOT NULL,
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(address_upper, address_lower)
);

COMMENT ON TABLE messages IS
'V2 canonical SeanerBUS ingestion. The payload is typed FluxLifecycleEventV2; raw Capn Proto bytes are preserved.';
COMMENT ON TABLE jobs IS
'Latest materialized FluxLifecycleEventV2 projection for each Flux job.';
COMMENT ON TABLE job_events IS
'Immutable projection of each accepted typed FluxLifecycleEventV2 publication.';

COMMIT;
