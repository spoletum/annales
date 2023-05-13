DROP INDEX IF EXISTS stream_events_stream_version_idx;

DROP TABLE IF EXISTS events;

CREATE TABLE events (
    event_id BIGSERIAL PRIMARY KEY,
    stream_id CHAR(36) NOT NULL REFERENCES streams(stream_id),
    stream_version INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    event_encoding TEXT NOT NULL,
    event_source TEXT NOT NULL,
    event_data BYTEA NOT NULL,
    event_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE events ADD CONSTRAINT unique_stream_id_version UNIQUE (stream_id, stream_version);

CREATE INDEX stream_events_stream_version_idx ON events(stream_id, stream_version);