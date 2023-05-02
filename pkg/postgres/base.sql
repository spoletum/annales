DROP PROCEDURE IF EXISTS append_event;

DROP INDEX IF EXISTS stream_events_stream_version_idx;

DROP TABLE IF EXISTS events;

DROP TABLE IF EXISTS streams;

CREATE TABLE streams (
    stream_id CHAR(36) PRIMARY KEY,
    stream_version INTEGER NOT NULL
);

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

CREATE PROCEDURE append_event(
    p_stream_id VARCHAR,
    p_expected_version BIGINT,
    p_event_type VARCHAR,
    p_encoding VARCHAR,
    p_source VARCHAR,
    p_data BYTEA
) LANGUAGE plpgsql AS $$
DECLARE
    affected_rows INTEGER;
BEGIN
    IF p_expected_version = 0 THEN
        BEGIN
            INSERT INTO streams (stream_id, stream_version)
            VALUES (p_stream_id, 1);
        EXCEPTION WHEN unique_violation THEN
            RAISE EXCEPTION 'Invalid stream version';
        END;
    ELSE
        BEGIN
            UPDATE streams
            SET stream_version = stream_version + 1
            WHERE stream_id = p_stream_id AND stream_version = p_expected_version;
            GET DIAGNOSTICS affected_rows = ROW_COUNT;
            IF affected_rows <> 1 THEN
                RAISE EXCEPTION 'Invalid stream version';
            END IF;
        END;
    END IF;

    INSERT INTO events (stream_id, stream_version, event_type, event_encoding, event_source, event_data, event_ts)
    VALUES (p_stream_id, p_expected_version + 1, p_event_type, p_encoding, p_source, p_data, NOW());
END;
$$;
