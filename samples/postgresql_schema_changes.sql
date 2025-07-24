-- Migration: Add binary format columns
ALTER TABLE event_data 
ADD COLUMN payload_binary BYTEA,
ADD COLUMN format_type VARCHAR(20) DEFAULT 'json';

-- Index for format type filtering
CREATE INDEX idx_event_data_format ON event_data(format_type);

-- Application-layer handling
CREATE OR REPLACE FUNCTION get_event_payload(event_id INTEGER, preferred_format VARCHAR DEFAULT 'json')
RETURNS BYTEA AS $$
BEGIN
    IF preferred_format = 'binary' THEN
        RETURN (SELECT payload_binary FROM event_data WHERE id = event_id);
    ELSE
        RETURN (SELECT payload::BYTEA FROM event_data WHERE id = event_id);
    END IF;
END;
$$ LANGUAGE plpgsql;
