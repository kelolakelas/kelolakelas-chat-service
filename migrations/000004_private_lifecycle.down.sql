ALTER TABLE messages DROP CONSTRAINT messages_lifecycle_key;
ALTER TABLE messages DROP COLUMN source_event_id;
ALTER TABLE messages DROP CONSTRAINT messages_body_check;
-- Refuses rollback while larger lifecycle messages exist; never delete them.
ALTER TABLE messages ADD CONSTRAINT messages_body_check CHECK(char_length(body) BETWEEN 1 AND 2000);
