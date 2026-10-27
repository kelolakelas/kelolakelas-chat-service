ALTER TABLE messages ADD COLUMN source_event_id uuid UNIQUE;
ALTER TABLE messages DROP CONSTRAINT messages_body_check;
-- Public Send still limits user text to 2000 code points. Typed lifecycle
-- events may contain 50 slots, reason and an authoritative checkout URL.
ALTER TABLE messages ADD CONSTRAINT messages_body_check CHECK(char_length(body) BETWEEN 1 AND 8192);
ALTER TABLE messages ADD CONSTRAINT messages_lifecycle_key CHECK(source_event_id IS NULL OR (sender_kind IN ('parent','member') AND client_message_id='private-event:'||source_event_id::text));
